#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PullbackVolatilityCompressionStrategy - 回调入场策略 V8
使用固定参数（追求高质量信号）
"""

import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

@dataclass
class Contract:
    ProductID: str
    MainContractID: str
    VolumeMultiple: int
    PriceTick: float

@dataclass
class Signal:
    symbol: str
    entry_time: str
    entry_price: float
    exit_time: str
    exit_price: float
    exit_reason: str
    pnl_pct: float
    pnl_amount: float
    pnl_per_hand: float
    entry_idx: int
    pullback_60min: float
    max_price: float
    resistance: float

class PullbackVolatilityCompressionStrategy:
    """
    回调入场策略 V8 - 固定参数（高质量信号）
    """
    
    def __init__(self, config: Dict = None):
        # V8 固定参数 - 追求质量
        self.config = {
            'min_pullback_pct': 0.015,
            'max_pullback_pct': 0.08,
            'bb_width_threshold': 0.012,
            'big_negative_ratio': 2.0,
            'volume_ratio': 1.3,
            'trailing_stop_pct': 0.02,
            'max_hold_bars': 500,
            'cooldown_bars': 30,  # 冷却 30 根 K 线
        }
        
        if config:
            self.config.update(config)
        
        self.contracts = {}
    
    def load_contracts(self, contracts_path: str):
        import json
        with open(contracts_path, 'r', encoding='utf-8') as f:
            contracts_list = json.load(f)
        self.contracts = {c['MainContractID']: Contract(**{
            k: v for k, v in c.items() 
            if k in ['ProductID', 'MainContractID', 'VolumeMultiple', 'PriceTick']
        }) for c in contracts_list}
    
    def get_kline_data(self, cursor, symbol: str, duration: int, limit: int = 500) -> List[Dict]:
        cursor.execute("""
            SELECT datetime, open, high, low, close, volume FROM kline_data 
            WHERE symbol = ? AND duration = ?
            ORDER BY datetime DESC LIMIT ?
        """, [symbol, duration, limit])
        rows = cursor.fetchall()
        return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 
                 'close': row[4], 'volume': row[5]} for row in reversed(rows)]
    

    def _calc_atr(self, data: List[Dict], period: int = 14) -> List[Optional[float]]:
        """计算 ATR"""
        atr = []
        tr = []
        for i in range(len(data)):
            if i == 0:
                tr.append(data[i]['high'] - data[i]['low'])
            else:
                hl = data[i]['high'] - data[i]['low']
                hpc = abs(data[i]['high'] - data[i-1]['close'])
                lpc = abs(data[i]['low'] - data[i-1]['close'])
                tr.append(max(hl, hpc, lpc))
        for i in range(len(data)):
            if i < period - 1:
                atr.append(None)
            elif i == period - 1:
                atr.append(sum(tr[:period]) / period)
            else:
                atr.append((atr[-1] * (period - 1) + tr[i]) / period)
        return atr
    
    def _calc_atr_percentile(self, data: List[Dict], atr: List[Optional[float]], period: int = 50) -> Optional[float]:
        """计算当前 ATR 的历史百分位 (减少数据需求)"""
        valid_atr = [a for a in atr[-period:] if a is not None]
        if len(valid_atr) < 5 or atr[-1] is None:
            return None
        current = atr[-1]
        rank = sum(1 for a in valid_atr if a <= current)
        return rank / len(valid_atr)
    
    def _check_compression(self, data: List[Dict]) -> Tuple:
        """检查波动率压缩（V8 优化版 - 过滤涨停/跌停导致的假压缩）"""
        amplitudes = []
        for bar in data[-30:]:
            if bar['open'] > 0:
                amp = (bar['high'] - bar['low']) / bar['open']
                amplitudes.append(amp)
        
        zero_amp_count = sum(1 for amp in amplitudes if amp < 0.001)
        if len(amplitudes) > 0 and zero_amp_count > len(amplitudes) * 0.5:
            return None, None, None, False
        
        if len(amplitudes) >= 20:
            pre_compression_amp = amplitudes[:10]
            avg_pre_amp = np.mean(pre_compression_amp)
            if avg_pre_amp < 0.002:  # 放宽阈值
                return None, None, None, False
        
        if len(data) > 1 and data[-1]['open'] > 0 and data[-2]['close'] > 0:
            gap = abs(data[-1]['open'] - data[-2]['close']) / data[-2]['close']
            if gap > 0.03:
                return None, None, None, False
        
        closes = [d['close'] for d in data[-20:]]
        bb_mid = np.mean(closes)
        bb_std = np.std(closes)
        bb_width = 4 * bb_std / bb_mid if bb_mid and bb_mid > 0 else None
        bb_up = bb_mid + 2 * bb_std if bb_mid and bb_std else None
        
        if bb_std and bb_mid:
            relative_std = bb_std / bb_mid
            if relative_std < 0.001:
                return None, None, None, False
        
        return bb_mid, bb_up, bb_width, True
    
    def _check_compression_v2(self, data: List[Dict]) -> Tuple:
        """V2 压缩检查 - 只用 ATR 百分位，不用 BB 带宽"""
        if len(data) < 50:
            return None, None, None, False
        
        # 1. ATR 百分位 < 20% (唯一压缩条件)
        atr = self._calc_atr(data, period=14)
        atr_pct = self._calc_atr_percentile(data, atr, period=50)
        if atr_pct is None or atr_pct >= 0.2:
            return None, None, None, False
        
        # 2. 平均振幅 < 0.5%
        recent_ranges = [(data[i]['high'] - data[i]['low']) / data[i]['close'] for i in range(-10, 0)]
        avg_range = np.mean(recent_ranges)
        if avg_range >= 0.005:
            return None, None, None, False
        
        # 3. 计算 BB 用于突破判断（不作为过滤条件）
        closes = [d['close'] for d in data[-20:]]
        bb_mid = np.mean(closes)
        bb_std = np.std(closes)
        bb_width = 4 * bb_std / bb_mid if bb_mid and bb_mid > 0 else None
        bb_up = bb_mid + 2 * bb_std if bb_mid and bb_std else None
        
        return bb_mid, bb_up, bb_width, True
    
    def _check_breakout(self, curr: Dict, current_5: List[Dict], bb_up: float) -> bool:
        # 必须是阳线（做多）
        if curr['close'] <= curr['open']:
            return False
        
        body = abs(curr['close'] - curr['open']) / curr['open'] if curr['open'] > 0 else 0
        avg_body = np.mean([abs(d['close']-d['open'])/d['open'] for d in current_5[-10:-1]])
        
        if body < avg_body * 2:
            return False
        
        vol_ma = np.mean([d['volume'] for d in current_5[-11:-1]])
        vol_ratio = curr['volume'] / vol_ma if vol_ma > 0 else 0
        if vol_ratio < self.config['volume_ratio']:
            return False
        
        # 放宽突破条件：最高价触及上轨 + 收盘在中轨上方
        bb_mid = np.mean([d['close'] for d in current_5[-20:]])
        if bb_up is None or curr['high'] < float(bb_up) or curr['close'] <= bb_mid:
            return False
        
        return True
    

    def _get_contract(self, symbol: str) -> Optional[Contract]:
        parts = symbol.split('.')
        if len(parts) != 2:
            return None
        return self.contracts.get(parts[1])
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[Signal]:
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        min_pullback = self.config['min_pullback_pct']
        max_pullback = self.config['max_pullback_pct']
        
        signals = []
        last_signal_idx = 0
        
        closes_60 = [d['close'] for d in data_60min]
        ma20_60 = np.mean(closes_60[-20:])
        ma50_60 = np.mean(closes_60[-50:]) if len(closes_60) >= 50 else ma20_60
        
        if ma20_60 <= ma50_60:
            return None
        
        for i in range(201, len(data_5min) - 30):
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-200:i]
            current_time = data_5min[i-1]['time']
            
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 50:
                continue
            
            closes_60 = [d['close'] for d in data_60_completed]
            ma20_60 = np.mean(closes_60[-20:])
            ma50_60 = np.mean(closes_60[-50:]) if len(closes_60) >= 50 else ma20_60
            
            if ma20_60 <= ma50_60:
                continue
            
            high_60 = max(d['high'] for d in data_60_completed[-20:])
            current_price = current_5[-1]['close']
            pullback = (high_60 - current_price) / high_60 * 100
            
            if not (min_pullback * 100 <= pullback <= max_pullback * 100):
                continue
            
            bb_mid, bb_up, bb_width, is_valid = self._check_compression(current_5)
            if not is_valid:
                continue
            if bb_width is None or bb_width >= self.config['bb_width_threshold']:
                continue
            
            curr = data_5min[i-1]
            if not self._check_breakout(curr, current_5, bb_up):
                continue
            
            contract = self._get_contract(symbol)
            if not contract:
                continue
            
            signal = self._simulate_trade(symbol, data_5min, i, contract, pullback)
            if signal:
                signals.append(signal)
                last_signal_idx = i
                i += 50
        
        return signals if signals else None
    
    def _simulate_trade(self, symbol: str, data_5min: List[Dict], entry_idx: int, 
                       contract: Contract, pullback_from_high: float) -> Optional[Signal]:
        curr = data_5min[entry_idx - 1]
        entry_price = curr['close']
        entry_time = curr['time']
        resistance = max(d['high'] for d in data_5min[entry_idx-31:entry_idx-1])
        
        position = 2
        exit_price = None
        exit_time = None
        exit_reason = ""
        max_price = entry_price
        
        for j in range(entry_idx, min(entry_idx + self.config['max_hold_bars'], len(data_5min) - 1)):
            curr_bar = data_5min[j]
            max_price = max(max_price, curr_bar['high'])
            
            trailing_stop = max_price * (1 - self.config['trailing_stop_pct'])
            
            if curr_bar['low'] <= trailing_stop:
                exit_price = trailing_stop
                exit_time = curr_bar['time']
                exit_reason = "移动止损"
                break
            
            if j >= 10:
                recent_closes = [data_5min[k]['close'] for k in range(j-10, j)]
                curr_body = abs(curr_bar['close'] - curr_bar['open']) / curr_bar['open']
                avg_body_recent = np.mean([abs(data_5min[k]['close']-data_5min[k]['open'])/data_5min[k]['open'] for k in range(j-10, j)])
                vol_ma_recent = np.mean([data_5min[k]['volume'] for k in range(j-10, j)])
                vol_ratio = curr_bar['volume'] / vol_ma_recent if vol_ma_recent > 0 else 0
                
                is_big_negative = (curr_bar['close'] < curr_bar['open']) and (curr_body >= avg_body_recent * 2)
                is_volume_surge = vol_ratio >= self.config['volume_ratio']
                is_break_low = curr_bar['close'] < min(recent_closes)
                
                if is_big_negative and is_volume_surge and is_break_low:
                    exit_price = curr_bar['close']
                    exit_time = curr_bar['time']
                    exit_reason = "三重确认"
                    break
        else:
            last = data_5min[min(entry_idx + self.config['max_hold_bars'], len(data_5min) - 1)]
            exit_price = last['close']
            exit_time = last['time']
            exit_reason = "到期"
        
        total_pnl = (exit_price - entry_price) * contract.VolumeMultiple * position
        pnl_pct = total_pnl / (entry_price * contract.VolumeMultiple * position)
        pnl_per_hand = total_pnl / position
        
        return Signal(
            symbol=symbol,
            entry_time=entry_time,
            entry_price=entry_price,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_reason=exit_reason,
            pnl_pct=pnl_pct,
            pnl_amount=total_pnl,
            pnl_per_hand=pnl_per_hand,
            entry_idx=entry_idx - 1,
            pullback_60min=pullback_from_high,
            max_price=max_price,
            resistance=resistance
        )
