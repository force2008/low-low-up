#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalStrategy - 趋势反转策略
60 分钟回调 + 突破下降趋势线 + 5 分钟 MACD 背离进场
60 分钟三重确认出场 + 5 分钟低点止损
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
    stop_loss: float

class TrendReversalStrategy:
    """
    趋势反转策略 V1
    """
    
    def __init__(self, config: Dict = None):
        self.config = {
            'ma60_period': 20,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'stop_loss_ticks': 2,
            'trend_line_lookback': 10,
            'cooldown_bars': 50,  # 冷却 50 根 5 分钟 K 线
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
    
    def _calc_macd(self, data: List[Dict], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple:
        """计算 MACD (优化版)"""
        closes = [d['close'] for d in data]
        n = len(closes)
        
        # EMA 计算 (向量化)
        ema_fast = [None] * n
        ema_slow = [None] * n
        
        # 初始化
        if n >= fast:
            ema_fast[fast-1] = sum(closes[:fast]) / fast
            multiplier = 2 / (fast + 1)
            for i in range(fast, n):
                ema_fast[i] = (closes[i] - ema_fast[i-1]) * multiplier + ema_fast[i-1]
        
        if n >= slow:
            ema_slow[slow-1] = sum(closes[:slow]) / slow
            multiplier = 2 / (slow + 1)
            for i in range(slow, n):
                ema_slow[i] = (closes[i] - ema_slow[i-1]) * multiplier + ema_slow[i-1]
        
        # MACD 线
        macd_line = [None] * n
        for i in range(n):
            if ema_fast[i] and ema_slow[i]:
                macd_line[i] = ema_fast[i] - ema_slow[i]
        
        # Signal 线
        signal_line = [None] * n
        macd_valid = [m for m in macd_line if m is not None]
        if len(macd_valid) >= signal:
            signal_line_start = n - len(macd_valid)
            signal_val = sum(macd_valid[:signal]) / signal
            multiplier = 2 / (signal + 1)
            for i in range(signal, len(macd_valid)):
                signal_val = (macd_valid[i] - signal_val) * multiplier + signal_val
                idx = signal_line_start + i
                if idx < n:
                    signal_line[idx] = signal_val
        
        # MACD 柱
        histogram = [None] * n
        for i in range(n):
            if macd_line[i] and signal_line[i]:
                histogram[i] = macd_line[i] - signal_line[i]
        
        return macd_line, signal_line, histogram
    
    def _check_downtrend(self, data_60: List[Dict], lookback: int = 20) -> bool:
        """检查 60 分钟是否处于下跌趋势 (优化版)"""
        if len(data_60) < lookback:
            return False
        
        recent = data_60[-lookback:]
        mid = lookback // 2
        
        # 直接比较，避免重复创建列表
        first_high = max(d['high'] for d in recent[:mid])
        second_high = max(d['high'] for d in recent[mid:])
        first_low = max(d['low'] for d in recent[:mid])
        second_low = max(d['low'] for d in recent[mid:])
        
        return first_high > second_high and first_low > second_low
    
    def _calc_trend_line(self, data_60: List[Dict], lookback: int = 10) -> Optional[float]:
        """计算下降趋势线 (优化版)"""
        if len(data_60) < lookback:
            return None
        
        recent = data_60[-lookback:]
        
        # 找最高和次高点
        highs = sorted([(i, d['high']) for i, d in enumerate(recent)], key=lambda x: x[1], reverse=True)
        if len(highs) < 2:
            return None
        
        h1_idx, h1_val = highs[0]
        h2_idx, h2_val = highs[1]
        
        # 确保时间顺序
        if h1_idx < h2_idx:
            h1_idx, h1_val, h2_idx, h2_val = h2_idx, h2_val, h1_idx, h1_val
        
        if h1_idx == h2_idx:
            return None
        
        # 计算趋势线
        slope = (h2_val - h1_val) / (h1_idx - h2_idx)
        trend_line_val = h1_val + slope * (len(data_60) - 1 - (len(data_60) - lookback + h1_idx))
        
        return trend_line_val
    
    def _check_breakout(self, data_60: List[Dict], trend_line: float) -> bool:
        """检查是否突破下降趋势线"""
        if trend_line is None:
            return False
        
        # 最新 K 线收盘价突破趋势线
        return data_60[-1]['close'] > trend_line
    
    def _check_macd_divergence(self, data_5: List[Dict]) -> bool:
        """
        检查 5 分钟 MACD 底背离（简化版）
        """
        if len(data_5) < 80:
            return False
        
        macd_line, signal_line, histogram = self._calc_macd(
            data_5, 
            self.config['macd_fast'],
            self.config['macd_slow'],
            self.config['macd_signal']
        )
        
        # 找最近两个显著低点
        lows = []
        for i in range(20, len(data_5[-60:])):
            local_min = min(d['low'] for d in data_5[-60:][i-5:i+6])
            if data_5[-60:][i]['low'] == local_min:
                lows.append((i, data_5[-60:][i]['low']))
        
        if len(lows) < 2:
            return False
        
        low1_idx, low1_val = lows[-1]
        low2_idx, low2_val = lows[-2]
        
        # 价格创新低（放宽到 0.3%）
        price_lower = low1_val < low2_val * 0.997
        
        # 检查最近 MACD 是否上升
        recent_macd = [m for m in macd_line[-20:] if m is not None]
        if len(recent_macd) < 5:
            return False
        
        macd_rising = recent_macd[-1] > recent_macd[-5] if recent_macd[-5] else False
        
        return price_lower or macd_rising
    
    def _get_contract(self, symbol: str) -> Optional[Contract]:
        parts = symbol.split('.')
        if len(parts) != 2:
            return None
        return self.contracts.get(parts[1])
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        """运行回测"""
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        contract = self._get_contract(symbol)
        if not contract:
            return None
        
        signals = []
        last_signal_idx = 0
        
        for i in range(100, len(data_5min) - 10):
            # 冷却时间检查
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            current_5 = data_5min[i-100:i]
            current_time = data_5min[i-1]['time']
            
            # 获取对应的 60 分钟数据
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 30:
                continue
            
            # 1. 检查 60 分钟下跌趋势
            if not self._check_downtrend(data_60_completed):
                continue
            
            # 2. 计算下降趋势线
            trend_line = self._calc_trend_line(data_60_completed)
            if trend_line is None:
                continue
            
            # 3. 检查突破
            if not self._check_breakout(data_60_completed, trend_line):
                continue
            
            # 4. 检查 5 分钟 MACD 底背离
            if not self._check_macd_divergence(current_5):
                continue
            
            # 生成信号
            entry_price = data_5min[i-1]['close']
            entry_time = data_5min[i-1]['time']
            
            # 计算止损（5 分钟低点 - 2 ticks）
            recent_low = min(d['low'] for d in current_5[-30:])
            stop_loss = recent_low - contract.PriceTick * self.config['stop_loss_ticks']
            
            signal = self._simulate_trade(symbol, data_5min, data_60min, i, contract, stop_loss)
            if signal:
                signals.append(signal)
                last_signal_idx = i
        
        return signals if signals else None
    
    def _simulate_trade(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict], 
                       entry_idx: int, contract: Contract, stop_loss: float) -> Optional[Signal]:
        """模拟交易"""
        entry_price = data_5min[entry_idx - 1]['close']
        entry_time = data_5min[entry_idx - 1]['time']
        
        exit_price = None
        exit_time = None
        exit_reason = ""
        max_price = entry_price
        
        for j in range(entry_idx, min(entry_idx + 500, len(data_5min) - 1)):
            curr_bar = data_5min[j]
            max_price = max(max_price, curr_bar['high'])
            
            # 1. 止损检查（5 分钟低点 - 2 ticks）
            if curr_bar['low'] <= stop_loss:
                exit_price = stop_loss
                exit_time = curr_bar['time']
                exit_reason = "止损"
                break
            
            # 2. 60 分钟三重确认出场
            current_time = curr_bar['time']
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            
            if len(data_60_completed) >= 20:
                last_60 = data_60_completed[-1]
                
                # 检查三重确认
                recent_closes = [d['close'] for d in data_60_completed[-10:]]
                curr_body = abs(last_60['close'] - last_60['open']) / last_60['open']
                avg_body = np.mean([abs(d['close']-d['open'])/d['open'] for d in data_60_completed[-10:-1]])
                
                vol_ma = np.mean([d['volume'] for d in data_60_completed[-10:-1]])
                vol_ratio = last_60['volume'] / vol_ma if vol_ma > 0 else 0
                
                is_big_negative = (last_60['close'] < last_60['open']) and (curr_body >= avg_body * 2)
                is_volume_surge = vol_ratio >= 1.3
                is_break_low = last_60['close'] < min(recent_closes)
                
                if is_big_negative and is_volume_surge and is_break_low:
                    exit_price = last_60['close']
                    exit_time = last_60['time']
                    exit_reason = "三重确认"
                    break
        
        # 到期出场
        if exit_price is None:
            last = data_5min[min(entry_idx + 500, len(data_5min) - 1)]
            exit_price = last['close']
            exit_time = last['time']
            exit_reason = "到期"
        
        # 计算盈亏
        position = 1
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
            stop_loss=stop_loss
        )
