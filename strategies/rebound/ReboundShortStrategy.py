#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
反弹做空策略 V1 - 60 分钟下跌趋势 + 5 分钟反弹后做空
与 PullbackVolatilityCompressionStrategy 相反
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class Contract:
    MainContractID: str
    VolumeMultiple: int = 10

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

class ReboundShortStrategy:
    """
    反弹做空策略 V1 - 60 分钟下跌趋势 + 5 分钟反弹后做空
    """
    
    def __init__(self, config: Dict = None):
        self.config = {
            'min_pullback_pct': 0.015,  # 最小反弹 1.5%
            'max_pullback_pct': 0.05,   # 最大反弹 5%
            'bb_width_threshold': 0.012,  # 布林带带宽阈值 1.2%
            'volume_ratio': 1.5,        # 成交量放大倍数
            'trailing_stop_pct': 0.02,  # 移动止损 2%
            'max_hold_bars': 200,       # 最大持仓 K 线数
        }
        if config:
            self.config.update(config)
        
        self.contracts = {}
    
    def load_contracts(self, contracts_path: str):
        """加载合约信息"""
        import json
        with open(contracts_path, 'r', encoding='utf-8') as f:
            contracts_data = json.load(f)
        self.contracts = {c['MainContractID']: Contract(**{
            'MainContractID': c.get('MainContractID', ''),
            'VolumeMultiple': c.get('VolumeMultiple', 10)
        }) for c in contracts_data}
    
    def get_kline_data(self, cursor, symbol: str, duration: int, limit: int = 500) -> List[Dict]:
        """获取 K 线数据"""
        cursor.execute("""
            SELECT datetime, open, high, low, close, volume 
            FROM kline_data 
            WHERE symbol = ? AND duration = ?
            ORDER BY datetime DESC 
            LIMIT ?
        """, (symbol, duration, limit))
        rows = cursor.fetchall()
        return [{
            'time': row[0],
            'open': row[1],
            'high': row[2],
            'low': row[3],
            'close': row[4],
            'volume': row[5]
        } for row in reversed(rows)]
    
    def _check_compression(self, data: List[Dict]) -> Tuple:
        """
        检查波动率压缩（优化版 - 过滤涨停/跌停导致的假压缩）
        """
        # 检查振幅
        amplitudes = []
        for bar in data[-30:]:
            if bar['open'] > 0:
                amp = (bar['high'] - bar['low']) / bar['open']
                amplitudes.append(amp)
        
        # 过滤假压缩
        zero_amp_count = sum(1 for amp in amplitudes if amp < 0.001)
        if len(amplitudes) > 0 and zero_amp_count > len(amplitudes) * 0.5:
            return None, None, None, False
        
        # 检查压缩前波动
        if len(amplitudes) >= 20:
            pre_compression_amp = amplitudes[:10]
            avg_pre_amp = np.mean(pre_compression_amp)
            if avg_pre_amp < 0.003:
                return None, None, None, False
        
        # 检查跳空
        if len(data) > 1 and data[-1]['open'] > 0 and data[-2]['close'] > 0:
            gap = abs(data[-1]['open'] - data[-2]['close']) / data[-2]['close']
            if gap > 0.03:
                return None, None, None, False
        
        # 计算布林带
        closes = [d['close'] for d in data[-20:]]
        bb_mid = np.mean(closes)
        bb_std = np.std(closes)
        bb_width = 4 * bb_std / bb_mid if bb_mid and bb_mid > 0 else None
        bb_low = bb_mid - 2 * bb_std if bb_mid and bb_std else None
        
        # 检查相对标准差
        if bb_std and bb_mid:
            relative_std = bb_std / bb_mid
            if relative_std < 0.001:
                return None, None, None, False
        
        return bb_mid, bb_low, bb_width, True
    
    def _check_breakdown(self, curr: Dict, current_5: List[Dict], bb_low: float) -> bool:
        """检查向下跌破（做空信号）"""
        body = abs(curr['close'] - curr['open']) / curr['open'] if curr['open'] > 0 else 0
        avg_body = np.mean([abs(d['close']-d['open'])/d['open'] for d in current_5[-10:-1]])
        
        # 大阴线
        if body < avg_body * 2:
            return False
        
        # 成交量放大
        vol_ma = np.mean([d['volume'] for d in current_5[-11:-1]])
        vol_ratio = curr['volume'] / vol_ma if vol_ma > 0 else 0
        if vol_ratio < self.config['volume_ratio']:
            return False
        
        # 跌破下轨
        if bb_low is None or curr['close'] >= float(bb_low):
            return False
        
        return True
    
    def _get_contract(self, symbol: str) -> Optional[Contract]:
        parts = symbol.split('.')
        if len(parts) != 2:
            return None
        return self.contracts.get(parts[1])
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        """运行回测"""
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        min_rebound = self.config['min_pullback_pct']
        max_rebound = self.config['max_pullback_pct']
        
        signals = []
        
        # 60 分钟下跌趋势筛选（与做多相反）
        closes_60 = [d['close'] for d in data_60min]
        ma20_60 = np.mean(closes_60[-20:])
        ma50_60 = np.mean(closes_60[-50:]) if len(closes_60) >= 50 else ma20_60
        
        # 下跌趋势：MA20 < MA50
        if ma20_60 >= ma50_60:
            return None
        
        # 5 分钟周期找入场点
        for i in range(201, len(data_5min) - 30):
            current_5 = data_5min[i-200:i]
            current_time = data_5min[i-1]['time']
            
            # 检查 60 分钟 K 线是否完成
            if len(data_60min) > 0:
                last_60_time = data_60min[-1]['time']
                data_60_completed = [d for d in data_60min if d['time'] <= last_60_time]
            else:
                continue
            
            if len(data_60_completed) < 50:
                continue
            
            # 重新计算 60 分钟趋势和反弹
            closes_60 = [d['close'] for d in data_60_completed]
            ma20_60 = np.mean(closes_60[-20:])
            ma50_60 = np.mean(closes_60[-50:]) if len(closes_60) >= 50 else ma20_60
            
            # 下跌趋势
            if ma20_60 >= ma50_60:
                continue
            
            # 使用 60 分钟最低价和 5 分钟当前收盘价计算反弹
            low_60 = min(d['low'] for d in data_60_completed[-20:])
            current_price = current_5[-1]['close']
            rebound = (current_price - low_60) / low_60 * 100
            
            if not (min_rebound * 100 <= rebound <= max_rebound * 100):
                continue
            
            # 5 分钟波动率压缩
            bb_mid, bb_low, bb_width, is_valid = self._check_compression(current_5)
            if not is_valid:
                continue
            if bb_width is None or bb_width >= self.config['bb_width_threshold']:
                continue
            
            # 大阴线跌破
            curr = data_5min[i-1]
            if not self._check_breakdown(curr, current_5, bb_low):
                continue
            
            # 计算合约信息
            contract = self._get_contract(symbol)
            if not contract:
                continue
            
            # 计算入场和出场
            signal = self._simulate_trade(symbol, data_5min, i, contract, rebound)
            if signal:
                signals.append(signal)
        
        return signals if signals else None
    
    def _simulate_trade(self, symbol: str, data_5min: List[Dict], entry_idx: int, 
                       contract: Contract, rebound_from_low: float) -> Optional[Signal]:
        """模拟交易（做空）"""
        curr = data_5min[entry_idx - 1]
        entry_price = curr['close']
        entry_time = curr['time']
        resistance = min(d['low'] for d in data_5min[entry_idx-31:entry_idx-1])
        
        position = 2  # 2 手
        exit_price = None
        exit_time = None
        exit_reason = ""
        min_price = entry_price
        
        # 做空：价格上涨时止损，价格下跌时盈利
        for j in range(entry_idx, min(entry_idx + self.config['max_hold_bars'], len(data_5min) - 1)):
            curr_bar = data_5min[j]
            min_price = min(min_price, curr_bar['low'])
            
            # 移动止损（做空是向上止损）
            trailing_stop = min_price * (1 + self.config['trailing_stop_pct'])
            
            if curr_bar['high'] >= trailing_stop:
                exit_price = trailing_stop
                exit_time = curr_bar['time']
                exit_reason = "移动止损"
                break
            
            # 三重确认（反向：大阳线 + 放量 + 突破）
            if j >= 10:
                recent_closes = [data_5min[k]['close'] for k in range(j-10, j)]
                curr_body = abs(curr_bar['close'] - curr_bar['open']) / curr_bar['open']
                avg_body_recent = np.mean([abs(data_5min[k]['close']-data_5min[k]['open'])/data_5min[k]['open'] for k in range(j-10, j)])
                vol_ma_recent = np.mean([data_5min[k]['volume'] for k in range(j-10, j)])
                vol_ratio = curr_bar['volume'] / vol_ma_recent if vol_ma_recent > 0 else 0
                
                # 大阳线 + 放量 + 突破高点
                is_big_positive = (curr_bar['close'] > curr_bar['open']) and (curr_body >= avg_body_recent * 2)
                is_volume_surge = vol_ratio >= self.config['volume_ratio']
                is_break_high = curr_bar['close'] > max(recent_closes)
                
                if is_big_positive and is_volume_surge and is_break_high:
                    exit_price = curr_bar['close']
                    exit_time = curr_bar['time']
                    exit_reason = "三重确认"
                    break
        else:
            last = data_5min[min(entry_idx + self.config['max_hold_bars'], len(data_5min) - 1)]
            exit_price = last['close']
            exit_time = last['time']
            exit_reason = "到期"
        
        # 做空盈亏计算（价格下跌盈利）
        total_pnl = (entry_price - exit_price) * contract.VolumeMultiple * position
        pnl_pct = total_pnl / (entry_price * contract.VolumeMultiple * position)
        
        return Signal(
            symbol=symbol,
            entry_time=entry_time,
            entry_price=entry_price,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_reason=exit_reason,
            pnl_pct=pnl_pct,
            pnl_amount=total_pnl,
            pnl_per_hand=total_pnl / position,
            entry_idx=entry_idx,
            pullback_60min=rebound_from_low,
            max_price=min_price,
            resistance=resistance
        )
