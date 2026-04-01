#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV2Strategy - 趋势反转策略 V2 (优化版)
60 分钟超跌 + 5 分钟 MACD 背离 + 趋势过滤 + 止损止盈
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
    take_profit: float

class TrendReversalV2Strategy:
    """趋势反转策略 V2"""
    
    def __init__(self, config: Dict = None):
        self.config = {
            'rsi_period': 14,
            'rsi_oversold': 50,
            'vol_multiplier': 1.3,
            'breakout_pct': 0.005,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'stop_loss_pct': 0.03,  # 止损 3%
            'take_profit_pct': 0.05,  # 止盈 5%
            'cooldown_bars': 30,
            'max_holding_bars': 100,  # 最大持有 100 个 5 分钟 bar
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
    
    def _calc_rsi(self, data: List[Dict], period: int = 14) -> List[Optional[float]]:
        closes = [d['close'] for d in data]
        rsi = []
        
        for i in range(len(closes)):
            if i < period:
                rsi.append(None)
            else:
                gains = []
                losses = []
                for j in range(i-period+1, i+1):
                    change = closes[j] - closes[j-1]
                    gains.append(max(change, 0))
                    losses.append(max(-change, 0))
                
                avg_gain = np.mean(gains)
                avg_loss = np.mean(losses)
                
                if avg_loss == 0:
                    rsi.append(100)
                else:
                    rs = avg_gain / avg_loss
                    rsi.append(100 - (100 / (1 + rs)))
        
        return rsi
    
    def _calc_macd(self, data: List[Dict]) -> Tuple:
        closes = [d['close'] for d in data]
        n = len(closes)
        
        ema12 = [None] * n
        ema26 = [None] * n
        
        for i in range(n):
            if i < 11:
                ema12[i] = np.mean(closes[max(0,i-11):i+1])
            else:
                ema12[i] = (closes[i] - ema12[i-1]) * 2/13 + ema12[i-1]
            
            if i < 25:
                ema26[i] = np.mean(closes[max(0,i-25):i+1])
            else:
                ema26[i] = (closes[i] - ema26[i-1]) * 2/27 + ema26[i-1]
        
        macd_line = [ema12[i] - ema26[i] if i >= 25 else None for i in range(n)]
        
        macd_valid = [m for m in macd_line if m is not None]
        signal_line = [None] * n
        if len(macd_valid) >= 9:
            signal_val = np.mean(macd_valid[-9:])
            for i in range(n-25-9, n):
                if i >= 0:
                    signal_line[i] = signal_val
        
        return macd_line, signal_line
    
    def _calc_ma(self, data: List[Dict], period: int) -> List[Optional[float]]:
        """计算移动平均"""
        result = []
        for i in range(len(data)):
            if i < period - 1:
                result.append(None)
            else:
                avg = sum(d['close'] for d in data[i-period+1:i+1]) / period
                result.append(avg)
        return result
    
    def _check_oversold(self, data_60: List[Dict]) -> bool:
        """检查 60 分钟超跌"""
        if len(data_60) < 40:
            return False
        
        recent_high = max(d['high'] for d in data_60[-30:])
        current_price = data_60[-1]['close']
        pullback = (recent_high - current_price) / recent_high
        
        return pullback > 0.03
    
    def _check_price_breakout(self, data_60: List[Dict]) -> bool:
        """检查价格突破"""
        if len(data_60) < 30:
            return False
        
        recent = data_60[-10:]
        prev = data_60[-30:-10]
        
        recent_high = max(d['high'] for d in recent)
        prev_high = max(d['high'] for d in prev)
        breakout = recent_high > prev_high * (1 + self.config['breakout_pct'])
        
        up_days = sum(1 for d in recent if d['close'] > d['open'])
        more_up = up_days >= 5
        
        return breakout and more_up
    
    def _check_macd_divergence(self, data_5: List[Dict]) -> bool:
        """检查 5 分钟 MACD 底背离"""
        if len(data_5) < 60:
            return False
        
        macd_line, signal_line = self._calc_macd(data_5)
        
        macd_valid = [m for m in macd_line if m is not None]
        if len(macd_valid) < 10:
            return False
        
        return macd_valid[-1] > macd_valid[-5]
    
    def _check_trend_filter(self, data_60: List[Dict]) -> bool:
        """趋势过滤：只做上升趋势或震荡，不做明显下跌"""
        if len(data_60) < 60:
            return True  # 数据不足时不限制
        
        # 检查 60 分钟 MA20 和 MA60
        ma20 = self._calc_ma(data_60, 20)
        ma60 = self._calc_ma(data_60, 60)
        
        if not ma20[-1] or not ma60[-1]:
            return True
        
        # 价格在 MA60 之上，或 MA20 在 MA60 之上（金叉）
        price_above_ma60 = data_60[-1]['close'] > ma60[-1]
        ma_bullish = ma20[-1] > ma60[-1]
        
        # 或者检查近期是否止跌
        recent_lows = [d['low'] for d in data_60[-10:]]
        prev_lows = [d['low'] for d in data_60[-20:-10]]
        low_rising = min(recent_lows) > min(prev_lows) * 0.98  # 低点不再创新低
        
        return price_above_ma60 or ma_bullish or low_rising
    
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
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-100:i]
            current_time = data_5min[i-1]['time']
            
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 50:
                continue
            
            # 1. 趋势过滤 (新增)
            if not self._check_trend_filter(data_60_completed):
                continue
            
            # 2. 60 分钟超跌
            if not self._check_oversold(data_60_completed):
                continue
            
            # 3. 5 分钟 MACD 背离
            if not self._check_macd_divergence(current_5):
                continue
            
            entry_price = data_5min[i-1]['close']
            entry_time = data_5min[i-1]['time']
            
            stop_loss = entry_price * (1 - self.config['stop_loss_pct'])
            take_profit = entry_price * (1 + self.config['take_profit_pct'])
            
            signal = self._simulate_trade(symbol, data_5min, i, contract, stop_loss, take_profit)
            if signal:
                signals.append(signal)
                last_signal_idx = i
        
        return signals if signals else None
    
    def _simulate_trade(self, symbol: str, data_5min: List[Dict], entry_idx: int, 
                       contract: Contract, stop_loss: float, take_profit: float) -> Optional[Signal]:
        entry_price = data_5min[entry_idx - 1]['close']
        entry_time = data_5min[entry_idx - 1]['time']
        
        exit_price = None
        exit_time = None
        exit_reason = ""
        max_bars = self.config.get('max_holding_bars', 100)
        
        for j in range(entry_idx, min(entry_idx + max_bars, len(data_5min) - 1)):
            curr_bar = data_5min[j]
            
            if curr_bar['low'] <= stop_loss:
                exit_price = stop_loss
                exit_time = curr_bar['time']
                exit_reason = "止损"
                break
            
            if curr_bar['high'] >= take_profit:
                exit_price = take_profit
                exit_time = curr_bar['time']
                exit_reason = "止盈"
                break
        
        if exit_price is None:
            last = data_5min[min(entry_idx + max_bars, len(data_5min) - 1)]
            exit_price = last['close']
            exit_time = last['time']
            exit_reason = "到期"
        
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
            stop_loss=stop_loss,
            take_profit=take_profit
        )
