#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
反弹做空策略 V2 - 分阶段止盈版本
支持输出多阶段信号：开仓、第一手止盈、第二手平仓
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
    stage: int = 0  # 0=开仓，1=第一手止盈，2=第二手平仓
    position: int = 2  # 手数

class ReboundShortStrategy_V2:
    """反弹做空策略 V2 - 分阶段止盈"""
    
    def __init__(self, config: Dict = None):
        self.config = {
            'min_pullback_pct': 0.005,       # 最小反弹 0.5%
            'max_pullback_pct': 0.10,        # 最大反弹 10%
            'min_distance_from_low': 0.0,    # 距离前低≥0% (去掉限制)
            'atr_threshold': 0.010,          # ATR<1.0% (方案 B) (替代布林带)
            'volume_ratio': 1.5,             # 成交量 1.5 倍
            'trailing_stop_pct': 0.02,       # 移动止损 2%
            'max_hold_bars': 200,            # 最大持仓
            'cooldown_bars': 30,             # 冷却时间 30 根 K 线
        }
        if config:
            self.config.update(config)
        
        self.contracts = {}
    
    def load_contracts(self, contracts_path: str):
        import json
        with open(contracts_path, 'r', encoding='utf-8') as f:
            contracts_data = json.load(f)
        self.contracts = {c['MainContractID']: Contract(**{
            'MainContractID': c.get('MainContractID', ''),
            'VolumeMultiple': c.get('VolumeMultiple', 10)
        }) for c in contracts_data}
    
    def get_kline_data(self, cursor, symbol: str, duration: int, limit: int = 500) -> List[Dict]:
        cursor.execute("""
            SELECT datetime, open, high, low, close, volume 
            FROM kline_data 
            WHERE symbol = ? AND duration = ?
            ORDER BY datetime DESC 
            LIMIT ?
        """, (symbol, duration, limit))
        rows = cursor.fetchall()
        return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(rows)]
    
    def _check_atr(self, data: List[Dict]) -> tuple:
        '''使用 ATR 指标检测波动率'''
        if len(data) < 15:
            return False, 0.0
        
        # 计算 ATR (14 周期)
        tr_list = []
        for i in range(1, min(15, len(data))):
            high = data[-i]['high']
            low = data[-i]['low']
            prev_close = data[-i-1]['close']
            
            # 真实波幅 = max(最高 - 最低，|最高 - 昨收 |，|最低 - 昨收|)
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        
        atr = np.mean(tr_list) if tr_list else 0
        current_price = data[-1]['close']
        atr_pct = atr / current_price * 100 if current_price > 0 else 0
        
        # ATR < 0.8% 表示低波动
        is_low_volatility = atr_pct < 0.8
        
        return is_low_volatility, atr_pct
    
    def _check_breakdown(self, curr: Dict, current_5: List[Dict]) -> bool:
        # 必须是大阴线（收盘价<开盘价）
        if curr['close'] >= curr['open']:
            return False
        
        body = abs(curr['close'] - curr['open']) / curr['open'] if curr['open'] > 0 else 0
        avg_body = np.mean([abs(d['close']-d['open'])/d['open'] for d in current_5[-10:-1]])
        
        if body < avg_body * 2:
            return False
        
        vol_ma = np.mean([d['volume'] for d in current_5[-11:-1]])
        vol_ratio = curr['volume'] / vol_ma if vol_ma > 0 else 0
        if vol_ratio < self.config['volume_ratio']:
            return False
        
        # 计算布林带下轨
        closes = [d['close'] for d in current_5[-20:]]
        bb_mid = np.mean(closes)
        bb_std = np.std(closes)
        bb_low = bb_mid - 2 * bb_std if bb_mid and bb_std else None
        
        if bb_low is None or curr['close'] >= bb_low:
            return False
        
        return True
    
    def _get_contract(self, symbol: str) -> Optional[Contract]:
        parts = symbol.split('.')
        if len(parts) != 2:
            return None
        return self.contracts.get(parts[1])
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        signals = []
        last_signal_idx = 0
        
        # 检查 60 分钟趋势
        closes_60 = [d['close'] for d in data_60min]
        ma20_60 = np.mean(closes_60[-20:])
        ma50_60 = np.mean(closes_60[-50:]) if len(closes_60) >= 50 else ma20_60
        
        if ma20_60 >= ma50_60:
            return None
        
        for i in range(201, len(data_5min) - 30):
            # 冷却时间检查
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-200:i]
            
            # 检查反弹条件
            low_60 = min(d['low'] for d in data_60min[-20:])
            current_price = current_5[-1]['close']
            rebound = (current_price - low_60) / low_60 * 100
            
            if not (self.config['min_pullback_pct']*100 <= rebound <= self.config['max_pullback_pct']*100):
                continue
            
            if rebound < self.config['min_distance_from_low']*100:  # 距离前低不足
                continue
            
            is_low_vol, atr_pct = self._check_atr(current_5)
            if not is_low_vol:
                continue
            
            curr = data_5min[i-1]
            if not self._check_breakdown(curr, current_5):
                continue
            
            contract = self._get_contract(symbol)
            if not contract:
                continue
            
            # 获取分阶段信号
            trade_signals = self._simulate_trade_multi_stage(symbol, data_5min, i, contract, rebound)
            if trade_signals:
                signals.extend(trade_signals)
                last_signal_idx = i
        
        return signals if signals else None
    
    def _simulate_trade_multi_stage(self, symbol: str, data_5min: List[Dict], entry_idx: int, 
                                    contract: Contract, rebound_from_low: float) -> List[Signal]:
        """模拟交易 - 分阶段止盈，返回多个 Signal"""
        curr = data_5min[entry_idx - 1]
        entry_price = curr['close']
        entry_time = curr['time']
        
        # 计算前低
        low_60 = min(d['low'] for d in data_5min[max(0, entry_idx-200):entry_idx])
        
        position = 2  # 2 手
        exit_price = None
        exit_time = None
        exit_reason = ""
        min_price = entry_price
        partial_exit_done = False
        partial_exit_price = None
        cost_basis = entry_price
        
        # 模拟持仓过程
        for j in range(entry_idx, min(entry_idx + self.config['max_hold_bars'], len(data_5min) - 1)):
            curr_bar = data_5min[j]
            min_price = min(min_price, curr_bar['low'])
            
            # 计算距离前低的百分比
            distance_to_low = (min_price - low_60) / low_60 * 100 if low_60 > 0 else 100
            
            # 分阶段止盈逻辑
            if not partial_exit_done and distance_to_low < 1.0 and min_price < entry_price:
                partial_exit_done = True
                partial_exit_price = min_price
                cost_basis = entry_price  # 剩余仓位成本价
            
            if partial_exit_done:
                # 第二手止损在成本线
                if curr_bar['high'] >= cost_basis:
                    exit_price = cost_basis
                    exit_time = curr_bar['time']
                    exit_reason = "第二手保本"
                    break
                
                # 第二手移动止损
                trailing_stop = min_price * (1 + self.config['trailing_stop_pct'])
                if curr_bar['high'] >= trailing_stop:
                    exit_price = trailing_stop
                    exit_time = curr_bar['time']
                    exit_reason = "第二手止损"
                    break
            else:
                # 未部分止盈前，按原规则
                trailing_stop = min_price * (1 + self.config['trailing_stop_pct'])
                
                if curr_bar['high'] >= trailing_stop:
                    exit_price = trailing_stop
                    exit_time = curr_bar['time']
                    exit_reason = "移动止损"
                    break
                
                # 三重确认
                if j >= 10:
                    recent_closes = [data_5min[k]['close'] for k in range(j-10, j)]
                    curr_body = abs(curr_bar['close'] - curr_bar['open']) / curr_bar['open']
                    avg_body_recent = np.mean([abs(data_5min[k]['close']-data_5min[k]['open'])/data_5min[k]['open'] for k in range(j-10, j)])
                    vol_ma_recent = np.mean([data_5min[k]['volume'] for k in range(j-10, j)])
                    vol_ratio = curr_bar['volume'] / vol_ma_recent if vol_ma_recent > 0 else 0
                    
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
        
        # 生成多阶段信号
        signals_list = []
        
        # 信号 1: 开仓
        signals_list.append(Signal(
            symbol=symbol, entry_time=entry_time, entry_price=entry_price,
            exit_time=entry_time, exit_price=entry_price, exit_reason="开仓",
            pnl_pct=0, pnl_amount=0, pnl_per_hand=0,
            entry_idx=entry_idx, pullback_60min=rebound_from_low,
            max_price=entry_price, resistance=low_60,
            stage=0, position=2
        ))
        
        if partial_exit_done and partial_exit_price:
            # 信号 2: 第一手止盈
            partial_profit = (entry_price - partial_exit_price) * contract.VolumeMultiple * 1
            signals_list.append(Signal(
                symbol=symbol, entry_time=entry_time, entry_price=entry_price,
                exit_time=exit_time, exit_price=partial_exit_price, exit_reason="第一手止盈",
                pnl_pct=partial_profit / (entry_price * contract.VolumeMultiple),
                pnl_amount=partial_profit, pnl_per_hand=partial_profit,
                entry_idx=entry_idx, pullback_60min=rebound_from_low,
                max_price=partial_exit_price, resistance=low_60,
                stage=1, position=1
            ))
            
            # 信号 3: 第二手平仓
            remaining_profit = (entry_price - exit_price) * contract.VolumeMultiple * 1
            signals_list.append(Signal(
                symbol=symbol, entry_time=entry_time, entry_price=entry_price,
                exit_time=exit_time, exit_price=exit_price, exit_reason=exit_reason,
                pnl_pct=remaining_profit / (entry_price * contract.VolumeMultiple),
                pnl_amount=remaining_profit, pnl_per_hand=remaining_profit,
                entry_idx=entry_idx, pullback_60min=rebound_from_low,
                max_price=min_price, resistance=low_60,
                stage=2, position=1
            ))
        else:
            # 未触发分阶段止盈，只有开仓和平仓
            total_pnl = (entry_price - exit_price) * contract.VolumeMultiple * position
            signals_list.append(Signal(
                symbol=symbol, entry_time=entry_time, entry_price=entry_price,
                exit_time=exit_time, exit_price=exit_price, exit_reason=exit_reason,
                pnl_pct=total_pnl / (entry_price * contract.VolumeMultiple * position),
                pnl_amount=total_pnl, pnl_per_hand=total_pnl / position,
                entry_idx=entry_idx, pullback_60min=rebound_from_low,
                max_price=min_price, resistance=low_60,
                stage=2, position=2
            ))
        
        return signals_list
