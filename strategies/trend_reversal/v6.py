#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV6Strategy V3 - 趋势反转策略 V6 (最终优化版)
60 分钟 MACD 能量背离 + 5 分钟绿柱堆放量入场

60 分钟信号 (严格要求):
1. 红柱能量背离：当前红柱堆能量 < 前红柱堆能量 × 0.8
   - 必须等红柱堆完成（已出现绿柱）
2. 或 DIF 二次拐头：绿柱期间 (hist>0)，DIF 连续下降后拐头向上
   - 必须确认是绿柱 (hist > 0)

5 分钟入场:
1. 绿柱堆 (histogram > 0) 连续 ≥ 3 根
2. 放量 (volume > MA20 × 1.5) 或 阳柱 (close > open)

止损:
5 分钟绿柱堆最低价 - 2 ticks
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

class TrendReversalV6Strategy:
    """趋势反转策略 V6 (最终优化版) - MACD 能量背离"""
    
    def __init__(self, config: Dict = None):
        self.config = {
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'stop_loss_ticks': 2,
            'take_profit_pct': 0.05,
            'cooldown_bars': 30,
            'max_holding_bars': 100,
            'energy_ratio_threshold': 0.8,
            'volume_multiplier': 1.5,
            'min_green_bars': 3,
            'min_dif_fall_bars': 2,
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
    
    def _calc_macd(self, closes: np.ndarray) -> Tuple:
        n = len(closes)
        ema12 = np.zeros(n)
        ema26 = np.zeros(n)
        ema12[0] = ema26[0] = closes[0]
        for i in range(1, n):
            ema12[i] = (closes[i] - ema12[i-1]) * 2/13 + ema12[i-1]
            ema26[i] = (closes[i] - ema26[i-1]) * 2/27 + ema26[i-1]
        dif = ema12 - ema26
        dea = np.zeros(n)
        dea[0] = dif[0]
        for i in range(1, n):
            dea[i] = (dif[i] - dea[i-1]) * 2/10 + dea[i-1]
        histogram = dif - dea
        return dif, dea, histogram
    
    def _find_red_bar_stacks_60min(self, histogram: np.ndarray, current_idx: int, max_lookback: int = 100) -> List[Dict]:
        """60 分钟找红柱堆 (histogram < 0)，只找已完成的红柱堆"""
        red_stacks = []
        start_idx = max(0, current_idx - max_lookback)
        
        i = start_idx
        while i <= current_idx:
            if histogram[i] >= 0:
                i += 1
                continue
            
            stack_start = i
            while i <= current_idx and histogram[i] < 0:
                i += 1
            stack_end = i - 1
            
            # 检查红柱堆是否已完成（后面出现了绿柱）
            is_complete = (i <= current_idx and histogram[i] > 0)
            
            stack_bars = stack_end - stack_start + 1
            
            if stack_bars >= 3 and is_complete:
                energy_sum = np.abs(np.sum(histogram[stack_start:stack_end+1]))
                red_stacks.append({
                    'start_idx': stack_start,
                    'end_idx': stack_end,
                    'bars': stack_bars,
                    'energy_sum': energy_sum,
                    'is_complete': is_complete,
                })
            
            i = stack_start + 1  # 继续往后找
        
        return red_stacks
    
    def _check_60min_energy_divergence(self, data_60: List[Dict], dif: np.ndarray, dea: np.ndarray,
                                        histogram: np.ndarray, current_idx: int) -> Tuple[bool, str]:
        """
        60 分钟能量背离判断 (最终优化版)
        
        条件 1: 红柱能量背离
          - 必须等红柱堆完成（已出现绿柱）
          - 当前红柱堆能量 < 前红柱堆能量 × 0.8
        
        条件 2: DIF 二次拐头 (绿柱期间)
          - 当前 histogram > 0 (绿柱)
          - DIF 连续下降 ≥ 2 根 K 线
          - 当前 DIF 拐头向上 (DIF[i] > DIF[i-1])
        """
        if len(data_60) < 60:
            return False, "数据不足"
        
        closes = np.array([d['close'] for d in data_60])
        current_price = closes[current_idx]
        current_dif = dif[current_idx]
        current_hist = histogram[current_idx]
        
        reasons = []
        signal_triggered = False
        
        # ========== 条件 1: 红柱能量背离 (必须已完成) ==========
        red_stacks = self._find_red_bar_stacks_60min(histogram, current_idx)
        
        if len(red_stacks) >= 2:
            # 只考虑已完成的红柱堆
            complete_stacks = [s for s in red_stacks if s.get('is_complete', False)]
            
            if len(complete_stacks) >= 2:
                current_stack = complete_stacks[-1]
                prev_stack = complete_stacks[-2]
                
                energy_ratio = current_stack['energy_sum'] / prev_stack['energy_sum'] if prev_stack['energy_sum'] > 0 else 1.0
                threshold = self.config['energy_ratio_threshold']
                
                if energy_ratio < threshold:
                    signal_triggered = True
                    reasons.append(f"红柱背离 ({energy_ratio:.2f} < {threshold}, 已完成)")
        
        # ========== 条件 2: DIF 二次拐头 (绿柱期间) ==========
        min_fall_bars = self.config.get('min_dif_fall_bars', 2)
        
        # 当前必须是绿柱
        if current_hist > 0:
            if current_idx >= min_fall_bars + 1:
                dif_falling = all(dif[i] > dif[i+1] for i in range(current_idx - min_fall_bars, current_idx - 1))
                dif_turning = current_dif > dif[current_idx - 1]
                
                if dif_falling and dif_turning:
                    signal_triggered = True
                    reasons.append(f"DIF 拐头 ({current_dif:.2f} > {dif[current_idx-1]:.2f}, 绿柱)")
        
        # ========== 价格位置过滤 ==========
        ma60 = np.mean(closes[max(0, current_idx-59):current_idx+1])
        price_filter = current_price >= ma60 * 0.95
        
        if not price_filter:
            reasons.append(f"价格过滤 (价格低于 MA60 超过 5%)")
        
        is_signal = signal_triggered and price_filter
        reason = " | ".join(reasons) if reasons else "无背离信号"
        
        return is_signal, reason
    
    def _check_5min_entry(self, data_5: List[Dict], histogram: np.ndarray, 
                          volumes: np.ndarray, current_idx: int) -> Tuple[bool, str, Optional[Dict]]:
        """5 分钟入场信号"""
        if len(data_5) < 20:
            return False, "数据不足", None
        
        stack_end = current_idx
        stack_start = current_idx
        
        while stack_start >= 0 and histogram[stack_start] > 0:
            stack_start -= 1
        stack_start += 1
        
        stack_bars = stack_end - stack_start + 1
        min_bars = self.config.get('min_green_bars', 3)
        
        if stack_bars < min_bars:
            return False, "绿柱不足", None
        
        energy_sum = np.sum(histogram[stack_start:stack_end+1])
        
        vol_ma20 = np.mean(volumes[max(0, stack_start-20):stack_start]) if stack_start >= 20 else np.mean(volumes[:stack_start])
        avg_volume = np.mean(volumes[stack_start:stack_end+1])
        is_high_volume = avg_volume > vol_ma20 * self.config['volume_multiplier']
        
        current_bar = data_5[current_idx]
        is_yang = current_bar['close'] > current_bar['open']
        
        reasons = []
        if stack_bars >= min_bars:
            reasons.append(f"绿柱堆 ({stack_bars}根)")
        if is_high_volume:
            reasons.append("放量")
        if is_yang:
            reasons.append("阳柱")
        
        is_entry = stack_bars >= min_bars and (is_high_volume or is_yang)
        reason = " | ".join(reasons) if reasons else "无信号"
        
        green_stack = {
            'start_idx': stack_start,
            'end_idx': stack_end,
            'bars': stack_bars,
            'energy_sum': energy_sum,
            'is_high_volume': is_high_volume,
        }
        
        return is_entry, reason, green_stack
    
    def _calc_stop_loss(self, data_5: List[Dict], green_stack: Dict, contract: Contract) -> float:
        stack_start = green_stack['start_idx']
        stack_end = green_stack['end_idx']
        lows = np.array([d['low'] for d in data_5[stack_start:stack_end+1]])
        min_low = np.min(lows)
        stop_loss = min_low - contract.PriceTick * self.config['stop_loss_ticks']
        return stop_loss
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        contract = self._get_contract(symbol)
        if not contract:
            return None
        
        signals = []
        last_signal_idx = 0
        
        closes_60 = np.array([d['close'] for d in data_60min])
        dif_60, dea_60, hist_60 = self._calc_macd(closes_60)
        
        print(f"\n{'='*60}")
        print(f"📊 V6 回测 (最终优化版): {symbol}")
        print(f"{'='*60}")
        
        for i in range(100, len(data_5min) - 10):
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-100:i]
            current_time = data_5min[i-1]['time']
            
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 50:
                continue
            
            k60_idx = len(data_60_completed) - 1
            
            trend_ok, trend_reason = self._check_60min_energy_divergence(
                data_60_completed, dif_60, dea_60, hist_60, k60_idx)
            if not trend_ok:
                continue
            
            closes_5 = np.array([d['close'] for d in current_5])
            volumes_5 = np.array([d['volume'] for d in current_5])
            dif_5, dea_5, hist_5 = self._calc_macd(closes_5)
            
            entry_ok, entry_reason, green_stack = self._check_5min_entry(
                current_5, hist_5, volumes_5, len(current_5)-1)
            if not entry_ok:
                continue
            
            entry_price = data_5min[i-1]['close']
            entry_time = data_5min[i-1]['time']
            
            stop_loss = self._calc_stop_loss(current_5, green_stack, contract)
            take_profit = entry_price * (1 + self.config['take_profit_pct'])
            
            signal = self._simulate_trade(symbol, data_5min, i, contract, stop_loss, take_profit)
            if signal:
                signals.append(signal)
                last_signal_idx = i
                
                print(f"✅ {entry_time} @ {entry_price:.2f}")
                print(f"   60 分钟：{trend_reason}")
                print(f"   5 分钟：{entry_reason}")
                print(f"   止损：{stop_loss:.2f}")
        
        if signals:
            print(f"\n📈 共 {len(signals)} 个信号")
        else:
            print(f"\n⚠️ 无交易信号")
        
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
    
    def _get_contract(self, symbol: str) -> Optional[Contract]:
        parts = symbol.split('.')
        if len(parts) != 2:
            return None
        return self.contracts.get(parts[1])


def main():
    import sqlite3
    import sys
    
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    contracts_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/main_contracts.json'
    
    print("\n🎯 TrendReversalV6 策略回测 (最终优化版 - 红柱完成后入场)\n")
    
    strategy = TrendReversalV6Strategy()
    strategy.load_contracts(contracts_path)
    
    symbol = sys.argv[1] if len(sys.argv) > 1 else "CFFEX.IC2606"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500", (symbol,))
    data_5min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200", (symbol,))
    data_60min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    conn.close()
    
    print(f"5 分钟：{len(data_5min)}条 | 60 分钟：{len(data_60min)}条")
    
    signals = strategy.run_backtest(symbol, data_5min, data_60min)
    
    if signals:
        total_pnl = sum(s.pnl_amount for s in signals)
        wins = len([s for s in signals if s.pnl_amount > 0])
        print(f"\n💰 总盈亏：{total_pnl:+,.0f}元 | 胜率：{wins/len(signals)*100:.1f}%")


if __name__ == '__main__':
    main()
