#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV5Strategy - 趋势反转策略 V5
多周期共振：60 分钟 MACD 定趋势 + 5 分钟 DIF 拐头入场
- 60 分钟：MACD 红柱堆低点抬高 + DIF 底背离
- 5 分钟：DIF 拐头向上入场
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

class TrendReversalV5Strategy:
    """趋势反转策略 V5 - 多周期共振"""
    
    def __init__(self, config: Dict = None):
        self.config = {
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'stop_loss_pct': 0.03,
            'take_profit_pct': 0.05,
            'cooldown_bars': 30,
            'max_holding_bars': 100,
            # V5 新增
            'min_red_bars': 3,
            'max_lookback_60': 100,
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
        """计算 MACD"""
        n = len(closes)
        
        ema12 = np.zeros(n)
        ema26 = np.zeros(n)
        
        ema12[0] = closes[0]
        ema26[0] = closes[0]
        
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
    
    def _find_red_bar_stacks_60(self, histogram: np.ndarray, current_idx: int) -> List[Dict]:
        """60 分钟找红柱堆"""
        red_stacks = []
        start_idx = max(0, current_idx - self.config['max_lookback_60'])
        
        i = current_idx
        while i >= start_idx:
            if histogram[i] >= 0:
                i -= 1
                continue
            
            stack_end = i
            stack_start = i
            while stack_start >= start_idx and histogram[stack_start] < 0:
                stack_start -= 1
            stack_start += 1
            
            stack_bars = stack_end - stack_start + 1
            
            if stack_bars >= self.config['min_red_bars']:
                red_stacks.append({
                    'start_idx': stack_start,
                    'end_idx': stack_end,
                    'bars': stack_bars,
                })
            
            i = stack_start - 1
        
        return red_stacks
    
    def _check_60min_trend(self, data_60: List[Dict], dif: np.ndarray, 
                           histogram: np.ndarray, current_idx: int) -> Tuple[bool, str]:
        """
        60 分钟趋势判断
        1. MACD 红柱堆低点抬高
        2. DIF 底背离（可选）
        """
        if len(data_60) < 60:
            return False, "数据不足"
        
        closes = np.array([d['close'] for d in data_60])
        lows = np.array([d['low'] for d in data_60])
        current_price = closes[current_idx]
        
        # 1. 找红柱堆
        red_stacks = self._find_red_bar_stacks_60(histogram, current_idx)
        
        if len(red_stacks) < 2:
            return False, f"红柱堆不足 (仅{len(red_stacks)}个)"
        
        # 2. 获取最近两个红柱堆
        current_stack = red_stacks[0]
        prev_stack = red_stacks[1]
        
        # 3. 找红柱堆期间的最低价
        current_low = np.min(lows[current_stack['start_idx']:current_stack['end_idx']+1])
        prev_low = np.min(lows[prev_stack['start_idx']:prev_stack['end_idx']+1])
        
        # 4. 判断低点抬高
        threshold = prev_low * 0.98
        low_rising = current_low > threshold
        
        if not low_rising:
            return False, f"红柱堆低点创新低 ({current_low:.0f} < {threshold:.0f})"
        
        # 5. DIF 底背离（加分项）
        current_dif = dif[current_idx]
        prev_dif_idx = prev_stack['end_idx']
        prev_dif = dif[prev_dif_idx]
        
        dif_rising = current_dif > prev_dif
        
        reasons = []
        reasons.append(f"红柱堆低点抬高 ({current_low:.0f} > {threshold:.0f})")
        if dif_rising:
            reasons.append(f"DIF 抬高 ({current_dif:.2f} > {prev_dif:.2f})")
        
        reason = " | ".join(reasons)
        
        # 6. 价格位置过滤（不能低于 MA60 太多）
        ma60 = np.mean(closes[max(0,current_idx-59):current_idx+1])
        if current_price < ma60 * 0.95:
            return False, f"价格低于 MA60 超过 5% ({current_price:.0f} < {ma60*0.95:.0f})"
        
        return True, reason
    
    def _check_5min_entry(self, data_5: List[Dict], dif: np.ndarray, 
                          current_idx: int) -> Tuple[bool, str]:
        """
        5 分钟 DIF 拐头入场
        DIF 从下降转为上升
        """
        if len(data_5) < 20:
            return False, "数据不足"
        
        # 1. DIF 拐头：当前 DIF > 前 1 根 DIF
        dif_rising = dif[current_idx] > dif[current_idx - 1] if current_idx >= 1 else False
        
        # 2. DIF 连续下降后拐头（更可靠）
        if current_idx >= 3:
            dif_falling = all(dif[i] > dif[i+1] for i in range(current_idx-3, current_idx-1))
            dif_turning = dif[current_idx] > dif[current_idx-1]
            dif_turning = dif_falling and dif_turning
        else:
            dif_turning = dif_rising
        
        # 3. DIF 从负值区域拐头（超跌反弹）
        dif_from_negative = dif[current_idx-1] < 0 and dif[current_idx] > dif[current_idx-1]
        
        reasons = []
        if dif_turning:
            reasons.append("DIF 拐头")
        if dif_from_negative:
            reasons.append("负区拐头")
        
        is_entry = dif_turning or dif_from_negative
        reason = " | ".join(reasons) if reasons else "DIF 未拐头"
        
        return is_entry, reason
    
    def _check_trend_filter(self, data_60: List[Dict]) -> Tuple[bool, str]:
        """趋势过滤：价格不能低于 MA60 太多"""
        if len(data_60) < 60:
            return True, "数据不足"
        
        closes = np.array([d['close'] for d in data_60])
        current_price = closes[-1]
        ma60 = np.mean(closes[-60:])
        
        if current_price < ma60 * 0.95:
            return False, f"价格低于 MA60 超过 5%"
        
        return True, "趋势过滤通过"
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        """运行回测 (V5 版本)"""
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        contract = self._get_contract(symbol)
        if not contract:
            return None
        
        signals = []
        last_signal_idx = 0
        
        # 预先计算 60 分钟 MACD
        closes_60 = np.array([d['close'] for d in data_60min])
        dif_60, dea_60, hist_60 = self._calc_macd(closes_60)
        
        print(f"\n{'='*60}")
        print(f"📊 V5 回测：{symbol}")
        print(f"{'='*60}")
        
        for i in range(100, len(data_5min) - 10):
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-100:i]
            current_time = data_5min[i-1]['time']
            
            # 找到对应的 60 分钟数据
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 50:
                continue
            
            k60_idx = len(data_60_completed) - 1
            
            # 1. 60 分钟趋势判断
            trend_ok, trend_reason = self._check_60min_trend(
                data_60_completed, dif_60, hist_60, k60_idx)
            if not trend_ok:
                continue
            
            # 2. 趋势过滤（价格位置）
            filter_ok, filter_reason = self._check_trend_filter(data_60_completed)
            if not filter_ok:
                continue
            
            # 3. 5 分钟 DIF 拐头入场
            closes_5 = np.array([d['close'] for d in current_5])
            dif_5, dea_5, hist_5 = self._calc_macd(closes_5)
            
            entry_ok, entry_reason = self._check_5min_entry(current_5, dif_5, len(current_5)-1)
            if not entry_ok:
                continue
            
            # 生成信号
            entry_price = data_5min[i-1]['close']
            entry_time = data_5min[i-1]['time']
            
            stop_loss = entry_price * (1 - self.config['stop_loss_pct'])
            take_profit = entry_price * (1 + self.config['take_profit_pct'])
            
            signal = self._simulate_trade(symbol, data_5min, i, contract, stop_loss, take_profit)
            if signal:
                signals.append(signal)
                last_signal_idx = i
                
                print(f"✅ {entry_time} @ {entry_price:.2f}")
                print(f"   60 分钟：{trend_reason}")
                print(f"   5 分钟：{entry_reason}")
        
        if signals:
            print(f"\n📈 共 {len(signals)} 个信号")
        else:
            print(f"\n⚠️ 无交易信号")
        
        return signals if signals else None
    
    def _simulate_trade(self, symbol: str, data_5min: List[Dict], entry_idx: int, 
                       contract: Contract, stop_loss: float, take_profit: float) -> Optional[Signal]:
        """模拟交易"""
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
    
    print("\n🎯 TrendReversalV5 策略回测 (多周期共振版)\n")
    
    strategy = TrendReversalV5Strategy()
    strategy.load_contracts(contracts_path)
    
    symbol = sys.argv[1] if len(sys.argv) > 1 else "CFFEX.IC2606"
    
    # 加载数据
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500", (symbol,))
    data_5min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200", (symbol,))
    data_60min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    conn.close()
    
    print(f"5 分钟：{len(data_5min)}条 | 60 分钟：{len(data_60min)}条")
    
    # 运行回测
    signals = strategy.run_backtest(symbol, data_5min, data_60min)
    
    if signals:
        total_pnl = sum(s.pnl_amount for s in signals)
        wins = len([s for s in signals if s.pnl_amount > 0])
        print(f"\n💰 总盈亏：{total_pnl:+,.0f}元 | 胜率：{wins/len(signals)*100:.1f}%")


if __name__ == '__main__':
    main()
