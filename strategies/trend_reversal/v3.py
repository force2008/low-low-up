#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV3Strategy - 趋势反转策略 V3
优化：基于 MACD 柱状图颜色的动态高低点判断
- MACD 绿柱 (上涨段) 期间的高点
- MACD 红柱 (下跌段) 期间的低点
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

class TrendReversalV3Strategy:
    """趋势反转策略 V3 - MACD 柱状图优化版"""
    
    def __init__(self, config: Dict = None):
        self.config = {
            'rsi_period': 14,
            'vol_multiplier': 1.3,
            'breakout_pct': 0.005,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'stop_loss_pct': 0.03,
            'take_profit_pct': 0.05,
            'cooldown_bars': 30,
            'max_holding_bars': 100,
            # V3 新增：MACD 柱状图判断参数
            'macd_hist_lookback': 20,  # MACD 柱状图回溯周期
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
    
    def _calc_macd(self, data: List[Dict]) -> Tuple:
        """计算 MACD (含柱状图)"""
        closes = [d['close'] for d in data]
        n = len(closes)
        
        # EMA12 和 EMA26
        ema12 = np.zeros(n)
        ema26 = np.zeros(n)
        
        ema12[0] = closes[0]
        ema26[0] = closes[0]
        
        for i in range(1, n):
            ema12[i] = (closes[i] - ema12[i-1]) * 2/13 + ema12[i-1]
            ema26[i] = (closes[i] - ema26[i-1]) * 2/27 + ema26[i-1]
        
        macd_line = ema12 - ema26
        
        # Signal line
        sig_line = np.zeros(n)
        sig_line[0] = macd_line[0]
        for i in range(1, n):
            sig_line[i] = (macd_line[i] - sig_line[i-1]) * 2/10 + sig_line[i-1]
        
        # Histogram (MACD 柱状图)
        histogram = macd_line - sig_line
        
        return macd_line, sig_line, histogram
    
    def _find_macd_segment_high_low(self, histogram: np.ndarray, closes: np.ndarray, 
                                     current_idx: int, lookback: int = 20) -> Tuple[Optional[float], Optional[float]]:
        """
        根据 MACD 柱状图颜色找出上涨段高点和下跌段低点
        
        Args:
            histogram: MACD 柱状图数组
            closes: 收盘价数组
            current_idx: 当前位置
            lookback: 回溯周期
            
        Returns:
            (green_high, red_low): 绿柱期间高点，红柱期间低点
        """
        if current_idx < lookback:
            return None, None
        
        # 获取回溯周期内的数据
        hist_segment = histogram[current_idx-lookback:current_idx+1]
        close_segment = closes[current_idx-lookback:current_idx+1]
        
        # 找出绿柱 (histogram > 0) 期间的高点
        green_mask = hist_segment > 0
        green_high = None
        if np.any(green_mask):
            green_high = np.max(close_segment[green_mask])
        
        # 找出红柱 (histogram < 0) 期间的低点
        red_mask = hist_segment < 0
        red_low = None
        if np.any(red_mask):
            red_low = np.min(close_segment[red_mask])
        
        return green_high, red_low
    
    def _check_oversold_v3(self, data_60: List[Dict]) -> Tuple[bool, str, Optional[float]]:
        """
        V3 优化版超跌判断 - 基于 MACD 柱状图颜色
        
        Returns:
            (is_oversold, reason, pullback_depth)
        """
        if len(data_60) < 60:
            return False, "数据不足", None
        
        closes = np.array([d['close'] for d in data_60])
        macd_line, sig_line, histogram = self._calc_macd(data_60)
        
        current_idx = len(data_60) - 1
        lookback = self.config.get('macd_hist_lookback', 20)
        
        # 获取 MACD 柱状图高低点
        green_high, red_low = self._find_macd_segment_high_low(histogram, closes, current_idx, lookback)
        
        current_price = closes[-1]
        
        # 判断逻辑
        reasons = []
        pullback = 0
        
        # 1. 如果有绿柱高点，从绿柱高点回调
        if green_high is not None:
            pullback_green = (green_high - current_price) / green_high
            if pullback_green > 0.02:  # 绿柱高点回调 2%
                reasons.append(f"绿柱高点回调{pullback_green*100:.1f}%")
                pullback = max(pullback, pullback_green)
        
        # 2. 如果有红柱低点，判断是否接近红柱低点 (超跌反弹)
        if red_low is not None:
            distance_from_red_low = (current_price - red_low) / red_low
            if -0.01 < distance_from_red_low < 0.02:  # 接近红柱低点±1-2%
                reasons.append(f"接近红柱低点{distance_from_red_low*100:+.1f}%")
                pullback = max(pullback, 0.02)  # 视为超跌
        
        # 3. 传统 30 根 K 线高点回调 (作为备选)
        recent_high = np.max([d['high'] for d in data_60[-30:]])
        pullback_traditional = (recent_high - current_price) / recent_high
        if pullback_traditional > 0.03:
            reasons.append(f"30K 线高点回调{pullback_traditional*100:.1f}%")
            pullback = max(pullback, pullback_traditional)
        
        is_oversold = pullback > 0.02  # 至少 2% 回调
        
        if is_oversold:
            reason = " | ".join(reasons) if reasons else f"回调{pullback*100:.1f}%"
        else:
            reason = f"回调不足{pullback*100:.1f}%"
        
        return is_oversold, reason, pullback
    
    def _check_macd_divergence_v3(self, data_5: List[Dict]) -> Tuple[bool, str]:
        """
        V3 优化版 MACD 背离判断 - 更严格的底背离
        
        Returns:
            (is_divergence, reason)
        """
        if len(data_5) < 60:
            return False, "数据不足"
        
        closes = np.array([d['close'] for d in data_5])
        macd_line, sig_line, histogram = self._calc_macd(data_5)
        
        # 检查最近 20 根 K 线
        lookback = 20
        current_idx = len(data_5) - 1
        
        # 1. MACD 上升 (当前值 > 5 根 K 线前)
        macd_rising = macd_line[current_idx] > macd_line[current_idx - 5] if current_idx >= 5 else False
        
        # 2. 柱状图由红转绿 (动能转正)
        hist_current = histogram[current_idx]
        hist_prev = histogram[current_idx - 3] if current_idx >= 3 else hist_current
        hist_turning_green = hist_current > 0 and hist_prev < 0
        
        # 3. 柱状图低点抬高 (底背离)
        hist_low_recent = np.min(histogram[current_idx-10:current_idx+1])
        hist_low_prev = np.min(histogram[current_idx-20:current_idx-10])
        hist_divergence = hist_low_recent > hist_low_prev
        
        # 4. 价格创新低但 MACD 未创新低 (经典底背离)
        price_low_recent = np.min(closes[current_idx-10:current_idx+1])
        price_low_prev = np.min(closes[current_idx-20:current_idx-10])
        price_new_low = price_low_recent < price_low_prev
        macd_not_new_low = macd_line[current_idx] > np.min(macd_line[current_idx-20:current_idx-10])
        classic_divergence = price_new_low and macd_not_new_low
        
        reasons = []
        if macd_rising: reasons.append("MACD 上升")
        if hist_turning_green: reasons.append("柱转绿")
        if hist_divergence: reasons.append("柱低点抬高")
        if classic_divergence: reasons.append("底背离")
        
        # 满足任一条件即可
        is_divergence = macd_rising or hist_divergence or classic_divergence
        
        reason = " | ".join(reasons) if reasons else "无背离信号"
        
        return is_divergence, reason
    
    def _check_trend_filter(self, data_60: List[Dict]) -> Tuple[bool, str]:
        """趋势过滤"""
        if len(data_60) < 60:
            return True, "数据不足"
        
        closes = np.array([d['close'] for d in data_60])
        
        # MA20 和 MA60
        ma20 = np.mean(closes[-20:])
        ma60 = np.mean(closes[-60:])
        
        price_above_ma60 = closes[-1] > ma60
        ma_bullish = ma20 > ma60
        
        # 低点不再创新低
        recent_lows = np.min(closes[-10:])
        prev_lows = np.min(closes[-20:-10])
        low_rising = recent_lows > prev_lows * 0.98
        
        reasons = []
        if price_above_ma60: reasons.append("价格>MA60")
        if ma_bullish: reasons.append("MA20>MA60")
        if low_rising: reasons.append("低点抬高")
        
        is_uptrend = price_above_ma60 or ma_bullish or low_rising
        reason = " | ".join(reasons) if reasons else "下跌趋势"
        
        return is_uptrend, reason
    
    def run_backtest(self, symbol: str, data_5min: List[Dict], data_60min: List[Dict]) -> Optional[List[Signal]]:
        """运行回测 (V3 版本)"""
        if len(data_5min) < 200 or len(data_60min) < 50:
            return None
        
        contract = self._get_contract(symbol)
        if not contract:
            return None
        
        signals = []
        last_signal_idx = 0
        
        print(f"\n{'='*60}")
        print(f"📊 V3 回测：{symbol}")
        print(f"{'='*60}")
        
        for i in range(100, len(data_5min) - 10):
            if i - last_signal_idx < self.config['cooldown_bars']:
                continue
            
            current_5 = data_5min[i-100:i]
            current_time = data_5min[i-1]['time']
            
            data_60_completed = [d for d in data_60min if d['time'] < current_time]
            if len(data_60_completed) < 50:
                continue
            
            # 1. 趋势过滤
            trend_ok, trend_reason = self._check_trend_filter(data_60_completed)
            if not trend_ok:
                continue
            
            # 2. V3 超跌判断
            oversold_ok, oversold_reason, pullback = self._check_oversold_v3(data_60_completed)
            if not oversold_ok:
                continue
            
            # 3. V3 MACD 背离
            macd_ok, macd_reason = self._check_macd_divergence_v3(current_5)
            if not macd_ok:
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
                
                print(f"✅ {entry_time} @ {entry_price:.2f} | {oversold_reason} | {macd_reason}")
        
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
    
    print("\n🎯 TrendReversalV3 策略回测 (MACD 柱状图优化版)\n")
    
    strategy = TrendReversalV3Strategy()
    strategy.load_contracts(contracts_path)
    
    symbol = sys.argv[1] if len(sys.argv) > 1 else "CFFEX.IF2603"
    
    # 加载数据
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500", (symbol,))
    data_5min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200", (symbol,))
    data_60min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
    
    conn.close()
    
    print(f"5 分钟：{len(data_5min)} 条 | 60 分钟：{len(data_60min)} 条")
    
    # 运行回测
    signals = strategy.run_backtest(symbol, data_5min, data_60min)
    
    if signals:
        total_pnl = sum(s.pnl_amount for s in signals)
        wins = len([s for s in signals if s.pnl_amount > 0])
        print(f"\n💰 总盈亏：{total_pnl:+,.0f}元 | 胜率：{wins/len(signals)*100:.1f}%")


if __name__ == '__main__':
    main()
