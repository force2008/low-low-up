#!/usr/bin/env python3
"""
批量 Indicator 分析器 - 连接回测引擎，批量测试所有指标

策略逻辑（新版）：
- 入场条件：
  1. 60分钟MACD在绿柱堆期间
  2. 当前绿柱堆的最低价 > 前一个绿柱堆的最低价（绿柱堆抬升）
  3. 当前DIF拐头了（DIF向上）
- 入场价格：60分钟收盘价
- 止损/止盈：5分钟的前一个绿柱堆最低价
- 如果止损价 > 开仓价，跳过（下降趋势不做）
"""

import sys
import os
import argparse
import csv
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.strategy_config import Config, DataLoader
from backtest.cumulative_curve import CumulativeCurveAnalyzer, TradePnL
from backtest.indicators import RSI, MACD, BOLL, SMA, EMA, ATR, CCI
from backtest.visualization import CumulativeChartGenerator, HAS_MATPLOTLIB


# ========================================================================
# 新版策略helper函数（模块级别）
# ========================================================================

def find_green_stack_low(df, idx):
    """找到从idx往前连续的绿柱堆区域的最低价

    思路：从idx往前找到绿柱区域的最低点，返回该绿柱堆期间的最低价
    """
    if idx < 0:
        return float('inf')

    # 往前找到绿柱的起点（从当前位置往前，第一个绿柱的位置）
    start = idx
    while start > 0 and df[start][8] < 0:
        start -= 1

    # 现在start是绿柱区域的起点，遍历整个绿柱区域找最低价
    low = float('inf')
    for j in range(start, idx + 1):
        low = min(low, df[j][2])  # low在data的第2列

    return low


def find_prev_green_stack_low(df, idx):
    """找到前一个绿柱堆区域的最低价

    思路：跳过当前绿柱堆，往前找一个完整的绿柱区域
    """
    # 先跳过当前绿柱
    i = idx
    while i > 0 and df[i][8] < 0:
        i -= 1

    # 跳过中间的红柱
    while i > 0 and df[i][8] >= 0:
        i -= 1

    if i <= 0:
        return float('inf')

    # 现在i是前一个绿柱区域的某点，往前找到起点
    start = i
    while start > 0 and df[start][8] < 0:
        start -= 1

    # 遍历整个前绿柱区域找最低价
    low = float('inf')
    for j in range(start, i + 1):
        low = min(low, df[j][2])

    return low


def check_green_stack_rise(df_60m, idx, green_stacks_60m):
    """检查绿柱堆是否抬升（当前绿柱堆期间的最低价 > 前一个绿柱堆期间的最低价）

    这里用绿柱区域内的最低价来比较，而非StackIdentifier的结果
    """
    if idx < 10:
        return False, "数据不足"

    # 确保当前在绿柱堆中
    if df_60m[idx][8] >= 0:
        return False, "不在绿柱堆中"

    # 找到当前绿柱堆期间的最低价
    current_low = find_green_stack_low(df_60m, idx)
    if current_low == float('inf'):
        return False, "找不到当前绿柱堆"

    # 找到前一个绿柱堆期间的最低价
    prev_low = find_prev_green_stack_low(df_60m, idx)
    if prev_low == float('inf'):
        return False, "找不到前绿柱堆"

    # 比较：当前绿柱堆最低价 > 前一个绿柱堆最低价 = 抬升
    if current_low > prev_low:
        return True, f"绿柱堆抬升: {current_low:.2f}>{prev_low:.2f}"

    return False, f"未抬升: {current_low:.2f}<={prev_low:.2f}"


def check_dif_turn(df_60m, idx):
    """检查DIF是否拐头向上"""
    if idx < 4:
        return False, "数据不足"

    d4 = df_60m[idx-4][6]
    d3 = df_60m[idx-3][6]
    d2 = df_60m[idx-2][6]
    d1 = df_60m[idx-1][6]
    d0 = df_60m[idx][6]

    drop = (d3 > d2) or (d3 > d1) or (d2 > d1)
    rise = (d0 > d1) or (d0 > d2)

    if drop and rise:
        return True, "DIF拐头"
    return False, "未拐头"


def get_5m_stop_loss(df_5m, idx, green_stacks_5m):
    """获取5分钟前一个绿柱堆最低价作为止损价

    用绿柱区域内找最低价的逻辑，确保找的是真正的前一个绿柱堆
    """
    if idx < 10:
        return float('inf'), "数据不足"

    # 跳过当前绿柱，找前一个绿柱堆
    # 1. 先跳过当前在绿柱中
    i = idx
    while i > 0 and df_5m[i][8] < 0:
        i -= 1

    # 2. 跳过红柱过渡区
    while i > 0 and df_5m[i][8] >= 0:
        i -= 1

    if i <= 0:
        return float('inf'), "找不到前绿柱堆"

    # 3. 现在i是前一个绿柱区域的某点，找到它的起点
    start = i
    while start > 0 and df_5m[start][8] < 0:
        start -= 1

    # 4. 在整个前绿柱区域内找最低价
    low = float('inf')
    for j in range(start, i + 1):
        low = min(low, df_5m[j][2])

    return low, f"前绿柱堆最低价: {low:.2f}"


def check_5m_entry(df_5m, idx, green_stacks_5m):
    """检查5分钟入场条件"""
    if idx < 10:
        return False, "数据不足"

    hist = df_5m[idx][8] if idx < len(df_5m) else 0
    if hist < 0:
        return True, "在绿柱堆"
    if idx > 0 and df_5m[idx-1][8] < 0:
        return True, "刚离绿柱堆"
    for lb in range(1, 5):
        if idx - lb >= 0 and df_5m[idx - lb][8] < 0:
            return True, "绿柱堆回调"
    return False, "不符"


# ========================================================================

@dataclass
class BatchIndicatorResult:
    name: str = ''
    total_trades: int = 0
    total_pnl: float = 0.0
    winning_rate: float = 0.0
    max_cumsum: float = 0.0
    min_cumsum: float = 0.0
    lower_bound: float = 0.0
    upper_bound: float = 0.0
    profit_zone: Tuple[float, float] = None
    loss_zone: Tuple[float, float] = None
    is_valid: bool = False
    cumsum_data: List[float] = field(default_factory=list)
    sorted_indicators: List[float] = field(default_factory=list)
    pnl_by_indicator: List[float] = field(default_factory=list)


class BatchIndicatorAnalyzer:
    def __init__(self, db_path: str = None, contracts_path: str = None):
        self.config = Config()
        if db_path:
            self.config.DB_PATH = db_path
        if contracts_path:
            self.config.CONTRACTS_PATH = contracts_path

        self.loader = DataLoader(self.config.DB_PATH, self.config.CONTRACTS_PATH)
        self.buffer_size = 100

    def load_symbol_data(self, symbol: str):
        df_5m = self.loader.load_kline_fast(symbol, 300, self.config.MAX_5M_BARS)
        df_60m = self.loader.load_kline_fast(symbol, 3600, self.config.MAX_60M_BARS)
        return df_5m, df_60m

    def _extract_price(self, data, price_type, index):
        start = max(0, index - self.buffer_size)
        end = index + 1
        if price_type == 'open':
            return [row[0] for row in data[start:end]]
        elif price_type == 'high':
            return [row[1] for row in data[start:end]]
        elif price_type == 'low':
            return [row[2] for row in data[start:end]]
        else:
            return [row[3] for row in data[start:end]]

    def get_indicator_value(self, data, index, indicator_name):
        if index < self.buffer_size:
            return None

        parts = indicator_name.split('_')
        if len(parts) < 2:
            return None

        base = parts[0]
        period = 14
        price_type = 'close'

        try:
            period = int(parts[-1])
        except:
            pass

        if len(parts) >= 4:
            price_type = parts[1]
        elif base in ['RSI', 'SMA', 'EMA']:
            price_type = parts[1] if len(parts) == 3 else 'close'

        prices = self._extract_price(data, price_type, index)
        if len(prices) < period + 1:
            return None

        try:
            if base == 'RSI':
                vals = RSI.calculate_simple(prices, period)
                return vals[-1] if vals else None
            elif base == 'SMA':
                vals = SMA.calculate_simple(prices, period)
                return vals[-1] if vals else None
            elif base == 'EMA':
                vals = EMA.calculate_simple(prices, period)
                return vals[-1] if vals else None
            elif base == 'MACD':
                res = MACD.calculate_simple(prices, 12, 26, 9)
                return res['hist'][-1] if res.get('hist') else None
            elif base == 'BOLL':
                res = BOLL.calculate_simple(prices, 20)
                return res['upper'][-1] if res.get('upper') else None
            elif base == 'ATR':
                res = ATR.calculate_simple(data, period)
                return res[-1] if res else None
            elif base == 'CCI':
                res = CCI.calculate_simple(data, period)
                return res[-1] if res else None
        except:
            pass
        return None

    def run_backtest_with_indicators(self, symbol: str, indicators: List[str], start_date: str = None):
        """运行回测，使用新版策略"""
        from strategy.macd import MACDCalculator
        from strategy.stack import StackIdentifier
        from strategy.index_map import IndexMapper

        df_5m, df_60m = self.load_symbol_data(symbol)
        if not df_5m or not df_60m:
            return [], {}

        symbol_info = self.loader.get_symbol_info(symbol)

        # 计算指标
        df_5m = MACDCalculator.calculate(df_5m)
        df_5m, green_stacks_5m, _ = StackIdentifier.identify(df_5m)

        df_60m = MACDCalculator.calculate(df_60m)
        df_60m, green_stacks_60m, _ = StackIdentifier.identify(df_60m)

        index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)
        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1

        trades = []
        indicator_values = {ind: [] for ind in indicators}

        position = None
        last_entry_time = None
        initial_stop_loss = None

        for i, row_5m in enumerate(df_5m):
            time_str = row_5m[0][:19]

            if start_date and not time_str.startswith(start_date):
                continue

            idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1

            if position is None:
                # ============ 新版策略入场检查 ============
                if last_entry_time:
                    entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                    curr_dt = datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S')
                    hours_passed = (curr_dt - entry_dt).total_seconds() / 3600
                    if hours_passed < self.config.COOLDOWN_HOURS:
                        continue

                if idx_60m >= 10:
                    hist_60m = df_60m[idx_60m][8]

                    if hist_60m < 0:
                        # 条件1: 绿柱堆抬升
                        rise_ok, _ = check_green_stack_rise(df_60m, idx_60m, green_stacks_60m)

                        if rise_ok:
                            # 条件2: DIF拐头
                            dif_ok, _ = check_dif_turn(df_60m, idx_60m)

                            if dif_ok:
                                # 条件3: 5分钟入场条件
                                cond_5m, _ = check_5m_entry(df_5m, i, green_stacks_5m)

                                if cond_5m:
                                    # ===== 获取止损价 = 5分钟前绿柱堆最低价 =====
                                    stop_loss, _ = get_5m_stop_loss(df_5m, i, green_stacks_5m)

                                    if stop_loss == float('inf'):
                                        continue

                                    # ===== 入场价 = 60分钟收盘价 =====
                                    entry_price = df_60m[idx_60m][4]

                                    # 关键：止损价 > 入场价则跳过
                                    if stop_loss > entry_price:
                                        continue

                                    # ===== 检查仓位 =====
                                    contract_value = entry_price * volume_multiple
                                    if contract_value > self.config.TARGET_NOTIONAL:
                                        continue

                                    position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
                                    position = {
                                        'entry_idx': i,
                                        'entry_time': row_5m[0],
                                        'entry_price': entry_price,
                                        'position_size': position_size,
                                    }
                                    last_entry_time = row_5m[0]
                                    initial_stop_loss = stop_loss

                                    # 记录指标
                                    for ind in indicators:
                                        ind_val = self.get_indicator_value(df_5m, i, ind)
                                        indicator_values[ind].append(ind_val)

            else:
                # ============ 出场检查 ============
                current_low = row_5m[3]

                if initial_stop_loss and current_low <= initial_stop_loss:
                    price_diff = initial_stop_loss - position['entry_price']
                    pnl = price_diff * position['position_size'] * volume_multiple
                    pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100

                    trade = TradePnL(
                        entry_time=position['entry_time'],
                        entry_price=position['entry_price'],
                        exit_time=row_5m[0],
                        exit_price=initial_stop_loss,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                    )
                    trades.append(trade)
                    position = None
                    initial_stop_loss = None

        # 最后未平仓的
        if position:
            last_row = df_5m[-1]
            price_diff = last_row[4] - position['entry_price']
            pnl = price_diff * position['position_size'] * volume_multiple
            pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100

            trade = TradePnL(
                entry_time=position['entry_time'],
                entry_price=position['entry_price'],
                exit_time=last_row[0],
                exit_price=last_row[4],
                pnl=pnl,
                pnl_pct=pnl_pct,
            )
            trades.append(trade)

        return trades, indicator_values

    def analyze_single_indicator(self, indicator_values, pnl_list):
        if not indicator_values or not pnl_list:
            return BatchIndicatorResult()

        valid_pairs = [(iv, pnl) for iv, pnl in zip(indicator_values, pnl_list)
                     if iv is not None and iv != 0]
        if not valid_pairs:
            return BatchIndicatorResult()

        indicator_vals = [x[0] for x in valid_pairs]
        pnls = [x[1] for x in valid_pairs]

        paired = list(zip(indicator_vals, pnls))
        paired.sort(key=lambda x: x[0])

        sorted_ind = [x[0] for x in paired]
        sorted_pnl = [x[1] for x in paired]

        cumsum = []
        total = 0.0
        for pnl in sorted_pnl:
            total += pnl
            cumsum.append(total)

        max_idx = cumsum.index(max(cumsum)) if cumsum else 0
        min_idx = cumsum.index(min(cumsum)) if cumsum else 0

        lower = sorted_ind[min_idx] if min_idx < len(sorted_ind) else sorted_ind[0]
        upper = sorted_ind[max_idx] if max_idx < len(sorted_ind) else sorted_ind[-1]

        total_pnl = sum(pnls)
        winning = sum(1 for p in pnls if p > 0)
        win_rate = winning / len(pnls) * 100 if pnls else 0

        is_valid = False
        if cumsum:
            profit_range = max(cumsum) - min(cumsum)
            is_valid = profit_range > abs(total_pnl) * 0.5 and len(valid_pairs) >= 3

        return BatchIndicatorResult(
            name='',
            total_trades=len(valid_pairs),
            total_pnl=total_pnl,
            winning_rate=win_rate,
            max_cumsum=max(cumsum) if cumsum else 0,
            min_cumsum=min(cumsum) if cumsum else 0,
            lower_bound=lower,
            upper_bound=upper,
            is_valid=is_valid,
            cumsum_data=cumsum,
            sorted_indicators=sorted_ind,
            pnl_by_indicator=sorted_pnl,
        )

    def batch_analyze(self, symbol: str, start_date: str = None, output_dir: str = './backtest/reports'):
        print(f"\n{'=' * 60}")
        print(f"批量分析指标: {symbol}")
        print(f"{'=' * 60}")

        indicators = self._generate_indicator_list()
        print(f"\n共 {len(indicators)} 个指标待测试")

        print(f"\n▶ 运行回测...")
        trades, all_ind_values = self.run_backtest_with_indicators(symbol, indicators, start_date)
        print(f"  完成 {len(trades)} 笔交易")

        if not trades:
            print("❌ 没有交易")
            return []

        print(f"\n▶ 分析指标...")
        results = []

        for i, ind in enumerate(indicators):
            if (i + 1) % 50 == 0:
                print(f"  进度: {i + 1}/{len(indicators)}")

            ind_vals = all_ind_values.get(ind, [])
            pnl_list = [t.pnl for t in trades]

            if len(ind_vals) != len(pnl_list):
                continue

            result = self.analyze_single_indicator(ind_vals, pnl_list)
            result.name = ind

            if result.total_trades >= 3:
                results.append(result)

                if HAS_MATPLOTLIB:
                    # 文件名加合约名，保存到 reports/charts 目录
                    output_dir_charts = output_dir + '/charts'
                    print(f"  生成图表: {ind}")
                    gen = CumulativeChartGenerator(output_dir_charts)
                    # 用合约名_指标名作为文件名
                    file_prefix = symbol.replace('.', '_').replace('/', '_')
                    gen.generate_single_chart(f"{file_prefix}_{ind}", result.sorted_indicators,
                                       result.pnl_by_indicator, f"{symbol} - {ind}")

        results.sort(key=lambda x: x.total_pnl, reverse=True)

        print(f"\n{'=' * 60}")
        print("Top 20 有效指标:")
        print(f"{'=' * 60}")

        count = 0
        for r in results:
            if not r.is_valid:
                continue
            if count >= 20:
                break
            print(f"\n[{count + 1}] {r.name}")
            print(f"    交易数: {r.total_trades}, 胜率: {r.winning_rate:.1f}%")
            print(f"    总盈亏: {r.total_pnl:.2f}")
            print(f"    边界: [{r.lower_bound:.2f}, {r.upper_bound:.2f}]")
            count += 1

        return results

    def _generate_indicator_list(self):
        indicators = []
        # RSI
        for period in [6, 8, 10, 12, 14, 16, 20, 25, 30, 40, 50]:
            for pt in ['close', 'high', 'low']:
                indicators.append(f'RSI_{pt}_{period}')
        # SMA
        for period in [5, 10, 15, 20, 30, 50]:
            for pt in ['close', 'high', 'low']:
                indicators.append(f'SMA_{pt}_{period}')
        # EMA
        for period in [5, 10, 15, 20, 30, 50]:
            for pt in ['close', 'high', 'low']:
                indicators.append(f'EMA_{pt}_{period}')
        # MACD
        indicators.append('MACD_12_26_9')
        return indicators


def main():
    parser = argparse.ArgumentParser(description='批量指标分析')
    parser.add_argument('--symbol', '-s', type=str, help='合约代码')
    parser.add_argument('--start-date', type=str, help='开始日期')
    parser.add_argument('--output', '-o', type=str, default='./backtest/reports', help='输出目录')
    parser.add_argument('--save-csv', action='store_true', help='保存CSV')
    parser.add_argument('cmd', nargs='?', help='子命令')
    args = parser.parse_args()

    analyzer = BatchIndicatorAnalyzer()

    if args.symbol:
        results = analyzer.batch_analyze(args.symbol, args.start_date, args.output)
    else:
        contracts = analyzer.loader.load_main_contracts()
        print(f"共 {len(contracts)} 个主力合约")
        for product_id, contract in list(contracts.items())[:5]:
            exchange = contract.get('ExchangeID', '')
            if exchange not in ['SHFE', 'DCE', 'CZCE', 'CFFEX']:
                continue
            symbol = f"{exchange}.{contract['MainContractID']}"
            print(f"\n分析 {symbol}...")
            results = analyzer.batch_analyze(symbol, args.start_date, args.output)


if __name__ == '__main__':
    main()