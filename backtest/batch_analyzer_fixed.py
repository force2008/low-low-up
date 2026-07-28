#!/usr/bin/env python3
"""
批量 Indicator 分析器 - 连接回测引擎，批量测试所有指标

实现功能：
1. 运行回测，记录每笔交易入场时的 indicator 值
2. 批量测试所有指标组合
3. 生成 cumulative sum 可视化图表
4. 找出有效的边界条件
5. 输出结果到 CSV/PDF
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
import random

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.strategy_config import Config, DataLoader
from backtest.cumulative_curve import CumulativeCurveAnalyzer, TradePnL
from backtest.indicators import RSI, MACD, BOLL, SMA, EMA, ATR, CCI
from backtest.visualization import CumulativeChartGenerator, HAS_MATPLOTLIB


@dataclass
class BatchIndicatorResult:
    """单个指标批量测试结果"""
    name: str                           # 指标名称
    total_trades: int = 0               # 总交易数
    total_pnl: float = 0.0              # 总盈亏
    winning_rate: float = 0.0            # 胜率
    max_cumsum: float = 0.0             # 最大累积收益
    min_cumsum: float = 0.0             # 最小累积收益
    lower_bound: float = 0.0            # 最佳下界
    upper_bound: float = 0.0            # 最佳上界
    profit_zone: Tuple[float, float] = None  # 盈利区间
    loss_zone: Tuple[float, float] = None    # 亏损区间
    is_valid: bool = False               # 是否有效
    cumsum_data: List[float] = field(default_factory=list)  # 累积曲线数据
    sorted_indicators: List[float] = field(default_factory=list)  # 排序后的指标值
    pnl_by_indicator: List[float] = field(default_factory=list)  # 排序后的 PnL


class BatchIndicatorAnalyzer:
    """批量指标分析器"""

    def __init__(self, db_path: str = None, contracts_path: str = None):
        self.config = Config()
        if db_path:
            self.config.DB_PATH = db_path
        if contracts_path:
            self.config.CONTRACTS_PATH = contracts_path

        self.loader = DataLoader(self.config.DB_PATH, self.config.CONTRACTS_PATH)

        # 计算缓冲区大小
        self.buffer_size = 100  # 指标需要的历史数据量

    def load_symbol_data(self, symbol: str) -> Tuple[List, List]:
        """加载 5m 和 60m 数据"""
        df_5m = self.loader.load_kline_fast(symbol, 300, self.config.MAX_5M_BARS)
        df_60m = self.loader.load_kline_fast(symbol, 3600, self.config.MAX_60M_BARS)
        return df_5m, df_60m

    def get_indicator_value(self, data: List, index: int, indicator_name: str) -> Optional[float]:
        """获取指定时刻的指标值

        Args:
            data: K线数据
            index: 当前索引
            indicator_name: 指标名称，如 'RSI_close_14'

        Returns:
            指标值，如果无法计算则返回 None
        """
        if index < self.buffer_size:
            return None

        # 解析指标名称
        parts = indicator_name.split('_')
        if len(parts) < 2:
            return None

        base = parts[0]
        price_type = 'close'
        period = 14

        # 解析参数
        if len(parts) >= 3:
            try:
                period = int(parts[-1])
            except ValueError:
                period = 14

        if len(parts) >= 4:
            price_type = parts[1]
        elif base in ['RSI', 'SMA', 'EMA']:
            price_type = parts[1] if len(parts) == 3 else 'close'

        # 获取价格序列
        prices = self._extract_price(data, price_type, index)
        if len(prices) < period + 1:
            return None

        # 计算指标值
        try:
            if base == 'RSI':
                values = RSI.calculate_simple(prices, period)
                # _extract_price 已取向前100条，values[-1] 即当前索引的RSI
                return values[-1] if values else None
            elif base == 'SMA':
                values = SMA.calculate_simple(prices, period)
                return values[-1] if values else None
            elif base == 'EMA':
                values = EMA.calculate_simple(prices, period)
                return values[-1] if values else None
            elif base == 'MACD':
                # MACD 需要 fast, slow, signal
                fast = 12
                slow = 26
                signal = 9
                if len(parts) >= 4:
                    try:
                        fast = int(parts[1])
                        slow = int(parts[2])
                        signal = int(parts[3])
                    except ValueError:
                        pass
                macd_result = MACD.calculate_simple(prices, fast, slow, signal)
                return macd_result['hist'][-1] if macd_result.get('hist') else None
            elif base == 'BOLL':
                if len(parts) >= 3:
                    try:
                        period = int(parts[1])
                    except ValueError:
                        period = 20
                result = BOLL.calculate_simple(prices, period)
                return result['upper'][-1] if result.get('upper') else None
            elif base == 'ATR':
                # ATR 直接用 high/low/close 计算
                result = ATR.calculate_simple(data, period)
                return result[-1] if result else None
            elif base == 'CCI':
                result = CCI.calculate_simple(data, period)
                return result[-1] if result else None
            else:
                return None
        except Exception as e:
            return None

    def _extract_price(self, data: List, price_type: str, index: int) -> List[float]:
        """提取价格序列（从当前索引向前 buffer_size 个点）"""
        start_idx = max(0, index - self.buffer_size)
        end_idx = index + 1

        if price_type == 'open':
            return [row[0] for row in data[start_idx:end_idx]]
        elif price_type == 'high':
            return [row[1] for row in data[start_idx:end_idx]]
        elif price_type == 'low':
            return [row[2] for row in data[start_idx:end_idx]]
        else:  # close
            return [row[3] for row in data[start_idx:end_idx]]

    # ========================================================================
    # 新策略的辅助函数
    # ========================================================================

    def _get_green_stack_low(self, df, idx, green_stacks):
        """获取绿柱堆最低价"""
        stack_low = float('inf')
        in_stack = False
        for stack_idx, stack_info in green_stacks.items():
            if stack_idx <= idx:
                stack_low = min(stack_low, stack_info.get('low', float('inf')))
                in_stack = True
        return (stack_low if in_stack else float('inf'), in_stack)

    def _check_60m_green_stack_rise(self, df_60m, idx, green_stacks_60m):
        """检查绿柱堆是否抬升"""
        if idx < 10:
            return False, "数据不足"
        if df_60m[idx][8] >= 0:
            return False, "不在绿柱堆中"

        current_stack_low = float('inf')
        prev_stack_low = float('inf')
        sorted_stacks = sorted(green_stacks_60m.keys(), reverse=True)

        for i, stack_idx in enumerate(sorted_stacks):
            if stack_idx <= idx:
                stack_info = green_stacks_60m[stack_idx]
                stack_low = stack_info.get('low', float('inf'))
                if i == 0:
                    current_stack_low = stack_low
                elif i == 1:
                    prev_stack_low = stack_low
                else:
                    break

        if current_stack_low == float('inf') or prev_stack_low == float('inf'):
            return False, "找不到绿柱堆"

        if current_stack_low > prev_stack_low:
            return True, f"绿柱堆抬升"
        return False, "未抬升"

    def _check_60m_dif_turn_new(self, df_60m, idx):
        """检查DIF是否拐头"""
        if idx < 4:
            return False, "数据不足"
        dif_4 = df_60m[idx-4][6]
        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]
        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1) or (dif_2 > dif_1)
        has_rise = (dif_0 > dif_1) or (dif_0 > dif_2)
        if has_drop and has_rise:
            return True, "DIF拐头"
        return False, "未拐头"

    def _get_5m_stop_loss(self, df_5m, idx, green_stacks_5m):
        """获取5分钟前一个绿柱堆最低价作为止损价"""
        if not green_stacks_5m:
            return float('inf'), "无绿柱堆"
        sorted_stacks = sorted(green_stacks_5m.keys(), reverse=True)
        for stack_idx in sorted_stacks:
            if stack_idx < idx:
                stack_info = green_stacks_5m[stack_idx]
                return stack_info.get('low', float('inf')), "前绿柱堆最低价"
        return float('inf'), "找不到"

    def _check_5m_entry_condition(self, df_5m, idx, green_stacks_5m):
        """检查5分钟入场条件"""
        if idx < 10:
            return False, "数据不足"
        hist = df_5m[idx][8] if idx < len(df_5m) else 0
        if hist < 0:
            return True, "在绿柱堆"
        if idx > 0 and df_5m[idx-1][8] < 0:
            return True, "刚离绿柱堆"
        for lookback in range(1, 5):
            if idx - lookback >= 0 and df_5m[idx - lookback][8] < 0:
                return True, "绿柱堆回调"
        return False, "不符"

    def run_backtest_with_indicators(self, symbol: str,
    def run_backtest_with_indicators(self, symbol: str,
                                   indicators: List[str],
                                   start_date: str = None) -> Tuple[List[TradePnL], Dict]:
        """运行回测，同时记录每笔交易的 indicator 值

        Returns:
            (交易列表, {indicator_name: [每次入场的指标值列表]})
        """
        from strategy.macd import MACDCalculator, ATRCalculator
        from strategy.stack import StackIdentifier
        from strategy.index_map import IndexMapper
        from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp

        df_5m, df_60m = self.load_symbol_data(symbol)
        if not df_5m or not df_60m:
            return [], {}

        symbol_info = self.loader.get_symbol_info(symbol)

        # 计算指标
        df_5m = MACDCalculator.calculate(df_5m)
        df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)

        df_60m = MACDCalculator.calculate(df_60m)
        df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

        index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)
        strategy = StrategyLowLowUp(symbol_info or {})

        trades = []
        indicator_values = {ind: [] for ind in indicators}

        position = None
        last_entry_time = None
        initial_stop_loss = None

        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1

        for i, row_5m in enumerate(df_5m):
            time_str = row_5m[0][:19]

            if start_date and not time_str.startswith(start_date):
                continue

            idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1

            if position is None:
                # ==================================================================
                # 入场检查 - 新策略逻辑
                #
                # 入场条件：
                #   1. 60分钟MACD在绿柱堆期间
                #   2. 当前绿柱堆的最低价 > 前一个绿柱堆的最低价（绿柱堆抬升）
                #   3. 当前DIF拐头了（DIF向上）
                #
                # 入场价格：60分钟收盘价（而非5分钟）
                #
                # 止损/止盈：5分钟的前一个绿柱堆最低价
                #   - 如果止损价 > 开仓价，则跳过开仓（下降趋势不做）
                #
                # 无需底背离验证
                # ==================================================================

                if last_entry_time is not None:
                    entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                    current_dt = datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S')
                    hours_passed = (current_dt - entry_dt).total_seconds() / 3600
                    if hours_passed < self.config.COOLDOWN_HOURS:
                        continue

                if idx_60m >= 10:
                    hist_60m = df_60m[idx_60m][8]

                    signal_found = False

                    # 只有在绿柱堆中才检查入场
                    if hist_60m < 0:
                        # 条件1: 检查绿柱堆是否抬升（当前绿柱堆最低价 > 前一个绿柱堆最低价）
                        rise_ok, rise_msg = _check_60m_green_stack_rise(self, df_60m, idx_60m, green_stacks_60m)

                        if rise_ok:
                            # 条件2: 检查DIF是否拐头
                            dif_turn, dif_msg = _check_60m_dif_turn_new(self, df_60m, idx_60m)

                            if dif_turn:
                                # 条件3: 检查5分钟入场条件
                                cond_5m, cond_5m_msg = _check_5m_entry_condition(self, df_5m, i, green_stacks_5m)

                                if cond_5m:
                                    signal_found = True

                    if signal_found:
                        # ============================================================
                        # 获取止损价：5分钟的前一个绿柱堆最低价
                        # ============================================================
                        initial_stop, stop_msg = _get_5m_stop_loss(self, df_5m, i, green_stacks_5m)

                        if initial_stop == float('inf'):
                            continue

                        # 获取60分钟收盘价作为入场价
                        entry_price = df_60m[idx_60m][4]

                        # 关键检查：止损价 > 开仓价 则跳过（下降趋势不做）
                        if initial_stop > entry_price:
                            continue

                        # 检查仓位
                        contract_value = entry_price * volume_multiple
                        if contract_value > self.config.TARGET_NOTIONAL:
                            continue

                        position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
                        position = {
                            'entry_idx': i,
                            'entry_time': row_5m[0],
                            'entry_price': entry_price,  # 60分钟收盘价
                            'position_size': position_size,
                        }
                        last_entry_time = row_5m[0]
                        initial_stop_loss = initial_stop

                        # 记录每个指标的入场值
                        for ind in indicators:
                            ind_val = self.get_indicator_value(df_5m, i, ind)
                            indicator_values[ind].append(ind_val)

            else:
                # 出场检查
                current_low = row_5m[3]

                if initial_stop_loss is not None and current_low <= initial_stop_loss:
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

        # 处理未平仓的仓位
        if position is not None:
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

    def analyze_single_indicator(self, indicator_values: List[float],
                               pnl_list: List[float]) -> BatchIndicatorResult:
        """分析单个指标"""
        if not indicator_values or not pnl_list:
            return BatchIndicatorResult(name='', total_trades=0)

        # 过滤掉 None 值
        valid_pairs = [(iv, pnl) for iv, pnl in zip(indicator_values, pnl_list)
                    if iv is not None and iv != 0]
        if not valid_pairs:
            return BatchIndicatorResult(name='', total_trades=0)

        indicator_vals = [x[0] for x in valid_pairs]
        pnls = [x[1] for x in valid_pairs]

        # 按指标值排序
        paired = list(zip(indicator_vals, pnls))
        paired.sort(key=lambda x: x[0])

        sorted_indicators = [x[0] for x in paired]
        sorted_pnls = [x[1] for x in paired]

        # 计算累积曲线
        analyzer = CumulativeCurveAnalyzer()
        cumsum = []
        total = 0.0
        for pnl in sorted_pnls:
            total += pnl
            cumsum.append(total)

        # 找边界
        lower_bound, upper_bound = 0.0, 0.0
        max_profit = float('-inf')
        max_idx = 0
        min_idx = 0

        if cumsum:
            max_idx = cumsum.index(max(cumsum))
            min_idx = cumsum.index(min(cumsum))
            max_profit = max(cumsum) - min(cumsum)

            if min_idx <= max_idx:
                lower_bound = sorted_indicators[min_idx] if min_idx < len(sorted_indicators) else sorted_indicators[0]
                upper_bound = sorted_indicators[max_idx] if max_idx < len(sorted_indicators) else sorted_indicators[-1]
            else:
                lower_bound = sorted_indicators[0]
                upper_bound = sorted_indicators[-1]

        # 找盈利/亏损区间
        profit_zone = None
        loss_zone = None
        if cumsum:
            # 简单方法：从最大值位置向左找最低点，向右找最低点
            if max_idx > 0:
                left_min = min(cumsum[:max_idx + 1])
                if left_min < 0:
                    loss_zone = (sorted_indicators[0], sorted_indicators[max_idx])

            if max_idx < len(cumsum) - 1:
                right_min = min(cumsum[max_idx:])
                if right_min > 0 or cumsum[-1] > 0:
                    profit_zone = (sorted_indicators[max_idx], sorted_indicators[-1])

        total_pnl = sum(pnls)
        winning = sum(1 for p in pnls if p > 0)
        winning_rate = winning / len(pnls) * 100 if pnls else 0

        # 判断是否有效：累积曲线有明显波动（至少3笔交易）
        is_valid = False
        if cumsum:
            profit_range = max(cumsum) - min(cumsum)
            is_valid = profit_range > abs(total_pnl) * 0.5 and len(valid_pairs) >= 3

        result = BatchIndicatorResult(
            name='',
            total_trades=len(valid_pairs),
            total_pnl=total_pnl,
            winning_rate=winning_rate,
            max_cumsum=max(cumsum) if cumsum else 0,
            min_cumsum=min(cumsum) if cumsum else 0,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            profit_zone=profit_zone,
            loss_zone=loss_zone,
            is_valid=is_valid,
            cumsum_data=cumsum,
            sorted_indicators=sorted_indicators,
            pnl_by_indicator=sorted_pnls,
        )

        return result

    def batch_analyze(self, symbol: str,
                   start_date: str = None,
                   output_dir: str = './backtest/reports'):
        """批量分析所有指标"""
        print(f"\n{'=' * 60}")
        print(f"批量分析指标: {symbol}")
        print(f"{'=' * 60}")

        # 生成指标列表
        indicators = self._generate_indicator_list()
        print(f"\n共 {len(indicators)} 个指标待测试")

        # 运行回测
        print(f"\n▶ 运行回测...")
        trades, all_indicator_values = self.run_backtest_with_indicators(
            symbol, indicators, start_date
        )
        print(f"  完成 {len(trades)} 笔交易")

        if not trades:
            print("❌ 没有交易")
            return []

        # 分析每个指标
        print(f"\n▶ 分析指标...")
        results = []

        # 初始化图表生成器
        if HAS_MATPLOTLIB:
            chart_generator = CumulativeChartGenerator(output_dir)

        if not trades:
            print("❌ 没有交易")
            return []

        # 分析每个指标
        print(f"\n▶ 分析指标...")
        results = []

        for i, ind in enumerate(indicators):
            if (i + 1) % 50 == 0:
                print(f"  进度: {i + 1}/{len(indicators)}")

            ind_values = all_indicator_values.get(ind, [])
            pnl_list = [t.pnl for t in trades]

            if len(ind_values) != len(pnl_list):
                continue

            result = self.analyze_single_indicator(ind_values, pnl_list)
            result.name = ind

            if result.total_trades >= 3:
                results.append(result)

                # 生成图表
                if HAS_MATPLOTLIB and chart_generator and result.is_valid:
                    print(f"  生成图表: {ind}")
                    chart_generator.generate_single_chart(
                        ind,
                        result.sorted_indicators,
                        result.pnl_by_indicator,
                        f"{symbol} - {ind}"
                    )

        # 按总盈亏排序
        results.sort(key=lambda x: x.total_pnl, reverse=True)

        # 输出结果
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
            print(f"    盈利区间: {r.profit_zone}")
            print(f"    亏损区间: {r.loss_zone}")
            count += 1

        return results

    def _generate_indicator_list(self) -> List[str]:
        """生成所有指标名称"""
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

        # MACD 简化版 - 只用 histogram
        for fast in [12]:
            for slow in [26]:
                for signal in [9]:
                    indicators.append(f'MACD_{fast}_{slow}_{signal}')

        return indicators


def quick_test():
    """快速测试"""
    analyzer = BatchIndicatorAnalyzer()

    # 加载合约
    contracts = analyzer.loader.load_main_contracts()
    print(f"已加载 {len(contracts)} 个合约")

    if contracts:
        # 取第一个合约测试
        symbol = list(contracts.keys())[0]
        exchange = contracts[symbol].get('ExchangeID', '')
        full_symbol = f"{exchange}.{contracts[symbol]['MainContractID']}"

        print(f"\n测试合约: {full_symbol}")

        # ��单��试一个指标
        test_indicators = ['RSI_close_14', 'SMA_close_20']
        trades, ind_values = analyzer.run_backtest_with_indicators(full_symbol, test_indicators)

        print(f"产生 {len(trades)} 笔交易")

        if trades:
            for ind in test_indicators:
                vals = ind_values.get(ind, [])
                valid = [v for v in vals if v is not None]
                print(f"  {ind}: {len(valid)} 个有效值")


def main():
    parser = argparse.ArgumentParser(description='批量指标分析')
    parser.add_argument('--symbol', '-s', type=str, help='合约代码，如 CU.SHF')
    parser.add_argument('--start-date', type=str, help='开始日期')
    parser.add_argument('--output', '-o', type=str, default='./backtest/reports', help='输出目录')
    args = parser.parse_args()

    analyzer = BatchIndicatorAnalyzer()

    if args.symbol:
        # 测试指定合约
        results = analyzer.batch_analyze(args.symbol, args.start_date, args.output)
    else:
        # 默认测试所有主力合约
        contracts = analyzer.loader.load_main_contracts()
        print(f"共 {len(contracts)} 个主力合约")

        all_results = {}
        for product_id, contract in contracts.items():
            exchange = contract.get('ExchangeID', '')
            symbol = f"{exchange}.{contract['MainContractID']}"

            # 跳过非期货
            if exchange not in ['SHFE', 'DCE', 'CZCE', 'CFFEX']:
                continue

            try:
                results = analyzer.batch_analyze(symbol, args.start_date, args.output)
                if results:
                    # 保存有效结果
                    valid = [r for r in results if r.is_valid]
                    if valid:
                        all_results[symbol] = valid
            except Exception as e:
                print(f"  ❌ {symbol}: {e}")

        print(f"\n{'=' * 60}")
        print("汇总:")
        print(f"{'=' * 60}")
        for symbol, results in all_results.items():
            print(f"\n{symbol}: {len(results)} 个有效指标")


if __name__ == '__main__':
    main()