#!/usr/bin/env python3
"""
Cumulative Curve Analyzer - 累积和曲线分析

实现音频中讲解的核心方法：
- 按 indicator 值排序
- 计算累积 PnL
- 找到盈利/亏损区间边界
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import math


@dataclass
class TradePnL:
    """单笔交易的 PnL 数据"""
    entry_time: str
    entry_price: float
    exit_time: str
    exit_price: float
    pnl: float
    pnl_pct: float
    indicator_at_entry: float = 0.0       # 入场时的 indicator 值
    indicator_name: str = ''             # indicator 名称
    exit_reason: str = ''
    conditions: str = ''


class CumulativeCurveAnalyzer:
    """累积和曲线分析器"""

    def __init__(self, bin_count: int = 50):
        self.bin_count = bin_count  # 分箱数量

    def compute_cumsum(self, indicator_values: List[float],
                     pnl_list: List[float]) -> List[float]:
        """计算累积和曲线

        Args:
            indicator_values: 每笔交易入场时的 indicator 值
            pnl_list: 每笔交易的 PnL

        Returns:
            累积和曲线数据
        """
        if not indicator_values or not pnl_list:
            return []

        # 按 indicator 值排序，保持配对
        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        # 计算累积和
        cumsum = []
        total = 0.0
        for _, pnl in paired:
            total += pnl
            cumsum.append(total)

        return cumsum

    def find_profitable_zones(self, indicator_values: List[float],
                          pnl_list: List[float],
                          min_bin_trades: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """找到盈利和亏损区间

        这个方法把 indicator 值分箱，计算每个箱的累计 PnL，
        然后找出：
        - 盈利区间：累计 PnL > 0
        - 亏损区间：累计 PnL < 0

        Args:
            indicator_values: indicator 值列表
            pnl_list: PnL 列表
            min_bin_trades: 每个箱最少交易数

        Returns:
            (盈利区间列表, 亏损区间列表)
        """
        if not indicator_values or len(indicator_values) < min_bin_trades * 2:
            return [], []

        # 配对并排序
        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        # 分箱
        n = len(paired)
        bin_size = max(1, n // self.bin_count)

        bins = []
        for i in range(0, n, bin_size):
            bin_data = paired[i:i + bin_size]
            if len(bin_data) < 2:
                continue

            ind_vals = [x[0] for x in bin_data]
            pnls = [x[1] for x in bin_data]

            # 该箱的累计 PnL
            bin_cumsum = sum(pnls)

            bins.append({
                'min': min(ind_vals),
                'max': max(ind_vals),
                'count': len(bin_data),
                'cumsum': bin_cumsum,
                'avg_pnl': bin_cumsum / len(bin_data) if bin_data else 0
            })

        # 找连续盈利/亏损区间
        profit_zones = []
        loss_zones = []

        current_profit_zone = None
        current_loss_zone = None

        for bin_i, bin_data in enumerate(bins):
            if bin_data['cumsum'] > 0:
                # 盈利箱
                if current_profit_zone is None:
                    current_profit_zone = [bin_data['min'], bin_data['max']]
                else:
                    current_profit_zone[1] = bin_data['max']
                if current_loss_zone is not None:
                    loss_zones.append(tuple(current_loss_zone))
                    current_loss_zone = None
            else:
                # 亏损箱
                if current_loss_zone is None:
                    current_loss_zone = [bin_data['min'], bin_data['max']]
                else:
                    current_loss_zone[1] = bin_data['max']
                if current_profit_zone is not None:
                    profit_zones.append(tuple(current_profit_zone))
                    current_profit_zone = None

        # 处理最后一个区间
        if current_profit_zone is not None:
            profit_zones.append(tuple(current_profit_zone))
        if current_loss_zone is not None:
            loss_zones.append(tuple(current_loss_zone))

        return profit_zones, loss_zones

    def find_best_boundary(self, indicator_values: List[float],
                         pnl_list: List[float]) -> Tuple[float, float, float]:
        """找到最佳边界（累积曲线的峰值点）

        Returns:
            (下界, 上界, 最大累积收益)
        """
        if not indicator_values or not pnl_list:
            return 0.0, 0.0, 0.0

        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        cumsum = []
        total = 0.0
        for _, pnl in paired:
            total += pnl
            cumsum.append(total)
        if not indicator_values or not pnl_list:
            return 0.0, 0.0, 0.0

        # 配对并排序
        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        # 计算累积和
        cumsum = []
        total = 0.0
        for _, pnl in paired:
            total += pnl
            cumsum.append(total)

        # 找最高点和最低点
        if not cumsum:
            return 0.0, 0.0, 0.0

        max_idx = cumsum.index(max(cumsum))
        min_idx = cumsum.index(min(cumsum))

        # 返回边界值
        lower_bound = paired[min_idx][0] if min_idx <= max_idx else paired[0][0]
        upper_bound = paired[max_idx][0] if max_idx >= min_idx else paired[-1][0]
        max_profit = max(cumsum) - min(cumsum)

        return lower_bound, upper_bound, max_profit

    def find_zero_crossing_bounds(self, indicator_values: List[float],
                                pnl_list: List[float]) -> Tuple[float, float, List]:
        """找到 0 线穿越点（音频中推荐的方法）

        扫描累积曲线，找到累积PNL从负变正、或从正变负的交叉点
        这些位置的indicator值 就是边界

        Returns:
            (左边界, 右边界, 交叉点列表)
        """
        if not indicator_values or not pnl_list:
            return 0.0, 0.0, []

        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        # 计算累积曲线
        cumsum = []
        total = 0.0
        for _, pnl in paired:
            total += pnl
            cumsum.append(total)

        # 找0线穿越点
        crossings = []  # (index, indicator_value, cumsum_before, cumsum_after)
        for i in range(1, len(cumsum)):
            before = cumsum[i - 1]
            after = cumsum[i]
            # 从负变正 或 从正变负
            if (before < 0 <= after) or (before > 0 >= after):
                crossings.append((i, paired[i][0], before, after))

        if not crossings or not cumsum:
            return 0.0, 0.0, []

        # 找到累积曲线的最高点
        max_idx = cumsum.index(max(cumsum))

        # 最高点左边第一个穿越点
        left_cross = None
        for idx, ind_val, _, _ in crossings:
            if idx <= max_idx:
                left_cross = ind_val

        # 最高点右边第一个穿越点
        right_cross = None
        for idx, ind_val, _, _ in reversed(crossings):
            if idx >= max_idx:
                right_cross = ind_val

        return left_cross or 0.0, right_cross or 0.0, crossings

    def generate_curve_data(self, indicator_values: List[float],
                          pnl_list: List[float]) -> Dict:
        """生成曲线数据用于可视化

        Returns:
            包含绘图所需数据的字典
        """
        if not indicator_values:
            return {}

        paired = list(zip(indicator_values, pnl_list))
        paired.sort(key=lambda x: x[0])

        indicator_sorted = [x[0] for x in paired]
        pnl_sorted = [x[1] for x in paired]

        cumsum = []
        total = 0.0
        for pnl in pnl_sorted:
            total += pnl
            cumsum.append(total)

        # 找边界
        lower, upper, max_profit = self.find_best_boundary(indicator_values, pnl_list)

        # 统计分析
        total_pnl = sum(pnl_list)
        winning = sum(1 for p in pnl_list if p > 0)
        winning_rate = winning / len(pnl_list) * 100 if pnl_list else 0

        return {
            'indicator_sorted': indicator_sorted,
            'pnl_sorted': pnl_sorted,
            'cumsum': cumsum,
            'lower_bound': lower,
            'upper_bound': upper,
            'max_profit': max_profit,
            'total_pnl': total_pnl,
            'winning_rate': winning_rate,
            'total_trades': len(pnl_list),
            'avg_pnl': total_pnl / len(pnl_list) if pnl_list else 0
        }


def quick_test():
    """快速测试"""
    # 模拟���据: RSI 值和对应 PnL
    indicator_values = [
        20, 25, 28, 30, 32, 35, 37, 38, 40, 42,
        45, 48, 50, 52, 55, 58, 60, 62, 65, 70
    ]

    # 模拟: 低 RSI 时赚钱，高 RSI 时亏钱
    pnl_list = [
        100, 80, 60, 40, 20, 10, -10, -30, -50, -80,
        -100, -120, -80, -60, -40, -30, -20, -10, -5, -2
    ]

    analyzer = CumulativeCurveAnalyzer()

    print("=== 模拟 RSI 数据 ===")
    print(f"Indicator 范围: {min(indicator_values)} ~ {max(indicator_values)}")
    print(f"Total PnL: {sum(pnl_list)}")

    # 计算累积曲线
    cumsum = analyzer.compute_cumsum(indicator_values, pnl_list)
    print(f"\n累积曲线: {[round(x, 2) for x in cumsum]}")

    # 找边界
    lower, upper, max_p = analyzer.find_best_boundary(indicator_values, pnl_list)
    print(f"\n最佳边界:")
    print(f"  下界: {lower}")
    print(f"  上界: {upper}")
    print(f"  最大收益: {max_p}")

    # 找区间
    profit_zones, loss_zones = analyzer.find_profitable_zones(indicator_values, pnl_list)
    print(f"\n盈利区间: {profit_zones}")
    print(f"亏损区间: {loss_zones}")


if __name__ == '__main__':
    quick_test()