#!/usr/bin/env python3
"""
策略逻辑

- Strategy: 包含所有 60 分钟和 5 分钟的信号判断逻辑
"""

from typing import List, Tuple, Dict, Optional
from .stack import StackIdentifier


class Strategy:
    """策略逻辑"""

    def __init__(self, symbol_info: dict = None):
        self.symbol_info = symbol_info or {}
        self.product_id = self.symbol_info.get('ProductID', '')

    def check_60m_dif_turn_in_green(self, data_with_macd: list, idx_60m: int, green_stacks: dict) -> tuple:
        """检查 60 分钟绿柱堆内 DIF 是否拐头（宽松版：有下跌+有反弹）"""
        if idx_60m < 2:
            return False, ""

        # 找到当前绿柱堆
        current_green = None
        for stack_idx, stack in green_stacks.items():
            if stack['start_idx'] <= idx_60m <= stack['end_idx']:
                current_green = stack
                break

        if not current_green:
            return False, "不在绿柱堆内"

        # 检查绿柱堆内 DIF 是否有下跌+反弹（宽松逻辑）
        if idx_60m >= 4:
            dif_4 = data_with_macd[idx_60m - 4][6]
            dif_3 = data_with_macd[idx_60m - 3][6]
            dif_2 = data_with_macd[idx_60m - 2][6]
            dif_1 = data_with_macd[idx_60m - 1][6]
            dif_0 = data_with_macd[idx_60m][6]

            # 条件1：之前有下跌（dif_3 > dif_2 或 dif_3 > dif_1）
            # 条件2：当前在反弹（dif_0 > dif_2 或 dif_0 > dif_1）
            has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
            has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

            if has_drop and has_rise:
                return True, "绿柱堆内DIF拐头"

        return False, "DIF未拐头"

    def check_60m_dif_turn_in_red(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟红柱堆内 DIF 是否拐头向下"""
        if idx_60m < 2:
            return False, ""

        # 检查最近3个DIF是否下降
        dif_values = [data_with_macd[i][6] for i in range(idx_60m - 2, idx_60m + 1)]

        if len(dif_values) < 3:
            return False, "数据不足"

        if dif_values[-1] < dif_values[-2] < dif_values[-3]:
            return True, "红柱堆内DIF拐头向下"

        return False, "DIF未拐头向下"

    def check_60m_divergence(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟底背离

        与 strategy_0328.py 的逻辑一致：
        - 如果当前绿柱低点 >= 前一个绿柱低点（未创新低或持平），则底背离确认
        - 逻辑：下跌未创新低，说明空头力量衰减
        """
        if idx_60m < 5:
            return False, "数据不足", 0, 0

        # 获取当前绿柱堆的最低点（从绿柱堆数据中获取，而非当前 bar）
        current_stack_id = None
        for stack_id, stack in StackIdentifier.identify(data_with_macd)[1].items():
            if stack['start_idx'] <= idx_60m <= stack['end_idx'] and data_with_macd[idx_60m][8] < 0:
                current_stack_id = stack_id
                break

        if current_stack_id is None:
            return False, "不在绿柱堆", 0, 0

        green_stacks = StackIdentifier.identify(data_with_macd)[1]
        current_low = green_stacks.get(current_stack_id, {}).get('lowest_low', data_with_macd[idx_60m][3])

        # 找前一个绿柱堆
        stack_ids = sorted(green_stacks.keys())
        current_idx_in_list = stack_ids.index(current_stack_id)

        if current_idx_in_list == 0:
            return False, "无前序绿柱堆", current_low, 0

        prev_stack_id = stack_ids[current_idx_in_list - 1]
        prev_low = green_stacks.get(prev_stack_id, {}).get('lowest_low', float('inf'))

        if prev_low == float('inf'):
            return False, "无前序绿柱堆", current_low, 0

        # 检查底背离：当前绿柱堆最低点 >= 前一个（未创新低或持平）= 底背离确认
        # 这是与原逻辑相反的：原逻辑要求创新低，这里要求未创新低
        if current_low >= prev_low:
            return True, f"底背离确认: 当前低={current_low:.2f}, 前低={prev_low:.2f}", current_low, prev_low

        return False, f"绿柱低点创新低 (当前:{current_low:.2f} < 前低:{prev_low:.2f})", current_low, prev_low

    def check_60m_bottom_rise_in_red(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟红柱堆内的底抬升"""
        if idx_60m < 5:
            return False, "数据不足", 0, 0

        # 获取当前红柱堆的最低点
        current_low = data_with_macd[idx_60m][3]

        # 找前一个红柱堆
        prev_red_start = None
        for i in range(idx_60m - 1, -1, -1):
            if data_with_macd[i][8] > 0:
                if prev_red_start is None:
                    prev_red_start = i
            elif prev_red_start is not None:
                break

        if prev_red_start is None or idx_60m - prev_red_start > 50:
            return False, "无前序红柱堆", 0, 0

        # 获取前一个红柱堆的最低点
        prev_low = min(data_with_macd[i][3] for i in range(prev_red_start, idx_60m))

        # 检查底抬升：当前最低点高于前低
        if current_low > prev_low:
            return True, f"底抬升: 当前低={current_low:.2f}, 前低={prev_low:.2f}", current_low, prev_low

        return False, "未底抬升", current_low, prev_low