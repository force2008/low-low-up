#!/usr/bin/env python3
"""
绿柱堆/红柱堆识别器

- StackIdentifier: 识别 MACD 红绿柱堆，计算绿柱堆低点等
"""

from typing import List, Tuple, Dict


class StackIdentifier:
    """
    MACD 红绿柱堆识别器

    注意：代码中使用的术语与标准术语对应关系：
    - hist > 0: 红柱（多头/上涨）
    - hist < 0: 绿柱（空头/下跌）
    - current_stack = 1: 红柱堆（hist > 0 的连续区域）
    - current_stack = -1: 绿柱堆（hist < 0 的连续区域）
    - green_stacks: 存储绿柱期间的 K 线数据（低点在底背离判断中使用）
    """
    @staticmethod
    def identify(data: List[tuple]) -> Tuple[List[tuple], Dict[int, dict], Dict[int, dict]]:
        n = len(data)
        if n == 0:
            return [], {}, {}

        result = []
        green_stacks = {}
        green_gaps = {}

        current_stack = 0
        stack_id = 0
        stack_high = 0.0
        stack_low = float('inf')
        stack_hist_sum = 0.0
        stack_start_idx = 0
        stack_end_idx = 0

        gap_id = 0
        gap_low = float('inf')
        gap_start_idx = -1
        in_gap = False

        last_green_complete = False
        last_green_hist_sum = 0.0
        last_green_low = float('inf')
        last_green_end_idx = -1

        for i in range(n):
            row_len = len(data[i])
            # 支持带 ATR 的数据（11列）、不带 ATR 的数据（10列）和原始 MACD 数据（9列）
            if row_len >= 11:
                time, open, high, low, close, volume, dif, dea, hist, ma20, atr = data[i]
            elif row_len >= 10:
                time, open, high, low, close, volume, dif, dea, hist, ma20 = data[i]
            elif row_len >= 9:
                time, open, high, low, close, volume, dif, dea, hist = data[i]
                ma20 = 0.0
            else:
                raise ValueError(f"数据行长度不足：{row_len}，需要至少 9 列")
            prev_hist = data[i-1][8] if i > 0 else 0

            if prev_hist < 0 and hist > 0:
                last_green_complete = True
                last_green_hist_sum = stack_hist_sum
                last_green_low = stack_low
                last_green_end_idx = i - 1

            if hist > 0:  # 红柱（多头），开始或继续红柱堆
                if current_stack != 1:
                    if current_stack == -1:
                        gap_id = stack_id
                        gap_low = low
                        gap_start_idx = i
                        in_gap = True

                    stack_id += 1
                    current_stack = 1
                    stack_high = hist
                    stack_low = hist
                    stack_hist_sum = hist
                    stack_start_idx = i
                    stack_end_idx = i
                else:
                    stack_high = max(stack_high, hist)
                    stack_low = min(stack_low, hist)
                    stack_hist_sum += abs(hist)
                    stack_end_idx = i

                if in_gap:
                    gap_low = min(gap_low, low)

            elif hist < 0:  # 绿柱（空头），开始或继续绿柱堆
                if current_stack != -1:
                    if in_gap and gap_start_idx >= 0:
                        green_gaps[gap_id] = {
                            'low': gap_low,
                            'start_idx': gap_start_idx,
                            'end_idx': i - 1
                        }
                        in_gap = False

                    stack_id += 1
                    current_stack = -1
                    stack_high = hist
                    stack_low = low
                    stack_hist_sum = abs(hist)
                    stack_start_idx = i
                    stack_end_idx = i
                    last_green_complete = False
                else:
                    stack_high = max(stack_high, hist)
                    stack_low = min(stack_low, low)
                    stack_hist_sum += abs(hist)
                    stack_end_idx = i
            else:
                current_stack = 0
                if in_gap:
                    gap_low = min(gap_low, low)

            # 绿柱期间：持续更新绿柱堆的 K 线数据（用于底背离的低点判断）
            if current_stack == -1:
                if stack_id not in green_stacks:
                    green_stacks[stack_id] = {
                        'low': float('inf'),
                        'high': float('-inf'),
                        'start_idx': stack_start_idx,
                        'end_idx': stack_end_idx,
                        'hist_sum': 0.0
                    }
                green_stacks[stack_id]['low'] = min(green_stacks[stack_id]['low'], low)
                green_stacks[stack_id]['high'] = max(green_stacks[stack_id]['high'], high)
                green_stacks[stack_id]['hist_sum'] = stack_hist_sum
                green_stacks[stack_id]['end_idx'] = stack_end_idx

            result.append((time, open, high, low, close, volume, dif, dea, hist, ma20,
                          current_stack, stack_id,
                          1 if (prev_hist < 0 and hist > 0) else 0))

        if last_green_complete:
            temp_green_id = stack_id + 1
            green_stacks[temp_green_id] = {
                'low': last_green_low,
                'high': float('-inf'),
                'start_idx': -1,
                'end_idx': last_green_end_idx,
                'hist_sum': last_green_hist_sum,
                'immediate': True
            }

        return result, green_stacks, green_gaps