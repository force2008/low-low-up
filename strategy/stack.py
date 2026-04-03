#!/usr/bin/env python3
"""
绿柱堆/红柱堆识别器

- StackIdentifier: 识别 MACD 红绿柱堆，计算绿柱堆低点等
"""

from typing import List, Tuple, Dict


class StackIdentifier:
    """绿柱堆/红柱堆识别"""

    @staticmethod
    def identify(data_with_macd: list) -> tuple:
        """识别绿柱堆和红柱堆

        返回: (data, green_stacks, green_gaps)
        - data: 原始数据
        - green_stacks: 绿柱堆信息，格式 {start_idx: {'start_idx': int, 'end_idx': int, 'lowest_low': float}}
        - green_gaps: 绿柱堆内的跳空缺口
        """
        if len(data_with_macd) == 0:
            return [], {}, {}

        green_stacks = {}
        green_gaps = {}
        current_stack = None
        stack_start_idx = None
        gap_count = 0

        for i in range(len(data_with_macd)):
            hist = data_with_macd[i][8]

            if hist > 0:  # 红柱
                if current_stack == 'green' and stack_start_idx is not None:
                    # 绿柱堆结束，记录
                    if stack_start_idx not in green_stacks:
                        green_stacks[stack_start_idx] = {
                            'start_idx': stack_start_idx,
                            'end_idx': i - 1,
                            'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                        }
                    current_stack = None
                    stack_start_idx = None
                current_stack = 'red'
                gap_count = 0

            elif hist < 0:  # 绿柱
                if current_stack != 'green':
                    # 新的绿柱堆开始
                    if current_stack == 'green' and stack_start_idx is not None:
                        # 记录之前的绿柱堆
                        if stack_start_idx not in green_stacks:
                            green_stacks[stack_start_idx] = {
                                'start_idx': stack_start_idx,
                                'end_idx': i - 1,
                                'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                            }

                    stack_start_idx = i
                    current_stack = 'green'
                    gap_count = 0
                else:
                    # 继续绿柱堆，检查绿柱堆内的向上跳空缺口
                    if i > 0:
                        prev_low = data_with_macd[i-1][3]
                        curr_low = data_with_macd[i][3]
                        if curr_low > prev_low:  # 向上跳空
                            gap_key = stack_start_idx
                            if gap_key not in green_gaps:
                                green_gaps[gap_key] = []
                            green_gaps[gap_key].append({
                                'idx': i,
                                'gap': curr_low - prev_low
                            })

            else:  # hist == 0
                if current_stack == 'green' and stack_start_idx is not None:
                    gap_count += 1
                    if gap_count >= 3:  # 连续 3 根零轴，可能转换
                        if stack_start_idx not in green_stacks:
                            green_stacks[stack_start_idx] = {
                                'start_idx': stack_start_idx,
                                'end_idx': i - 1,
                                'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                            }
                        current_stack = None
                        stack_start_idx = None
                        gap_count = 0

        # 处理最后一个绿柱堆
        if current_stack == 'green' and stack_start_idx is not None:
            if stack_start_idx not in green_stacks:
                green_stacks[stack_start_idx] = {
                    'start_idx': stack_start_idx,
                    'end_idx': len(data_with_macd) - 1,
                    'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, len(data_with_macd)))
                }

        return data_with_macd, green_stacks, green_gaps