#!/usr/bin/env python3
"""
MACD 计算和堆识别工具类
"""

import numpy as np
from typing import List, Tuple, Dict


# ============== MACD 计算 ==============

class MACDCalculator:
    @staticmethod
    def ema(values: List[float], span: int) -> List[float]:
        if len(values) == 0:
            return []

        multiplier = 2 / (span + 1)
        result = [values[0]]

        for i in range(1, len(values)):
            ema_val = values[i] * multiplier + result[-1] * (1 - multiplier)
            result.append(ema_val)

        return result

    @staticmethod
    def calculate(data: List[tuple]) -> List[tuple]:
        n = len(data)
        if n == 0:
            return []

        closes = [r[4] for r in data]

        ema12 = MACDCalculator.ema(closes, 12)
        ema26 = MACDCalculator.ema(closes, 26)

        dif = [ema12[i] - ema26[i] for i in range(n)]

        dea = MACDCalculator.ema(dif, 9)

        hist = [2 * (dif[i] - dea[i]) for i in range(n)]

        ma20 = []
        for i in range(n):
            if i < 2:
                ma20.append(0.0)
            else:
                ma20_val = sum(closes[i-1:i+1]) / 2
                ma20.append(ma20_val)

        return [(data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                dif[i], dea[i], hist[i], ma20[i]) for i in range(n)]


# ============== ATR 计算 ==============

class ATRCalculator:
    """
    ATR (Average True Range) 计算器
    用于衡量市场波动率
    """

    @staticmethod
    def calculate(data: List[tuple], period: int = 14) -> List[tuple]:
        """
        计算 ATR 指标

        Args:
            data: K线数据，每根K线包含 (time, open, high, low, close, volume)
            period: ATR 周期，默认14

        Returns:
            包含 ATR 值的 K线数据列表
        """
        n = len(data)

        # 检查输入数据格式
        # 如果数据只有 6 列（原始K线），需要先计算MACD
        if len(data[0]) == 6:
            data = MACDCalculator.calculate(data)
            n = len(data)

        # 如果数据不足 period 根，仍然返回带 ATR 的数据
        if n == 0:
            return data

        # 计算 True Range
        # TR = max(High - Low, |High - PrevClose|, |Low - PrevClose|)
        tr_list = []
        for i in range(n):
            high = data[i][2]
            low = data[i][3]

            if i == 0:
                # 第一根K线，没有前一根收盘价，使用 High - Low
                tr = high - low
            else:
                prev_close = data[i - 1][4]
                tr = max(
                    high - low,                    # 当日波动
                    abs(high - prev_close),        # 与前收盘价差
                    abs(low - prev_close)          # 与前收盘价差
                )
            tr_list.append(tr)

        # 计算 ATR (使用 EMA 方式)
        # ATR = EMA(TR, period)
        atr_values = ATRCalculator.ema(tr_list, period)

        # 将 ATR 添加到数据中返回
        result = []
        for i in range(n):
            row = data[i] + (atr_values[i] if i < len(atr_values) else 0.0,)
            result.append(row)

        return result

    @staticmethod
    def ema(values: List[float], span: int) -> List[float]:
        """计算 EMA"""
        if len(values) == 0:
            return []

        multiplier = 2 / (span + 1)
        result = [values[0]]

        for i in range(1, len(values)):
            ema_val = values[i] * multiplier + result[-1] * (1 - multiplier)
            result.append(ema_val)

        return result

    @staticmethod
    def get_atr_percentile(data: List[tuple], idx: int, lookback: int = 100) -> float:
        """
        获取当前 ATR 在历史 ATR 中的百分位

        Args:
            data: 包含 ATR 值的 K线数据
            idx: 当前索引
            lookback: 回看周期

        Returns:
            百分位值 (0-1)，越低表示当前波动率越低
        """
        # 找到包含 ATR 的列索引（ATR 在第11列，索引10之后）
        # 数据格式: time, open, high, low, close, volume, dif, dea, hist, ma20, atr, ...
        if len(data[0]) <= 10:
            return 0.5  # 没有 ATR 数据

        # 获取最近 lookback 根K线的 ATR 值
        start_idx = max(0, idx - lookback)
        atr_values = [data[i][10] for i in range(start_idx, idx + 1) if len(data[i]) > 10 and data[i][10] > 0]

        if len(atr_values) < 10:
            return 0.5  # 数据不足

        # 计算当前 ATR 在历史 ATR 中的百分位
        current_atr = data[idx][10]
        count_below = sum(1 for v in atr_values if v < current_atr)
        percentile = count_below / len(atr_values)

        return percentile


# ============== 堆识别 ==============

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
            # 支持带 ATR 的数据（11列）和不带 ATR 的数据（10列）
            if len(data[i]) >= 11:
                time, open, high, low, close, volume, dif, dea, hist, ma20, atr = data[i]
            else:
                time, open, high, low, close, volume, dif, dea, hist, ma20 = data[i]
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


# ============== 索引映射 ==============

class IndexMapper:
    @staticmethod
    def precompute_60m_index(df_5m: List[tuple], df_60m: List[tuple]) -> List[int]:
        if not df_5m or not df_60m:
            return []

        index_map = []
        idx_60m = 0
        n_60m = len(df_60m)

        for row_5m in df_5m:
            time_5m = row_5m[0]

            while idx_60m < n_60m - 1 and df_60m[idx_60m + 1][0] <= time_5m:
                idx_60m += 1

            index_map.append(idx_60m)

        return index_map