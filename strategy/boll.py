# -*- coding: utf-8 -*-
"""
BOLL 布林带计算器

输入输出格式与 MACDCalculator / ATRCalculator 保持一致：
- 输入: [(datetime, open, high, low, close, volume), ...]
- 输出: [(datetime, open, high, low, close, volume, ma, upper, middle, lower), ...]

追加的列：
    6: ma   移动平均线（与 middle 相同）
    7: upper   上轨
    8: middle  中轨
    9: lower   下轨
"""

from typing import List, Tuple
import numpy as np


class BOLLCalculator:
    """布林带计算器"""

    @staticmethod
    def calculate(
        data: List[Tuple],
        period: int = 26,
        multiplier: float = 2.0,
    ) -> List[Tuple]:
        """
        计算 BOLL 布林带。

        Args:
            data: K 线元组列表，每根 K 线至少 6 列：
                  (datetime, open, high, low, close, volume)
            period: 计算周期，默认 20
            multiplier: 标准差倍数，默认 2.0

        Returns:
            追加 (ma, upper, middle, lower) 四列后的 K 线列表
        """
        if len(data) == 0:
            return []

        closes = np.array([float(bar[4]) for bar in data])

        # 当数据量不足 period 时，用全部可用数据计算，避免前面大量 NaN
        ma = np.empty(len(closes), dtype=float)
        std = np.empty(len(closes), dtype=float)

        for i in range(len(closes)):
            start = max(0, i - period + 1)
            window = closes[start : i + 1]
            ma[i] = window.mean()
            # ddof=0 与通达信/文华保持一致
            std[i] = window.std(ddof=0)

        upper = ma + multiplier * std
        lower = ma - multiplier * std
        middle = ma.copy()

        result = []
        for i, bar in enumerate(data):
            result.append(
                tuple(bar[:6]) + (ma[i], upper[i], middle[i], lower[i])
            )

        return result
