#!/usr/bin/env python3
"""
MACD 和 ATR 指标计算器

- MACDCalculator: MACD 指标计算 (DIF, DEA, HIST)
- ATRCalculator: ATR 波动率指标计算
"""

from typing import List, Tuple

# MACD 参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9


class MACDCalculator:
    """MACD 计算器"""

    @staticmethod
    def ema(values: list, span: int) -> list:
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
    def calculate(data: list) -> list:
        """计算 MACD，返回包含 MACD 数据的列表

        返回格式: [(datetime, open, high, low, close, volume, dif, dea, hist), ...]
        """
        n = len(data)
        if n == 0:
            return []

        closes = [r[4] for r in data]

        ema12 = MACDCalculator.ema(closes, MACD_FAST)
        ema26 = MACDCalculator.ema(closes, MACD_SLOW)

        dif = [ema12[i] - ema26[i] for i in range(n)]

        dea = MACDCalculator.ema(dif, MACD_SIGNAL)

        hist = [2 * (dif[i] - dea[i]) for i in range(n)]

        result = []
        for i in range(n):
            result.append((
                data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                dif[i], dea[i], hist[i]
            ))

        return result


class ATRCalculator:
    """ATR 计算器"""

    @staticmethod
    def calculate(data: list, period: int = 14) -> list:
        """计算 ATR

        返回格式: [(datetime, open, high, low, close, volume, atr), ...]
        """
        if len(data) < period + 1:
            return data

        result = []
        for i in range(len(data)):
            row = list(data[i])

            if i == 0:
                row.append(0)  # ATR 初始为 0
            else:
                high = data[i][2]
                low = data[i][3]
                prev_close = data[i-1][4]

                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )

                if i == period:
                    atr = sum([max(data[j][2] - data[j][3], abs(data[j][2] - data[j-1][4]), abs(data[j][3] - data[j-1][4])) for j in range(1, period + 1)]) / period
                elif i > period:
                    prev_atr = result[i-1][6]
                    atr = (prev_atr * (period - 1) + tr) / period
                else:
                    atr = 0

                row.append(atr)

            result.append(tuple(row))

        return result