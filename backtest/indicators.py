#!/usr/bin/env python3
"""
Indicator Definitions - 指标定义

支持各种 indicator 的计算，包括：
- RSI: Relative Strength Index
- MACD: Moving Average Convergence Divergence
- BOLL: Bollinger Bands
- SMA: Simple Moving Average
- EMA: Exponential Moving Average
- ATR: Average True Range
- CCI: Commodity Channel Index

每个 indicator 支持不同参数和价格类型组合
"""

from typing import List, Dict, Tuple, Optional, Callable
from dataclasses import dataclass
import math


# 价格类型映射
PRICE_OPEN = 0
PRICE_HIGH = 1
PRICE_LOW = 2
PRICE_CLOSE = 3
PRICE_VOLUME = 4


def get_price(values: List, price_type: str) -> List[float]:
    """提取指定价格类型的数据"""
    if price_type == 'open':
        return [row[PRICE_OPEN] for row in values]
    elif price_type == 'high':
        return [row[PRICE_HIGH] for row in values]
    elif price_type == 'low':
        return [row[PRICE_LOW] for row in values]
    elif price_type == 'close':
        return [row[PRICE_CLOSE] for row in values]
    else:
        return [row[PRICE_CLOSE] for row in values]


class RSI:
    """RSI 指标"""

    @staticmethod
    def calculate(data: List, period: int = 14, price_type: str = 'close') -> List[Tuple]:
        """计算 RSI

        Args:
            data: K线数据 [(time, open, high, low, close, volume, ...), ...]
            period: RSI 周期
            price_type: 价格类型 ('open', 'high', 'low', 'close')

        Returns:
            [(time, open, high, low, close, volume, rsi), ...]
        """
        prices = get_price(data, price_type)

        if len(prices) < period + 1:
            return data

        gains = []
        losses = []

        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]
            gains.append(max(diff, 0.0))
            losses.append(max(-diff, 0.0))

        result = []

        # 初始化
        for i in range(len(data)):
            row = list(data[i])
            row.append(None)  # RSI 初始为 None
            result.append(tuple(row))

        # 第一个有效值
        avg_gain = sum(gains[1:period + 1]) / period
        avg_loss = sum(losses[1:period + 1]) / period

        if avg_loss == 0:
            result[period] = (result[period][:-1] + (100.0,))
        else:
            rs = avg_gain / avg_loss
            rsi_val = 100.0 - 100.0 / (1.0 + rs)
            result[period] = (result[period][:-1] + (rsi_val,))

        # 后续值(注意:gains/losses 索引从 0 开始，对应 prices 索引 i)
        for i in range(period + 1, len(prices)):
            gain_idx = i - 1  # gains 索引
            loss_idx = i - 1
            avg_gain = (avg_gain * (period - 1) + gains[gain_idx]) / period
            avg_loss = (avg_loss * (period - 1) + losses[loss_idx]) / period

            if avg_loss == 0:
                rsi_val = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_val = 100.0 - 100.0 / (1.0 + rs)

            result[i] = (result[i][:-1] + (rsi_val,))

        return result

    @staticmethod
    def calculate_simple(values: List[float], period: int = 14) -> List[Optional[float]]:
        """简化的 RSI 计算（只返回值列表）"""
        if len(values) < period + 1:
            return [None] * len(values)

        gains = [max(values[i] - values[i - 1], 0.0) for i in range(1, len(values))]
        losses = [max(values[i - 1] - values[i], 0.0) for i in range(1, len(values))]

        result = [None] * len(values)

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        if avg_loss == 0:
            result[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[period] = 100.0 - 100.0 / (1.0 + rs)

        for i in range(period, len(values)):
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period

            if avg_loss == 0:
                result[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i] = 100.0 - 100.0 / (1.0 + rs)

        return result


class MACD:
    """MACD 指标"""

    @staticmethod
    def calculate(data: List, fast: int = 12, slow: int = 26, signal: int = 9) -> List[Tuple]:
        """计算 MACD

        Args:
            data: K线数据
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期

        Returns:
            [(time, open, high, low, close, volume, dif, dea, hist), ...]
        """
        closes = [row[PRICE_CLOSE] for row in data]

        if len(closes) < slow:
            return data

        # 计算 EMA
        ema_fast = EMA.calculate_simple(closes, fast)
        ema_slow = EMA.calculate_simple(closes, slow)

        # DIF = EmaFast - EmaSlow
        dif = [ema_fast[i] - ema_slow[i] if ema_fast[i] and ema_slow[i] else None
               for i in range(len(closes))]

        # DEA = EMA(DIF, signal)
        dif_values = [d if d is not None else 0.0 for d in dif]
        dea = EMA.calculate_simple(dif_values, signal)

        result = []
        for i in range(len(data)):
            row = list(data[i])

            if dif[i] is not None and dea[i] is not None:
                hist = 2 * (dif[i] - dea[i])
            else:
                hist = None

            row.extend([dif[i], dea[i], hist])
            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(values: List[float], fast: int = 12,
                       slow: int = 26, signal: int = 9) -> Dict[str, List[Optional[float]]]:
        """简化 MACD 计算"""
        if len(values) < slow:
            return {'dif': [], 'dea': [], 'hist': []}

        ema_fast = EMA.calculate_simple(values, fast)
        ema_slow = EMA.calculate_simple(values, slow)

        dif = [ema_fast[i] - ema_slow[i] if ema_fast[i] and ema_slow[i] else None
              for i in range(len(values))]

        dif_values = [d if d is not None else 0.0 for d in dif]
        dea = EMA.calculate_simple(dif_values, signal)

        hist = [2 * (dif[i] - dea[i]) if dif[i] is not None and dea[i] is not None else None
               for i in range(len(values))]

        return {'dif': dif, 'dea': dea, 'hist': hist}


class BOLL:
    """Bollinger Bands 指标"""

    @staticmethod
    def calculate(data: List, period: int = 20, std_multiplier: float = 2.0) -> List[Tuple]:
        """计算布林带

        Returns:
            [(time, open, high, low, close, volume, middle, upper, lower), ...]
        """
        closes = [row[PRICE_CLOSE] for row in data]

        if len(closes) < period:
            return data

        result = []

        for i in range(len(data)):
            row = list(data[i])

            if i < period - 1:
                row.extend([None, None, None])
            else:
                # 计算均线和标准差
                window = closes[i - period + 1:i + 1]
                middle = sum(window) / period

                variance = sum((x - middle) ** 2 for x in window) / period
                std = math.sqrt(variance)

                upper = middle + std_multiplier * std
                lower = middle - std_multiplier * std

                row.extend([middle, upper, lower])

            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(values: List[float], period: int = 20,
                      std_multiplier: float = 2.0) -> Dict[str, List[Optional[float]]]:
        """简化布林带计算"""
        if len(values) < period:
            return {'middle': [], 'upper': [], 'lower': []}

        middle = []
        upper = []
        lower = []

        for i in range(len(values)):
            if i < period - 1:
                middle.append(None)
                upper.append(None)
                lower.append(None)
            else:
                window = values[i - period + 1:i + 1]
                m = sum(window) / period

                variance = sum((x - m) ** 2 for x in window) / period
                std = math.sqrt(variance)

                middle.append(m)
                upper.append(m + std_multiplier * std)
                lower.append(m - std_multiplier * std)

        return {'middle': middle, 'upper': upper, 'lower': lower}


class SMA:
    """Simple Moving Average"""

    @staticmethod
    def calculate(data: List, period: int = 20, price_type: str = 'close') -> List[Tuple]:
        """计算 SMA"""
        prices = get_price(data, price_type)

        result = []
        for i in range(len(data)):
            row = list(data[i])

            if i < period - 1:
                row.append(None)
            else:
                sma_val = sum(prices[i - period + 1:i + 1]) / period
                row.append(sma_val)

            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(values: List[float], period: int = 20) -> List[Optional[float]]:
        """简化 SMA 计算"""
        result = []

        for i in range(len(values)):
            if i < period - 1:
                result.append(None)
            else:
                sma_val = sum(values[i - period + 1:i + 1]) / period
                result.append(sma_val)

        return result


class EMA:
    """Exponential Moving Average"""

    @staticmethod
    def calculate(data: List, period: int = 20, price_type: str = 'close') -> List[Tuple]:
        """计算 EMA"""
        prices = get_price(data, price_type)

        if len(prices) == 0:
            return data

        alpha = 2 / (period + 1)

        result = []
        for i in range(len(data)):
            row = list(data[i])

            if i == 0:
                row.append(prices[0])
            else:
                ema_val = prices[i] * alpha + result[i - 1][6] * (1 - alpha)
                row.append(ema_val)

            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(values: List[float], period: int = 20) -> List[Optional[float]]:
        """简化 EMA 计算"""
        if not values:
            return []

        result = [None] * len(values)
        alpha = 2 / (period + 1)

        result[0] = values[0]

        for i in range(1, len(values)):
            result[i] = values[i] * alpha + result[i - 1] * (1 - alpha)

        return result


class ATR:
    """Average True Range"""

    @staticmethod
    def calculate(data: List, period: int = 14) -> List[Tuple]:
        """计算 ATR"""
        if len(data) < period + 1:
            return data

        tr_list = []

        # 计算 True Range
        for i in range(len(data)):
            if i == 0:
                tr = data[i][PRICE_HIGH] - data[i][PRICE_LOW]
            else:
                high_low = data[i][PRICE_HIGH] - data[i][PRICE_LOW]
                high_prev = abs(data[i][PRICE_HIGH] - data[i - 1][PRICE_CLOSE])
                low_prev = abs(data[i][PRICE_LOW] - data[i - 1][PRICE_CLOSE])
                tr = max(high_low, high_prev, low_prev)

            tr_list.append(tr)

        # 计算 ATR
        result = []
        for i in range(len(data)):
            row = list(data[i])

            if i < period:
                row.append(0)  # 初始为 0
            elif i == period:
                atr = sum(tr_list[1:period + 1]) / period
                row.append(atr)
            else:
                prev_atr = result[i - 1][6]
                atr = (prev_atr * (period - 1) + tr_list[i]) / period
                row.append(atr)

            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(data: List, period: int = 14) -> List[Optional[float]]:
        """简化 ATR 计算"""
        if len(data) < period + 1:
            return [None] * len(data)

        tr_list = []

        for i in range(len(data)):
            if i == 0:
                tr = data[i][PRICE_HIGH] - data[i][PRICE_LOW]
            else:
                high_low = data[i][PRICE_HIGH] - data[i][PRICE_LOW]
                high_prev = abs(data[i][PRICE_HIGH] - data[i - 1][PRICE_CLOSE])
                low_prev = abs(data[i][PRICE_LOW] - data[i - 1][PRICE_CLOSE])
                tr = max(high_low, high_prev, low_prev)

            tr_list.append(tr)

        result = [None] * len(data)

        if len(tr_list) > period:
            result[period] = sum(tr_list[1:period + 1]) / period

            for i in range(period + 1, len(tr_list)):
                result[i] = (result[i - 1] * (period - 1) + tr_list[i]) / period

        return result


class CCI:
    """Commodity Channel Index"""

    @staticmethod
    def calculate(data: List, period: int = 20) -> List[Tuple]:
        """计算 CCI"""
        if len(data) < period:
            return data

        result = []

        for i in range(len(data)):
            row = list(data[i])

            if i < period - 1:
                row.append(None)
            else:
                # Typical Price = (High + Low + Close) / 3
                window = data[i - period + 1:i + 1]
                tp = [(row[PRICE_HIGH] + row[PRICE_LOW] + row[PRICE_CLOSE]) / 3 for row in window]

                sma_tp = sum(tp) / period
                md = sum(abs(tp_j - sma_tp) for tp_j in tp) / period

                if md == 0:
                    cci = 0
                else:
                    current_tp = (data[i][PRICE_HIGH] + data[i][PRICE_LOW] + data[i][PRICE_CLOSE]) / 3
                    cci = (current_tp - sma_tp) / (0.015 * md)

                row.append(cci)

            result.append(tuple(row))

        return result

    @staticmethod
    def calculate_simple(data: List, period: int = 20) -> List[Optional[float]]:
        """简化 CCI 计算"""
        if len(data) < period:
            return [None] * len(data)

        result = [None] * len(data)

        for i in range(period - 1, len(data)):
            window = data[i - period + 1:i + 1]
            tp = [(row[PRICE_HIGH] + row[PRICE_LOW] + row[PRICE_CLOSE]) / 3 for row in window]

            sma_tp = sum(tp) / period
            md = sum(abs(tp_j - sma_tp) for tp_j in tp) / period

            if md == 0:
                result[i] = 0
            else:
                current_tp = (data[i][PRICE_HIGH] + data[i][PRICE_LOW] + data[i][PRICE_CLOSE]) / 3
                result[i] = (current_tp - sma_tp) / (0.015 * md)

        return result


class IndicatorRegistry:
    """Indicator 注册表 - 用于批量枚举"""

    def __init__(self):
        self._indicators: Dict[str, Callable] = {}

    def register(self, name: str, func: Callable):
        """注册一个 indicator"""
        self._indicators[name] = func

    def get(self, name: str) -> Callable:
        """获取 indicator 函数"""
        return self._indicators.get(name)

    def list_indicators(self) -> List[str]:
        """列出所有已注册的 indicators"""
        return list(self._indicators.keys())


def generate_indicator_name(base: str, **params) -> str:
    """生成标准化的 indicator 名称"""
    parts = [base]
    for k, v in sorted(params.items()):
        parts.append(str(v))
    return '_'.join(parts)


def quick_test():
    """快速测试"""
    # 创建测试数据
    import random
    random.seed(42)

    data = []
    base_price = 100
    for i in range(100):
        close = base_price + random.uniform(-2, 2)
        data.append((
            f'2024-01-{i:02d} 09:00:00',
            close - 0.5,  # open
            close + 1,    # high
            close - 1,    # low
            close,        # close
            1000          # volume
        ))
        base_price = close

    print("=== RSI 测试 ===")
    rsi_data = RSI.calculate(data, period=14, price_type='close')
    print(f"RSI 最后5个值: {[round(r[-1], 2) if r[-1] else None for r in rsi_data[-5:]]}")

    print("\n=== MACD 测试 ===")
    macd_data = MACD.calculate(data, fast=12, slow=26, signal=9)
    print(f"MACD 最后5个值 (hist): {[round(r[-1], 2) if r[-1] else None for r in macd_data[-5:]]}")

    print("\n=== BOLL 测试 ===")
    boll_data = BOLL.calculate(data, period=20, std_multiplier=2.0)
    print(f"BOLL 最后5个值 (upper): {[round(r[-1], 2) if r[-1] else None for r in boll_data[-5:]]}")

    print("\n=== SMA 测试 ===")
    sma_data = SMA.calculate(data, period=20, price_type='close')
    print(f"SMA 最后5个值: {[round(r[-1], 2) if r[-1] else None for r in sma_data[-5:]]}")

    print("\n=== ATR 测试 ===")
    atr_data = ATR.calculate(data, period=14)
    print(f"ATR 最后5个值: {[round(r[-1], 2) for r in atr_data[-5:]]}")

    print("\n=== CCI 测试 ===")
    cci_data = CCI.calculate(data, period=20)
    print(f"CCI 最后5个值: {[round(r[-1], 2) if r[-1] else None for r in cci_data[-5:]]}")


if __name__ == '__main__':
    quick_test()