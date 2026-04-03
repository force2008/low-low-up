#!/usr/bin/env python3
"""
策略公共模块

包含以下可复用类：
- MACDCalculator: MACD 指标计算
- ATRCalculator: ATR 指标计算
- StackIdentifier: 绿柱堆/红柱堆识别
- Strategy: 策略信号判断逻辑
- StrategySignalManager: 信号管理
"""

from .macd import MACDCalculator, ATRCalculator
from .stack import StackIdentifier
from .logic import Strategy
from .signal_manager import StrategySignalManager

__all__ = [
    'MACDCalculator',
    'ATRCalculator',
    'StackIdentifier',
    'Strategy',
    'StrategySignalManager',
]