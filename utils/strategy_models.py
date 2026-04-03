#!/usr/bin/env python3
"""
策略信号和交易数据结构

- SignalType: 信号类型枚举
- Signal: 交易信号数据类
- Trade: 交易记录数据类
- Position: 持仓信息数据类
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class SignalType(Enum):
    """信号类型枚举"""
    ENTRY_LONG = "ENTRY_LONG"      # 做多入场
    EXIT_LONG = "EXIT_LONG"        # 平多出场


@dataclass
class Signal:
    """交易信号"""
    signal_type: SignalType
    symbol: str
    price: float
    time: str
    reason: str
    stop_loss: float = 0.0
    position_size: int = 1
    extra_data: dict = field(default_factory=dict)


@dataclass
class Trade:
    """交易记录"""
    entry_time: str
    entry_price: float
    exit_time: str = None
    exit_price: float = None
    position_size: int = 0
    pnl: float = 0
    pnl_pct: float = 0
    exit_reason: str = ""
    initial_stop: float = 0.0
    stop_update_count: int = 0
    entry_conditions: str = ""


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    direction: str  # "long" or "short"
    entry_time: str
    entry_price: float
    position_size: int
    initial_stop: float  # 初始止损价
    current_stop: float  # 当前止损价
    stop_reason: str = ""