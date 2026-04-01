#!/usr/bin/env python3
"""
实盘策略引擎
"""

import sys
import os
from typing import Dict, List, Optional

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from strategy_utils import Config, DataLoader
from strategy_models import Signal, Position, SignalType
from strategy_indicators import MACDCalculator, StackIdentifier, IndexMapper, ATRCalculator
from strategy_logic import Strategy


# ============== 实盘策略引擎 ==============

class LiveStrategyEngine:
    """实盘策略引擎"""

    def __init__(self, symbol: str, config: Config = None):
        self.symbol = symbol
        self.config = config if config else Config()

        self.data_loader = DataLoader(self.config.DB_PATH, self.config.CONTRACTS_PATH)
        self.strategy = Strategy(self.data_loader.get_symbol_info(symbol))

        self.df_5m: List[tuple] = []
        self.df_5m_with_macd: List[tuple] = []
        self.df_60m: List[tuple] = []
        self.df_60m_with_macd: List[tuple] = []

        self.green_stacks_5m: Dict[int, dict] = {}
        self.green_gaps_5m: Dict[int, dict] = {}
        self.green_stacks_60m: Dict[int, dict] = {}
        self.green_gaps_60m: Dict[int, dict] = {}

        self.position: Optional[Position] = None
        self.signals: List[Signal] = []

        self.last_entry_time = None
        self.precheck_signals_green: List[dict] = []
        self.precheck_signals_red: List[dict] = []

        self.last_60m_bar_time: Optional[str] = None

    def initialize(self):
        """初始化，加载历史数据"""
        self.df_5m = self.data_loader.load_kline_fast(
            self.symbol,
            self.config.DURATION_5M,
            self.config.MAX_5M_BARS
        )

        self.df_60m = self.data_loader.load_kline_fast(
            self.symbol,
            self.config.DURATION_60M,
            self.config.MAX_60M_BARS
        )

        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )

        # 计算 ATR（添加到第11列，索引10）
        self.df_5m_with_atr = ATRCalculator.calculate(self.df_5m, period=14)

        self.df_60m_with_macd, self.green_stacks_60m, self.green_gaps_60m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_60m)
        )

        if self.df_60m:
            self.last_60m_bar_time = self.df_60m[-1][0]

    def on_5m_bar(self, bar: tuple):
        """处理新的 5 分钟 K 线（实盘入口）"""
        self.df_5m.append(bar)
        if len(self.df_5m) > self.config.MAX_5M_BARS:
            self.df_5m.pop(0)

        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )

        # 更新 ATR 数据
        self.df_5m_with_atr = ATRCalculator.calculate(self.df_5m, period=14)

        current_time = bar[0]
        idx_60m = self._find_60m_index(current_time)

        if idx_60m >= 0 and len(self.df_60m_with_macd) > 0:
            current_60m_time = self.df_60m_with_macd[idx_60m][0] if idx_60m < len(self.df_60m_with_macd) else None

            if current_60m_time != self.last_60m_bar_time:
                self.last_60m_bar_time = current_60m_time
                self._check_strategy_on_60m_complete()

        self._check_5m_entry()
        self._check_stop_loss()

    def _find_60m_index(self, time_5m: str) -> int:
        if not self.df_60m:
            return -1

        for i in range(len(self.df_60m) - 1, -1, -1):
            if self.df_60m[i][0] <= time_5m:
                return i

        return 0

    def _check_strategy_on_60m_complete(self):
        """60 分钟 K 线完成后检查策略条件"""
        if len(self.df_60m_with_macd) < 5:
            return

        idx_60m = len(self.df_60m_with_macd) - 1
        hist_60m = self.df_60m_with_macd[idx_60m][8]

        if hist_60m < 0:
            dif_turn, _ = self.strategy.check_60m_dif_turn_in_green(
                self.df_60m_with_macd, idx_60m
            )

            if dif_turn:
                diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                    self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)

                if diver_ok:
                    current_time = self.df_60m_with_macd[idx_60m][0]

                    existing_signal = next((s for s in self.precheck_signals_green
                                          if s['created_time'] == current_time), None)
                    if not existing_signal:
                        self.precheck_signals_green.append({
                            'type': 'green',
                            'created_time': current_time,
                            'expiry_time': current_time,
                            'current_green_low': current_green_low,
                            'prev_prev_green_low': prev_prev_green_low
                        })

        elif hist_60m > 0:
            dif_turn_red, reason = self.strategy.check_60m_dif_turn_in_red(
                self.df_60m_with_macd, idx_60m
            )

            if dif_turn_red:
                current_time = self.df_60m_with_macd[idx_60m][0]

                existing_signal = next((s for s in self.precheck_signals_red
                                      if s['created_time'] == current_time), None)
                if not existing_signal:
                    self.precheck_signals_red.append({
                        'type': 'red',
                        'created_time': current_time,
                        'expiry_time': current_time,
                        'current_green_low': None,
                        'prev_prev_green_low': None
                    })

    def _check_5m_entry(self):
        """检查 5 分钟入场条件"""
        if self.position is not None:
            return

        if len(self.df_5m_with_macd) < 5:
            return

        if self.last_entry_time:
            current_dt = datetime.strptime(self.df_5m_with_macd[-1][0][:19], '%Y-%m-%d %H:%M:%S')
            hours_passed = (current_dt - self.last_entry_time).total_seconds() / 3600
            if hours_passed < self.config.COOLDOWN_HOURS:
                return

        idx_5m = len(self.df_5m_with_macd) - 1
        idx_60m = len(self.df_60m_with_macd) - 1
        current_time = self.df_5m_with_macd[idx_5m][0][:19]

        # 检查预检查信号队列
        all_signals = self.precheck_signals_green + self.precheck_signals_red

        if all_signals:
            current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

            def parse_time(time_str: str):
                time_str = time_str[:19]
                return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

            self.precheck_signals_green = [
                s for s in self.precheck_signals_green
                if parse_time(s['expiry_time']) + timedelta(minutes=30) > current_dt
            ]
            self.precheck_signals_red = [
                s for s in self.precheck_signals_red
                if parse_time(s['expiry_time']) + timedelta(minutes=30) > current_dt
            ]

            for signal in (self.precheck_signals_green + self.precheck_signals_red)[:]:
                signal_type = signal.get('type', 'unknown')

                diver_ok, diver_reason, _, _ = self.strategy.check_60m_divergence(
                    self.df_60m_with_macd, idx_60m
                )
                if not diver_ok:
                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    continue

                if signal_type == 'green':
                    signal_source = "绿柱堆内 DIF 拐头"
                else:
                    signal_source = "红柱堆内 DIF 拐头"

                # 检查5分钟是否为阳柱（红K线）
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )

                if cond_5m:
                    initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                        self.df_5m_with_atr, idx_5m, self.green_stacks_5m, self.green_gaps_5m,
                        self.df_60m_with_macd, self.green_stacks_60m
                    )

                    if initial_stop_loss is None:
                        continue

                    entry_price = self.df_5m_with_macd[idx_5m][4]
                    self._create_entry_signal(entry_price, initial_stop_loss, stop_reason,
                                           f"{diver_reason} + {reason_5m}", current_time, signal_source)

                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    return

        # 传统逻辑检查
        hist_60m = self.df_60m_with_macd[idx_60m][8]
        hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0

        if hist_60m > 0 and hist_60m_prev < 0:
            diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
                self.df_60m_with_macd, idx_60m
            )

            if diver_ok:
                # 5分钟阳柱确认即可入场
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )

                if cond_5m:
                        initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                            self.df_5m_with_macd, idx_5m, self.green_stacks_5m, self.green_gaps_5m
                        )

                        if initial_stop_loss is None:
                            initial_stop_loss = prev_prev_low
                            stop_reason = f"60m 底背离低点:{initial_stop_loss:.2f}"

                        entry_price = self.df_5m_with_macd[idx_5m][4]
                        self._create_entry_signal(entry_price, initial_stop_loss, stop_reason,
                                                 f"{diver_reason} + {reason_5m}",
                                                 current_time, "绿柱堆结束转红")

    def _create_entry_signal(self, entry_price: float, stop_loss: float, stop_reason: str,
                            reason: str, current_time: str, source: str = None):
        """创建入场信号"""
        symbol_info = self.data_loader.get_symbol_info(self.symbol)
        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1

        contract_value = entry_price * volume_multiple
        if contract_value > self.config.TARGET_NOTIONAL:
            return

        position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))

        signal = Signal(
            signal_type=SignalType.ENTRY_LONG,
            symbol=self.symbol,
            price=entry_price,
            time=current_time,
            reason=reason,
            stop_loss=stop_loss,
            position_size=position_size,
            extra_data={'stop_reason': stop_reason, 'source': source}
        )

        self.signals.append(signal)
        self.position = Position(
            symbol=self.symbol,
            direction="long",
            entry_time=current_time,
            entry_price=entry_price,
            position_size=position_size,
            initial_stop=stop_loss,
            current_stop=stop_loss,
            stop_reason=stop_reason
        )
        self.last_entry_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

    def _check_stop_loss(self):
        """检查止损"""
        if self.position is None:
            return

        current_bar = self.df_5m_with_macd[-1]
        current_low = current_bar[2]
        current_time = current_bar[0]

        if current_low <= self.position.current_stop:
            signal = Signal(
                signal_type=SignalType.EXIT_LONG,
                symbol=self.position.symbol,
                price=self.position.current_stop,
                time=current_time,
                reason=f"止损触发 ({self.position.stop_reason})"
            )

            self.signals.append(signal)
            self.position = None
            self.last_entry_time = datetime.strptime(current_time[:19], '%Y-%m-%d %H:%M:%S')
            return

        mobile_stop, stop_reason = self.strategy.get_mobile_stop(
            self.df_5m_with_macd, len(self.df_5m_with_macd) - 1,
            self.green_stacks_5m, self.green_gaps_5m
        )

        if mobile_stop and mobile_stop > self.position.current_stop:
            self.position.current_stop = mobile_stop
            self.position.stop_reason = stop_reason

    def get_signals(self, clear: bool = False) -> List[Signal]:
        signals = self.signals.copy()
        if clear:
            self.signals.clear()
        return signals

    def get_position(self) -> Optional[Position]:
        return self.position