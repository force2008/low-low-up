#!/usr/bin/env python3
"""
实盘策略引擎
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from backtest.strategy_utils import Config, DataLoader
from backtest.strategy_models import Signal, Position, SignalType
from backtest.strategy_indicators import MACDCalculator, StackIdentifier, IndexMapper, ATRCalculator
from backtest.strategy_logic import Strategy


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
        self.last_signal_60m_idx: Optional[int] = None  # 防止同一60分钟拐头重复出信号
        self.precheck_signals_green: List[dict] = []
        self.precheck_signals_red: List[dict] = []

        self.last_60m_bar_time: Optional[str] = None

        # 正在合成的当前 60m bar：(datetime, open, high, low, close, volume)
        self._current_60m_bar: Optional[tuple] = None

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

        # 合成 60m bar：检查是否进入新的小时
        bar_hour = bar[0][:13]  # 'YYYY-MM-DD HH'
        new_60m_bar = None  # 新开始的 60m bar（小时变化时）
        if self._current_60m_bar is None:
            # 第一根 bar，创建新的 60m bar
            self._current_60m_bar = (bar[0], bar[1], bar[2], bar[3], bar[4], bar[5])
        elif self._current_60m_bar[0][:13] != bar_hour:
            # 进入新的小时
            prev_date = self._current_60m_bar[0][:10]
            curr_date = bar[0][:10]
            if prev_date == curr_date:
                # 同一天：追加到 df_60m
                self.df_60m.append(self._current_60m_bar)
                if len(self.df_60m) > self.config.MAX_60M_BARS:
                    self.df_60m.pop(0)
                # 重新计算 60m MACD
                self.df_60m_with_macd, self.green_stacks_60m, self.green_gaps_60m = StackIdentifier.identify(
                    MACDCalculator.calculate(self.df_60m)
                )

                # 检测绿柱堆转红柱堆（hist 从负转正）
                idx_60m = len(self.df_60m_with_macd) - 1
                hist_60m = self.df_60m_with_macd[idx_60m][8]
                hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0
                current_60m_time = self.df_60m_with_macd[idx_60m][0]
                if hist_60m > 0 and hist_60m_prev < 0:
                    diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                        self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)
                    if diver_ok:
                        existing_signal = next((s for s in self.precheck_signals_green
                                              if s['created_time'] == current_60m_time), None)
                        if not existing_signal:
                            self.precheck_signals_green.append({
                                'type': 'green',
                                'created_time': current_60m_time,
                                'expiry_time': bar[0],
                                'current_green_low': current_green_low,
                                'prev_prev_green_low': prev_prev_green_low
                            })
            # 开始新的 60m bar
            self._current_60m_bar = (bar[0], bar[1], bar[2], bar[3], bar[4], bar[5])
            new_60m_bar = self._current_60m_bar
        else:
            # 继续当前 60m bar：更新 high/low/close
            self._current_60m_bar = (
                self._current_60m_bar[0],
                self._current_60m_bar[1],
                max(self._current_60m_bar[2], bar[2]),
                min(self._current_60m_bar[3], bar[3]),
                bar[4],
                self._current_60m_bar[5] + bar[5]
            )
            # 非小时切换时，追加当前 _current_60m_bar 到 df_60m 末尾，重新计算 MACD
            # 这样可以检测小时内发生的绿柱堆转红柱堆
            if len(self.df_60m) > 0:
                df_60m_temp = self.df_60m[:-1] + [self._current_60m_bar]
                df_60m_with_macd_temp, _, _ = StackIdentifier.identify(
                    MACDCalculator.calculate(df_60m_temp)
                )
                if len(df_60m_with_macd_temp) > 0:
                    idx_60m = len(df_60m_with_macd_temp) - 1
                    hist_60m = df_60m_with_macd_temp[idx_60m][8]
                    hist_60m_prev = df_60m_with_macd_temp[idx_60m-1][8] if idx_60m > 0 else 0
                    current_60m_time = df_60m_with_macd_temp[idx_60m][0]
                    if hist_60m > 0 and hist_60m_prev < 0:
                        diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                            self.strategy.check_60m_divergence(df_60m_with_macd_temp, idx_60m)
                        if diver_ok:
                            existing_signal = next((s for s in self.precheck_signals_green
                                                  if s['created_time'] == current_60m_time), None)
                            if not existing_signal:
                                self.precheck_signals_green.append({
                                    'type': 'green',
                                    'created_time': current_60m_time,
                                    'expiry_time': bar[0],
                                    'current_green_low': current_green_low,
                                    'prev_prev_green_low': prev_prev_green_low
                                })

        current_time = bar[0]
        idx_60m = self._find_60m_index(current_time)

        # 每根 5m bar 都检查 60m 策略条件（与回测一致）
        # 用 last_60m_bar_time 确保每个 60m bar 只添加一次预检信号
        if idx_60m >= 0 and len(self.df_60m_with_macd) > 0:
            current_60m_time = self.df_60m_with_macd[idx_60m][0] if idx_60m < len(self.df_60m_with_macd) else None

            if current_60m_time != self.last_60m_bar_time:
                self.last_60m_bar_time = current_60m_time

        self._check_strategy_on_60m_complete(bar, new_60m_bar=new_60m_bar)
        self._check_5m_entry()
        self._check_stop_loss()

    def _find_60m_index(self, time_5m: str) -> int:
        # 当正在合成 60m bar 时（_current_60m_bar 已有内容），
        # 已完成的 df_60m 的最后一项就是当前 60m bar 之前的那根
        if self._current_60m_bar is not None:
            return len(self.df_60m) - 1 if self.df_60m else -1
        # 没有正在合成的 bar（初始化前），在 df_60m 中查找
        if not self.df_60m:
            return -1
        for i in range(len(self.df_60m) - 1, -1, -1):
            if self.df_60m[i][0] <= time_5m:
                return i
        return 0

    def _check_strategy_on_60m_complete(self, bar, new_60m_bar=None):
        """60 分钟 K 线完成后检查策略条件"""
        if len(self.df_60m_with_macd) < 5:
            return

        idx_60m = len(self.df_60m_with_macd) - 1
        hist_60m = self.df_60m_with_macd[idx_60m][8]
        hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0
        current_60m_time = self.df_60m_with_macd[idx_60m][0]

        # 绿柱堆内 DIF 拐头
        if hist_60m < 0:
            dif_turn, turn_reason = self.strategy.check_60m_dif_turn_in_green(
                self.df_60m_with_macd, idx_60m, self.green_stacks_60m
            )

            if dif_turn:
                diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                    self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)

                if diver_ok:
                    existing_signal = next((s for s in self.precheck_signals_green
                                          if s['created_time'] == current_60m_time), None)
                    if not existing_signal:
                        self.precheck_signals_green.append({
                            'type': 'green',
                            'created_time': current_60m_time,
                            'expiry_time': bar[0],
                            'current_green_low': current_green_low,
                            'prev_prev_green_low': prev_prev_green_low
                        })

        # 绿柱堆转红柱堆（hist 从负转正）
        # 仅在非小时切换时检查（小时切换时在 on_5m_bar 中已处理）
        # 条件：new_60m_bar is None（不是小时切换）且 hist 从负转正
        elif new_60m_bar is None and hist_60m > 0 and hist_60m_prev < 0:
            diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)

            if diver_ok:
                existing_signal = next((s for s in self.precheck_signals_green
                                      if s['created_time'] == current_60m_time), None)
                if not existing_signal:
                    self.precheck_signals_green.append({
                        'type': 'green',
                        'created_time': current_60m_time,
                        'expiry_time': bar[0],
                        'current_green_low': current_green_low,
                        'prev_prev_green_low': prev_prev_green_low
                    })

        # 红柱堆内 DIF 拐头
        elif hist_60m > 0 and hist_60m_prev > 0:
            dif_turn_red, reason = self.strategy.check_60m_dif_turn_in_red(
                self.df_60m_with_macd, idx_60m
            )
            if dif_turn_red:
                existing_signal = next((s for s in self.precheck_signals_red
                                      if s['created_time'] == current_60m_time), None)
                if not existing_signal:
                    self.precheck_signals_red.append({
                        'type': 'red',
                        'created_time': current_60m_time,
                        'expiry_time': bar[0],
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

        # 防止同一 60 分钟拐头重复出信号
        if self.last_signal_60m_idx is not None and idx_60m == self.last_signal_60m_idx:
            return

        current_time = self.df_5m_with_macd[idx_5m][0][:19]

        # 检查预检查信号队列
        all_signals = self.precheck_signals_green + self.precheck_signals_red

        if all_signals:
            current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

            def parse_time(time_str: str):
                time_str = time_str[:19]
                return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

            # 120分钟过期，避免数据稀疏导致预检信号提前失效
            self.precheck_signals_green = [
                s for s in self.precheck_signals_green
                if parse_time(s['expiry_time']) + timedelta(minutes=120) > current_dt
            ]
            self.precheck_signals_red = [
                s for s in self.precheck_signals_red
                if parse_time(s['expiry_time']) + timedelta(minutes=120) > current_dt
            ]

            for signal in (self.precheck_signals_green + self.precheck_signals_red)[:]:
                signal_type = signal.get('type', 'unknown')

                # 绿柱堆信号用 check_60m_divergence，红柱堆信号用 check_60m_bottom_rise_in_red
                if signal_type == 'green':
                    diver_ok, diver_reason, _, _ = self.strategy.check_60m_divergence(
                        self.df_60m_with_macd, idx_60m
                    )
                    signal_source = "绿柱堆内 DIF 拐头"
                else:
                    diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = self.strategy.check_60m_bottom_rise_in_red(
                        self.df_60m_with_macd, idx_60m
                    )
                    signal_source = "红柱堆内 DIF 拐头"

                if not diver_ok:
                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    continue

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
                                           f"{diver_reason} + {reason_5m}", current_time, signal_source, idx_60m)

                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    return

        # 传统逻辑检查（仅在有足够 60m 数据时）
        if len(self.df_60m_with_macd) > idx_60m and idx_60m >= 1:
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
                                                     current_time, "绿柱堆结束转红", idx_60m)

    def _create_entry_signal(self, entry_price: float, stop_loss: float, stop_reason: str,
                            reason: str, current_time: str, source: str = None, idx_60m: int = None):
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

        # 记录本次信号对应的 60 分钟 bar 索引，防止同一拐头重复出信号
        self.last_signal_60m_idx = idx_60m if idx_60m is not None else len(self.df_60m_with_macd) - 1

    def _create_exit_signal(self, price: float, reason: str, current_time: str):
        """创建平仓信号"""
        signal = Signal(
            signal_type=SignalType.EXIT_LONG,
            symbol=self.position.symbol,
            price=price,
            time=current_time,
            reason=reason
        )
        self.signals.append(signal)
        self.position = None
        self.last_entry_time = datetime.strptime(current_time[:19], '%Y-%m-%d %H:%M:%S')

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