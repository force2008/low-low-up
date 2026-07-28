#!/usr/bin/env python3
"""
SignalChecker: 策略信号检测器
"""

import json
import logging
from datetime import datetime, timedelta

from strategy import MACDCalculator, ATRCalculator, StackIdentifier
from strategy.index_map import IndexMapper
from utils.signal_manager import StrategySignalManager
from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp as Strategy


STRATEGY_SIGNAL_FILE = "strategy_signals_v2.json"
MAX_5M_BARS = 500
MAX_60M_BARS = 1000
TARGET_NOTIONAL = 200000


class SignalChecker:
    """信号检测器 - 定时检查策略信号"""

    def __init__(self, db_manager, instruments, strategy_signal_manager=None):
        self.db_manager = db_manager
        self.instruments = instruments
        self.strategy_signal_manager = strategy_signal_manager

        self.instrument_map = {}
        for inst in instruments:
            key = inst.get("MainContractID") or inst.get("InstrumentID", "")
            self.instrument_map[key] = inst

        self.precheck_signals_green = {}
        self.precheck_signals_red = {}
        self.positions = {}
        self.last_entry_times = {}
        self.cooldown_hours = 4
        self.last_60m_bar_times = {}
        self.index_map_60m = {}
        self.strategy_name = Strategy({}).name

    def check_60m_precheck(self, symbol: str, end_time: str = None):
        """检查 60 分钟预检测信号"""
        try:
            data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)
            if len(data_60m) < 20:
                return

            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            if len(data_60m_with_macd) < 5:
                return

            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            idx_60m = len(data_60m_with_macd) - 1
            current_60m_time = data_60m_with_macd[idx_60m][0]

            if end_time:
                try:
                    end_dt = datetime.strptime(end_time[:19], '%Y-%m-%d %H:%M:%S')
                    actual_dt = datetime.strptime(current_60m_time[:19], '%Y-%m-%d %H:%M:%S')
                    time_diff = (end_dt - actual_dt).total_seconds() / 60
                    if time_diff > 10:
                        return
                except:
                    pass

            last_time = self.last_60m_bar_times.get(symbol)
            if last_time == current_60m_time:
                return
            self.last_60m_bar_times[symbol] = current_60m_time

            strategy = Strategy({})
            signal_dict, signal_reason = strategy.check_60m_precheck(
                data_60m_with_macd, idx_60m, green_stacks_60m
            )

            if signal_dict:
                sig_type = signal_dict['type']
                if sig_type == 'green':
                    if symbol not in self.precheck_signals_green:
                        self.precheck_signals_green[symbol] = []
                    existing = next((s for s in self.precheck_signals_green[symbol] if s['created_time'] == current_60m_time), None)
                    if not existing:
                        self.precheck_signals_green[symbol].append({
                            'type': signal_dict['type'],
                            'sub_type': signal_dict.get('sub_type', 'dif_turn'),
                            'created_time': current_60m_time,
                        })
                        self._log(f"📊 {symbol} {signal_reason}，预检测信号")
                elif sig_type == 'red':
                    if symbol not in self.precheck_signals_red:
                        self.precheck_signals_red[symbol] = []
                    existing = next((s for s in self.precheck_signals_red[symbol] if s['created_time'] == current_60m_time), None)
                    if not existing:
                        self.precheck_signals_red[symbol].append({
                            'type': signal_dict['type'],
                            'sub_type': signal_dict.get('sub_type', 'dif_turn'),
                            'created_time': current_60m_time,
                        })
                        self._log(f"📊 {symbol} {signal_reason}，预检测信号")

        except Exception as e:
            self._log(f"✗ {symbol} 60分钟信号检查失败：{e}")

    def check_entry_signal(self, symbol: str, end_time: str = None):
        """检查入场信号"""
        try:
            position = self.positions.get(symbol)
            current_time = datetime.now()

            if not position and symbol in self.last_entry_times:
                last_entry = self.last_entry_times[symbol]
                hours_passed = (current_time - last_entry).total_seconds() / 3600
                if hours_passed < self.cooldown_hours:
                    return

            all_precheck = []
            if symbol in self.precheck_signals_green:
                all_precheck.extend(self.precheck_signals_green[symbol])
            if symbol in self.precheck_signals_red:
                all_precheck.extend(self.precheck_signals_red[symbol])

            valid_precheck = []
            for sig in all_precheck:
                try:
                    sig_time = datetime.strptime(sig['created_time'][:19], '%Y-%m-%d %H:%M:%S')
                    hours_old = (current_time - sig_time).total_seconds() / 3600
                    if hours_old < 8:
                        valid_precheck.append(sig)
                except:
                    pass

            if not valid_precheck:
                return

            data_5m = self.db_manager.get_kline_data(symbol, MAX_5M_BARS, 300, end_time)

            if not end_time:
                sig_times = [datetime.strptime(s['created_time'][:19], '%Y-%m-%d %H:%M:%S') for s in valid_precheck]
                earliest_sig = min(sig_times) - timedelta(hours=1)
                end_time_for_60m = earliest_sig.strftime('%Y-%m-%d %H:%M:%S') + '.000000'
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time_for_60m)
            else:
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)

            if len(data_5m) < 20 or len(data_60m) < 20:
                return

            data_5m_with_macd = MACDCalculator.calculate(data_5m)
            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            data_5m_with_atr = ATRCalculator.calculate(data_5m_with_macd, 14)

            if len(data_5m_with_atr) < 5 or len(data_60m_with_macd) < 5:
                return

            if symbol not in self.index_map_60m or len(self.index_map_60m.get(symbol, [])) != len(data_5m_with_macd):
                self.index_map_60m[symbol] = IndexMapper.precompute_60m_index(data_5m_with_macd, data_60m_with_macd)
            index_map = self.index_map_60m[symbol]

            _, green_stacks_5m, _ = StackIdentifier.identify(data_5m_with_macd)
            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            strategy = Strategy({})
            idx_5m = len(data_5m_with_macd) - 1
            idx_5m = min(idx_5m, len(data_5m_with_macd) - 1)
            current_5m_time = data_5m_with_macd[idx_5m][0][:19]

            idx_60m = index_map[idx_5m] if idx_5m < len(index_map) else len(data_60m_with_macd) - 1

            if position:
                self._check_stop_loss(symbol, data_5m_with_macd, position)
                return

            for sig in valid_precheck:
                sig_type = sig['type']

                if sig_type == 'green':
                    sub_type = sig.get('sub_type', 'dif_turn')

                    diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                    if not diver_ok:
                        continue

                    current_open = data_5m_with_macd[idx_5m][1]
                    current_price = data_5m_with_macd[idx_5m][4]
                    if current_price <= current_open:
                        continue

                    entry_price = current_price if sub_type == 'green_to_red' else current_open

                    stop_loss, stop_reason = strategy.get_initial_stop_loss(
                        data_5m_with_macd, idx_5m,
                        green_stacks_5m, {},
                        data_60m_with_macd, green_stacks_60m
                    )

                    if stop_loss is None or stop_loss >= entry_price:
                        continue

                    instrument_info = self.instrument_map.get(symbol, {})
                    volume_multiple = instrument_info.get('VolumeMultiple', 1)
                    contract_value = entry_price * volume_multiple

                    if contract_value > TARGET_NOTIONAL:
                        continue

                    self.positions[symbol] = {
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'entry_time': current_time
                    }
                    self.last_entry_times[symbol] = current_time

                    signal_data = {
                        'signal_type': 'ENTRY_LONG',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'position_size': 1,
                        'strategy_name': strategy.name,
                        'reason': f"5分钟绿柱堆阳柱+60分钟底背离，入场价{entry_price:.2f}，止损{stop_loss:.2f}",
                        'time': current_5m_time
                    }

                    self._log(f"📈 {symbol} 策略开仓信号：{signal_data}")

                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    self._send_signal_notification(symbol, signal_data)
                    break

                elif sig_type == 'red':
                    diver_ok, diver_reason, _, _ = strategy.check_60m_bottom_rise_in_red(data_60m_with_macd, idx_60m)
                    if not diver_ok:
                        continue

                    in_green = False
                    for stack in green_stacks_5m.values():
                        if stack['start_idx'] <= idx_5m <= stack['end_idx']:
                            in_green = True
                            break

                    if not in_green:
                        continue

                    current_open = data_5m_with_macd[idx_5m][1]
                    current_price = data_5m_with_macd[idx_5m][4]
                    if current_price <= current_open:
                        continue

                    entry_price = current_open

                    stop_loss, stop_reason = strategy.get_initial_stop_loss(
                        data_5m_with_macd, idx_5m,
                        green_stacks_5m, {},
                        data_60m_with_macd, green_stacks_60m
                    )

                    if stop_loss is None or stop_loss >= entry_price:
                        continue

                    instrument_info = self.instrument_map.get(symbol, {})
                    volume_multiple = instrument_info.get('VolumeMultiple', 1)
                    contract_value = entry_price * volume_multiple

                    if contract_value > TARGET_NOTIONAL:
                        continue

                    self.positions[symbol] = {
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'entry_time': current_time
                    }
                    self.last_entry_times[symbol] = current_time

                    signal_data = {
                        'signal_type': 'ENTRY_LONG',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'position_size': 1,
                        'strategy_name': strategy.name,
                        'reason': f"5分钟绿柱堆阳柱+60分钟红柱堆底抬升，入场价{entry_price:.2f}，止损{stop_loss:.2f}",
                        'time': current_5m_time
                    }

                    self._log(f"📈 {symbol} 策略开仓信号（红柱堆）：{signal_data}")

                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    self._send_signal_notification(symbol, signal_data)
                    break

        except Exception as e:
            self._log(f"✗ {symbol} 入场信号检查失败：{e}")

    def _check_stop_loss(self, symbol: str, data_5m_with_macd: list, position: dict):
        """检查止损"""
        try:
            idx_5m = len(data_5m_with_macd) - 1
            current_low = data_5m_with_macd[idx_5m][3]
            current_time = data_5m_with_macd[idx_5m][0][:19]

            stop_loss = position.get('stop_loss', 0)
            if stop_loss > 0 and current_low <= stop_loss:
                signal_data = {
                    'signal_type': 'EXIT_LONG',
                    'price': stop_loss,
                    'stop_loss': stop_loss,
                    'position_size': 0,
                    'strategy_name': self.strategy_name,
                    'reason': f"5分钟价格跌破止损价{stop_loss:.2f}，触发止损",
                    'time': current_time
                }

                self._log(f"📉 {symbol} 策略平仓信号（止损）：{signal_data}")

                if self.strategy_signal_manager:
                    self.strategy_signal_manager.add_signal(symbol, signal_data)

                del self.positions[symbol]

                if symbol in self.precheck_signals_green:
                    self.precheck_signals_green[symbol] = []
                if symbol in self.precheck_signals_red:
                    self.precheck_signals_red[symbol] = []

        except Exception as e:
            self._log(f"✗ {symbol} 止损检查失败：{e}")

    def run_all_checks(self, end_time: str = None):
        """运行所有信号检查"""
        for inst in self.instruments:
            symbol = inst.get("MainContractID") or inst.get("InstrumentID", "")
            if not symbol:
                continue

            exchange_id = inst.get("ExchangeID", "")
            full_symbol = f"{exchange_id}.{symbol}"

            self.check_60m_precheck(full_symbol, end_time)
            self.check_entry_signal(full_symbol, end_time)

    def _log(self, message):
        """日志输出"""
        print(message)

    def _send_signal_notification(self, symbol: str, signal_data: dict):
        """发送飞书信号通知"""
        try:
            from utils.feishu_notifier import send_feishu_strategy_signal
            send_feishu_strategy_signal(symbol, signal_data)
            self._log(f"✓ {symbol} 飞书信号已发送")
        except Exception as e:
            self._log(f"✗ {symbol} 飞书信号发送失败：{e}")


class SignalTimer:
    """信号检测定时器 - 每5分钟执行一次"""

    def __init__(self, signal_checker, interval_seconds=300):
        self.signal_checker = signal_checker
        self.interval_seconds = interval_seconds
        self.running = False
        self.next_signal_time = None
        self.logger = logging.getLogger(__name__)

    def calculate_next_wait_time(self):
        """计算到下一个5分钟整点的等待时间"""
        now = datetime.now()
        current_minute = now.minute
        next_5_minute = ((current_minute // 5) + 1) * 5
        next_hour = now.hour

        if next_5_minute >= 60:
            next_5_minute = 5
            next_hour = now.hour + 1
            if next_hour >= 24:
                next_hour = 0

        next_time = now.replace(minute=next_5_minute % 60, second=5, microsecond=0)
        if next_hour != now.hour:
            next_time = next_time.replace(hour=next_hour)

        if next_5_minute == current_minute and now.second >= 5:
            next_time = now + timedelta(minutes=5)
            next_time = next_time.replace(second=5, microsecond=0)

        wait_seconds = (next_time - now).total_seconds()
        self.next_signal_time = next_time
        return max(wait_seconds, 1)

    def run(self):
        """运行定时器"""
        self.running = True

        while self.running:
            try:
                wait_time = self.calculate_next_wait_time()
                self.logger.info(f"[信号检测] 下次检测时间：{self.next_signal_time}，等待 {wait_time:.0f} 秒")

                import time
                time.sleep(wait_time)

                if not self.running:
                    break

                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"开始执行信号检测 - {datetime.now()}")
                self.logger.info(f"{'='*50}")

                self.signal_checker.run_all_checks()

                self.logger.info(f"信号检测完成 - {datetime.now()}")

            except Exception as e:
                self.logger.error(f"信号检测出错：{e}")

    def stop(self):
        """停止定时器"""
        self.running = False