#!/usr/bin/env python3
"""
SignalTimer: 定时检测策略信号（每5分钟执行一次）
"""

import json
import sys
import os
import atexit
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from ctp.base_mdapi import CMdSpiBase, mdapi

# 从拆分后的模块导入（公共类）
from strategy import MACDCalculator, ATRCalculator, StackIdentifier
from strategy.index_map import IndexMapper
from utils.database_manager import DatabaseManager
from utils.signal_manager import StrategySignalManager

# 导入策略
from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp as Strategy


# 配置日志
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, datetime.now().strftime("SignalTimer_%Y%m%d_%H%M%S.log"))

# 配置日志处理器
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

# 配置日志格式
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 配置日志器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# 策略信号文件
STRATEGY_SIGNAL_FILE = "strategy_signals_v2.json"

# K 线数据量配置
MAX_5M_BARS = 500
MAX_60M_BARS = 1000


def check_log_size():
    """检查日志文件大小"""
    global log_filename, file_handler

    try:
        if os.path.exists(log_filename):
            file_size = os.path.getsize(log_filename)
            if file_size > 100 * 1024 * 1024:
                new_log_filename = os.path.join(log_dir, datetime.now().strftime("SignalTimer_%Y%m%d_%H%M%S.log"))
                logger.info(f"日志文件大小 {file_size/1024/1024:.2f}MB 超过限制，切换到新日志文件")

                logger.removeHandler(file_handler)
                file_handler.close()

                log_filename = new_log_filename
                new_file_handler = logging.FileHandler(log_filename, encoding='utf-8')
                new_file_handler.setLevel(logging.INFO)
                new_file_handler.setFormatter(formatter)

                file_handler = new_file_handler
                logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f"检查日志文件大小失败：{e}")


def print_log(*args, **kwargs):
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# 信号检测器类
class SignalChecker:
    """信号检测器 - 定时检查策略信号"""

    def __init__(self, db_manager, instruments, strategy_signal_manager=None):
        self.db_manager = db_manager
        self.instruments = instruments

        # 合约映射
        self.instrument_map = {}
        for inst in instruments:
            key = inst.get("MainContractID") or inst.get("InstrumentID", "")
            self.instrument_map[key] = inst

        # 策略信号管理器
        self.strategy_signal_manager = strategy_signal_manager

        # 预检测信号队列
        self.precheck_signals_green = {}
        self.precheck_signals_red = {}

        # 持仓状态
        self.positions = {}

        # 上次入场时间
        self.last_entry_times = {}

        # 信号冷却时间
        self.cooldown_hours = 4

        # 回放模式：当前回放时间（用于替代 datetime.now()）
        self.replay_time = None

        # 记录上次处理的 60m bar 时间
        self.last_60m_bar_times = {}

        # 60分钟索引映射
        self.index_map_60m = {}

        # 策略名称
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

            # 检查是否已处理
            last_time = self.last_60m_bar_times.get(symbol)
            if last_time == current_60m_time:
                return
            self.last_60m_bar_times[symbol] = current_60m_time

            # 使用策略方法检查
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
                        print_log(f"📊 {symbol} {signal_reason}，预检测信号")
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
                        print_log(f"📊 {symbol} {signal_reason}，预检测信号")

        except Exception as e:
            print_log(f"✗ {symbol} 60分钟信号检查失败：{e}")

    def check_entry_signal(self, symbol: str, end_time: str = None):
        """检查入场信号"""
        try:
            position = self.positions.get(symbol)
            # 回放模式使用回放时间，否则使用当前时间
            current_time = self.replay_time if self.replay_time else datetime.now()

            # 冷却时间检查
            if not position and symbol in self.last_entry_times:
                last_entry = self.last_entry_times[symbol]
                hours_passed = (current_time - last_entry).total_seconds() / 3600
                if hours_passed < self.cooldown_hours:
                    return

            # 获取预检测信号
            all_precheck = []
            if symbol in self.precheck_signals_green:
                all_precheck.extend(self.precheck_signals_green[symbol])
            if symbol in self.precheck_signals_red:
                all_precheck.extend(self.precheck_signals_red[symbol])

            # 过滤过期信号
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
                print_log(f"  [{symbol}] 无有效预检测信号")
                return

            # 读取数据（回放模式：不过滤时间，获取完整数据）
            data_5m = self.db_manager.get_kline_data(symbol, MAX_5M_BARS, 300, end_time)

            if not end_time:
                sig_times = [datetime.strptime(s['created_time'][:19], '%Y-%m-%d %H:%M:%S')
                           for s in valid_precheck]
                earliest_sig = min(sig_times) - timedelta(hours=1)
                end_time_for_60m = earliest_sig.strftime('%Y-%m-%d %H:%M:%S') + '.000000'
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time_for_60m)
            else:
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)

            if not end_time:
                sig_times = [datetime.strptime(s['created_time'][:19], '%Y-%m-%d %H:%M:%S')
                           for s in valid_precheck]
                earliest_sig = min(sig_times) - timedelta(hours=1)
                end_time_for_60m = earliest_sig.strftime('%Y-%m-%d %H:%M:%S') + '.000000'
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time_for_60m)
            else:
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)

            if len(data_5m) < 20 or len(data_60m) < 20:
                return

            # 计算指标
            data_5m_with_macd = MACDCalculator.calculate(data_5m)
            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            data_5m_with_atr = ATRCalculator.calculate(data_5m_with_macd, 14)

            if len(data_5m_with_atr) < 5 or len(data_60m_with_macd) < 5:
                return

            # 构建索引映射
            if symbol not in self.index_map_60m or len(self.index_map_60m.get(symbol, [])) != len(data_5m_with_macd):
                self.index_map_60m[symbol] = IndexMapper.precompute_60m_index(data_5m_with_macd, data_60m_with_macd)
            index_map = self.index_map_60m[symbol]

            # 识别绿柱堆
            _, green_stacks_5m, _ = StackIdentifier.identify(data_5m_with_macd)
            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            strategy = Strategy({})

            # 回放模式：找到离检测时间最近的K线索引
            if self.replay_time:
                idx_5m = 0
                for i, row in enumerate(data_5m_with_macd):
                    row_time = datetime.strptime(row[0][:19], '%Y-%m-%d %H:%M:%S')
                    if row_time <= self.replay_time:
                        idx_5m = i
                    else:
                        break
                # 检查是否还有数据
                if idx_5m < len(data_5m_with_macd) - 1:
                    pass  # 找到有效的索引
            else:
                idx_5m = len(data_5m_with_macd) - 1

            idx_5m = min(idx_5m, len(data_5m_with_macd) - 1)
            current_5m_time = data_5m_with_macd[idx_5m][0][:19]
            current_5m_price = data_5m_with_macd[idx_5m][4]

            idx_60m = index_map[idx_5m] if idx_5m < len(index_map) else len(data_60m_with_macd) - 1

            # 有持仓则检查止损
            if position:
                self._check_stop_loss(symbol, data_5m_with_macd, position)
                return

            # 检查入场信号
            for sig in valid_precheck:
                sig_type = sig['type']

                if sig_type == 'green':
                    sub_type = sig.get('sub_type', 'dif_turn')

                    diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                    if not diver_ok:
                        continue

                    # 5分钟阳柱
                    current_open = data_5m_with_macd[idx_5m][1]
                    current_price = data_5m_with_macd[idx_5m][4]
                    if current_price <= current_open:
                        continue

                    # 入场价
                    if sub_type == 'green_to_red':
                        entry_price = current_price
                    else:
                        entry_price = current_open

                    # 止损
                    stop_loss, stop_reason = strategy.get_initial_stop_loss(
                        data_5m_with_macd, idx_5m,
                        green_stacks_5m, {},
                        data_60m_with_macd, green_stacks_60m
                    )

                    if stop_loss is None:
                        continue

                    if stop_loss >= entry_price:
                        continue

                    # 合约价值检查
                    instrument_info = self.instrument_map.get(symbol, {})
                    volume_multiple = instrument_info.get('VolumeMultiple', 1)
                    contract_value = entry_price * volume_multiple
                    TARGET_NOTIONAL = 200000

                    if contract_value > TARGET_NOTIONAL:
                        continue

                    # 入场
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

                    print_log(f"📈 {symbol} 策略开仓信号：{signal_data}")

                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    # 发送飞书通知
                    try:
                        from utils.feishu_notifier import send_feishu_strategy_signal
                        send_feishu_strategy_signal(symbol, signal_data)
                        print_log(f"✓ {symbol} 飞书信号已发送")
                    except Exception as e:
                        print_log(f"✗ {symbol} 飞书信号发送失败：{e}")

                    break

                elif sig_type == 'red':
                    diver_ok, diver_reason, _, _ = strategy.check_60m_bottom_rise_in_red(data_60m_with_macd, idx_60m)
                    if not diver_ok:
                        continue

                    # 检查5分钟在绿柱堆
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

                    if stop_loss is None:
                        continue

                    if stop_loss >= entry_price:
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

                    print_log(f"📈 {symbol} 策略开仓信号（红柱堆）：{signal_data}")

                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    # 发送飞书通知
                    try:
                        from utils.feishu_notifier import send_feishu_strategy_signal
                        send_feishu_strategy_signal(symbol, signal_data)
                        print_log(f"✓ {symbol} 飞书信号已发送")
                    except Exception as e:
                        print_log(f"✗ {symbol} 飞书信号发送失败：{e}")

                    break

        except Exception as e:
            print_log(f"✗ {symbol} 入场信号检查失败：{e}")

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

                print_log(f"📉 {symbol} 策略平仓信号（止损）：{signal_data}")

                if self.strategy_signal_manager:
                    self.strategy_signal_manager.add_signal(symbol, signal_data)

                del self.positions[symbol]

                if symbol in self.precheck_signals_green:
                    self.precheck_signals_green[symbol] = []
                if symbol in self.precheck_signals_red:
                    self.precheck_signals_red[symbol] = []

        except Exception as e:
            print_log(f"✗ {symbol} 止损检查失败：{e}")

    def run_all_checks(self, end_time: str = None):
        """运行所有信号检查"""
        for inst in self.instruments:
            symbol = inst.get("MainContractID") or inst.get("InstrumentID", "")
            if not symbol:
                continue

            exchange_id = inst.get("ExchangeID", "")
            full_symbol = f"{exchange_id}.{symbol}"

            # 60分钟预检测
            self.check_60m_precheck(full_symbol, end_time)

            # 入场信号检查
            self.check_entry_signal(full_symbol, end_time)

    def replay_date(self, target_date: str):
        """回放指定一天的K线数据

        Args:
            target_date: 日期字符串，格式 YYYYMMDD 或 YYYY-MM-DD
        """
        # 解析日期
        if len(target_date) == 8:
            target_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
        elif '-' in target_date:
            pass
        else:
            print_log(f"日期格式错误：{target_date}，请使用 YYYYMMDD 或 YYYY-MM-DD 格式")
            return

        print_log("=" * 70)
        print_log(f"开始回放：{target_date}")
        print_log("=" * 70)

        # 生成当天的交易时间点（每5分钟一个）
        # 假设交易时间 9:00-10:15, 10:30-11:30, 13:30-15:00, 21:00-02:30
        trading_hours = [
            ("09:00", "10:15"),
            ("10:30", "11:30"),
            ("13:30", "15:00"),
            ("21:00", "23:59"),
            ("00:00", "02:30"),
        ]

        all_times = []
        for start_h, end_h in trading_hours:
            start_hour, start_min = map(int, start_h.split(':'))
            end_hour, end_min = map(int, end_h.split(':'))

            current_hour = start_hour
            current_min = start_min
            end_hour_abs = end_hour if end_hour >= start_hour else end_hour + 24

            while (current_hour < end_hour_abs) or (current_hour == end_hour_abs and current_min <= end_min):
                if current_hour >= 24:
                    current_hour -= 24
                all_times.append(f"{target_date} {current_hour:02d}:{current_min:02d}:00")
                current_min += 5
                if current_min >= 60:
                    current_hour += 1
                    current_min -= 60

        print_log(f"总时间点：{len(all_times)} 个")

        # 逐个时间点回放
        for idx, check_time in enumerate(all_times):
            print_log(f"\n[{idx+1}/{len(all_times)}] 检测时间：{check_time}")

            # 设置回放时间（在回放模式下替代 datetime.now()）
            self.replay_time = datetime.strptime(check_time, '%Y-%m-%d %H:%M:%S')

            # 检查60分钟预检测信号
            for inst in self.instruments:
                symbol = inst.get("MainContractID") or inst.get("InstrumentID", "")
                if not symbol:
                    continue
                exchange_id = inst.get("ExchangeID", "")
                full_symbol = f"{exchange_id}.{symbol}"

                self.check_60m_precheck(full_symbol, check_time)

            # 检查入场信号
            for inst in self.instruments:
                symbol = inst.get("MainContractID") or inst.get("InstrumentID", "")
                if not symbol:
                    continue
                exchange_id = inst.get("ExchangeID", "")
                full_symbol = f"{exchange_id}.{symbol}"

                self.check_entry_signal(full_symbol, check_time)

        # 回放结束后清除回放时间
        self.replay_time = None

        print_log("\n" + "=" * 70)
        print_log(f"回放完成：{target_date}")
        print_log("=" * 70)


# 定时器类
class SignalTimer:
    """定时器 - 每5分钟执行一次信号检测"""

    def __init__(self, db_manager, instruments, strategy_signal_manager=None, interval_seconds=300):
        self.db_manager = db_manager
        self.instruments = instruments
        self.strategy_signal_manager = strategy_signal_manager
        self.interval_seconds = interval_seconds
        self.running = False
        self.signal_checker = SignalChecker(db_manager, instruments, strategy_signal_manager)

    def calculate_next_wait_time(self):
        """计算到下一个5分钟整点（延迟5秒后）需要等待的秒数"""
        now = datetime.now()
        # 下一个5分钟整点：例如 10:00 -> 10:05 -> 10:10
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

        # 如果下一个5分钟已经过了（当前正好在5分钟时刻），则加5分钟
        if next_5_minute == current_minute and now.second >= 5:
            next_time = now + timedelta(minutes=5)
            next_time = next_time.replace(second=5, microsecond=0)

        wait_seconds = (next_time - now).total_seconds()
        return max(wait_seconds, 1)

    def run(self):
        """运行定时器"""
        self.running = True

        while self.running:
            try:
                wait_time = self.calculate_next_wait_time()
                print_log(f"等待 {wait_time:.0f} 秒，下一次信号检测时间：{datetime.now() + timedelta(seconds=wait_time)}")

                # 使用倒计时显示进度
                for remaining in range(int(wait_time), 0, -30):
                    if not self.running:
                        break
                    time.sleep(min(30, remaining))
                    if remaining <= 30:
                        time.sleep(remaining)

                if not self.running:
                    break

                # 执行信号检测
                current_time = datetime.now()
                print_log(f"\n{'='*50}")
                print_log(f"开始执行信号检测 - {current_time}")
                print_log(f"{'='*50}")

                self.signal_checker.run_all_checks()

                print_log(f"信号检测完成 - {datetime.now()}")

            except Exception as e:
                print_log(f"信号检测出错：{e}")

    def stop(self):
        """停止定时器"""
        self.running = False


# 从配置导入排除列表
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.strategy_config import Config

EXCLUDED_PRODUCTS = [p.upper() for p in Config.EXCLUDED_PRODUCTS]


def is_excluded_product(instrument_id: str) -> bool:
    if not instrument_id:
        return False
    if instrument_id.lower().endswith('_f'):
        return True
    product = ""
    for char in instrument_id:
        if char.isalpha():
            product += char
        else:
            break
    return product.upper() in EXCLUDED_PRODUCTS


def load_main_contracts(json_file="./data/contracts/main_contracts.json"):
    if not os.path.exists(json_file):
        print_log(f"错误：文件不存在 {json_file}")
        return []

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            contracts = data
        elif isinstance(data, dict) and "main_contracts" in data:
            contracts = data["main_contracts"]
        else:
            print_log(f"错误：JSON 文件格式不正确")
            return []

        filtered_contracts = []
        for inst in contracts:
            inst_id = inst.get("MainContractID", "") or inst.get("InstrumentID", "")
            if is_excluded_product(inst_id):
                print_log(f"排除不活跃合约：{inst_id}")
            else:
                filtered_contracts.append(inst)

        print_log(f"从 {json_file} 加载了 {len(filtered_contracts)} 个主力合约")
        return filtered_contracts

    except Exception as e:
        print_log(f"读取 JSON 文件失败：{e}")
        return []


# 飞书通知函数
def send_feishu_strategy_signal(symbol: str, signal_data: dict):
    """发送飞书策略信号"""
    from utils.feishu_notifier import FeishuNotifier, send_feishu_strategy_signal as send_signal
    try:
        send_signal(symbol, signal_data)
    except Exception as e:
        print_log(f"飞书信号发送失败：{e}")


if __name__ == '__main__':
    print_log("=" * 70)
    print_log("策略信号定时检测程序启动")
    print_log("=" * 70)

    # 解析命令行参数
    db_path_arg = None
    use_online = False
    replay_date = None

    for arg in sys.argv[1:]:
        if arg == "online":
            use_online = True
            print_log("使用线上数据库")
        elif arg == "test":
            use_online = False
            print_log("使用测试数据库")
        elif arg.startswith("--replay="):
            replay_date = arg.split("=", 1)[1]
            print_log(f"回放模式：{replay_date}")
        else:
            db_path_arg = arg
            print_log(f"使用自定义数据库路径：{arg}")

    # 加载合约
    json_file = "./data/contracts/main_contracts.json"
    instruments = load_main_contracts(json_file)

    if not instruments:
        print_log(f"错误：没有找到合约列表")
        sys.exit(1)

    # 初始化数据库
    db_manager = DatabaseManager(db_path=db_path_arg, use_online=use_online)
    print_log(f"数据库路径：{db_manager.db_path}")

    # 初始化策略信号管理器
    strategy_signal_manager = StrategySignalManager(STRATEGY_SIGNAL_FILE)

    # 初始化信号检测器
    signal_checker = SignalChecker(db_manager, instruments, strategy_signal_manager)

    # 回放模式
    if replay_date:
        signal_checker.replay_date(replay_date)
    else:
        # 定时模式：初始化定时器（每5分钟执行一次）
        signal_timer = SignalTimer(db_manager, instruments, strategy_signal_manager, interval_seconds=300)

        print_log(f"信号检测间隔：5分钟")
        print_log(f"监控合约数：{len(instruments)}")
        print_log("=" * 70)
        print_log("程序运行中，按 Ctrl+C 退出...")

        try:
            signal_timer.run()
        except KeyboardInterrupt:
            print_log("\n收到退出信号，正在关闭程序...")
            signal_timer.stop()
            print_log("程序已退出")