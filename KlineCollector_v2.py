# @Project: https://github.com/Jedore/ctp.examples
# @File:    KlineCollector_v2.py
# @Time:    02/04/2026
# @Author:  Assistant
# @Description: 订阅合约 tick 数据，合成 5 分钟 K 线，存储到 SQLite 数据库，每次从数据库读取数据计算 MACD 等指标（低内存版本）

import json
import sys
import os
import atexit
import logging
import sqlite3
import threading
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from ctp.base_mdapi import CMdSpiBase, mdapi
from config.trading_time_config import (
    PRODUCT_TRADING_MINUTES,
    TRADING_DAYS_PER_YEAR,
    get_annual_factor
)
from utils.feishu_notifier import FeishuNotifier, send_feishu_strategy_signal, send_feishu_test


# 配置日志
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, datetime.now().strftime("KlineCollector_v2_%Y%m%d_%H%M%S.log"))

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

# 限制日志大小
MAX_LOG_SIZE = 100 * 1024 * 1024  # 100MB

# 策略信号文件
STRATEGY_SIGNAL_FILE = "strategy_signals_v2.json"

# MACD 计算参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# K 线数据量配置（从数据库读取的历史数据量）
MAX_5M_BARS = 500  # 5 分钟 K 线
MAX_60M_BARS = 200  # 60 分钟 K 线


def check_log_size():
    """检查日志文件大小，如果超过限制则创建新日志文件"""
    global log_filename, file_handler

    try:
        if os.path.exists(log_filename):
            file_size = os.path.getsize(log_filename)
            if file_size > MAX_LOG_SIZE:
                new_log_filename = os.path.join(log_dir, datetime.now().strftime("KlineCollector_v2_%Y%m%d_%H%M%S.log"))
                logger.info(f"日志文件大小 {file_size/1024/1024:.2f}MB 超过限制，切换到新日志文件：{new_log_filename}")

                logger.removeHandler(file_handler)
                file_handler.close()

                log_filename = new_log_filename
                new_file_handler = logging.FileHandler(log_filename, encoding='utf-8')
                new_file_handler.setLevel(logging.INFO)
                new_file_handler.setFormatter(formatter)

                file_handler = new_file_handler
                logger.addHandler(file_handler)

                logger.info(f"新日志文件创建成功")
    except Exception as e:
        logger.error(f"检查日志文件大小失败：{e}")


def print_log(*args, **kwargs):
    """ 日志输出函数 """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# ==================== MACD 指标计算器 ====================

class MACDCalculator:
    """MACD 计算器"""

    @staticmethod
    def ema(values: list, span: int) -> list:
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


# ==================== 绿柱堆/红柱堆识别 ====================

class StackIdentifier:
    """绿柱堆/红柱堆识别"""

    @staticmethod
    def identify(data_with_macd: list) -> tuple:
        """识别绿柱堆和红柱堆

        返回: (data, green_stacks, green_gaps)
        """
        if len(data_with_macd) == 0:
            return [], {}, {}

        green_stacks = {}
        green_gaps = {}
        current_stack = None
        stack_start_idx = None
        gap_count = 0

        for i in range(len(data_with_macd)):
            hist = data_with_macd[i][8]

            if hist > 0:  # 红柱
                if current_stack == 'green' and stack_start_idx is not None:
                    # 绿柱堆结束，记录
                    if stack_start_idx not in green_stacks:
                        green_stacks[stack_start_idx] = {
                            'start_idx': stack_start_idx,
                            'end_idx': i - 1,
                            'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                        }
                    current_stack = None
                    stack_start_idx = None
                current_stack = 'red'
                gap_count = 0

            elif hist < 0:  # 绿柱
                if current_stack != 'green':
                    # 新的绿柱堆开始
                    if current_stack == 'green' and stack_start_idx is not None:
                        # 记录之前的绿柱堆
                        if stack_start_idx not in green_stacks:
                            green_stacks[stack_start_idx] = {
                                'start_idx': stack_start_idx,
                                'end_idx': i - 1,
                                'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                            }

                    stack_start_idx = i
                    current_stack = 'green'
                    gap_count = 0
                else:
                    # 继续绿柱堆，检查绿柱堆内的向上跳空缺口
                    if i > 0:
                        prev_low = data_with_macd[i-1][3]
                        curr_low = data_with_macd[i][3]
                        if curr_low > prev_low:  # 向上跳空
                            gap_key = stack_start_idx
                            if gap_key not in green_gaps:
                                green_gaps[gap_key] = []
                            green_gaps[gap_key].append({
                                'idx': i,
                                'gap': curr_low - prev_low
                            })

            else:  # hist == 0
                if current_stack == 'green' and stack_start_idx is not None:
                    gap_count += 1
                    if gap_count >= 3:  # 连续 3 根零轴，可能转换
                        if stack_start_idx not in green_stacks:
                            green_stacks[stack_start_idx] = {
                                'start_idx': stack_start_idx,
                                'end_idx': i - 1,
                                'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, i))
                            }
                        current_stack = None
                        stack_start_idx = None
                        gap_count = 0

        # 处理最后一个绿柱堆
        if current_stack == 'green' and stack_start_idx is not None:
            if stack_start_idx not in green_stacks:
                green_stacks[stack_start_idx] = {
                    'start_idx': stack_start_idx,
                    'end_idx': len(data_with_macd) - 1,
                    'lowest_low': min(data_with_macd[j][3] for j in range(stack_start_idx, len(data_with_macd)))
                }

        return data_with_macd, green_stacks, green_gaps


# ==================== 策略逻辑 ====================

class Strategy:
    """策略逻辑"""

    def __init__(self, symbol_info: dict = None):
        self.symbol_info = symbol_info or {}
        self.product_id = self.symbol_info.get('ProductID', '')

    def check_60m_dif_turn_in_green(self, data_with_macd: list, idx_60m: int, green_stacks: dict) -> tuple:
        """检查 60 分钟绿柱堆内 DIF 是否拐头"""
        if idx_60m < 2:
            return False, ""

        # 找到当前绿柱堆
        current_green = None
        for stack_idx, stack in green_stacks.items():
            if stack['start_idx'] <= idx_60m <= stack['end_idx']:
                current_green = stack
                break

        if not current_green:
            return False, "不在绿柱堆内"

        # 检查绿柱堆内 DIF 是否拐头
        dif_values = [data_with_macd[i][6] for i in range(current_green['start_idx'], idx_60m + 1)]

        if len(dif_values) < 3:
            return False, "数据不足"

        # 检查最近3个DIF是否上升
        if dif_values[-1] > dif_values[-2] > dif_values[-3]:
            return True, "绿柱堆内DIF拐头"

        return False, "DIF未拐头"

    def check_60m_dif_turn_in_red(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟红柱堆内 DIF 是否拐头向下"""
        if idx_60m < 2:
            return False, ""

        # 检查最近3个DIF是否下降
        dif_values = [data_with_macd[i][6] for i in range(idx_60m - 2, idx_60m + 1)]

        if len(dif_values) < 3:
            return False, "数据不足"

        if dif_values[-1] < dif_values[-2] < dif_values[-3]:
            return True, "红柱堆内DIF拐头向下"

        return False, "DIF未拐头向下"

    def check_60m_divergence(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟底背离"""
        if idx_60m < 5:
            return False, "数据不足", 0, 0

        hist_60m = data_with_macd[idx_60m][8]
        if hist_60m >= 0:
            return False, "不在绿柱", 0, 0

        # 获取当前绿柱堆的最低点
        current_low = data_with_macd[idx_60m][3]

        # 找前一个绿柱堆
        prev_green_start = None
        for i in range(idx_60m - 1, -1, -1):
            if data_with_macd[i][8] < 0:
                if prev_green_start is None:
                    prev_green_start = i
            elif prev_green_start is not None:
                break

        if prev_green_start is None or idx_60m - prev_green_start > 50:
            return False, "无前序绿柱堆", 0, 0

        # 获取前一个绿柱堆的最低点
        prev_low = min(data_with_macd[i][3] for i in range(prev_green_start, idx_60m))

        # 检查底背离：当前绿柱堆最低点创新低，但MACD绿柱面积未创新低
        if current_low >= prev_low:
            return False, "未创新低", current_low, prev_low

        # 检查绿柱面积（简化：用绿柱高度之和）
        current_green_area = sum(abs(data_with_macd[i][8]) for i in range(idx_60m - 4, idx_60m + 1) if data_with_macd[i][8] < 0)
        prev_green_area = sum(abs(data_with_macd[i][8]) for i in range(prev_green_start - 4, prev_green_start + 1) if data_with_macd[i][8] < 0)

        if current_green_area >= prev_green_area:
            return True, f"底背离: 当前低={current_low:.2f}, 前低={prev_low:.2f}, 当前面积={current_green_area:.2f}, 前面积={prev_green_area:.2f}", current_low, prev_low

        return False, "绿柱面积未背离", current_low, prev_low

    def check_60m_bottom_rise_in_red(self, data_with_macd: list, idx_60m: int) -> tuple:
        """检查 60 分钟红柱堆内的底抬升"""
        if idx_60m < 5:
            return False, "数据不足", 0, 0

        # 获取当前红柱堆的最低点
        current_low = data_with_macd[idx_60m][3]

        # 找前一个红柱堆
        prev_red_start = None
        for i in range(idx_60m - 1, -1, -1):
            if data_with_macd[i][8] > 0:
                if prev_red_start is None:
                    prev_red_start = i
            elif prev_red_start is not None:
                break

        if prev_red_start is None or idx_60m - prev_red_start > 50:
            return False, "无前序红柱堆", 0, 0

        # 获取前一个红柱堆的最低点
        prev_low = min(data_with_macd[i][3] for i in range(prev_red_start, idx_60m))

        # 检查底抬升：当前最低点高于前低
        if current_low > prev_low:
            return True, f"底抬升: 当前低={current_low:.2f}, 前低={prev_low:.2f}", current_low, prev_low

        return False, "未底抬升", current_low, prev_low


# ==================== 策略信号管理器 ====================

class StrategySignalManager:
    """策略信号管理器"""

    def __init__(self, signal_file=STRATEGY_SIGNAL_FILE):
        self.signal_file = signal_file
        self.signals = self._load_signals()

    def _load_signals(self) -> list:
        """加载信号文件"""
        if os.path.exists(self.signal_file):
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                print_log(f"加载策略信号文件：{self.signal_file}, 共 {len(signals)} 条信号")
                return signals
            except Exception as e:
                print_log(f"加载策略信号文件失败：{e}")
                return []
        return []

    def _save_signals(self):
        """保存信号文件"""
        try:
            with open(self.signal_file, 'w', encoding='utf-8') as f:
                json.dump(self.signals, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print_log(f"保存策略信号文件失败：{e}")

    def add_signal(self, symbol: str, signal_data: dict):
        """添加策略信号"""
        signal_record = {
            'symbol': symbol,
            'signal_type': signal_data.get('signal_type', ''),
            'price': signal_data.get('price', 0),
            'stop_loss': signal_data.get('stop_loss', 0),
            'position_size': signal_data.get('position_size', 0),
            'reason': signal_data.get('reason', ''),
            'time': signal_data.get('time', datetime.now().isoformat()),
            'created_at': datetime.now().isoformat()
        }
        self.signals.append(signal_record)
        self._save_signals()
        print_log(f"📝 {symbol} 保存策略信号：{signal_record['signal_type']} @ {signal_record['price']}")


# ==================== 数据库管理 ====================

class DatabaseManager:
    """ 数据库管理类 """

    _lock = threading.Lock()  # 类级别的锁

    ONLINE_DB_PATH = "./data/db/kline_data.db"
    TEST_DB_PATH = "./data/db/kline_data_test.db"

    def __init__(self, db_path=None, use_online=False):
        if db_path is not None:
            self.db_path = db_path
        elif use_online:
            self.db_path = self.ONLINE_DB_PATH
        else:
            self.db_path = self.TEST_DB_PATH

        self.conn = None
        self.cursor = None
        self.init_database()

    def init_database(self):
        """ 初始化数据库 """
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS kline_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                close_oi INTEGER NOT NULL,
                vwap REAL NOT NULL,
                symbol TEXT NOT NULL,
                duration INTEGER NOT NULL,
                update_time TEXT NOT NULL,
                source INTEGER NOT NULL DEFAULT 2,
                UNIQUE(datetime, symbol, duration)
            )
        ''')

        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN update_time TEXT")
        except Exception:
            pass

        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN source INTEGER DEFAULT 2")
        except Exception:
            pass

        self.conn.commit()
        print_log(f"数据库初始化完成：{self.db_path}")

    def insert_kline(self, symbol, date_time, open_price, close_price, high_price, low_price, volume, open_interest, duration=300, source=2):
        """ 插入 K 线数据 """
        try:
            with DatabaseManager._lock:
                vwap = close_price
                update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.cursor.execute('''
                    INSERT OR REPLACE INTO kline_data
                    (datetime, open, close, high, low, volume, close_oi, vwap, symbol, duration, update_time, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (date_time, open_price, close_price, high_price, low_price, volume, open_interest, vwap, symbol, duration, update_time, source))
                self.conn.commit()
        except Exception as e:
            print_log(f"插入 K 线数据失败：{e}")

    def get_kline_history(self, symbol: str, limit: int = 100, duration: int = 300) -> pd.DataFrame:
        """获取 K 线历史数据"""
        try:
            with DatabaseManager._lock:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT datetime, open, high, low, close, volume, close_oi, vwap, update_time, source
                    FROM kline_data
                    WHERE symbol = ? AND duration = ?
                    ORDER BY datetime DESC
                    LIMIT ?
                ''', (symbol, duration, limit))

                rows = cursor.fetchall()

                if len(rows) > 0:
                    df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'close_oi', 'vwap', 'update_time', 'source'])
                    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
                    df = df.sort_values('datetime').reset_index(drop=True)
                    return df

                return pd.DataFrame()
        except Exception as e:
            print_log(f"获取 K 线历史失败：{e}")
            return pd.DataFrame()

    def get_kline_data(self, symbol: str, limit: int, duration: int) -> list:
        """直接从数据库获取 K 线数据（返回 tuple 列表，用于策略计算）"""
        try:
            with DatabaseManager._lock:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT datetime, open, high, low, close, volume
                    FROM kline_data
                    WHERE symbol = ? AND duration = ?
                    ORDER BY datetime DESC
                    LIMIT ?
                ''', (symbol, duration, limit))

                rows = cursor.fetchall()
                # 反转为正序
                result = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in reversed(rows)]
                return result
        except Exception as e:
            print_log(f"获取 K 线数据失败：{e}")
            return []

    def close(self):
        """ 关闭数据库连接 """
        if self.conn:
            self.conn.close()
            self.conn = None


# ==================== K 线合成器 ====================

class KlineAggregator:
    """ K 线合成器 - 每次从数据库读取数据计算指标 """

    KLINE_PERIODS = {
        "5min": (5, 300),
        "30min": (30, 1800),
        "60min": (60, 3600),
        "day": (1440, 86400)
    }

    def __init__(self, db_manager, instruments, strategy_signal_manager=None):
        self.db_manager = db_manager
        self.instruments = instruments

        # 多周期 K 线数据（只保留当前正在合成的 K 线，不缓存历史数据）
        self.current_klines = {}
        for period_name in self.KLINE_PERIODS:
            self.current_klines[period_name] = {}

        # 合约映射
        self.instrument_map = {}
        for inst in instruments:
            key = inst.get("MainContractID") or inst.get("InstrumentID", "")
            self.instrument_map[key] = inst

        # 策略信号管理器
        self.strategy_signal_manager = strategy_signal_manager

        # 策略配置
        self.db_path = db_manager.db_path
        self.contracts_path = "./data/contracts/main_contracts.json"

        # 预检测信号队列（60分钟产生，5分钟检查）
        self.precheck_signals_green = {}  # {symbol: [signal, ...]}
        self.precheck_signals_red = {}    # {symbol: [signal, ...]}

        # 持仓状态 {symbol: position_info}
        self.positions = {}

        # 上次入场时间 {symbol: datetime}
        self.last_entry_times = {}

        # 信号冷却时间（小时）
        self.cooldown_hours = 4

        # 记录上次处理的 60m bar 时间（避免重复处理）
        self.last_60m_bar_times = {}

        print_log(f"策略引擎已启用（每次从数据库读取模式）")
        print_log(f"  数据库路径：{self.db_path}")
        print_log(f"  监控合约数：{len(self.instrument_map)}")

    def _get_kline_time(self, dt: datetime, period_minutes: int) -> datetime:
        """获取当前时间所在的 K 线周期起始时间"""
        if period_minutes == 1440:
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return dt.replace(minute=(dt.minute // period_minutes) * period_minutes, second=0, microsecond=0)

    def add_tick(self, instrument_name, price, volume, open_interest, timestamp):
        """ 添加 tick 数据，合成多周期 K 线 """

        dt = datetime.fromtimestamp(timestamp)

        for period_name, (period_minutes, period_seconds) in self.KLINE_PERIODS.items():
            kline_time = self._get_kline_time(dt, period_minutes)

            if instrument_name not in self.current_klines[period_name]:
                self.current_klines[period_name][instrument_name] = {
                    "open": price,
                    "close": price,
                    "high": price,
                    "low": price,
                    "vol": volume,
                    "open_interest": open_interest,
                    "time": kline_time,
                    "timestamp": timestamp
                }
                continue

            kline = self.current_klines[period_name][instrument_name]

            if kline_time > kline["time"]:
                # 保存上一个 K 线
                self.save_kline(instrument_name, kline, period_seconds)

                # 开始新的 K 线
                kline["open"] = price
                kline["close"] = price
                kline["high"] = price
                kline["low"] = price
                kline["vol"] = volume
                kline["open_interest"] = open_interest
                kline["time"] = kline_time
                kline["timestamp"] = timestamp
            else:
                # 更新当前 K 线
                kline["close"] = price
                kline["high"] = max(kline["high"], price)
                kline["low"] = min(kline["low"], price)
                kline["vol"] += volume
                kline["open_interest"] = open_interest
                kline["timestamp"] = timestamp

    def save_kline(self, instrument_name, kline, duration=300):
        """ 保存 K 线到数据库 """
        # 定期检查日志文件大小
        if not hasattr(self, '_kline_save_count'):
            self._kline_save_count = 0
        self._kline_save_count += 1
        if self._kline_save_count % 100 == 0:
            check_log_size()

        instrument = self.instrument_map.get(instrument_name, {})
        exchange_id = instrument.get("ExchangeID", "")

        symbol = f"{exchange_id}.{instrument_name}"

        date_time_str = kline["time"].strftime("%Y-%m-%d %H:%M:%S")
        self.db_manager.insert_kline(
            symbol=symbol,
            date_time=date_time_str,
            open_price=kline["open"],
            close_price=kline["close"],
            high_price=kline["high"],
            low_price=kline["low"],
            volume=kline["vol"],
            open_interest=kline["open_interest"],
            duration=duration,
            source=2
        )

        # 5 分钟 K 线保存后检查策略信号
        if duration == 300:
            print_log(f"保存 K 线：{symbol} {date_time_str} O={kline['open']:.2f} H={kline['high']:.2f} L={kline['low']:.2f} C={kline['close']:.2f} V={kline['vol']}")
            # 每次都从数据库读取最新数据来计算信号
            self.check_strategy_signal_v2(symbol)
        else:
            period_name = self._get_period_name_by_duration(duration)
            # 60 分钟 K 线完成后，检查是否产生预检测信号
            if duration == 3600:
                self.check_60m_signal_v2(symbol)
            print_log(f"保存{period_name}K 线：{symbol} {date_time_str}")

    def _get_period_name_by_duration(self, duration: int) -> str:
        """根据周期秒数获取周期名称"""
        for period_name, (_, period_seconds) in self.KLINE_PERIODS.items():
            if period_seconds == duration:
                return period_name
        return f"{duration}s"

    def check_60m_signal_v2(self, symbol: str):
        """检查 60 分钟 K 线完成后是否产生预检测信号"""
        try:
            # 从数据库读取 60 分钟 K 线数据
            data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600)
            if len(data_60m) < 20:
                return

            # 计算 MACD
            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            if len(data_60m_with_macd) < 5:
                return

            # 识别绿柱堆
            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            idx_60m = len(data_60m_with_macd) - 1
            hist_60m = data_60m_with_macd[idx_60m][8]
            hist_60m_prev = data_60m_with_macd[idx_60m - 1][8] if idx_60m > 0 else 0
            current_60m_time = data_60m_with_macd[idx_60m][0]

            strategy = Strategy()

            # 检查是否已处理过这个 60m bar
            last_time = self.last_60m_bar_times.get(symbol)
            if last_time == current_60m_time:
                return
            self.last_60m_bar_times[symbol] = current_60m_time

            # 绿柱堆内 DIF 拐头
            if hist_60m < 0:
                dif_turn, _ = strategy.check_60m_dif_turn_in_green(data_60m_with_macd, idx_60m, green_stacks_60m)
                if dif_turn:
                    diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                    if diver_ok:
                        if symbol not in self.precheck_signals_green:
                            self.precheck_signals_green[symbol] = []
                        # 检查是否已存在
                        existing = next((s for s in self.precheck_signals_green[symbol] if s['created_time'] == current_60m_time), None)
                        if not existing:
                            self.precheck_signals_green[symbol].append({
                                'type': 'green',
                                'created_time': current_60m_time,
                            })
                            print_log(f"📊 {symbol} 60分钟绿柱堆内DIF拐头+底背离，预检测信号")

            # 绿柱堆转红柱堆
            elif hist_60m > 0 and hist_60m_prev < 0:
                diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                if diver_ok:
                    if symbol not in self.precheck_signals_green:
                        self.precheck_signals_green[symbol] = []
                    existing = next((s for s in self.precheck_signals_green[symbol] if s['created_time'] == current_60m_time), None)
                    if not existing:
                        self.precheck_signals_green[symbol].append({
                            'type': 'green',
                            'created_time': current_60m_time,
                        })
                        print_log(f"📊 {symbol} 60分钟绿柱堆转红柱堆+底背离，预检测信号")

            # 红柱堆内 DIF 拐头向下
            elif hist_60m > 0 and hist_60m_prev > 0:
                dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(data_60m_with_macd, idx_60m)
                if dif_turn_red:
                    diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_bottom_rise_in_red(data_60m_with_macd, idx_60m)
                    if diver_ok:
                        if symbol not in self.precheck_signals_red:
                            self.precheck_signals_red[symbol] = []
                        existing = next((s for s in self.precheck_signals_red[symbol] if s['created_time'] == current_60m_time), None)
                        if not existing:
                            self.precheck_signals_red[symbol].append({
                                'type': 'red',
                                'created_time': current_60m_time,
                            })
                            print_log(f"📊 {symbol} 60分钟红柱堆内DIF拐头+底抬升，预检测信号")

        except Exception as e:
            print_log(f"✗ {symbol} 60分钟信号检查失败：{e}")

    def check_strategy_signal_v2(self, symbol: str):
        """检查策略信号（每次从数据库读取数据）"""
        try:
            # 检查是否在持仓
            position = self.positions.get(symbol)

            # 获取当前时间
            current_time = datetime.now()

            # 检查冷却时间
            if not position and symbol in self.last_entry_times:
                last_entry = self.last_entry_times[symbol]
                hours_passed = (current_time - last_entry).total_seconds() / 3600
                if hours_passed < self.cooldown_hours:
                    return

            # 从数据库读取 5 分钟和 60 分钟 K 线
            data_5m = self.db_manager.get_kline_data(symbol, MAX_5M_BARS, 300)
            data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600)

            if len(data_5m) < 20 or len(data_60m) < 20:
                return

            # 计算 MACD
            data_5m_with_macd = MACDCalculator.calculate(data_5m)
            data_60m_with_macd = MACDCalculator.calculate(data_60m)

            if len(data_5m_with_macd) < 5 or len(data_60m_with_macd) < 5:
                return

            # 识别绿柱堆
            _, green_stacks_5m, _ = StackIdentifier.identify(data_5m_with_macd)

            strategy = Strategy()

            # 检查 5 分钟入场条件
            idx_5m = len(data_5m_with_macd) - 1
            current_5m_time = data_5m_with_macd[idx_5m][0][:19]
            current_5m_price = data_5m_with_macd[idx_5m][4]
            current_5m_low = data_5m_with_macd[idx_5m][3]

            # 检查预检测信号
            all_precheck = []
            if symbol in self.precheck_signals_green:
                all_precheck.extend(self.precheck_signals_green[symbol])
            if symbol in self.precheck_signals_red:
                all_precheck.extend(self.precheck_signals_red[symbol])

            # 过滤过期信号（超过 8 小时）
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

            # 如果有持仓，检查止损
            if position:
                self._check_stop_loss_v2(symbol, data_5m_with_macd, position)
                return

            # 检查入场条件
            for sig in valid_precheck:
                sig_type = sig['type']

                if sig_type == 'green':
                    # 检查 5 分钟绿柱堆内 DIF 金叉
                    diver_ok, diver_reason, current_green_low, _ = strategy.check_60m_divergence(data_60m_with_macd, len(data_60m_with_macd) - 1)
                    if not diver_ok:
                        continue

                    # 检查 5 分钟绿柱堆内价格创新低
                    in_green = False
                    green_start = None
                    for idx, stack in green_stacks_5m.items():
                        if stack['start_idx'] <= idx_5m <= stack['end_idx']:
                            in_green = True
                            green_start = stack['start_idx']
                            break

                    if not in_green:
                        continue

                    # 检查最近几根是否有DIF金叉
                    if idx_5m >= 4:
                        dif_now = data_5m_with_macd[idx_5m][6]
                        dif_prev = data_5m_with_macd[idx_5m - 1][6]
                        dif_prev2 = data_5m_with_macd[idx_5m - 2][6]
                        dea_now = data_5m_with_macd[idx_5m][7]
                        dea_prev = data_5m_with_macd[idx_5m - 1][7]

                        # DIF 金叉 DEA（DIF 从下方穿越到上方）
                        if dif_prev <= dea_prev and dif_now > dea_now:
                            # 入场：价格突破绿柱堆最高点
                            green_high = max(data_5m_with_macd[i][2] for i in range(green_start, idx_5m + 1))

                            # 检查是否已处理过这个信号时间
                            sig_time_key = f"{sig['created_time']}_{green_start}"
                            if not hasattr(self, '_processed_signals'):
                                self._processed_signals = {}
                            if self._processed_signals.get(symbol) == sig_time_key:
                                continue
                            self._processed_signals[symbol] = sig_time_key

                            # 入场
                            stop_loss = green_high - (green_high - current_green_low) * 0.5

                            self.positions[symbol] = {
                                'entry_price': green_high,
                                'stop_loss': stop_loss,
                                'entry_time': current_time
                            }
                            self.last_entry_times[symbol] = current_time

                            signal_data = {
                                'signal_type': 'ENTRY_LONG',
                                'price': green_high,
                                'stop_loss': stop_loss,
                                'position_size': 1,
                                'reason': f"5分钟绿柱堆DIF金叉+60分钟底背离，入场价{green_high:.2f}，止损{stop_loss:.2f}",
                                'time': current_5m_time
                            }

                            print_log(f"📈 {symbol} 策略开仓信号：{signal_data}")

                            if self.strategy_signal_manager:
                                self.strategy_signal_manager.add_signal(symbol, signal_data)

                            try:
                                send_feishu_strategy_signal(symbol, signal_data)
                                print_log(f"✓ {symbol} 飞书开仓信号已发送")
                            except Exception as e:
                                print_log(f"✗ {symbol} 飞书开仓信号发送失败：{e}")

                            break

        except Exception as e:
            print_log(f"✗ {symbol} 策略信号检查失败：{e}")

    def _check_stop_loss_v2(self, symbol: str, data_5m_with_macd: list, position: dict):
        """检查止损"""
        try:
            idx_5m = len(data_5m_with_macd) - 1
            current_low = data_5m_with_macd[idx_5m][3]
            current_time = data_5m_with_macd[idx_5m][0][:19]

            stop_loss = position.get('stop_loss', 0)
            if stop_loss > 0 and current_low <= stop_loss:
                # 触发止损，平仓
                signal_data = {
                    'signal_type': 'EXIT_LONG',
                    'price': stop_loss,
                    'stop_loss': stop_loss,
                    'position_size': 0,
                    'reason': f"5分钟价格跌破止损价{stop_loss:.2f}，触发止损",
                    'time': current_time
                }

                print_log(f"📉 {symbol} 策略平仓信号（止损）：{signal_data}")

                if self.strategy_signal_manager:
                    self.strategy_signal_manager.add_signal(symbol, signal_data)

                try:
                    send_feishu_strategy_signal(symbol, signal_data)
                    print_log(f"✓ {symbol} 飞书平仓信号已发送")
                except Exception as e:
                    print_log(f"✗ {symbol} 飞书平仓信号发送失败：{e}")

                # 清除持仓
                del self.positions[symbol]

        except Exception as e:
            print_log(f"✗ {symbol} 止损检查失败：{e}")

    def flush_all(self):
        """ 刷新所有周期的 K 线 """
        for period_name, klines in self.current_klines.items():
            _, period_seconds = self.KLINE_PERIODS.get(period_name, (5, 300))
            for instrument_name, kline in klines.items():
                self.save_kline(instrument_name, kline, period_seconds)
            klines.clear()
        print_log("所有周期 K 线已刷新到数据库")


# ==================== 行情 API 回调 ====================

class CMdSpi(CMdSpiBase):

    def __init__(self, instruments, kline_aggregator):
        super().__init__()
        self.instruments = instruments
        self.kline_aggregator = kline_aggregator
        self.tick_count = 0

    def subscribe_market_data(self):
        """ 订阅行情数据 """
        instrument_ids = []
        for inst in self.instruments:
            inst_id = inst.get("MainContractID") or inst.get("InstrumentID", "")
            if inst_id:
                instrument_ids.append(inst_id)

        encode_instruments = [inst_id.encode('utf-8') for inst_id in instrument_ids]

        print_log(f"订阅行情：{', '.join(instrument_ids)}")
        self._check_req(instrument_ids, self._api.SubscribeMarketData(encode_instruments, len(instrument_ids)))

    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """ 行情数据推送 """

        if pDepthMarketData:
            instrument_id = pDepthMarketData.InstrumentID if hasattr(pDepthMarketData, 'InstrumentID') else ""
            last_price = pDepthMarketData.LastPrice if hasattr(pDepthMarketData, 'LastPrice') else 0.0
            volume = pDepthMarketData.Volume if hasattr(pDepthMarketData, 'Volume') else 0
            open_interest = pDepthMarketData.OpenInterest if hasattr(pDepthMarketData, 'OpenInterest') else 0
            action_day = pDepthMarketData.ActionDay if hasattr(pDepthMarketData, 'ActionDay') else ""
            update_time = pDepthMarketData.UpdateTime if hasattr(pDepthMarketData, 'UpdateTime') else ""

            # 检查是否订阅
            subscribed_ids = set()
            for inst in self.instruments:
                inst_id = inst.get("MainContractID") or inst.get("InstrumentID", "")
                if inst_id:
                    subscribed_ids.add(inst_id)

            if instrument_id not in subscribed_ids:
                return

            if last_price <= 0:
                return

            if update_time and action_day:
                try:
                    timestamp = datetime.strptime(f"{action_day} {update_time}", "%Y%m%d %H:%M:%S").timestamp()
                except Exception as e:
                    print_log(f"时间解析失败：{e}")
                    return
            else:
                return

            # 添加到 K 线合成器
            self.kline_aggregator.add_tick(instrument_id, last_price, volume, open_interest, timestamp)

            self.tick_count += 1
            if self.tick_count % 100 == 0:
                total_klines = sum(len(klines) for klines in self.kline_aggregator.current_klines.values())
                first_period_klines = list(self.kline_aggregator.current_klines.values())[0] if self.kline_aggregator.current_klines else {}
                instruments = list(first_period_klines.keys())[:10]
                print_log(f"已处理 {self.tick_count} 个 tick，当前 K 线数：{total_klines} (合约：{', '.join(instruments)}{'...' if len(instruments) < len(first_period_klines) else ''})")

    def release(self):
        """ 释放资源 """
        try:
            self.kline_aggregator.flush_all()
            self._api.Release()
            print_log("行情 API 已释放")
        except Exception as e:
            print_log(f"释放行情 API 失败：{e}")


# ==================== 工具函数 ====================

def load_instruments_from_json(json_file):
    """ 从 JSON 文件加载合约列表 """
    if not os.path.exists(json_file):
        print_log(f"错误：文件不存在 {json_file}")
        return []

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "main_contracts" in data:
            return data["main_contracts"]
        else:
            print_log(f"错误：JSON 文件格式不正确")
            return []
    except Exception as e:
        print_log(f"读取 JSON 文件失败：{e}")
        return []


EXCLUDED_PRODUCTS = ["FB", "BB", "RS", "wr", "rr"]


def is_excluded_product(instrument_id: str) -> bool:
    """检查合约是否属于需要排除的产品"""
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
    """从 main_contracts.json 加载主力合约"""
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
        excluded_count = 0
        for inst in contracts:
            inst_id = inst.get("MainContractID", "") or inst.get("InstrumentID", "")
            if is_excluded_product(inst_id):
                excluded_count += 1
                print_log(f"排除不活跃合约：{inst_id}")
            else:
                filtered_contracts.append(inst)

        print_log(f"从 {json_file} 加载了 {len(filtered_contracts)} 个主力合约（排除 {excluded_count} 个不活跃合约）")
        return filtered_contracts

    except Exception as e:
        print_log(f"读取 JSON 文件失败：{e}")
        return []


# ==================== 主函数 ====================

md_spi_instance = None


def cleanup():
    """ 退出时清理资源 """
    global md_spi_instance

    print_log("\n清理资源...")

    if md_spi_instance:
        try:
            md_spi_instance.release()
        except Exception as e:
            print_log(f"清理行情 API 失败：{e}")

    print_log("资源清理完成")


atexit.register(cleanup)


if __name__ == '__main__':
    print_log("=" * 70)
    print_log("K 线数据采集程序启动 (v2 - 低内存模式)")
    print_log("=" * 70)

    # 解析命令行参数
    import sys
    db_path_arg = None
    use_online = False

    for arg in sys.argv[1:]:
        if arg == "online":
            use_online = True
            print_log("使用线上数据库")
        elif arg == "test":
            use_online = False
            print_log("使用测试数据库")
        else:
            db_path_arg = arg
            print_log(f"使用自定义数据库路径：{arg}")

    # 从 main_contracts.json 加载合约列表
    json_file = "./data/contracts/main_contracts.json"
    instruments = load_main_contracts(json_file)

    if not instruments:
        print_log(f"错误：没有找到合约列表，请检查 {json_file}")
        sys.exit(1)

    print_log(f"合约列表:")
    for inst in instruments:
        exchange_id = inst.get("ExchangeID", "")
        main_contract = inst.get("MainContractID", "") or inst.get("InstrumentID", "")
        print_log(f"  - {main_contract}: {inst.get('InstrumentName', '')} ({exchange_id})")

    # 初始化数据库
    db_manager = DatabaseManager(db_path=db_path_arg, use_online=use_online)
    print_log(f"数据库路径：{db_manager.db_path}")

    # 初始化策略信号管理器
    strategy_signal_manager = StrategySignalManager(STRATEGY_SIGNAL_FILE)

    # 初始化 K 线合成器
    kline_aggregator = KlineAggregator(db_manager, instruments, strategy_signal_manager)

    # 飞书启动通知
    print_log("发送飞书启动通知...")
    import socket
    hostname = socket.gethostname()
    start_msg = f"📊 K线采集服务启动(v2低内存) | 主机: {hostname} | 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 合约数: {len(instruments)} | 数据库: {db_manager.db_path}"
    try:
        notifier = FeishuNotifier()
        notifier.send_text(start_msg)
        print_log("✓ 飞书启动通知已发送")
    except Exception as e:
        print_log(f"✗ 飞书启动通知发送失败：{e}")

    # 初始化行情 API
    print_log("初始化行情 API...")
    md_spi = CMdSpi(instruments, kline_aggregator)
    md_spi_instance = md_spi

    # 订阅行情
    md_spi.subscribe_market_data()

    # 等待程序退出
    print_log("\n程序运行中，按 Ctrl+C 退出...")
    print_log("退出时会自动刷新所有 K 线到数据库")
    print_log("=" * 70)

    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print_log("\n收到退出信号，正在关闭程序...")