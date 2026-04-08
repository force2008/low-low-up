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

# 从拆分后的模块导入（公共类）
from strategy import MACDCalculator, ATRCalculator, StackIdentifier
from strategy.signal_manager import StrategySignalManager
from strategy.index_map import IndexMapper
from database import DatabaseManager

# 导入低-low-up 策略
from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp as Strategy


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
MAX_60M_BARS = 1000  # 60 分钟 K 线（增加数据量以覆盖节假日期间的数据跨度）


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


# ==================== Kline 聚合器 ====================


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

    def get_kline_data(self, symbol: str, limit: int, duration: int, end_time: str = None) -> list:
        """直接从数据库获取 K 线数据（返回 tuple 列表，用于策略计算）"""
        try:
            with DatabaseManager._lock:
                cursor = self.conn.cursor()
                if end_time:
                    cursor.execute('''
                        SELECT datetime, open, high, low, close, volume
                        FROM kline_data
                        WHERE symbol = ? AND duration = ? AND datetime <= ?
                        ORDER BY datetime DESC
                        LIMIT ?
                    ''', (symbol, duration, end_time, limit))
                else:
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

        # 60分钟索引映射（5m索引 -> 60m索引）
        self.index_map_60m = {}  # {symbol: [idx_60m, ...]}

        # 策略名称
        self.strategy_name = Strategy({}).name

        # 近期大幅下跌阈值（跌幅超过此比例时跳过开仓）
        self.large_drop_threshold = 0.05  # 5%

    def check_60m_all_limits(self, symbol: str, data_60m: list, idx_60m: int) -> bool:
        """检查60分钟K线是否都是一字板（涨跌停）- 委托给Strategy处理"""
        strategy = Strategy({})
        return strategy.check_60m_all_limits(data_60m, idx_60m)

    def is_large_60m_drop(self, symbol: str, data_60m: list, current_price: float, data_5m: list = None, lookback: int = 40) -> bool:
        """判断当前60分钟跌幅是否较大（超过过去40根K线跌幅的80分位值）- 委托给Strategy处理"""
        strategy = Strategy({})
        should_filter, reason = strategy.is_large_60m_drop(data_60m, current_price, data_5m, lookback)
        if should_filter:
            logger.info(f"[{symbol}] {reason}")
        return should_filter

    def check_recent_drop(self, symbol: str, precheck_time: str, current_time: str) -> float:
        """检查从预检测信号产生到当前时间的跌幅（统计过去20根K线的最大跌幅）

        Args:
            symbol: 合约代码
            precheck_time: 预检测信号产生的时间（60分钟K线时间）
            current_time: 当前时间

        Returns:
            最大跌幅比例（负值表示涨幅），如果无法计算返回 0
        """
        try:
            # 获取 precheck_time 时刻的 5分钟K线数据（从precheck_time开始到current_time）
            data_5m = self.db_manager.get_kline_data(symbol, 500, 300, current_time)

            if len(data_5m) < 5:
                return 0

            # 找到 precheck_time 对应的K线索引
            start_idx = -1
            for i, row in enumerate(data_5m):
                if row[0] >= precheck_time:
                    start_idx = i
                    break

            if start_idx < 0:
                start_idx = 0

            # 从 precheck_time 开始，统计后面20根K线的最大跌幅
            window_size = 20
            if len(data_5m) - start_idx < window_size:
                window_size = len(data_5m) - start_idx

            if window_size < 5:
                return 0

            # 获取 precheck_time 时的收盘价作为基准
            start_price = data_5m[start_idx][4]

            if start_price <= 0:
                return 0

            # 计算从 precheck_time 到现在，每一根K线相对于 start_price 的跌幅
            # 取最大跌幅（最负的值）
            max_drop = 0
            for i in range(start_idx, start_idx + window_size):
                kline_close = data_5m[i][4]
                drop = (kline_close - start_price) / start_price
                if drop < max_drop:
                    max_drop = drop

            return max_drop
        except Exception as e:
            logger.warning(f"[{symbol}] 计算近期跌幅失败：{e}")
            return 0

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
                self.check_60m_signal_v2(symbol, end_time=date_time_str)
            print_log(f"保存{period_name}K 线：{symbol} {date_time_str}")

    def _get_period_name_by_duration(self, duration: int) -> str:
        """根据周期秒数获取周期名称"""
        for period_name, (_, period_seconds) in self.KLINE_PERIODS.items():
            if period_seconds == duration:
                return period_name
        return f"{duration}s"

    def check_60m_signal_v2(self, symbol: str, end_time: str = None):
        """检查 60 分钟 K 线完成后是否产生预检测信号"""
        try:
            # 从数据库读取 60 分钟 K 线数据
            data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)
            if len(data_60m) < 20:
                return

            # 计算 MACD
            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            if len(data_60m_with_macd) < 5:
                return

            # 识别绿柱堆
            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            idx_60m = len(data_60m_with_macd) - 1
            current_60m_time = data_60m_with_macd[idx_60m][0]

            # ========== 关键修复：验证数据实际存在的时间 ==========
            # 如果传入了 end_time 但数据库没有这个时间点的数据，则不产生预检测信号
            # 例如：end_time=20:00:00 但数据库最后一根是 11:00，说明没有新数据
            if end_time:
                # 解析 end_time 和实际数据时间
                try:
                    end_dt = datetime.strptime(end_time[:19], '%Y-%m-%d %H:%M:%S')
                    actual_dt = datetime.strptime(current_60m_time[:19], '%Y-%m-%d %H:%M:%S')
                    time_diff = (end_dt - actual_dt).total_seconds() / 60
                    # 如果实际数据比 end_time 晚超过10分钟，说明 end_time 时间点没有新数据
                    if time_diff > 10:
                        # 没有新60m数据，跳过
                        return
                except:
                    pass

            hist_60m = data_60m_with_macd[idx_60m][8]
            hist_60m_prev = data_60m_with_macd[idx_60m - 1][8] if idx_60m > 0 else 0

            # ========== 检查连续一字板K线（涨跌停）==========
            # 如果当前K线和前一根K线都是一字板，跳过预检测信号
            if self.check_60m_all_limits(symbol, data_60m_with_macd, idx_60m):
                return

            strategy = Strategy({})

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
                                'sub_type': 'dif_turn',  # 绿柱堆内DIF拐头
                                'created_time': current_60m_time,
                            })
                            print_log(f"📊 {symbol} 60分钟绿柱堆内DIF拐头+底背离，预检测信号")

            # 绿柱堆转红柱堆（不需要检查绿柱堆限制）
            elif hist_60m > 0 and hist_60m_prev < 0:
                diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                if diver_ok:
                    if symbol not in self.precheck_signals_green:
                        self.precheck_signals_green[symbol] = []
                    existing = next((s for s in self.precheck_signals_green[symbol] if s['created_time'] == current_60m_time), None)
                    if not existing:
                        self.precheck_signals_green[symbol].append({
                            'type': 'green',
                            'sub_type': 'green_to_red',  # 绿柱堆转红柱堆
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

    def check_strategy_signal_v2(self, symbol: str, end_time: str = None):
        """检查策略信号（每次从数据库读取数据）"""
        try:
            # 检查是否在持仓
            position = self.positions.get(symbol)

            # 获取当前时间
            current_time = getattr(self, 'current_time', None) or datetime.now()

            # 检查冷却时间
            if not position and symbol in self.last_entry_times:
                last_entry = self.last_entry_times[symbol]
                hours_passed = (current_time - last_entry).total_seconds() / 3600
                if hours_passed < self.cooldown_hours:
                    return

            # 检查预检测信号（先定义，后续用于判断和获取60m数据范围）
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

            # 从数据库读取 5 分钟和 60 分钟 K 线（用 end_time 限制范围，避免用到未来数据）
            data_5m = self.db_manager.get_kline_data(symbol, MAX_5M_BARS, 300, end_time)

            # 如果没有传入 end_time，用预检测信号创建时间限制60m数据范围
            if not end_time:
                # 用最早的有效预检测信号时间作为60m的截止时间
                sig_times = [datetime.strptime(s['created_time'][:19], '%Y-%m-%d %H:%M:%S')
                           for s in valid_precheck]
                earliest_sig = min(sig_times)
                # 60m数据取到预检测信号前一小时
                earliest_sig = earliest_sig - timedelta(hours=1)
                end_time_for_60m = earliest_sig.strftime('%Y-%m-%d %H:%M:%S') + '.000000'
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time_for_60m)
            else:
                data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)

            if len(data_5m) < 20 or len(data_60m) < 20:
                return

            # 计算 MACD 和 ATR
            data_5m_with_macd = MACDCalculator.calculate(data_5m)
            data_60m_with_macd = MACDCalculator.calculate(data_60m)
            data_5m_with_atr = ATRCalculator.calculate(data_5m_with_macd, 14)

            if len(data_5m_with_atr) < 5 or len(data_60m_with_macd) < 5:
                return

            # 构建60分钟索引映射（与回测逻辑一致）
            if symbol not in self.index_map_60m or len(self.index_map_60m.get(symbol, [])) != len(data_5m_with_macd):
                self.index_map_60m[symbol] = IndexMapper.precompute_60m_index(data_5m_with_macd, data_60m_with_macd)
            index_map = self.index_map_60m[symbol]

            # 识别绿柱堆
            _, green_stacks_5m, _ = StackIdentifier.identify(data_5m_with_macd)
            _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)

            strategy = Strategy({})

            # 检查 5 分钟入场条件
            idx_5m = len(data_5m_with_macd) - 1
            current_5m_time = data_5m_with_macd[idx_5m][0][:19]
            current_5m_price = data_5m_with_macd[idx_5m][4]
            current_5m_low = data_5m_with_macd[idx_5m][3]

            # 使用index_map获取正确的60m索引
            idx_60m = index_map[idx_5m] if idx_5m < len(index_map) else len(data_60m_with_macd) - 1
            hist_60m = data_60m_with_macd[idx_60m][8]
            hist_60m_prev = data_60m_with_macd[idx_60m - 1][8] if idx_60m > 0 else 0

            # 如果有持仓，检查止损
            if position:
                self._check_stop_loss_v2(symbol, data_5m_with_macd, position)
                return

            # 检查入场条件
            for sig in valid_precheck:
                sig_type = sig['type']

                if sig_type == 'green':
                    # 获取信号的子类型
                    sub_type = sig.get('sub_type', 'dif_turn')

                    # 检查底背离（两种绿信号都需要）
                    diver_ok, diver_reason, current_green_low, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
                    if not diver_ok:
                        continue

                    # 绿柱堆内DIF拐头：需要检查当前5分钟是否在绿柱堆中
                    if sub_type == 'dif_turn':
                        in_green = False
                        green_start = None
                        for idx, stack in green_stacks_5m.items():
                            if stack['start_idx'] <= idx_5m <= stack['end_idx']:
                                in_green = True
                                green_start = stack['start_idx']
                                break

                        if not in_green:
                            continue
                    else:
                        # 绿柱堆转红柱堆：不需要检查绿柱堆限制
                        green_start = None

                    # 入场条件：5分钟阳柱（收盘价 > 开盘价）
                    current_open = data_5m_with_macd[idx_5m][1]
                    current_price = data_5m_with_macd[idx_5m][4]
                    if current_price <= current_open:
                        continue

                    # 入场价格：突破绿柱堆最高点（绿柱堆内DIF拐头）或当前价格（绿柱堆转红柱堆）
                    if sub_type == 'green_to_red':
                        # 绿柱堆转红柱堆：使用当前价格作为入场价
                        entry_price = current_price
                        # 获取当前绿柱堆的最低价用于计算止损
                        _, green_stacks_60m_temp, _ = StackIdentifier.identify(data_60m_with_macd)
                        if green_stacks_60m_temp:
                            last_green = max(green_stacks_60m_temp.keys())
                            current_green_low = green_stacks_60m_temp[last_green].get('lowest_low', current_price - 50)
                        else:
                            current_green_low = current_price - 50
                    else:
                        # 绿柱堆内DIF拐头：使用当前K线开盘价作为入场价
                        entry_price = current_open

                    # ========== 止损计算 ==========
                    # 如果当前ATR处于70分位以下，使用60分钟前一个绿柱堆最低价
                    # 否则使用5分钟绿柱堆低点（取前一个和前前中较低的）
                    use_60m_stop = False
                    atr_percentile = 0.0
                    stop_loss_reason = ""

                    # 检查ATR百分位 (ATR在第7列，索引6)
                    if len(data_5m_with_atr) > idx_5m and len(data_5m_with_atr[idx_5m]) > 6:
                        current_atr = data_5m_with_atr[idx_5m][6]
                        if current_atr > 0:
                            # 计算ATR百分位
                            lookback = min(200, idx_5m)
                            atr_values = [data_5m_with_atr[i][6] for i in range(max(0, idx_5m - lookback), idx_5m + 1)
                                        if len(data_5m_with_atr[i]) > 6 and data_5m_with_atr[i][6] > 0]
                            if len(atr_values) >= 20:
                                count_below = sum(1 for v in atr_values if v < current_atr)
                                atr_percentile = count_below / len(atr_values)
                                # 如果ATR百分位 < 0.3 (70分位以下)
                                if atr_percentile < 0.3:
                                    use_60m_stop = True
                                    stop_loss_reason = f"(ATR{atr_percentile:.0%}低，使用60分钟绿柱堆)"

                    if use_60m_stop and green_stacks_60m:
                        # 波动率低时使用60分钟前一个绿柱堆最低价（不是前前绿柱堆）
                        available_60m_ids = [sid for sid, info in green_stacks_60m.items()
                                            if info.get('end_idx', -1) >= 0 and info['end_idx'] < idx_60m]
                        if len(available_60m_ids) >= 1:
                            available_60m_ids.sort()
                            prev_60m_green_id = available_60m_ids[-1]  # 前一个绿柱堆
                            stop_loss = green_stacks_60m[prev_60m_green_id]['low']
                        else:
                            stop_loss = current_green_low
                    else:
                        # 使用5分钟前一个绿柱堆的低点（不是前前绿柱堆）
                        # 如果前一个绿柱堆的最低价比前前绿柱堆更低，则用前一个
                        available_green_ids = [sid for sid, info in green_stacks_5m.items()
                                              if info.get('end_idx', -1) >= 0 and info['end_idx'] < idx_5m]
                        if len(available_green_ids) >= 2:
                            available_green_ids.sort()
                            prev_green_id = available_green_ids[-1]      # 前一个绿柱堆
                            prev_prev_green_id = available_green_ids[-2]  # 前前绿柱堆
                            prev_low = green_stacks_5m[prev_green_id]['low']
                            prev_prev_low = green_stacks_5m[prev_prev_green_id]['low']
                            stop_loss = min(prev_low, prev_prev_low)  # 取较低的
                        elif len(available_green_ids) >= 1:
                            available_green_ids.sort()
                            stop_loss = green_stacks_5m[available_green_ids[-1]]['low']
                        else:
                            # 备用：如果没有绿柱堆，使用当前绿柱堆低点
                            stop_loss = current_green_low

                    # ========== 检查止损价是否 >= 入场价 ==========
                    # 如果止损价 >= 入场价，说明是下降趋势，不应该开仓
                    if stop_loss >= entry_price:
                        logger.info(f"[{symbol}] 止损价({stop_loss:.2f}) >= 入场价({entry_price:.2f})，下降趋势不开仓")
                        continue

                    # 检查是否已处理过这个信号时间
                    sig_time_key = f"{sig['created_time']}_{green_start if green_start else 'red'}"
                    if not hasattr(self, '_processed_signals'):
                        self._processed_signals = {}
                    if self._processed_signals.get(symbol) == sig_time_key:
                        continue
                    self._processed_signals[symbol] = sig_time_key

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
                        'reason': f"5分钟绿柱堆阳柱+60分钟底背离，入场价{entry_price:.2f}，止损{stop_loss:.2f} {stop_loss_reason}",
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

                elif sig_type == 'red':
                    # 红柱堆信号：检查底抬升
                    diver_ok, diver_reason, current_green_low, _ = strategy.check_60m_bottom_rise_in_red(data_60m_with_macd, len(data_60m_with_macd) - 1)
                    if not diver_ok:
                        continue

                    # 检查 5 分钟是否在绿柱堆中
                    in_green = False
                    green_start = None
                    for idx, stack in green_stacks_5m.items():
                        if stack['start_idx'] <= idx_5m <= stack['end_idx']:
                            in_green = True
                            green_start = stack['start_idx']
                            break

                    if not in_green:
                        continue

                    # 入场条件：5分钟阳柱（收盘价 > 开盘价）
                    current_open = data_5m_with_macd[idx_5m][1]
                    current_price = data_5m_with_macd[idx_5m][4]
                    if current_price <= current_open:
                        continue
                        logger.info(f"[{symbol}] 近期跌幅超过80分位值，跳过开仓，清空预检测信号")
                        # 清空该品种的所有预检测信号
                        if symbol in self.precheck_signals_green:
                            self.precheck_signals_green[symbol] = []
                        if symbol in self.precheck_signals_red:
                            self.precheck_signals_red[symbol] = []
                        return

                    # 入场：使用当前K线开盘价作为入场价
                    entry_price = current_open

                    # ========== 止损计算 ==========
                    # 如果当前ATR处于70分位以下，使用60分钟前一个绿柱堆最低价
                    # 否则使用5分钟绿柱堆低点（取前一个和前前中较低的）
                    use_60m_stop = False
                    atr_percentile = 0.0
                    stop_loss_reason = ""

                    # 检查ATR百分位 (ATR在第7列，索引6)
                    if len(data_5m_with_atr) > idx_5m and len(data_5m_with_atr[idx_5m]) > 6:
                        current_atr = data_5m_with_atr[idx_5m][6]
                        if current_atr > 0:
                            # 计算ATR百分位
                            lookback = min(200, idx_5m)
                            atr_values = [data_5m_with_atr[i][6] for i in range(max(0, idx_5m - lookback), idx_5m + 1)
                                        if len(data_5m_with_atr[i]) > 6 and data_5m_with_atr[i][6] > 0]
                            if len(atr_values) >= 20:
                                count_below = sum(1 for v in atr_values if v < current_atr)
                                atr_percentile = count_below / len(atr_values)
                                # 如果ATR百分位 < 0.3 (70分位以下)
                                if atr_percentile < 0.3:
                                    use_60m_stop = True
                                    stop_loss_reason = f"(ATR{atr_percentile:.0%}低，使用60分钟绿柱堆)"

                    if use_60m_stop and green_stacks_60m:
                        # 波动率低时使用60分钟前一个绿柱堆最低价（不是前前绿柱堆）
                        available_60m_ids = [sid for sid, info in green_stacks_60m.items()
                                            if info.get('end_idx', -1) >= 0 and info['end_idx'] < idx_60m]
                        if len(available_60m_ids) >= 1:
                            available_60m_ids.sort()
                            prev_60m_green_id = available_60m_ids[-1]  # 前一个绿柱堆
                            stop_loss = green_stacks_60m[prev_60m_green_id]['low']
                        else:
                            stop_loss = current_green_low
                    else:
                        # 使用5分钟前一个绿柱堆的低点（不是前前绿柱堆）
                        # 如果前一个绿柱堆的最低价比前前绿柱堆更低，则用前一个
                        available_green_ids = [sid for sid, info in green_stacks_5m.items()
                                              if info.get('end_idx', -1) >= 0 and info['end_idx'] < idx_5m]
                        if len(available_green_ids) >= 2:
                            available_green_ids.sort()
                            prev_green_id = available_green_ids[-1]      # 前一个绿柱堆
                            prev_prev_green_id = available_green_ids[-2]  # 前前绿柱堆
                            prev_low = green_stacks_5m[prev_green_id]['low']
                            prev_prev_low = green_stacks_5m[prev_prev_green_id]['low']
                            stop_loss = min(prev_low, prev_prev_low)  # 取较低的
                        elif len(available_green_ids) >= 1:
                            available_green_ids.sort()
                            stop_loss = green_stacks_5m[available_green_ids[-1]]['low']
                        else:
                            # 备用：如果没有绿柱堆，使用当前绿柱堆低点
                            stop_loss = current_green_low

                    # ========== 检查止损价是否 >= 入场价 ==========
                    # 如果止损价 >= 入场价，说明是下降趋势，不应该开仓
                    if stop_loss >= entry_price:
                        logger.info(f"[{symbol}] 止损价({stop_loss:.2f}) >= 入场价({entry_price:.2f})，下降趋势不开仓")
                        continue

                    # 检查是否已处理过这个信号时间
                    sig_time_key = f"{sig['created_time']}_{green_start}_red"
                    if not hasattr(self, '_processed_signals'):
                        self._processed_signals = {}
                    if self._processed_signals.get(symbol) == sig_time_key:
                        continue
                    self._processed_signals[symbol] = sig_time_key

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
                        'reason': f"5分钟绿柱堆阳柱+60分钟红柱堆底抬升，入场价{entry_price:.2f}，止损{stop_loss:.2f} {stop_loss_reason}",
                        'time': current_5m_time
                    }

                    print_log(f"📈 {symbol} 策略开仓信号（红柱堆）：{signal_data}")

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
                    'strategy_name': self.strategy_name,
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


# 从配置中导入排除的产品列表
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.strategy_config import Config

# 使用配置中的排除列表
EXCLUDED_PRODUCTS = [p.upper() for p in Config.EXCLUDED_PRODUCTS]


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