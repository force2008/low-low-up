# @Project: https://github.com/Jedore/ctp.examples
# @File:    KlineCollector.py
# @Time:    21/02/2026
# @Author:  Assistant
# @Description: 订阅合约 tick 数据，合成 5 分钟 K 线，存储到 SQLite 数据库，并检查高低波切换

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
from utils.feishu_notifier import FeishuNotifier, send_feishu_signal, send_feishu_high_volatility_alert, send_feishu_strategy_signal, send_feishu_test
from backtest.strategy_engine import LiveStrategyEngine as StrategyEngine, Signal as StrategySignal, SignalType
from backtest.strategy_utils import Config as StrategyConfig


# 配置日志
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, datetime.now().strftime("KlineCollector_%Y%m%d_%H%M%S.log"))

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

# 限制日志大小（避免日志文件过大）
MAX_LOG_SIZE = 100 * 1024 * 1024  # 100MB

def check_log_size():
    """检查日志文件大小，如果超过限制则创建新日志文件"""
    global log_filename, file_handler
    
    try:
        if os.path.exists(log_filename):
            file_size = os.path.getsize(log_filename)
            if file_size > MAX_LOG_SIZE:
                # 创建新的日志文件
                new_log_filename = os.path.join(log_dir, datetime.now().strftime("KlineCollector_%Y%m%d_%H%M%S.log"))
                logger.info(f"日志文件大小 {file_size/1024/1024:.2f}MB 超过限制，切换到新日志文件：{new_log_filename}")
                
                # 移除旧的文件处理器
                logger.removeHandler(file_handler)
                file_handler.close()
                
                # 创建新的日志文件
                log_filename = new_log_filename
                new_file_handler = logging.FileHandler(log_filename, encoding='utf-8')
                new_file_handler.setLevel(logging.INFO)
                new_file_handler.setFormatter(formatter)
                
                # 更新全局变量
                file_handler = new_file_handler
                logger.addHandler(file_handler)
                
                logger.info(f"新日志文件创建成功")
    except Exception as e:
        logger.error(f"检查日志文件大小失败：{e}")

# ==================== 波动率切换检测配置 ====================
HV_FAST_PERIOD = 10      # 快速波动率周期
HV_SLOW_PERIOD = 100      # 慢速波动率周期
RATIO_THRESH = 0.6       # hv_ratio 阈值
PERCENTILE_THRESH = 0.25 # 25% 分位线
EWMA_LAMBDA = 0.85       # EWMA 衰减系数
SIGNAL_COOLDOWN = 3600   # 信号冷却时间（秒）
SIGNAL_FILE = "volatility_switch_signals.json"
STRATEGY_SIGNAL_FILE = "strategy_signals.json"

# ==================== 波动率偏高告警配置 ====================
HV_ALERT_PERCENTILE = 0.75  # 比较分位线（75% 分位）
HV_ALERT_RATIO = 1.2        # 当前波动率超过 75 分位波动率的倍数阈值
HV_ALERT_COOLDOWN = 1800    # 告警冷却时间（30 分钟）
HV_ALERT_FILE = "volatility_high_alerts.json"


def print_log(*args, **kwargs):
    """ 日志输出函数 """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


class VolatilityCalculator:
    """波动率计算器"""
    
    def __init__(self):
        self.percentile_cache = {}
    
    @staticmethod
    def extract_product_from_symbol(symbol: str) -> str:
        """从合约代码中提取产品代码"""
        if not symbol:
            return "DEFAULT"
        product = ""
        for char in symbol:
            if char.isalpha():
                product += char
            else:
                break
        if not product:
            return "DEFAULT"
        return product
    
    def get_annual_factor_for_symbol(self, symbol: str) -> float:
        """获取某合约的年化因子"""
        product_id = self.extract_product_from_symbol(symbol)
        if product_id in PRODUCT_TRADING_MINUTES:
            return get_annual_factor(product_id)
        product_upper = product_id.upper()
        if product_upper in PRODUCT_TRADING_MINUTES:
            return get_annual_factor(product_upper)
        return get_annual_factor("DEFAULT")
    
    def calculate_hv(self, df: pd.DataFrame, period: int, symbol: str = None) -> float:
        """计算历史波动率 (HV)"""
        if len(df) < period:
            return 0.0
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        std = df['returns'].rolling(window=period).std().iloc[-1]
        if pd.isna(std):
            return 0.0
        if symbol:
            annual_factor = self.get_annual_factor_for_symbol(symbol)
        else:
            annual_factor = np.sqrt(58080 / 5)
        hv = std * annual_factor * 100
        return hv
    
    def calculate_hv_ratio(self, df: pd.DataFrame, symbol: str = None) -> float:
        """计算 HV 比率 (fast_hv / slow_hv)"""
        fast_hv = self.calculate_hv(df, HV_FAST_PERIOD, symbol)
        slow_hv = self.calculate_hv(df, HV_SLOW_PERIOD, symbol)
        if slow_hv == 0:
            return 1.0
        return fast_hv / slow_hv
    
    def calculate_volatility_percentile(self, df: pd.DataFrame, window: int = 60, symbol: str = None) -> float:
        """计算当前波动率在历史中的百分位"""
        if len(df) < window:
            return 0.5
        if symbol:
            annual_factor = self.get_annual_factor_for_symbol(symbol)
        else:
            annual_factor = np.sqrt(58080 / 5)
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        rolling_hv = df['returns'].rolling(window=HV_SLOW_PERIOD).std() * annual_factor * 100
        if len(rolling_hv.dropna()) < window:
            return 0.5
        historical_hv = rolling_hv.dropna().iloc[-window:].values
        current_hv = rolling_hv.iloc[-1]
        if pd.isna(current_hv):
            return 0.5
        percentile = (np.sum(historical_hv <= current_hv) - 1) / (len(historical_hv) - 1)
        return max(0.0, min(1.0, percentile))
    
    def is_ewma_declining(self, df: pd.DataFrame, symbol: str = None) -> bool:
        """判断 EWMA 波动率是否在下降"""
        if len(df) < 20:
            return False
        df = df.copy()
        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
        df = df.dropna(subset=['log_ret']).reset_index(drop=True)
        if len(df) < 20:
            return False
        log_rets = df['log_ret'].values
        ewma_var = 0.0
        ewma_values = []
        for i, log_ret in enumerate(log_rets):
            if i == 0:
                ewma_var = log_ret * log_ret
            else:
                ewma_var = EWMA_LAMBDA * ewma_var + (1 - EWMA_LAMBDA) * log_ret * log_ret
            if i >= 10:
                ewma_values.append(np.sqrt(ewma_var))
        if len(ewma_values) < 5:
            return False
        recent_values = ewma_values[-5:]
        x = np.arange(len(recent_values))
        slope = np.polyfit(x, recent_values, 1)[0]
        return slope < 0
    
    def calculate_hv_for_period(self, df: pd.DataFrame, period: int, symbol: str = None) -> float:
        """计算指定周期的历史波动率"""
        return self.calculate_hv(df, period, symbol)
    
    def calculate_ewma_volatility(self, df: pd.DataFrame, lambda_val: float = EWMA_LAMBDA, symbol: str = None) -> np.ndarray:
        """计算 EWMA 波动率序列"""
        df = df.copy()
        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
        df = df.dropna(subset=['log_ret']).reset_index(drop=True)
        
        if len(df) < 20:
            return np.array([])
        
        log_rets = df['log_ret'].values
        ewma_var = 0.0
        ewma_values = []
        
        for i, log_ret in enumerate(log_rets):
            if i == 0:
                ewma_var = log_ret * log_ret
            else:
                ewma_var = lambda_val * ewma_var + (1 - lambda_val) * log_ret * log_ret
            if i >= 10:  # 跳过前 10 个不稳定的值
                ewma_values.append(np.sqrt(ewma_var))
        
        return np.array(ewma_values)
    
    def check_high_volatility_alert(self, df: pd.DataFrame, symbol: str = None) -> dict:
        """检查当前波动率是否超过 75 分位的波动率（使用 EWMA 方法）"""
        if len(df) < HV_SLOW_PERIOD + 60:
            return {'is_alert': False, 'reason': '数据不足'}
        
        # 计算 EWMA 波动率序列
        ewma_values = self.calculate_ewma_volatility(df, lambda_val=EWMA_LAMBDA, symbol=symbol)
        
        if len(ewma_values) < 60:
            return {'is_alert': False, 'reason': '历史数据不足'}
        
        # 当前 EWMA 波动率（最后一个值）
        current_ewma_vol = ewma_values[-1]
        
        # 计算 75 分位的波动率
        hv_75_percentile = float(np.percentile(ewma_values, 75))
        
        if hv_75_percentile == 0:
            return {'is_alert': False, 'reason': '75 分位波动率为 0'}
        
        # 计算比率
        hv_ratio = current_ewma_vol / hv_75_percentile
        
        # 判断是否超过阈值
        is_alert = hv_ratio > HV_ALERT_RATIO
        
        details = {
            'current_hv': float(current_ewma_vol * 100),  # 转换为百分比
            'hv_75_percentile': float(hv_75_percentile * 100),  # 转换为百分比
            'hv_ratio': float(hv_ratio),
            'threshold': HV_ALERT_RATIO,
            'is_alert': bool(is_alert)
        }
        
        if is_alert:
            details['reason'] = f"当前波动率 ({current_ewma_vol * 100:.2f}%) 超过 75 分位波动率 ({hv_75_percentile * 100:.2f}%) 的 {HV_ALERT_RATIO} 倍，比率：{hv_ratio:.2f}"
        else:
            details['reason'] = f"当前波动率 ({current_ewma_vol * 100:.2f}%) 未超过阈值，比率：{hv_ratio:.2f}"
        
        return {'is_alert': is_alert, 'details': details}
    
    def check_high_to_low_switch(self, df: pd.DataFrame, symbol: str = None) -> dict:
        """检查是否发生高波切低波"""
        if len(df) < HV_SLOW_PERIOD + 10:
            return {'is_switch': False, 'reason': '数据不足'}
        percentile = self.calculate_volatility_percentile(df, symbol=symbol)
        hv_ratio = self.calculate_hv_ratio(df, symbol=symbol)
        ewma_declining = self.is_ewma_declining(df, symbol=symbol)
        current_hv = self.calculate_hv(df, HV_SLOW_PERIOD, symbol)
        
        # 将所有值转换为 Python 原生类型，确保 JSON 可序列化
        percentile = float(percentile)
        hv_ratio = float(hv_ratio)
        ewma_declining = bool(ewma_declining)
        current_hv = float(current_hv)
        fast_hv = float(self.calculate_hv(df, HV_FAST_PERIOD, symbol))
        slow_hv = float(self.calculate_hv(df, HV_SLOW_PERIOD, symbol))
        
        details = {
            'percentile': percentile,
            'hv_ratio': hv_ratio,
            'ewma_declining': ewma_declining,
            'current_hv': current_hv,
            'fast_hv': fast_hv,
            'slow_hv': slow_hv
        }
        is_low_vol = percentile < PERCENTILE_THRESH
        is_switch = (
            is_low_vol and 
            ewma_declining and 
            hv_ratio < RATIO_THRESH
        )
        details['is_low_vol'] = bool(is_low_vol)
        details['is_switch'] = bool(is_switch)
        if is_switch:
            details['reason'] = f"低波 (percentile={percentile:.2%}) + EWMA 下降 + hv_ratio={hv_ratio:.3f}"
        else:
            reasons = []
            if not is_low_vol:
                reasons.append(f"非低波 (percentile={percentile:.2%})")
            if not ewma_declining:
                reasons.append("EWMA 未下降")
            if hv_ratio >= RATIO_THRESH:
                reasons.append(f"hv_ratio={hv_ratio:.3f} >= {RATIO_THRESH}")
            details['reason'] = "; ".join(reasons)
        return {'is_switch': is_switch, 'details': details}


class SignalManager:
    """信号管理器"""
    
    def __init__(self, signal_file=SIGNAL_FILE, alert_file=HV_ALERT_FILE):
        self.signal_file = signal_file
        self.alert_file = alert_file
        self.signals = self._load_signals()
        self.alerts = self._load_alerts()
    
    def _load_signals(self) -> dict:
        """加载信号文件"""
        if os.path.exists(self.signal_file):
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                print_log(f"加载信号文件：{self.signal_file}, 共 {len(signals)} 个信号")
                return signals
            except Exception as e:
                print_log(f"加载信号文件失败：{e}")
                return {}
        return {}
    
    def _load_alerts(self) -> dict:
        """加载告警文件"""
        if os.path.exists(self.alert_file):
            try:
                with open(self.alert_file, 'r', encoding='utf-8') as f:
                    alerts = json.load(f)
                print_log(f"加载告警文件：{self.alert_file}, 共 {len(alerts)} 个告警")
                return alerts
            except Exception as e:
                print_log(f"加载告警文件失败：{e}")
                return {}
        return {}
    
    def _save_signals(self):
        """保存信号文件"""
        try:
            with open(self.signal_file, 'w', encoding='utf-8') as f:
                json.dump(self.signals, f, indent=2, ensure_ascii=False)
            print_log(f"保存信号文件：{self.signal_file}")
        except Exception as e:
            print_log(f"保存信号文件失败：{e}")
    
    def _save_alerts(self):
        """保存告警文件"""
        try:
            with open(self.alert_file, 'w', encoding='utf-8') as f:
                json.dump(self.alerts, f, indent=2, ensure_ascii=False)
            print_log(f"保存告警文件：{self.alert_file}")
        except Exception as e:
            print_log(f"保存告警文件失败：{e}")
    
    def can_generate_signal(self, symbol: str) -> bool:
        """检查是否可以生成信号（冷却时间内不重复）"""
        if symbol not in self.signals:
            return True
        last_time = self.signals[symbol].get('last_signal_time')
        if not last_time:
            return True
        try:
            last_dt = datetime.fromisoformat(last_time)
            elapsed = (datetime.now() - last_dt).total_seconds()
            return elapsed >= SIGNAL_COOLDOWN
        except Exception:
            return True
    
    def can_generate_alert(self, symbol: str) -> bool:
        """检查是否可以生成告警（冷却时间内不重复）"""
        if symbol not in self.alerts:
            return True
        last_time = self.alerts[symbol].get('last_alert_time')
        if not last_time:
            return True
        try:
            last_dt = datetime.fromisoformat(last_time)
            elapsed = (datetime.now() - last_dt).total_seconds()
            return elapsed >= HV_ALERT_COOLDOWN
        except Exception:
            return True
    
    def add_signal(self, symbol: str, details: dict):
        """添加信号"""
        self.signals[symbol] = {
            'symbol': symbol,
            'last_signal_time': datetime.now().isoformat(),
            'details': details,
            'trigger_count': self.signals.get(symbol, {}).get('trigger_count', 0) + 1
        }
        self._save_signals()
        print_log(f"★★★ {symbol} 添加信号：{details}")
    
    def add_high_volatility_alert(self, symbol: str, details: dict):
        """添加高波动率告警"""
        self.alerts[symbol] = {
            'symbol': symbol,
            'last_alert_time': datetime.now().isoformat(),
            'details': details,
            'alert_count': self.alerts.get(symbol, {}).get('alert_count', 0) + 1
        }
        self._save_alerts()
        print_log(f"⚠️ {symbol} 添加高波动率告警：{details}")


class StrategySignalManager:
    """策略信号管理器 - 保存交易信号到本地文件"""

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
        if 'extra_data' in signal_data:
            signal_record['extra_data'] = signal_data['extra_data']
        self.signals.append(signal_record)
        self._save_signals()
        print_log(f"📝 {symbol} 保存策略信号：{signal_record['signal_type']} @ {signal_record['price']}")


# 全局变量
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


# 注册退出处理函数
atexit.register(cleanup)


class DatabaseManager:
    """ 数据库管理类 """

    _lock = threading.Lock()  # 类级别的锁，所有实例共享

    # 数据库路径配置
    ONLINE_DB_PATH = "./data/db/kline_data.db"  # 线上数据库路径
    TEST_DB_PATH = "./data/db/kline_data_test.db"  # 测试数据库路径

    def __init__(self, db_path=None, use_online=False):
        """
        初始化数据库

        Args:
            db_path: 自定义数据库路径（优先级最高）
            use_online: 是否使用线上数据库，True 使用线上库，False 使用测试库
        """
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
        # 设置 check_same_thread=False 允许跨线程使用连接
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # 创建 K 线表（与 ImportKlineToSqlite.py 保持一致）
        # source: 1=akshare 导入，2=合成而来，3=tqsdk 导入
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
                UNIQUE(datetime, symbol)
            )
        ''')
        
        # 为现有表添加新字段（如果表已存在但没有这些字段）
        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN update_time TEXT")
            print_log("添加 update_time 字段")
        except Exception:
            pass  # 字段已存在
        
        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN source INTEGER DEFAULT 2")
            print_log("添加 source 字段")
        except Exception:
            pass  # 字段已存在
        
        self.conn.commit()
        
        self.conn.commit()
        print_log(f"数据库初始化完成：{self.db_path}")
    
    def insert_kline(self, symbol, date_time, open_price, close_price, high_price, low_price, volume, open_interest, duration=300, source=2):
        """ 插入 K 线数据
        
        Args:
            source: 数据来源，1=akshare 导入，2=合成而来，3=tqsdk 导入
        """
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
        """获取 K 线历史数据
        
        Args:
            symbol: 合约代码
            limit: 获取 K 线数量
            duration: 周期秒数 (300=5 分钟，1800=30 分钟，3600=60 分钟，86400=1 天)
        """
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
                    # 使用 ISO8601 格式解析，支持带微秒的时间格式
                    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
                    df = df.sort_values('datetime').reset_index(drop=True)
                    return df
                
                return pd.DataFrame()
        except Exception as e:
            print_log(f"获取 K 线历史失败：{e}")
            return pd.DataFrame()
    
    def close(self):
        """ 关闭数据库连接 """
        if self.conn:
            self.conn.close()
            self.conn = None


class KlineAggregator:
    """ K 线合成器 - 支持多周期 K 线合成 """
    
    # K 线周期配置 (周期名称：(分钟数，秒数))
    KLINE_PERIODS = {
        "5min": (5, 300),
        "30min": (30, 1800),
        "60min": (60, 3600),
        "day": (1440, 86400)  # 1 天 = 24*60 = 1440 分钟
    }
    
    def __init__(self, db_manager, instruments, vol_calculator=None, signal_manager=None, enable_strategy=True, strategy_signal_manager=None):
        self.db_manager = db_manager
        self.instruments = instruments
        # 多周期 K 线数据 {period_name: {instrument_name: kline_data}}
        self.current_klines = {}
        for period_name in self.KLINE_PERIODS:
            self.current_klines[period_name] = {}
        # 合约映射：使用 MainContractID（main_contracts.json 格式）或 InstrumentID（subscribe_market.json 格式）
        self.instrument_map = {}
        for inst in instruments:
            # 优先使用 MainContractID，如果没有则使用 InstrumentID
            key = inst.get("MainContractID") or inst.get("InstrumentID", "")
            self.instrument_map[key] = inst
        self.vol_calculator = vol_calculator
        self.signal_manager = signal_manager
        self.strategy_signal_manager = strategy_signal_manager
        self.db_conn = db_manager.conn
        self.db_cursor = db_manager.cursor

        # 策略引擎配置
        self.enable_strategy = enable_strategy
        self.strategy_engines = {}  # {symbol: StrategyEngine}
        self.strategy_engine_cache = {}  # {symbol: bool} 记录是否已初始化
        self.strategy_signal_file = STRATEGY_SIGNAL_FILE
        
        # 数据缓存：减少数据库查询
        self.kline_cache = {}  # {(symbol, duration): (timestamp, DataFrame)}
        self.cache_timeout = 30  # 缓存超时时间（秒）
        
        # 获取数据库路径和合约配置路径（使用 KlineCollector 的配置）
        self.db_path = db_manager.db_path
        self.contracts_path = "./data/contracts/main_contracts.json"  # 使用当前目录的 main_contracts.json
        
        # 延迟加载策略引擎：只在需要时初始化
        if enable_strategy:
            print_log("策略引擎已启用（延迟加载模式）")
            print_log(f"  数据库路径：{self.db_path}")
            print_log(f"  合约配置：{self.contracts_path}")
            print_log(f"  监控合约数：{len(self.instrument_map)}")
            print_log(f"  策略引擎将在首次检测信号时初始化")
    
    def _get_kline_time(self, dt: datetime, period_minutes: int) -> datetime:
        """获取当前时间所在的 K 线周期起始时间"""
        if period_minutes == 1440:  # 日线
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return dt.replace(minute=(dt.minute // period_minutes) * period_minutes, second=0, microsecond=0)
    
    def add_tick(self, instrument_name, price, volume, open_interest, timestamp):
        """ 添加 tick 数据，合成多周期 K 线 """
        
        dt = datetime.fromtimestamp(timestamp)
        
        # 为每个周期合成 K 线
        for period_name, (period_minutes, period_seconds) in self.KLINE_PERIODS.items():
            kline_time = self._get_kline_time(dt, period_minutes)
            
            # 初始化 K 线数据
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
            
            # 检查是否是新的 K 线周期
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
    
    def get_kline_history(self, symbol: str, limit: int = 100, duration: int = 300) -> pd.DataFrame:
        """获取 K 线历史数据 - 使用缓存减少数据库查询
        
        Args:
            symbol: 合约代码
            limit: 获取 K 线数量
            duration: 周期秒数 (300=5 分钟，1800=30 分钟，3600=60 分钟，86400=1 天)
        """
        cache_key = (symbol, duration)
        current_time = datetime.now()
        
        # 检查缓存
        if cache_key in self.kline_cache:
            cache_time, cached_df = self.kline_cache[cache_key]
            if (current_time - cache_time).total_seconds() < self.cache_timeout:
                # 缓存有效
                if len(cached_df) >= limit:
                    return cached_df.head(limit)
        
        # 缓存未命中或过期，从数据库查询
        df = self.db_manager.get_kline_history(symbol, limit, duration)
        
        # 更新缓存
        self.kline_cache[cache_key] = (current_time, df)
        
        # 定期清理缓存（保留最近使用的100个）
        if len(self.kline_cache) > 100:
            # 按时间排序，删除最旧的
            sorted_cache = sorted(self.kline_cache.items(), key=lambda x: x[1][0])
            for key, _ in sorted_cache[:50]:
                del self.kline_cache[key]
        
        return df
    
    def save_kline(self, instrument_name, kline, duration=300):
        """ 保存 K 线到数据库 
        
        Args:
            instrument_name: 合约名称
            kline: K 线数据字典
            duration: 周期秒数 (300=5 分钟，1800=30 分钟，3600=60 分钟，86400=1 天)
        """
        # 定期检查日志文件大小（每100次检查一次）
        if not hasattr(self, '_kline_save_count'):
            self._kline_save_count = 0
        self._kline_save_count += 1
        if self._kline_save_count % 100 == 0:
            check_log_size()
        # 获取交易所信息
        instrument = self.instrument_map.get(instrument_name, {})
        exchange_id = instrument.get("ExchangeID", "")
        
        # 构建 symbol：统一使用 ExchangeID.MainContractID 格式（与 ImportKlineToSqlite.py 保持一致）
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
            source=2  # 2=合成而来
        )
        
        # 只在 5 分钟 K 线保存后检查策略信号
        if duration == 300:
            print_log(f"保存 K 线：{symbol} {date_time_str} O={kline['open']:.2f} H={kline['high']:.2f} L={kline['low']:.2f} C={kline['close']:.2f} V={kline['vol']} OI={kline['open_interest']}")
            self.check_strategy_signal(symbol)
        else:
            # 其他周期只记录日志
            period_name = self._get_period_name_by_duration(duration)
            print_log(f"保存{period_name}K 线：{symbol} {date_time_str} O={kline['open']:.2f} H={kline['high']:.2f} L={kline['low']:.2f} C={kline['close']:.2f} V={kline['vol']} OI={kline['open_interest']}")
    
    def _get_period_name_by_duration(self, duration: int) -> str:
        """根据周期秒数获取周期名称"""
        for period_name, (_, period_seconds) in self.KLINE_PERIODS.items():
            if period_seconds == duration:
                return period_name
        return f"{duration}s"

    def check_strategy_signal(self, symbol: str):
        """检查趋势反转策略信号（MACD 多周期底背离策略 - strategy_0328.py）

        在 5 分钟 K 线完成后调用策略引擎检查信号，检测到信号后发送到飞书
        """
        if not self.enable_strategy:
            return

        if symbol not in self.strategy_engines:
            # 延迟加载策略引擎：首次使用时才初始化
            if symbol not in self.strategy_engine_cache:
                print_log(f"首次检测 {symbol} 信号，初始化策略引擎...")
                try:
                    # 使用 strategy_0328.py 的 Config 和 LiveStrategyEngine
                    config = StrategyConfig()
                    config.DB_PATH = self.db_path
                    config.CONTRACTS_PATH = self.contracts_path
                    engine = StrategyEngine(symbol, config)
                    engine.initialize()
                    self.strategy_engines[symbol] = engine
                    self.strategy_engine_cache[symbol] = True
                    print_log(f"✓ {symbol} 策略引擎初始化成功")
                except Exception as e:
                    print_log(f"✗ {symbol} 策略引擎初始化失败：{e}")
                    self.strategy_engine_cache[symbol] = False
                    return
            else:
                # 已经尝试过初始化但失败了
                return

        engine = self.strategy_engines[symbol]

        try:
            # 获取最新的 5 分钟 K 线
            bar = self.get_latest_5m_bar(symbol)
            if bar is None:
                return

            # 处理 5 分钟 K 线（策略引擎会合成 60 分钟 K 线并检查策略）
            engine.on_5m_bar(bar)

            # 获取生成的信号
            signals = engine.get_signals(clear=True)

            # 发送信号到飞书
            for signal in signals:
                if signal.signal_type == SignalType.ENTRY_LONG:
                    signal_data = {
                        'signal_type': 'ENTRY_LONG',
                        'price': signal.price,
                        'stop_loss': signal.stop_loss,
                        'position_size': signal.position_size,
                        'reason': signal.reason,
                        'time': signal.time
                    }
                    if hasattr(signal, 'extra_data') and signal.extra_data:
                        signal_data['extra_data'] = signal.extra_data
                    print_log(f"📈 {symbol} 策略开仓信号：{signal_data}")

                    # 保存到文件
                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    try:
                        send_feishu_strategy_signal(symbol, signal_data)
                        print_log(f"✓ {symbol} 飞书开仓信号已发送")
                    except Exception as e:
                        print_log(f"✗ {symbol} 飞书开仓信号发送失败：{e}")

                elif signal.signal_type == SignalType.EXIT_LONG:
                    signal_data = {
                        'signal_type': 'EXIT_LONG',
                        'price': signal.price,
                        'stop_loss': 0,
                        'position_size': 0,
                        'reason': signal.reason,
                        'time': signal.time
                    }
                    print_log(f"📉 {symbol} 策略平仓信号：{signal_data}")

                    # 保存到文件
                    if self.strategy_signal_manager:
                        self.strategy_signal_manager.add_signal(symbol, signal_data)

                    try:
                        send_feishu_strategy_signal(symbol, signal_data)
                        print_log(f"✓ {symbol} 飞书平仓信号已发送")
                    except Exception as e:
                        print_log(f"✗ {symbol} 飞书平仓信号发送失败：{e}")

        except Exception as e:
            print_log(f"✗ {symbol} 策略信号检查失败：{e}")
    
    def get_latest_5m_bar(self, symbol: str) -> tuple:
        """获取最新的 5 分钟 K 线
        
        返回：(datetime, open, high, low, close, volume)
        """
        try:
            df = self.get_kline_history(symbol, limit=1, duration=300)
            if len(df) > 0:
                row = df.iloc[0]
                return (
                    row['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume'])
                )
        except Exception as e:
            print_log(f"获取最新 K 线失败 {symbol}: {e}")
        return None
    
    def flush_all(self):
        """ 刷新所有周期的 K 线 """
        for period_name, klines in self.current_klines.items():
            _, period_seconds = self.KLINE_PERIODS.get(period_name, (5, 300))
            for instrument_name, kline in klines.items():
                self.save_kline(instrument_name, kline, period_seconds)
            klines.clear()
        print_log("所有周期 K 线已刷新到数据库")


class CMdSpi(CMdSpiBase):
    
    def __init__(self, instruments, kline_aggregator):
        super().__init__()
        self.instruments = instruments
        self.kline_aggregator = kline_aggregator
        self.tick_count = 0
    
    def subscribe_market_data(self):
        """ 订阅行情数据 """
        
        # 使用 MainContractID（main_contracts.json 格式）或 InstrumentID（subscribe_market.json 格式）
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
            
            # 检查是否是订阅的合约（兼容 MainContractID 和 InstrumentID）
            subscribed_ids = set()
            for inst in self.instruments:
                inst_id = inst.get("MainContractID") or inst.get("InstrumentID", "")
                if inst_id:
                    subscribed_ids.add(inst_id)
            
            if instrument_id not in subscribed_ids:
                return
            
            # 检查价格是否有效
            if last_price <= 0:
                return
            
            # 构建时间戳
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
                # 计算实际合约数量（每个周期都有独立的 K 线数据）
                total_klines = sum(len(klines) for klines in self.kline_aggregator.current_klines.values())
                # 获取第一个周期的合约列表用于显示
                first_period_klines = list(self.kline_aggregator.current_klines.values())[0] if self.kline_aggregator.current_klines else {}
                instruments = list(first_period_klines.keys())[:10]  # 只显示前 10 个
                print_log(f"已处理 {self.tick_count} 个 tick，当前 K 线数：{total_klines} (合约：{', '.join(instruments)}{'...' if len(instruments) < len(first_period_klines) else ''})")
    
    def release(self):
        """ 释放资源 """
        try:
            # 刷新所有 K 线
            self.kline_aggregator.flush_all()
            self._api.Release()
            print_log("行情 API 已释放")
        except Exception as e:
            print_log(f"释放行情 API 失败：{e}")


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


# 需要排除的不活跃合约列表（产品代码）
EXCLUDED_PRODUCTS = ["FB", "BB","RS","wr","rr"]


def is_excluded_product(instrument_id: str) -> bool:
    """检查合约是否属于需要排除的产品"""
    if not instrument_id:
        return False
    
    # 检查是否以_f 结尾（如 l_f）
    if instrument_id.lower().endswith('_f'):
        return True
    
    # 提取产品代码（字母部分）
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
        
        # 过滤掉不活跃合约
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


if __name__ == '__main__':
    print_log("=" * 70)
    print_log("K 线数据采集程序启动")
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
    
    # 初始化数据库（根据命令行参数选择数据库）
    db_manager = DatabaseManager(db_path=db_path_arg, use_online=use_online)
    print_log(f"数据库路径：{db_manager.db_path}")
    
    # 初始化波动率计算器和信号管理器
    vol_calculator = VolatilityCalculator()
    signal_manager = SignalManager(SIGNAL_FILE)
    strategy_signal_manager = StrategySignalManager(STRATEGY_SIGNAL_FILE)

    # 初始化 K 线合成器（传入波动率检测组件和策略信号管理器）
    kline_aggregator = KlineAggregator(db_manager, instruments, vol_calculator, signal_manager, strategy_signal_manager=strategy_signal_manager)

    # 飞书启动通知
    print_log("发送飞书启动通知...")
    import socket
    hostname = socket.gethostname()
    start_msg = f"📊 K线采集服务启动 | 主机: {hostname} | 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 合约数: {len(instruments)} | 数据库: {db_manager.db_path}"
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
