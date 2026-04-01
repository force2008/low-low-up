# @Project: https://github.com/Jedore/ctp.examples
# @File:    VolatilitySwitchMonitor.py
# @Time:    2026/03/11
# @Author:  Assistant
# @Description: 订阅主力合约行情，合成 5 分钟 K 线，检测高波切低波信号

import json
import os
import sys
import time
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from ctp.base_mdapi import CMdSpiBase, mdapi
from config import config
from config.trading_time_config import (
    PRODUCT_TRADING_MINUTES,
    TRADING_DAYS_PER_YEAR,
    get_annual_factor
)
from utils.feishu_notifier import FeishuNotifier


# ==================== 配置参数 ====================
# 波动率计算参数
HV_FAST_PERIOD = 10      # 快速波动率周期
HV_SLOW_PERIOD = 100      # 慢速波动率周期
RATIO_THRESH = 0.6       # hv_ratio 阈值
PERCENTILE_THRESH = 0.25 # 25% 分位线
EWMA_LAMBDA = 0.85       # EWMA 衰减系数 (RiskMetrics 标准)
SIGNAL_COOLDOWN = 3600   # 信号冷却时间（秒）= 1 小时

# 数据库配置
DB_PATH = "kline_data.db"
SIGNAL_FILE = "volatility_switch_signals.json"

# 日志配置
LOG_FILE = datetime.now().strftime("VolatilitySwitch_%Y%m%d_%H%M%S.log")


def log(*args, **kwargs):
    """日志输出"""
    message = ' '.join(str(arg) for arg in args)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
    # 写入日志文件
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")


class KlineManager:
    """K 线合成管理器"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.init_database()
        # 内存中的 K 线数据缓存 {symbol: {bar_time: {...}}}
        self.kline_cache = defaultdict(dict)
        # 当前 bar 的时间窗口
        self.current_bar_time = {}
        
    def init_database(self):
        """初始化数据库"""
        # 使用 check_same_thread=False 允许跨线程使用 SQLite 连接
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # 创建 K 线表
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
                duration INTEGER NOT NULL
            )
        ''')
        
        # 创建唯一索引
        self.cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_kline_data_datetime_symbol 
            ON kline_data(datetime, symbol)
        ''')
        
        self.conn.commit()
        log(f"数据库初始化完成：{self.db_path}")
    
    def get_bar_time(self, tick_time: datetime) -> datetime:
        """获取 5 分钟 K 线的时间窗口"""
        # 5 分钟 = 300 秒
        minutes = (tick_time.hour * 60 + tick_time.minute) // 5 * 5
        bar_hour = minutes // 60
        bar_minute = minutes % 60
        return tick_time.replace(hour=bar_hour, minute=bar_minute, second=0, microsecond=0)
    
    def update_tick(self, symbol: str, tick_data: dict):
        """更新 tick 数据到 K 线"""
        current_time = datetime.now()
        bar_time = self.get_bar_time(current_time)
        
        # 如果这是新的 bar，保存上一个 bar
        if symbol in self.current_bar_time and self.current_bar_time[symbol] != bar_time:
            self._save_bar(symbol)
        
        self.current_bar_time[symbol] = bar_time
        
        # 初始化或更新当前 bar
        if bar_time not in self.kline_cache[symbol]:
            self.kline_cache[symbol][bar_time] = {
                'open': tick_data.get('LastPrice', 0),
                'high': tick_data.get('LastPrice', 0),
                'low': tick_data.get('LastPrice', 0),
                'close': tick_data.get('LastPrice', 0),
                'volume': tick_data.get('Volume', 0),
                'close_oi': tick_data.get('OpenInterest', 0),
                'vwap': tick_data.get('LastPrice', 0),
                'count': 1
            }
        else:
            bar = self.kline_cache[symbol][bar_time]
            price = tick_data.get('LastPrice', 0)
            bar['high'] = max(bar['high'], price)
            bar['low'] = min(bar['low'], price)
            bar['close'] = price
            bar['volume'] = tick_data.get('Volume', 0)
            bar['close_oi'] = tick_data.get('OpenInterest', 0)
            bar['count'] += 1
    
    def _save_bar(self, symbol: str):
        """保存 K 线到数据库"""
        if symbol not in self.current_bar_time:
            return
            
        bar_time = self.current_bar_time[symbol]
        if bar_time not in self.kline_cache[symbol]:
            return
            
        bar = self.kline_cache[symbol][bar_time]
        if bar.get('count', 0) == 0:
            return
        
        try:
            # 删除该 symbol 在该时间的旧数据
            self.cursor.execute(
                "DELETE FROM kline_data WHERE symbol = ? AND datetime = ?",
                (symbol, bar_time.strftime('%Y-%m-%d %H:%M:%S'))
            )
            
            # 插入新数据
            self.cursor.execute('''
                INSERT INTO kline_data 
                (datetime, open, high, low, close, volume, close_oi, vwap, symbol, duration)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                bar_time.strftime('%Y-%m-%d %H:%M:%S'),
                bar['open'],
                bar['high'],
                bar['low'],
                bar['close'],
                bar['volume'],
                bar['close_oi'],
                bar['vwap'],
                symbol,
                300  # 5 分钟 = 300 秒
            ))
            
            self.conn.commit()
            log(f"保存 K 线：{symbol} @ {bar_time}")
            
        except Exception as e:
            log(f"保存 K 线失败：{e}")
    
    def get_kline_history(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        """获取 K 线历史数据"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT datetime, open, high, low, close, volume, close_oi, vwap
                FROM kline_data 
                WHERE symbol = ? 
                ORDER BY datetime DESC 
                LIMIT ?
            ''', (symbol, limit))
            
            rows = cursor.fetchall()
            
            if len(rows) > 0:
                df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'close_oi', 'vwap'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.sort_values('datetime').reset_index(drop=True)
                return df
            
            return pd.DataFrame()
        except Exception as e:
            log(f"获取 K 线历史失败：{e}")
            return pd.DataFrame()
    
    def close(self):
        """关闭数据库"""
        # 保存所有未保存的 bar
        for symbol in list(self.current_bar_time.keys()):
            self._save_bar(symbol)
        
        if self.conn:
            self.conn.close()
            self.conn = None
            log("数据库连接已关闭")


class VolatilityCalculator:
    """波动率计算器"""
    
    def __init__(self):
        self.percentile_cache = {}
        # 缓存合约到交易所的映射 {symbol: exchange_id}
        self.symbol_exchange_map = {}
    
    @staticmethod
    def extract_product_from_symbol(symbol: str) -> str:
        """
        从合约代码中提取产品代码（ProductID）
        
        规则：提取字母部分作为产品代码
        例如：
        - IC2606 -> IC
        - IF2603 -> IF
        - au2606 -> au
        - m2605 -> m
        """
        if not symbol:
            return "DEFAULT"
        
        # 提取产品前缀（字母部分）
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
        """
        获取某合约的年化因子
        
        根据产品代码从 PRODUCT_TRADING_MINUTES 中查找每日交易分钟数
        如果找不到具体产品，则使用交易所默认值
        """
        product_id = self.extract_product_from_symbol(symbol)
        
        # 首先尝试直接使用产品代码查找
        if product_id in PRODUCT_TRADING_MINUTES:
            return get_annual_factor(product_id)
        
        # 如果产品代码找不到，尝试大写形式（针对郑商所等大写品种）
        product_upper = product_id.upper()
        if product_upper in PRODUCT_TRADING_MINUTES:
            return get_annual_factor(product_upper)
        
        # 最后使用默认值
        return get_annual_factor("DEFAULT")
    
    def calculate_hv(self, df: pd.DataFrame, period: int, symbol: str = None) -> float:
        """计算历史波动率 (HV)"""
        if len(df) < period:
            return 0.0
        
        # 计算收益率
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        
        # 计算滚动标准差
        std = df['returns'].rolling(window=period).std().iloc[-1]
        
        if pd.isna(std):
            return 0.0
        
        # 根据合约获取年化因子
        if symbol:
            annual_factor = self.get_annual_factor_for_symbol(symbol)
        else:
            # 默认使用 240 分钟/天 * 242 天 = 58080 分钟/年
            annual_factor = np.sqrt(58080 / 5)
        
        hv = std * annual_factor * 100  # 转换为百分比
        return hv
    
    def calculate_ewma_volatility(self, df: pd.DataFrame, symbol: str = None) -> float:
        """
        计算 EWMA 波动率 (RiskMetrics 方法)
        
        公式：
        ewma_var = EWMA_LAMBDA * ewma_var[1] + (1 - EWMA_LAMBDA) * log_ret^2
        
        Args:
            df: K 线数据
            symbol: 合约代码
            
        Returns:
            float: EWMA 波动率 (百分比)
        """
        if len(df) < 10:
            return 0.0
        
        df = df.copy()
        # 使用对数收益率
        df['log_ret'] = np.log(df['close'] / df['close'].shift(1)).dropna()
        
        if len(df['log_ret']) < 10:
            return 0.0
        
        # 获取对数收益率数组
        log_rets = df['log_ret'].values
        
        # 初始化 EWMA 方差
        ewma_var = 0.0
        
        # 迭代计算 EWMA 方差
        for i, log_ret in enumerate(log_rets):
            if i == 0:
                # 第一个值：ewma_var = log_ret^2
                ewma_var = log_ret * log_ret
            else:
                # ewma_var = λ * ewma_var[1] + (1 - λ) * log_ret^2
                ewma_var = EWMA_LAMBDA * ewma_var + (1 - EWMA_LAMBDA) * log_ret * log_ret
        
        ewma_std = np.sqrt(ewma_var)
        
        if pd.isna(ewma_std) or ewma_std == 0:
            return 0.0
        
        # 根据合约获取年化因子
        if symbol:
            annual_factor = self.get_annual_factor_for_symbol(symbol)
        else:
            annual_factor = np.sqrt(58080 / 5)
        
        return ewma_std * annual_factor * 100
    
    def calculate_hv_ratio(self, df: pd.DataFrame, symbol: str = None) -> float:
        """计算 HV 比率 (fast_hv / slow_hv)"""
        fast_hv = self.calculate_hv(df, HV_FAST_PERIOD, symbol)
        slow_hv = self.calculate_hv(df, HV_SLOW_PERIOD, symbol)
        
        if slow_hv == 0:
            return 1.0
        
        return fast_hv / slow_hv
    
    def calculate_volatility_percentile(self, df: pd.DataFrame, window: int = 60, symbol: str = None) -> float:
        """
        计算当前波动率在历史中的百分位
        
        使用 scipy.stats.percentileofscore 计算当前波动率的百分位排名
        
        Args:
            df: K 线数据
            window: 历史窗口长度
            symbol: 合约代码
            
        Returns:
            float: 百分位值 (0-1)
        """
        if len(df) < window:
            return 0.5
        
        # 获取年化因子
        if symbol:
            annual_factor = self.get_annual_factor_for_symbol(symbol)
        else:
            annual_factor = np.sqrt(58080 / 5)
        
        # 计算滚动 HV
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        rolling_hv = df['returns'].rolling(window=HV_SLOW_PERIOD).std() * annual_factor * 100
        
        if len(rolling_hv.dropna()) < window:
            return 0.5
        
        # 获取历史 HV 序列
        historical_hv = rolling_hv.dropna().iloc[-window:].values
        current_hv = rolling_hv.iloc[-1]
        
        if pd.isna(current_hv):
            return 0.5
        
        # 使用 numpy 计算百分位：当前值在历史序列中的排名
        # 即有多少比例的历史值小于等于当前值
        percentile = (np.sum(historical_hv <= current_hv) - 1) / (len(historical_hv) - 1)
        
        # 确保在 0-1 范围内
        return max(0.0, min(1.0, percentile))
    
    def is_ewma_declining(self, df: pd.DataFrame, symbol: str = None) -> bool:
        """
        判断 EWMA 波动率是否在下降
        
        使用 RiskMetrics 方法计算 EWMA，然后检查最近 5 个值的趋势
        
        Args:
            df: K 线数据
            symbol: 合约代码
            
        Returns:
            bool: 是否在下降
        """
        if len(df) < 20:
            return False
        
        df = df.copy()
        # 使用对数收益率
        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
        
        # dropna 并重置索引，确保 values 是连续的
        df = df.dropna(subset=['log_ret']).reset_index(drop=True)
        
        if len(df) < 20:
            return False
        
        # 获取对数收益率数组
        log_rets = df['log_ret'].values
        
        # 计算 EWMA 方差（一次性遍历）
        ewma_var = 0.0
        ewma_values = []
        
        for i, log_ret in enumerate(log_rets):
            if i == 0:
                # 第一个值：ewma_var = log_ret^2
                ewma_var = log_ret * log_ret
            else:
                # ewma_var = λ * ewma_var[1] + (1 - λ) * log_ret^2
                ewma_var = EWMA_LAMBDA * ewma_var + (1 - EWMA_LAMBDA) * log_ret * log_ret
            
            # 从第 10 个值开始记录 EWMA（确保有足够的历史）
            if i >= 10:
                ewma_values.append(np.sqrt(ewma_var))
        
        if len(ewma_values) < 5:
            return False
        
        # 比较最近 5 个值的趋势
        recent_values = ewma_values[-5:]
        
        # 简单线性回归判断趋势
        x = np.arange(len(recent_values))
        slope = np.polyfit(x, recent_values, 1)[0]
        
        return slope < 0
    
    def check_high_to_low_switch(self, df: pd.DataFrame, symbol: str = None) -> dict:
        """
        检查是否发生高波切低波
        
        条件:
        1. 当前波动度低于 25% 分位线
        2. EWMA 波动率正在下降
        3. hv_ratio < 0.6
        
        Returns:
            dict: {'is_switch': bool, 'details': {...}}
        """
        if len(df) < HV_SLOW_PERIOD + 10:
            return {'is_switch': False, 'reason': '数据不足'}
        
        # 计算各项指标
        percentile = self.calculate_volatility_percentile(df, symbol=symbol)
        hv_ratio = self.calculate_hv_ratio(df, symbol=symbol)
        ewma_declining = self.is_ewma_declining(df, symbol=symbol)
        current_hv = self.calculate_hv(df, HV_SLOW_PERIOD, symbol)
        
        details = {
            'percentile': percentile,
            'hv_ratio': hv_ratio,
            'ewma_declining': ewma_declining,
            'current_hv': current_hv,
            'fast_hv': self.calculate_hv(df, HV_FAST_PERIOD, symbol),
            'slow_hv': self.calculate_hv(df, HV_SLOW_PERIOD, symbol)
        }
        
        # 检查条件
        is_low_vol = percentile < PERCENTILE_THRESH
        is_switch = (
            is_low_vol and 
            ewma_declining and 
            hv_ratio < RATIO_THRESH
        )
        
        details['is_low_vol'] = is_low_vol
        details['is_switch'] = is_switch
        
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
    
    def __init__(self, signal_file=SIGNAL_FILE):
        self.signal_file = signal_file
        self.signals = self._load_signals()
    
    def _load_signals(self) -> dict:
        """加载信号文件"""
        if os.path.exists(self.signal_file):
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                log(f"加载信号文件：{self.signal_file}, 共 {len(signals)} 个信号")
                return signals
            except Exception as e:
                log(f"加载信号文件失败：{e}")
                return {}
        return {}
    
    def _save_signals(self):
        """保存信号文件"""
        try:
            with open(self.signal_file, 'w', encoding='utf-8') as f:
                json.dump(self.signals, f, indent=2, ensure_ascii=False)
            log(f"保存信号文件：{self.signal_file}")
        except Exception as e:
            log(f"保存信号文件失败：{e}")
    
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
    
    def add_signal(self, symbol: str, details: dict):
        """添加信号"""
        self.signals[symbol] = {
            'symbol': symbol,
            'last_signal_time': datetime.now().isoformat(),
            'details': details,
            'trigger_count': self.signals.get(symbol, {}).get('trigger_count', 0) + 1
        }
        self._save_signals()
        log(f"添加信号：{symbol}, 详情：{details}")
    
    def get_new_signals(self, since: datetime = None) -> list:
        """获取指定时间之后的新信号"""
        if since is None:
            since = datetime.now() - timedelta(hours=1)
        
        new_signals = []
        for symbol, data in self.signals.items():
            try:
                signal_time = datetime.fromisoformat(data.get('last_signal_time', ''))
                if signal_time > since:
                    new_signals.append(data)
            except Exception:
                continue
        
        return new_signals


class VolatilityMonitorSpi(CMdSpiBase):
    """波动率监控行情回调"""
    
    def __init__(self, instruments: list, kline_manager: KlineManager, 
                 vol_calculator: VolatilityCalculator, signal_manager: SignalManager,
                 feishu_notifier: FeishuNotifier = None, contract_symbol_map: dict = None):
        super().__init__()
        self.instruments = instruments
        self.kline_manager = kline_manager
        self.vol_calculator = vol_calculator
        self.signal_manager = signal_manager
        self.feishu_notifier = feishu_notifier
        self.contract_symbol_map = contract_symbol_map or {}
        self.tick_count = 0
        self.last_check_time = {}
        
    def req(self, instruments: list):
        """订阅行情"""
        log(f"订阅行情：{instruments}")
        encode_instruments = [i.encode('utf-8') for i in instruments]
        self._check_req(instruments, self._api.SubscribeMarketData(encode_instruments, len(instruments)))
    
    def OnRspSubMarketData(self, pSpecificInstrument: mdapi.CThostFtdcSpecificInstrumentField,
                           pRspInfo: mdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """订阅行情响应"""
        self._check_rsp(pRspInfo, pSpecificInstrument, is_last=bIsLast)
    
    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """行情通知 - 核心处理函数"""
        if not pDepthMarketData:
            return
        
        # InstrumentID 可能是字符串或 bytes，需要兼容处理
        instrument_id = pDepthMarketData.InstrumentID
        if isinstance(instrument_id, bytes):
            symbol = instrument_id.decode('utf-8')
        else:
            symbol = str(instrument_id) if instrument_id else ""
        
        last_price = pDepthMarketData.LastPrice
        volume = pDepthMarketData.Volume
        open_interest = pDepthMarketData.OpenInterest
        
        self.tick_count += 1
        
        # 更新 K 线
        tick_data = {
            'LastPrice': last_price,
            'Volume': volume,
            'OpenInterest': open_interest
        }
        self.kline_manager.update_tick(symbol, tick_data)
        
        # 每 5 分钟检查一次波动率切换
        current_time = datetime.now()
        if symbol not in self.last_check_time:
            self.last_check_time[symbol] = current_time
        
        # 距离上次检查超过 5 分钟
        elapsed = (current_time - self.last_check_time[symbol]).total_seconds()
        if elapsed >= 300:  # 5 分钟
            self.last_check_time[symbol] = current_time
            self._check_volatility_switch(symbol)
    
    def _check_volatility_switch(self, instrument_id: str):
        """检查波动率切换
        
        Args:
            instrument_id: 合约代码（如 cu2604）
        """
        # 将 instrument_id 转换为数据库中的 symbol 格式（ExchangeID.InstrumentID）
        db_symbol = self.contract_symbol_map.get(instrument_id, instrument_id)
        
        # 获取 K 线历史（使用数据库 symbol 查询）
        df = self.kline_manager.get_kline_history(db_symbol, limit=100)
        
        if len(df) < HV_SLOW_PERIOD + 10:
            log(f"{instrument_id}: 数据不足 ({len(df)}条), symbol={db_symbol}")
            return
        
        # 检查是否满足高波切低波条件（传入 db_symbol 以使用正确的年化因子）
        result = self.vol_calculator.check_high_to_low_switch(df, symbol=db_symbol)
        
        if result['is_switch']:
            log(f"★★★ {instrument_id} ({db_symbol}) 检测到高波切低波信号！{result['details']['reason']}")
            
            # 检查冷却时间（使用 instrument_id 作为 key）
            if self.signal_manager.can_generate_signal(instrument_id):
                self.signal_manager.add_signal(instrument_id, result['details'])
                log(f"✓ {instrument_id} 信号已记录到文件")
                
                # 发送飞书通知
                if self.feishu_notifier:
                    self.feishu_notifier.send_volatility_switch_signal(instrument_id, result['details'])
            else:
                log(f"○ {instrument_id} 信号在冷却期内，跳过")
        else:
            log(f"{instrument_id}: {result['details']['reason']}")


def load_main_contracts(json_file="main_contracts.json") -> tuple:
    """从 main_contracts.json 加载主力合约
    
    Returns:
        tuple: (合约列表，合约到交易所的映射字典)
    """
    if not os.path.exists(json_file):
        log(f"错误：文件不存在 {json_file}")
        return [], {}
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            # 提取 MainContractID 和 ExchangeID
            contracts = []
            contract_exchange_map = {}
            for item in data:
                main_contract = item.get('MainContractID', '')
                exchange_id = item.get('ExchangeID', '')
                if main_contract:
                    contracts.append(main_contract)
                    # 构建 symbol: ExchangeID.MainContractID
                    symbol = f"{exchange_id}.{main_contract}"
                    contract_exchange_map[main_contract] = symbol
            
            log(f"从 {json_file} 加载了 {len(contracts)} 个主力合约")
            return contracts, contract_exchange_map
        else:
            log(f"错误：JSON 文件格式不正确")
            return [], {}
    except Exception as e:
        log(f"读取 JSON 文件失败：{e}")
        return [], {}


def main():
    """主函数"""
    log("=" * 70)
    log("波动率切换监控系统启动")
    log("=" * 70)
    log(f"配置参数:")
    log(f"  HV_FAST_PERIOD = {HV_FAST_PERIOD}")
    log(f"  HV_SLOW_PERIOD = {HV_SLOW_PERIOD}")
    log(f"  RATIO_THRESH = {RATIO_THRESH}")
    log(f"  PERCENTILE_THRESH = {PERCENTILE_THRESH}")
    log(f"  EWMA_LAMBDA = {EWMA_LAMBDA}")
    log(f"  SIGNAL_COOLDOWN = {SIGNAL_COOLDOWN}秒")
    log(f"  DB_PATH = {DB_PATH}")
    log(f"  SIGNAL_FILE = {SIGNAL_FILE}")
    
    # 加载主力合约（返回合约列表和 symbol 映射）
    instruments, contract_symbol_map = load_main_contracts()
    if not instruments:
        log("错误：没有加载到任何合约")
        return
    
    log(f"订阅合约列表：{instruments}")
    log(f"Symbol 映射示例：{list(contract_symbol_map.items())[:3]}")
    
    # 初始化组件
    kline_manager = KlineManager(DB_PATH)
    vol_calculator = VolatilityCalculator()
    signal_manager = SignalManager(SIGNAL_FILE)
    
    # 创建行情回调实例，传入 symbol 映射
    spi = VolatilityMonitorSpi(
        instruments=instruments,
        kline_manager=kline_manager,
        vol_calculator=vol_calculator,
        signal_manager=signal_manager,
        contract_symbol_map=contract_symbol_map
    )
    
    # 订阅行情（使用 InstrumentID 订阅）
    spi.req(instruments)
    
    # 等待
    try:
        spi.wait_last()
    except KeyboardInterrupt:
        log("\n用户中断，正在保存数据...")
    finally:
        kline_manager.close()
        log("程序退出")


if __name__ == '__main__':
    main()