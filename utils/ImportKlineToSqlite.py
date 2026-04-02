# @Project: https://github.com/Jedore/ctp.examples
# @File:    ImportKlineToSqlite.py
# @Time:    21/02/2026
# @Description: 从 main_contracts.json 获取合约，使用 akshare/tqsdk API 导入历史 K 线数据到 SQLite 数据库

import json
import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import argparse

# 配置日志
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, datetime.now().strftime("ImportKline_%Y%m%d_%H%M%S.log"))

# 创建 logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建文件 handler
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 创建控制台 handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# 创建 formatter
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 设置 formatter
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 添加 handler 到 logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 测试日志输出
logger.info(f"日志文件：{log_filename}")
logger.info("日志系统初始化完成")


def print_log(*args, **kwargs):
    """ 日志输出函数 """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# ==================== 配置常量 ====================

# 数据源类型
SOURCE_AKSHARE = 1
SOURCE_SYNTHESIZED = 2
SOURCE_TQSDK = 3

# K 线周期配置 (秒)
PERIOD_CONFIG = {
    "5": 300,      # 5 分钟
    "30": 1800,    # 30 分钟
    "60": 3600,    # 60 分钟
    "day": 86400   # 1 天
}

# tqsdk 各周期数据条数配置
TQSDK_DATA_LENGTH = {
    "5": 5000,     # 5 分钟 - 5000 条
    "30": 1000,    # 30 分钟 - 1000 条
    "60": 500,     # 60 分钟 - 500 条
    "day": 100     # 1 天 - 100 条
}

# tqsdk 周期转换为秒
def get_tqsdk_period_seconds(period: str) -> int:
    """
    将周期字符串转换为秒数，tqsdk 的 get_kline_serial 需要秒数作为周期参数
    
    Args:
        period: 周期字符串 (5=5 分钟，30=30 分钟，60=60 分钟，day=1 天)
    
    Returns:
        周期对应的秒数
    """
    if period == "5":
        return 5 * 60       # 5 分钟 = 300 秒
    elif period == "30":
        return 30 * 60      # 30 分钟 = 1800 秒
    elif period == "60":
        return 60 * 60      # 60 分钟 = 3600 秒
    elif period == "day":
        return 24 * 60 * 60 # 1 天 = 86400 秒
    else:
        return 6 * 60       # 默认 5 分钟


# 全局 tqsdk API 实例
_tqsdk_api = None
_tqsdk_time_to_str = None


def get_tqsdk_api():
    """获取或创建 tqsdk API 单例实例"""
    global _tqsdk_api, _tqsdk_time_to_str
    if _tqsdk_api is None:
        try:
            from tqsdk import TqApi, TqAuth
            from tqsdk.tafunc import time_to_str
            _tqsdk_api = TqApi(auth=TqAuth("15558190923", "283200"))
            _tqsdk_time_to_str = time_to_str
            print_log("tqsdk API 已初始化")
        except Exception as e:
            print_log(f"初始化 tqsdk API 失败：{e}")
            return None
    return _tqsdk_api


def close_tqsdk_api():
    """关闭 tqsdk API"""
    global _tqsdk_api
    if _tqsdk_api is not None:
        try:
            _tqsdk_api.close()
            print_log("tqsdk API 已关闭")
        except Exception as e:
            print_log(f"关闭 tqsdk API 失败：{e}")
        finally:
            _tqsdk_api = None


class DatabaseManager:
    """ 数据库管理类 """

    # 获取项目根目录
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "data", "db", "kline_data.db")

    def __init__(self, db_path=None):
        self.db_path = db_path or self.DEFAULT_DB_PATH
        self.conn = None
        self.cursor = None
        self.init_database()
    
    def init_database(self):
        """ 初始化数据库 """
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # 创建 K 线表
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
                source INTEGER NOT NULL DEFAULT 1
            )
        ''')
        
        # 为现有表添加新字段（如果表已存在但没有这些字段）
        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN update_time TEXT")
            print_log("添加 update_time 字段")
        except Exception:
            pass  # 字段已存在
        
        try:
            self.cursor.execute("ALTER TABLE kline_data ADD COLUMN source INTEGER DEFAULT 1")
            print_log("添加 source 字段")
        except Exception:
            pass  # 字段已存在
        
        self.conn.commit()
        
        # 删除旧的唯一索引（如果存在）
        try:
            self.cursor.execute("DROP INDEX IF EXISTS idx_kline_data_datetime_symbol")
            print_log("删除旧的唯一索引")
        except Exception:
            pass
        
        # 创建新的唯一索引（用于去重）- 添加 duration 字段，支持同一时间同一合约的不同周期
        self.cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_kline_data_datetime_symbol_duration 
            ON kline_data(datetime, symbol, duration)
        ''')
        
        self.conn.commit()
        print_log(f"数据库初始化完成：{self.db_path}")
    
    def insert_df(self, df: pd.DataFrame, source: int = SOURCE_AKSHARE, duration: int = None):
        """ 插入/更新 DataFrame 数据
        
        Args:
            df: K 线数据 DataFrame
            source: 数据来源，1=akshare, 2=合成，3=tqsdk
            duration: 周期秒数，用于删除时只删除对应周期的数据
        """
        try:
            # 获取 symbol
            symbol = df['symbol'].iloc[0]
            
            # 获取时间范围
            min_datetime = df['datetime'].min()
            max_datetime = df['datetime'].max()
            
            # 获取 duration（从 DataFrame 或参数）
            if duration is None:
                duration = df['duration'].iloc[0] if 'duration' in df.columns else None
            
            # 删除该 symbol 在时间范围内且对应 duration 的数据
            if duration is not None:
                self.cursor.execute(
                    "DELETE FROM kline_data WHERE symbol = ? AND duration = ? AND datetime >= ? AND datetime <= ?",
                    (symbol, duration, min_datetime, max_datetime)
                )
            else:
                self.cursor.execute(
                    "DELETE FROM kline_data WHERE symbol = ? AND datetime >= ? AND datetime <= ?",
                    (symbol, min_datetime, max_datetime)
                )
            deleted_count = self.cursor.rowcount
            
            # 过滤掉包含 NaN 的行
            original_count = len(df)
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
            filtered_count = original_count - len(df)
            if filtered_count > 0:
                print_log(f"过滤掉 {filtered_count} 条包含 NaN 的数据")
            
            # 批量插入数据
            insert_count = 0
            update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df.iterrows():
                # 确保所有值都是 Python 原生类型，避免 numpy 类型绑定错误
                close_oi_val = row['close_oi']
                try:
                    close_oi_int = int(close_oi_val) if not (isinstance(close_oi_val, float) and pd.isna(close_oi_val)) else 0
                except (TypeError, ValueError):
                    close_oi_int = 0
                
                # 检查其他字段是否有 NaN
                try:
                    open_val = float(row['open'])
                    high_val = float(row['high'])
                    low_val = float(row['low'])
                    close_val = float(row['close'])
                    volume_val = int(row['volume'])
                    vwap_val = float(row['vwap']) if pd.notna(row['vwap']) else close_val
                except (TypeError, ValueError):
                    # 跳过无效数据行
                    continue
                
                self.cursor.execute('''
                    INSERT INTO kline_data 
                    (datetime, open, high, low, close, volume, close_oi, vwap, symbol, duration, update_time, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row['datetime']),
                    open_val,
                    high_val,
                    low_val,
                    close_val,
                    volume_val,
                    close_oi_int,
                    vwap_val,
                    str(row['symbol']),
                    int(row['duration']),
                    update_time,
                    int(source)
                ))
                insert_count += 1
            
            self.conn.commit()
            
            # 输出统计信息
            print_log(f"数据更新完成：删除={deleted_count} 条，插入={insert_count} 条，symbol={symbol}")
            
        except Exception as e:
            print_log(f"插入/更新数据失败：{e}")
            import traceback
            print_log(traceback.format_exc())
    
    def close(self):
        """ 关闭数据库连接 """
        if self.conn:
            self.conn.close()
            self.conn = None
            print_log("数据库连接已关闭")


def convert_contract_symbol(symbol: str) -> str:
    """
    转换合约符号格式，将 3 位年份转换为 4 位年份
    例如：RM605 -> RM2605, AP605 -> AP2605, a2605 -> a2605 (已为 4 位则不变)
    
    规则：
    - 3 位数字格式：第 1 位是年份最后一位，后 2 位是月份 (如 605 = 26 年 05 月)
    - 4 位数字格式：前 2 位是年份，后 2 位是月份 (如 2605 = 26 年 05 月)
    
    转换逻辑：
    - 3 位 -> 4 位：在数字前加"2"，如 605 -> 2605
    """
    import re
    
    # 匹配末尾 3 位数字的合约（如 RM605, AP605, JR603）
    match = re.match(r'^([A-Za-z]+)(\d{3})$', symbol)
    if match:
        prefix = match.group(1)
        year_month = match.group(2)
        # 在 3 位数字前加"2"，变成 4 位年份格式
        # 605 -> 2605, 603 -> 2603
        return f"{prefix}2{year_month}"
    
    # 如果已经是 4 位年份或其他格式，直接返回
    return symbol


def convert_contract_symbol_for_tqsdk(symbol: str, exchange_id: str = "") -> str:
    """
    转换合约符号格式用于 tqsdk API
    tqsdk 需要特定的合约格式：
    - CZCE 交易所：需要将 4 位年份转为 3 位年份，如 AP2605 -> AP605
    - 其他交易所：保持 4 位年份格式，如 rb2605
    
    Args:
        symbol: 合约代码（4 位年份格式，如 AP2605, rb2605）
        exchange_id: 交易所 ID
    
    Returns:
        转换后的合约代码
    """
    import re
    
    # CZCE 交易所需要特殊处理
    if exchange_id.upper() == "CZCE":
        # 匹配末尾 4 位数字的合约（如 AP2605, SR2605）
        match = re.match(r'^([A-Za-z]+)(\d{4})$', symbol)
        if match:
            prefix = match.group(1)
            year_month = match.group(2)
            # 将 4 位年份转为 3 位年份：去掉年份的第一位
            # 2605 -> 605, 2603 -> 603
            if year_month.startswith('2'):
                return f"{prefix}{year_month[1:]}"
    
    # 其他交易所直接返回
    return symbol


def load_contracts_from_json(json_file):
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


def fetch_kline_from_akshare(symbol: str, period: str = "5", max_retries: int = 3, retry_delay: float = 2.0) -> pd.DataFrame:
    """ 
    从 akshare 获取 K 线数据，带重试机制
    注意：akshare 最多只能获取 1024 根 K 线
    
    Args:
        symbol: 合约代码
        period: 周期 (5=5 分钟，30=30 分钟，60=60 分钟)
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
    """
    try:
        import akshare as ak
    except ImportError:
        print_log("错误：未安装 akshare，请运行：pip install akshare")
        return None
    
    for attempt in range(max_retries):
        try:
            print_log(f"正在获取 {symbol} 的 K 线数据，周期={period}分钟 (尝试 {attempt+1}/{max_retries})")
            
            # 获取 K 线数据
            df = ak.futures_zh_minute_sina(symbol=symbol, period=period)
            
            if df is None or len(df) == 0:
                print_log(f"警告：未获取到数据")
                return None
            
            print_log(f"获取成功：{len(df)} 条 K 线数据 (akshare 限制最多 1024 条)")
            return df
            
        except Exception as e:
            error_msg = str(e)
            # 如果是限流或网络错误，等待后重试
            if "list index out of range" in error_msg or "Length mismatch" in error_msg:
                if attempt < max_retries - 1:
                    print_log(f"获取失败，{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print_log(f"获取 K 线数据失败（已达最大重试次数）：{e}")
                    return None
            elif "No data" in error_msg:
                print_log(f"警告：该合约无可用数据")
                return None
            else:
                print_log(f"获取 K 线数据失败：{e}")
                import traceback
                print_log(traceback.format_exc())
                return None
    
    return None


def fetch_kline_from_tqsdk(symbol: str, period: str = "5", max_retries: int = 3, retry_delay: float = 2.0) -> pd.DataFrame:
    """ 
    从 tqsdk (天勤) 获取 K 线数据，带重试机制
    使用全局 API 实例，避免重复初始化
    
    Args:
        symbol: 合约代码（格式：交易所。合约，如 SHFE.rb2605）
        period: 周期 (5=5 分钟，30=30 分钟，60=60 分钟，day=1 天)
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
    """
    # 获取全局 API 实例
    api = get_tqsdk_api()
    if api is None:
        print_log("错误：tqsdk API 未初始化")
        return None
    
    time_to_str = _tqsdk_time_to_str
    
    # 获取数据条数配置
    data_length = TQSDK_DATA_LENGTH.get(period, 1000)
    
    # 将周期转换为秒数（tqsdk 需要秒数作为周期参数）
    period_seconds = get_tqsdk_period_seconds(period)
    
    for attempt in range(max_retries):
        try:
            print_log(f"正在获取 {symbol} 的 K 线数据，周期={period}({period_seconds}秒), 条数={data_length} (尝试 {attempt+1}/{max_retries})")
            
            # 获取 K 线数据（tqsdk 的 period 参数需要是秒数）
            klines = api.get_kline_serial(symbol, period_seconds, data_length)
            klines['datetime'] = klines['datetime'].apply(time_to_str)
            if klines is None or len(klines) == 0:
                print_log(f"警告：未获取到数据")
                return None
            
            # 转换数据格式
            df = pd.DataFrame(klines)
            
            # 重命名列
            df = df.rename(columns={
                'datetime': 'datetime',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'open_oi': 'close_oi'  # tqsdk 中 open_oi 是持仓量
            })
            
            count = len(df)
            print_log(f"获取成功：{count} 条 K 线数据")
            return df
            
        except Exception as e:
            error_msg = str(e)
            # 如果是网络错误，等待后重试
            if "Connection" in error_msg or "Timeout" in error_msg or "network" in error_msg.lower():
                if attempt < max_retries - 1:
                    print_log(f"获取失败，{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print_log(f"获取 K 线数据失败（已达最大重试次数）：{e}")
                    return None
            elif "auth" in error_msg.lower() or "认证" in error_msg:
                print_log(f"认证失败：{e}")
                print_log("请确保已设置正确的 TQ 账号密码")
                return None
            else:
                print_log(f"获取 K 线数据失败：{e}")
                import traceback
                print_log(traceback.format_exc())
                return None
    
    return None


def process_contract(contract, exchange_id, period, duration, source, source_name, db_manager):
    """
    处理单个合约的 K 线数据导入
    
    Args:
        contract: 合约字典
        exchange_id: 交易所 ID
        period: 周期字符串
        duration: 周期秒数
        source: 数据源类型
        source_name: 数据源名称
        db_manager: 数据库管理器
    """
    instrument_id = contract.get("MainContractID", "")
    instrument_name = contract.get("InstrumentName", "")
    
    # 构建 symbol（添加交易所前缀）
    symbol = f"{exchange_id}.{instrument_id}"
    
    print_log(f"处理合约：{instrument_id} ({instrument_name}) - {exchange_id}")
    
    # 转换合约格式
    akshare_symbol = convert_contract_symbol(instrument_id)
    
    # 根据数据源获取数据
    if source == SOURCE_AKSHARE:
        df = fetch_kline_from_akshare(akshare_symbol, period, max_retries=3, retry_delay=2.0)
    elif source == SOURCE_TQSDK:
        # tqsdk 需要交易所。合约格式
        # CZCE 交易所需要特殊处理：AP2605 -> AP605
        tq_symbol_raw = convert_contract_symbol_for_tqsdk(instrument_id, exchange_id)
        tq_symbol = f"{exchange_id}.{tq_symbol_raw}"
        df = fetch_kline_from_tqsdk(tq_symbol, period, max_retries=3, retry_delay=2.0)
    else:
        print_log(f"未知数据源：{source}")
        return False
    
    if df is not None and len(df) > 0:
        # 确保 datetime 是字符串格式
        if not df['datetime'].dtype == object:
            df['datetime'] = df['datetime'].astype(str)
        
        # 重命名 hold 列为 close_oi（持仓量）
        if 'hold' in df.columns and 'close_oi' not in df.columns:
            df = df.rename(columns={'hold': 'close_oi'})
        
        # 添加必要列
        if 'vwap' not in df.columns:
            df['vwap'] = df['close']  # 使用 close 作为 vwap 的近似值
        df['symbol'] = symbol
        df['duration'] = duration
        
        # 插入/更新数据库（传入 duration 参数，确保只删除对应周期的数据）
        db_manager.insert_df(df, source=source, duration=duration)
        print_log(f"导入完成：{symbol} (周期={period}, 数据源={source_name})")
        return True
    else:
        print_log(f"警告：未获取到 {instrument_id} 的 K 线数据")
        return False


def import_all_periods(contracts, source, source_name, db_manager, use_sleep=True):
    """
    导入所有周期的 K 线数据
    
    Args:
        contracts: 合约列表
        source: 数据源类型
        source_name: 数据源名称
        db_manager: 数据库管理器
        use_sleep: 是否使用延时（akshare 需要，tqsdk 不需要）
    """
    total_contracts = len(contracts)
    
    # akshare 不导入日线（akshare 日线数据有限，且需要单独处理）
    # tqsdk 导入所有周期
    if source == SOURCE_AKSHARE:
        periods = ["5", "30", "60"]  # akshare 只导入 5 分钟、30 分钟、60 分钟
    else:
        periods = ["5", "30", "60", "day"]  # tqsdk 导入所有周期
    
    for period in periods:
        duration = PERIOD_CONFIG[period]
        print_log("\n" + "=" * 70)
        print_log(f"开始导入 {period} 周期数据")
        print_log("=" * 70)
        
        success_count = 0
        failed_contracts = []
        
        for idx, contract in enumerate(contracts, 1):
            instrument_id = contract.get("MainContractID", "")
            exchange_id = contract.get("ExchangeID", "")
            
            # 打印进度信息
            print_log(f"\n===============[{idx}/{total_contracts}] ===============")
            
            # 处理合约
            if process_contract(contract, exchange_id, period, duration, source, source_name, db_manager):
                success_count += 1
            else:
                failed_contracts.append(f"{exchange_id}.{instrument_id}")
            
            # 添加延时，避免限流（tqsdk 不需要）
            if use_sleep:
                time.sleep(1.0)
        
        # 打印周期统计信息
        print_log(f"\n--- {period} 周期完成 ---")
        print_log(f"成功：{success_count} 个")
        print_log(f"失败：{len(failed_contracts)} 个")
        if failed_contracts:
            print_log(f"失败的合约：{', '.join(failed_contracts)}")
    
    return len(periods) * total_contracts, success_count, failed_contracts


if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='K 线数据导入程序')
    parser.add_argument('--source', type=str, default='akshare', choices=['akshare', 'tqsdk'],
                        help='数据源类型 (akshare 或 tqsdk)')

    args = parser.parse_args()

    # 获取项目根目录
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_file = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")
    
    # 确定数据源
    source_name = args.source.lower()
    if source_name == 'akshare':
        source = SOURCE_AKSHARE
        use_sleep = True  # akshare 需要延时
    elif source_name == 'tqsdk':
        source = SOURCE_TQSDK
        use_sleep = False  # tqsdk 不需要延时
    else:
        print_log(f"错误：未知数据源 {source_name}")
        sys.exit(1)
    
    try:
        print_log("=" * 70)
        print_log(f"K 线数据导入程序 (数据源={source_name})")
        if source == SOURCE_AKSHARE:
            print_log("导入周期：5 分钟 -> 30 分钟 -> 60 分钟（akshare 不导入日线）")
        else:
            print_log("导入周期：5 分钟 -> 30 分钟 -> 60 分钟 -> 1 天")
        print_log("=" * 70)

        # 从 JSON 文件加载合约列表
        contracts = load_contracts_from_json(json_file)
        
        if not contracts:
            print_log(f"错误：没有找到合约列表，请检查 {json_file}")
            sys.exit(1)
        
        total_contracts = len(contracts)
        print_log(f"从 {json_file} 加载了 {total_contracts} 个合约")
        
        # 初始化数据库
        db_manager = DatabaseManager()
        
        # 导入所有周期
        total_ops, success_count, failed_contracts = import_all_periods(
            contracts, source, source_name, db_manager, use_sleep
        )
        
        # 关闭 tqsdk API（如果是 tqsdk 模式）
        if source == SOURCE_TQSDK:
            close_tqsdk_api()
        
        # 关闭数据库
        db_manager.close()
        
        # 打印统计信息
        print_log("\n" + "=" * 70)
        print_log("全部导入完成")
        print_log(f"总共处理：{total_ops} 次操作 ({total_contracts} 合约 x 4 周期)")
        print_log(f"成功：{success_count} 次")
        print_log(f"失败：{len(failed_contracts)} 次")
        if failed_contracts:
            print_log(f"失败的合约：{', '.join(failed_contracts)}")
        print_log("=" * 70)
        
    except Exception as e:
        print_log(f"程序运行出错：{e}")
        import traceback
        print_log(traceback.format_exc())