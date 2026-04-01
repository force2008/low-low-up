# @Project: https://github.com/Jedore/ctp.examples
# @File:    ExportAkshareData.py
# @Time:    12/03/2026
# @Author:  Assistant
# @Description: 从 akshare 获取主力合约，使用 akshare API 导入历史 K 线数据到 SQLite 数据库

import json
import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import akshare as ak


# 配置日志
log_filename = datetime.now().strftime("ExportAkshare_%Y%m%d_%H%M%S.log")

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


class DatabaseManager:
    """ 数据库管理类 """
    
    def __init__(self, db_path="kline_data.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.init_database()
    
    def init_database(self):
        """ 初始化数据库 """
        self.conn = sqlite3.connect(self.db_path)
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
        
        # 创建唯一索引（用于去重）
        self.cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_kline_data_datetime_symbol 
            ON kline_data(datetime, symbol)
        ''')
        
        self.conn.commit()
        print_log(f"数据库初始化完成：{self.db_path}")
    
    def insert_df(self, df: pd.DataFrame):
        """ 插入/更新 DataFrame 数据 """
        try:
            # 获取 symbol
            symbol = df['symbol'].iloc[0]
            
            # 获取时间范围
            min_datetime = df['datetime'].min()
            max_datetime = df['datetime'].max()
            
            # 删除该 symbol 在时间范围内的所有数据
            self.cursor.execute(
                "DELETE FROM kline_data WHERE symbol = ? AND datetime >= ? AND datetime <= ?",
                (symbol, min_datetime, max_datetime)
            )
            deleted_count = self.cursor.rowcount
            
            # 批量插入数据
            insert_count = 0
            for _, row in df.iterrows():
                self.cursor.execute('''
                    INSERT INTO kline_data 
                    (datetime, open, high, low, close, volume, close_oi, vwap, symbol, duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['datetime'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['close_oi'],
                    row['vwap'],
                    row['symbol'],
                    row['duration']
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


def fetch_contracts_from_akshare(exchange: str) -> list:
    """ 从 akshare 获取主力合约列表 """
    
    try:
        print_log(f"正在从 akshare 获取 {exchange} 的主力合约...")
        
        # 获取主力合约，返回逗号分隔的字符串
        contracts_str = ak.match_main_contract(symbol=exchange.lower())
        
        print_log(f"akshare 返回：{contracts_str}")
        
        # 分割字符串为列表
        contracts_list = [c.strip() for c in contracts_str.split(',') if c.strip()]
        
        print_log(f"解析得到 {len(contracts_list)} 个主力合约：{contracts_list}")
        
        return contracts_list
    except Exception as e:
        print_log(f"获取主力合约失败：{e}")
        import traceback
        print_log(traceback.format_exc())
        return []


def fetch_kline_from_akshare(symbol: str, period: str = "5") -> pd.DataFrame:
    """ 从 akshare 获取 K 线数据 """
    
    try:
        print_log(f"正在获取 {symbol} 的 K 线数据，周期={period}分钟")
        
        # 获取 K 线数据
        df = ak.futures_zh_minute_sina(symbol=symbol, period=period)
        
        if df is None or len(df) == 0:
            print_log(f"警告：未获取到数据")
            return None
        
        print_log(f"获取成功：{len(df)} 条 K 线数据")
        return df
    except Exception as e:
        print_log(f"获取 K 线数据失败：{e}")
        import traceback
        print_log(traceback.format_exc())
        return None


if __name__ == '__main__':
    try:
        print_log("=" * 70)
        print_log("K 线数据导入程序（使用 akshare API）")
        print_log("=" * 70)
        
        # 从 akshare 获取主力合约列表（以 CFFEX 为例）
        exchange = "cffex"
        contracts = fetch_contracts_from_akshare(exchange)
        
        if not contracts:
            print_log(f"错误：没有找到主力合约列表")
            sys.exit(1)
        
        # 初始化数据库
        db_manager = DatabaseManager()
        
        # 导入数据
        period = "5"  # 5 分钟
        duration = 300  # 5 分钟 = 300 秒
        
        for contract in contracts:
            instrument_id = contract
            
            print_log(f"\n处理合约：{instrument_id}")
            print_log("-" * 70)
            
            # 从 akshare 获取 K 线数据
            df = fetch_kline_from_akshare(instrument_id, period)
            
            if df is not None and len(df) > 0:
                # 转换 datetime 为字符串格式
                df['datetime'] = df['datetime'].astype(str)
                
                # 重命名 hold 列为 close_oi（持仓量）
                if 'hold' in df.columns:
                    df = df.rename(columns={'hold': 'close_oi'})
                
                # 添加必要列
                df['vwap'] = df['close']  # 使用 close 作为 vwap 的近似值
                df['symbol'] = instrument_id
                df['duration'] = duration
                
                # 插入/更新数据库
                db_manager.insert_df(df)
                print_log(f"导入完成：{instrument_id}")
            else:
                print_log(f"警告：未获取到 {instrument_id} 的 K 线数据")
        
        # 关闭数据库
        db_manager.close()
        
        print_log("\n" + "=" * 70)
        print_log("导入完成")
        print_log("=" * 70)
    except Exception as e:
        print_log(f"程序运行出错：{e}")
        import traceback
        print_log(traceback.format_exc())