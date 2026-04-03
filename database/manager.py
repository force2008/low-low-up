#!/usr/bin/env python3
"""
数据库管理模块

- DatabaseManager: K 线数据存取管理
"""

import sqlite3
import threading
import pandas as pd
from datetime import datetime
from typing import List, Optional


def print_log(msg: str):
    """日志输出"""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")


class DatabaseManager:
    """数据库管理类"""

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
        """初始化数据库"""
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
        """插入 K 线数据"""
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
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None