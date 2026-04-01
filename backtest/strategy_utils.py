#!/usr/bin/env python3
"""
策略工具类 - 包含配置和数据加载
"""

import sqlite3
import json
from typing import Dict, List, Optional
from datetime import datetime


# ============== 配置 ==============

class Config:
    DB_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/kline_data.db"
    CONTRACTS_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/main_contracts.json"

    DURATION_5M = 300
    DURATION_60M = 3600
    MAX_5M_BARS = 8000
    MAX_60M_BARS = 2000  # 60分钟最多加载2000根

    TARGET_NOTIONAL = 200000  # 20 万货值
    COOLDOWN_HOURS = 4  # 冷却期 4 小时


# ============== 数据加载 ==============

class DataLoader:
    def __init__(self, db_path: str, contracts_path: str):
        self.db_path = db_path
        self.contracts_path = contracts_path
        self._contracts_cache = None

    def load_main_contracts(self) -> Dict[str, dict]:
        if self._contracts_cache is not None:
            return self._contracts_cache

        with open(self.contracts_path, 'r') as f:
            contracts = json.load(f)
        self._contracts_cache = {c['ProductID']: c for c in contracts if c.get('IsTrading', 0) == 1}
        return self._contracts_cache

    def load_kline_fast(self, symbol: str, duration: int, limit: int = None) -> List[tuple]:
        """快速加载 K 线数据（加载最近的数据）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if limit:
            # 先获取最近的 limit 条记录，再按时间正序返回
            query = f"""SELECT datetime, open, high, low, close, volume
                       FROM kline_data
                       WHERE symbol = ? AND duration = ?
                       ORDER BY datetime DESC
                       LIMIT {limit}"""
        else:
            query = """SELECT datetime, open, high, low, close, volume
                       FROM kline_data WHERE symbol = ? AND duration = ?
                       ORDER BY datetime ASC"""

        cursor.execute(query, [symbol, duration])
        rows = cursor.fetchall()
        conn.close()

        result = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
        if limit:
            result.reverse()  # 反转为正序
        return result

    def get_symbol_info(self, symbol: str) -> Optional[dict]:
        contracts = self.load_main_contracts()
        symbol_short = symbol.split('.')[-1] if '.' in symbol else symbol

        best_match = None
        best_match_len = 0

        for product_id, contract in contracts.items():
            if symbol_short.startswith(product_id) and len(product_id) > best_match_len:
                best_match = contract
                best_match_len = len(product_id)

        return best_match
