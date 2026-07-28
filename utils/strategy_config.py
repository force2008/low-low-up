#!/usr/bin/env python3
"""
策略配置和数据加载工具

- Config: 策略配置类
- DataLoader: K线数据加载器
"""

import sqlite3
import json
import os
from typing import Dict, List, Optional
from datetime import datetime

# 获取项目根目录
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ============== 配置 ==============

class Config:
    """策略配置"""
    DB_PATH = os.path.join(_BASE_DIR, "data", "db", "kline_data.db")
    CONTRACTS_PATH = os.path.join(_BASE_DIR, "data", "contracts", "main_contracts.json")

    DURATION_5M = 300
    DURATION_60M = 3600
    MAX_5M_BARS = 8000
    MAX_60M_BARS = 2000  # 60分钟最多加载2000根

    TARGET_NOTIONAL = 200000  # 20 万货值
    COOLDOWN_HOURS = 4  # 冷却期 4 小时

    # 屏蔽胜率过低的品种（回测和实盘都不交易）- 按产品ID屏蔽
    EXCLUDED_PRODUCTS = ['rr', 'wr', 'pk']


# ============== 数据加载 ==============

class DataLoader:
    """K线数据加载器"""

    def __init__(self, db_path: str, contracts_path: str):
        self.db_path = db_path
        self.contracts_path = contracts_path
        self._contracts_cache = None

    def load_main_contracts(self) -> Dict[str, dict]:
        """加载主力合约列表"""
        if self._contracts_cache is not None:
            return self._contracts_cache

        with open(self.contracts_path, 'r', encoding='utf-8') as f:
            contracts = json.load(f)
        self._contracts_cache = {c['ProductID']: c for c in contracts if c.get('IsTrading', 0) == 1}
        return self._contracts_cache

    def load_kline_fast(self, symbol: str, duration: int, limit: int = None) -> List[tuple]:
        """快速加载 K 线数据（加载最近的数据）

        Args:
            symbol: 合约符号，支持多种格式:
                - CU.SHF -> shfe.cu2606 (根据main_contracts.json匹配)
                - SHFE.CU2606 -> shfe.cu2606
                - shfe.cu2606 -> shfe.cu2606
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 尝试用 main_contracts.json 匹配
        db_symbol = self._convert_to_db_symbol(symbol, cursor)

        if limit:
            query = f"""SELECT datetime, open, high, low, close, volume
                       FROM kline_data
                       WHERE symbol = ? AND duration = ?
                       ORDER BY datetime DESC
                       LIMIT {limit}"""
        else:
            query = """SELECT datetime, open, high, low, close, volume
                       FROM kline_data WHERE symbol = ? AND duration = ?
                       ORDER BY datetime ASC"""

        cursor.execute(query, [db_symbol, duration])
        rows = cursor.fetchall()
        conn.close()

        result = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
        if limit:
            result.reverse()  # 反转为正序
        return result

    def _convert_to_db_symbol(self, symbol: str, cursor) -> str:
        """将符号转换为数据库格式

        从 main_contracts.json 匹配，找到对应的数据库 symbol
        示例: CU.SHF -> shfe.cu2606, SHFE.CU2606 -> shfe.cu2606
        """
        # 先尝试直接转换
        symbol_lower = symbol.lower()

        # 如果已经是正确格式，直接返回
        cursor.execute("SELECT 1 FROM kline_data WHERE symbol = ? LIMIT 1", [symbol_lower])
        if cursor.fetchone():
            return symbol_lower

        # 尝试从 main_contracts.json 匹配
        contracts = self.load_main_contracts()

        if '.' in symbol:
            # 处理格式如 CU.SHF, SHFE.CU2606
            exchange = symbol.split('.')[0].lower()
            code = symbol.split('.')[-1].lower()

            # 查找匹配的合约
            for product_id, contract in contracts.items():
                if code.startswith(product_id.lower()):
                    # 找到匹配的合约，构建数据库格式（保持大写）
                    exchange_id = contract.get('ExchangeID', '').upper()
                    main_contract = contract.get('MainContractID', '').upper()
                    db_symbol = f"{exchange_id}.{main_contract}"

                    # 验证是否存在
                    cursor.execute("SELECT 1 FROM kline_data WHERE symbol = ? LIMIT 1", [db_symbol])
                    if cursor.fetchone():
                        return db_symbol

        return symbol_lower

    def get_symbol_info(self, symbol: str) -> Optional[dict]:
        """获取合约信息"""
        contracts = self.load_main_contracts()
        symbol_short = symbol.split('.')[-1] if '.' in symbol else symbol

        best_match = None
        best_match_len = 0

        for product_id, contract in contracts.items():
            if symbol_short.startswith(product_id) and len(product_id) > best_match_len:
                best_match = contract
                best_match_len = len(product_id)

        return best_match