# -*- coding: utf-8 -*-
"""
数据加载模块

包含：
- _load_contract_info, _get_contract_info
- _standardize_contract, _get_exact_instrument_id
- _load_hold_std, _positions_to_hold_std, _save_hold_std
- _parse_hold_std, _aggregate_actual_positions, _get_position_detail
- _get_actual_position_volume, compare_positions
- is_contract_in_trading_time, _is_time_in_sessions, _is_simulation_env
- _guess_exchange
"""

import json
import os
import re
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple

from .constants import PRODUCT_TRADING_SESSIONS, DAY_3SEG

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class PositionSyncManagerData:
    """持仓同步管理器 - 数据加载部分"""

    def _load_contract_info(self) -> bool:
        """从 main_contracts.json / instruments.json 加载合约信息（交易所、PriceTick、ProductID）"""
        self._contract_info: Dict[str, dict] = {}
        self._instrument_exact_case: Dict[str, str] = {}
        self._product_exchange_map: Dict[str, str] = {}

        # 1) 主力合约配置
        if not os.path.exists(self.main_contracts_path):
            self.print(f"[错误] 找不到 main_contracts.json: {self.main_contracts_path}")
            return False
        try:
            with open(self.main_contracts_path, "r", encoding="utf-8") as f:
                contracts = json.load(f)
            for c in contracts:
                exact = c.get("MainContractID", "").strip()
                cid = exact.upper()
                product_id = c.get("ProductID", "").strip().upper()
                exchange = c.get("ExchangeID", "").strip()
                if cid:
                    self._contract_info[cid] = {
                        "ExchangeID": exchange,
                        "PriceTick": float(c.get("PriceTick", 1)),
                        "ProductID": product_id,
                        "InstrumentName": c.get("InstrumentName", "").strip(),
                    }
                    if exact:
                        self._instrument_exact_case[cid] = exact
                if product_id and exchange:
                    self._product_exchange_map[product_id] = exchange
            self.print(f"[信息] 已加载 {len(contracts)} 个主力合约信息")
        except Exception as e:
            self.print(f"[错误] 加载 main_contracts.json 失败: {e}")
            return False

        # 2) 全部合约（补充大小写映射，特别是 GFEX lowercase 合约）
        inst_path = os.path.join(PROJECT_ROOT, "data", "contracts", "instruments.json")
        if os.path.exists(inst_path):
            try:
                with open(inst_path, "r", encoding="utf-8") as f:
                    instruments = json.load(f)
                added = 0
                for ins in instruments:
                    exact = ins.get("InstrumentID", "").strip()
                    cid = exact.upper()
                    product_id = ins.get("ProductID", "").strip().upper()
                    exchange = ins.get("ExchangeID", "").strip()
                    if cid:
                        self._instrument_exact_case[cid] = exact
                        if cid not in self._contract_info:
                            self._contract_info[cid] = {
                                "ExchangeID": exchange,
                                "PriceTick": float(ins.get("PriceTick", 1)),
                                "ProductID": product_id,
                                "InstrumentName": ins.get("InstrumentName", "").strip(),
                            }
                            added += 1
                    if product_id and exchange:
                        self._product_exchange_map[product_id] = exchange
                if added:
                    self.print(f"[信息] 从 instruments.json 补充加载 {added} 个合约信息")
            except Exception as e:
                self.print(f"[警告] 加载 instruments.json 失败: {e}")
        return True

    def _get_contract_info(self, instrument_id: str) -> dict:
        """获取合约信息，优先从 main_contracts.json，找不到时尝试推断"""
        inst = self._standardize_contract(instrument_id)
        info = self._contract_info.get(inst.upper())
        if info:
            return info
        # 推断 ProductID（去掉尾部数字）
        product_id = inst.rstrip("0123456789")
        return {
            "ExchangeID": self._guess_exchange(inst),
            "PriceTick": 1.0,
            "ProductID": product_id,
            "InstrumentName": inst,
        }

    def _standardize_contract(self, instrument_id: str) -> str:
        """标准化合约代码：
        - 优先从映射恢复原始大小写（GFEX小写、SHFE小写、DCE小写等）
        - CZCE 4位年月转3位年月（SA2405→SA405），保持与融航等柜台一致的3位格式
        """
        inst = instrument_id.strip().upper()

        # 1. 映射中有精确匹配，恢复原始大小写（这是 CTP 实际使用的格式）
        exact = self._instrument_exact_case.get(inst)
        if exact:
            return exact

        # 2. CZCE 合约：4位年月 → 3位年月
        m = re.match(r'^([A-Z]{1,3})(\d{2})(\d{2})$', inst)
        if m:
            product = m.group(1)
            year_digit = m.group(2)[-1]
            month = m.group(3)
            czce_fmt = f"{product}{year_digit}{month}"

            czce_products = {
                "CF", "RM", "MA", "SR", "TA", "OI", "FG", "SA", "AP",
                "SM", "SF", "PX", "PR", "PF", "PK", "PL", "SH", "UR",
                "CJ", "CY", "JR", "PM", "RS", "WH", "ZC",
            }
            if product in czce_products:
                # 优先从映射恢复大小写
                exact = self._instrument_exact_case.get(czce_fmt)
                if exact:
                    return exact
                # 如果 3 位格式存在于配置中，直接使用
                if czce_fmt in self._contract_info:
                    return czce_fmt
                # 4 位格式本身不在配置中， fallback 到 3 位格式
                if inst not in self._contract_info:
                    return czce_fmt

        # 3. 通过 ProductID 确定交易所：DCE/GFEX/SHFE/INE 统一小写
        product_id = inst.rstrip("0123456789")
        exchange = self._product_exchange_map.get(product_id)
        if exchange in ("DCE", "GFEX", "SHFE", "INE"):
            lower_inst = inst.lower()
            exact = self._instrument_exact_case.get(lower_inst.upper())
            if exact:
                return exact
            return lower_inst

        # 未知交易所（如其他需要小写的品种），保留原始大小写
        return instrument_id.strip()

    def _get_exact_instrument_id(self, instrument_id: str) -> str:
        """返回原始大小写的合约代码（GFEX 等需要小写）"""
        inst = instrument_id.strip().upper()
        return self._instrument_exact_case.get(inst, instrument_id.strip())

    def _load_hold_std(self) -> bool:
        if not os.path.exists(self.hold_std_path):
            self.print(f"[错误] 找不到 hold-std.json: {self.hold_std_path}")
            return False
        try:
            with open(self.hold_std_path, "r", encoding="utf-8") as f:
                self._hold_std = json.load(f)
            total_vol = 0
            for item in self._hold_std:
                vol = item.get("持仓量") or item.get("手数") or item.get("数量") or "0"
                try:
                    total_vol += int(float(str(vol).strip()))
                except (ValueError, TypeError):
                    pass
            self.print(f"[信息] 已加载标准持仓 {len(self._hold_std)} 个合约，共 {total_vol} 手")
            return True
        except Exception as e:
            self.print(f"[错误] 加载 hold-std.json 失败: {e}")
            return False

    def _positions_to_hold_std(self, positions: List[dict]) -> List[dict]:
        """将 CTP 原始持仓聚合转换为 hold-std.json 格式"""
        aggregated: Dict[Tuple[str, int], int] = {}
        for pos in positions:
            contract = self._standardize_contract(pos["InstrumentID"])
            direction = pos["PosiDirection"]
            volume = pos["Position"]
            if not contract or direction not in (2, 3):
                continue
            key = (contract, direction)
            aggregated[key] = aggregated.get(key, 0) + volume

        result: List[dict] = []
        for (contract, direction), volume in aggregated.items():
            result.append({
                "合约": contract,
                "买/卖": "买" if direction == 2 else "卖",
                "手数": volume,
            })
        return result

    def _save_hold_std(self) -> bool:
        """将 self._hold_std 保存到 hold-std.json"""
        try:
            with open(self.hold_std_path, "w", encoding="utf-8") as f:
                json.dump(self._hold_std, f, ensure_ascii=False, indent=2)
            self.print(f"[信息] 已保存标准持仓到 {self.hold_std_path}")
            return True
        except Exception as e:
            self.print(f"[错误] 保存 hold-std.json 失败: {e}")
            return False

    # ------------------------------------------------------------------
    # 交易时段判断
    # ------------------------------------------------------------------
    @staticmethod
    def _is_time_in_sessions(now_time: dt_time, sessions: List[Tuple[dt_time, dt_time]]) -> bool:
        """判断当前时间是否在任一交易时段内（支持跨午夜，如 21:00-01:00）"""
        for start, end in sessions:
            if start <= end:
                if start <= now_time <= end:
                    return True
            else:
                # 跨午夜
                if now_time >= start or now_time <= end:
                    return True
        return False

    def _is_simulation_env(self) -> bool:
        """判断是否为 TTS/模拟环境（7x24 运行，无需检查交易时段）"""
        return "openctp.cn" in (self._front or "")

    def is_contract_in_trading_time(self, instrument_id: str) -> bool:
        """检查指定合约当前是否处于可交易时段"""
        # TTS 模拟环境 7x24 运行，跳过时段检查
        if self._is_simulation_env():
            return True
        info = self._get_contract_info(instrument_id)
        product_id = info.get("ProductID", "")
        sessions = PRODUCT_TRADING_SESSIONS.get(product_id)
        if not sessions:
            # DCE 标准化后 ProductID 是大写，但配置里是小写，做兼容
            sessions = PRODUCT_TRADING_SESSIONS.get(product_id.lower())
        if not sessions:
            # 未配置的品种，默认走三段日盘
            sessions = DAY_3SEG
        now_time = datetime.now().time()
        return self._is_time_in_sessions(now_time, sessions)

    # ------------------------------------------------------------------
    # 持仓对比
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_direction(row: dict) -> str:
        """从字典中提取方向字段，兼容多种列名"""
        for key in ("买/卖", "多空", "方向", "持仓方向", "Direction", "direction", "买卖"):
            val = (row.get(key) or "").strip()
            if val:
                return val
        return ""

    @staticmethod
    def _extract_contract(row: dict) -> str:
        # 优先取代码类字段，避免"合约名"在某些柜台是中文描述导致对不齐
        for key in ("合约ID", "合约代码", "InstrumentID", "instrument_id", "合约", "合约名", "合约名称"):
            val = (row.get(key) or "").strip()
            if val:
                return val.upper()
        return ""

    @staticmethod
    def _extract_volume(row: dict) -> int:
        for key in ("持仓量", "手数", "数量", "总持仓", "Volume", "volume"):
            val = row.get(key)
            if val is not None:
                try:
                    return int(float(str(val).strip()))
                except (ValueError, TypeError):
                    continue
        return 0

    def _parse_hold_std(self) -> Dict[Tuple[str, int], int]:
        result: Dict[Tuple[str, int], int] = {}
        for i, row in enumerate(self._hold_std):
            raw_contract = self._extract_contract(row)
            contract = self._standardize_contract(raw_contract)
            direction_str = self._extract_direction(row)
            volume = self._extract_volume(row)
            if not contract:
                self.print(f"[调试-hold] 第{i}条 contract为空 raw='{raw_contract}' keys={list(row.keys())}")
                continue
            if volume <= 0:
                self.print(f"[调试-hold] 第{i}条 {contract} volume={volume}")
                continue
            if direction_str in ("买", "多头", "多", "Buy", "BUY", "buy", "B"):
                direction = 2
            elif direction_str in ("卖", "空头", "空", "Sell", "SELL", "sell", "S"):
                direction = 3
            else:
                self.print(f"[调试-hold] 第{i}条 {contract} 方向无法解析 '{direction_str}'")
                continue
            result[(contract, direction)] = result.get((contract, direction), 0) + volume
        return result

    def _aggregate_actual_positions(self) -> Dict[Tuple[str, int], int]:
        result: Dict[Tuple[str, int], int] = {}
        for pos in self._actual_positions:
            contract = self._standardize_contract(pos["InstrumentID"])
            key = (contract, pos["PosiDirection"])
            result[key] = result.get(key, 0) + pos["Position"]
        return result

    def _get_position_detail(self, contract: str, direction: int) -> dict:
        """获取指定合约+方向的持仓详情（总持仓、今仓、昨仓、交易所）"""
        pos_total = 0
        today_total = 0
        yd_total = 0
        exchange_id = ""
        contract_upper = contract.upper()
        for pos in self._actual_positions:
            if pos["InstrumentID"].upper() == contract_upper and pos["PosiDirection"] == direction:
                pos_total += pos["Position"]
                today_total += pos["TodayPosition"]
                yd_total += pos["YdPosition"]
                if not exchange_id:
                    exchange_id = pos.get("ExchangeID", "")
        return {
            "Position": pos_total,
            "TodayPosition": today_total,
            "YdPosition": yd_total,
            "ExchangeID": exchange_id,
        }

    def _get_actual_position_volume(self, contract: str, direction: int) -> int:
        """获取指定合约+方向的实际持仓总量（直接从 CTP 原始数据汇总）"""
        return self._get_position_detail(contract, direction)["Position"]

    def compare_positions(self) -> Tuple[bool, List[dict]]:
        target = self._parse_hold_std()
        actual = self._aggregate_actual_positions()

        self.print(f"[信息] 标准持仓: {len(target)} 个, 实际持仓: {len(actual)} 个")

        if set(target.keys()) != set(actual.keys()):
            self.print("[信息] 持仓合约/方向不一致")
            for k in set(target.keys()) - set(actual.keys()):
                self.print(f"  标准有但账户无: {k[0]} 方向={k[1]}")
            for k in set(actual.keys()) - set(target.keys()):
                self.print(f"  账户有但标准无: {k[0]} 方向={k[1]}")
            return False, []

        for key, t_vol in target.items():
            a_vol = actual.get(key, 0)
            if t_vol != a_vol:
                self.print(
                    f"[信息] 手数不一致: {key[0]} 方向={key[1]} "
                    f"标准={t_vol} 实际={a_vol}"
                )
                return False, []

        orders = []
        for (contract, direction), vol in target.items():
            info = self._get_contract_info(contract)
            orders.append({
                "instrument_id": contract,
                "direction": "buy" if direction == 2 else "sell",
                "volume": vol,
                "exchange_id": info["ExchangeID"],
                "product_id": info["ProductID"],
                "price_tick": info["PriceTick"],
            })
        return True, orders

    def _guess_exchange(self, instrument_id: str) -> str:
        # 优先从 main_contracts.json / instruments.json 的 ProductID 映射中查找
        product_id = instrument_id.rstrip("0123456789").upper()
        exchange = self._product_exchange_map.get(product_id)
        if exchange:
            return exchange
        # fallback 到硬编码前缀表
        prefix = instrument_id[:2].upper()
        mapping = {
            "IF": "CFFEX", "IC": "CFFEX", "IH": "CFFEX", "IM": "CFFEX",
            "TS": "CFFEX", "TF": "CFFEX", "T": "CFFEX", "TL": "CFFEX",
            "AU": "SHFE", "AG": "SHFE", "CU": "SHFE", "AL": "SHFE",
            "ZN": "SHFE", "PB": "SHFE", "NI": "SHFE", "SN": "SHFE",
            "RB": "SHFE", "HC": "SHFE", "FU": "SHFE", "BU": "SHFE",
            "RU": "SHFE", "SP": "SHFE", "AO": "SHFE", "BR": "SHFE",
            "NR": "SHFE", "SC": "INE", "LU": "INE", "BC": "INE",
            "EC": "INE", "AP": "CZCE", "CF": "CZCE", "CY": "CZCE",
            "FG": "CZCE", "MA": "CZCE", "OI": "CZCE", "RM": "CZCE",
            "SA": "CZCE", "SF": "CZCE", "SM": "CZCE", "SR": "CZCE",
            "TA": "CZCE", "UR": "CZCE", "PX": "CZCE", "PF": "CZCE",
            "PK": "CZCE", "PR": "CZCE", "PL": "CZCE", "SH": "CZCE",
            "A": "DCE", "B": "DCE", "C": "DCE", "CS": "DCE",
            "EB": "DCE", "EG": "DCE", "I": "DCE", "J": "DCE",
            "JD": "DCE", "JM": "DCE", "L": "DCE", "LH": "DCE",
            "M": "DCE", "P": "DCE", "PG": "DCE", "PP": "DCE",
            "RR": "DCE", "V": "DCE", "Y": "DCE", "FB": "DCE",
            "BB": "DCE", "LG": "DCE", "LC": "GFEX", "SI": "GFEX",
            "PS": "GFEX", "PT": "GFEX", "PD": "GFEX",
        }
        return mapping.get(prefix, "SHFE")