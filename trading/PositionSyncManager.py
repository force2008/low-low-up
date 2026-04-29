# -*- coding: utf-8 -*-
"""
持仓管理器（限价单版）

功能:
- 首次建仓：账户空仓时，按 hold-std.json / initial_positions.json 买入建仓（含30秒超时撤单重发）
- 持仓对比：对比账户实际持仓与 hold-std.json，不一致时自动补单/平仓（默认30分钟冷却，防TTS/线上来回切）
- 委托执行：从 signal.json 读取新增委托，在 CTP 上执行限价单（不自动撤单）
- 支持通过 main_contracts.json 自动查找合约所属交易所及 PriceTick

用法:
    from trading.PositionSyncManager import PositionSyncManager
    mgr = PositionSyncManager(hold_std_path, main_contracts_path)
    mgr.sync_and_trade()          # 建仓 + 持仓对比（首次建仓后每30分钟执行一次）
    mgr.execute_orders(signal_path)  # 执行委托（仅提交，不撤单）
    del mgr  # 释放CTP连接
"""

import json
import os
import re
import sys
import threading
import time
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import CTdSpiBase, tdapi

# 飞书通知
from utils.feishu_notifier import FeishuNotifier


# =====================================================================
# 各品种交易时段配置（基于中国期货市场实际规则）
# =====================================================================

def _sess(start_h, start_m, end_h, end_m):
    """快速构造时段元组，支持跨午夜"""
    return (dt_time(start_h, start_m), dt_time(end_h, end_m))


# 日盘统一三段（中金所股指/国债除外）
DAY_3SEG = [_sess(9, 0, 10, 15), _sess(10, 30, 11, 30), _sess(13, 30, 15, 0)]

# 中金所股指日盘
CFFEX_INDEX_DAY = [_sess(9, 30, 11, 30), _sess(13, 0, 15, 0)]
# 中金所国债日盘
CFFEX_BOND_DAY = [_sess(9, 15, 11, 30), _sess(13, 0, 15, 15)]

# 夜盘（跨午夜的在判断逻辑中自动处理）
NIGHT_2300 = _sess(21, 0, 23, 0)
NIGHT_0100 = _sess(21, 0, 1, 0)
NIGHT_0130 = _sess(21, 0, 1, 30)
NIGHT_0230 = _sess(21, 0, 2, 30)

# 按 ProductID 聚合的完整时段映射
PRODUCT_TRADING_SESSIONS: Dict[str, List] = {
    # === 中金所 - 股指（无夜盘） ===
    "IF": CFFEX_INDEX_DAY,
    "IC": CFFEX_INDEX_DAY,
    "IM": CFFEX_INDEX_DAY,
    "IH": CFFEX_INDEX_DAY,
    # === 中金所 - 国债（无夜盘） ===
    "T": CFFEX_BOND_DAY,
    "TF": CFFEX_BOND_DAY,
    "TS": CFFEX_BOND_DAY,
    "TL": CFFEX_BOND_DAY,

    # === 上期所 - 夜盘到 23:00 ===
    "bu": DAY_3SEG + [NIGHT_2300],
    "ru": DAY_3SEG + [NIGHT_2300],
    "zn": DAY_3SEG + [NIGHT_2300],
    "pb": DAY_3SEG + [NIGHT_2300],
    "al": DAY_3SEG + [NIGHT_2300],
    "cu": DAY_3SEG + [NIGHT_2300],
    "rb": DAY_3SEG + [NIGHT_2300],
    "hc": DAY_3SEG + [NIGHT_2300],
    "fu": DAY_3SEG + [NIGHT_2300],
    "sp": DAY_3SEG + [NIGHT_2300],
    "br": DAY_3SEG + [NIGHT_2300],
    "ao": DAY_3SEG + [NIGHT_2300],
    # === 上期所 - 夜盘到 01:00 ===
    "ni": DAY_3SEG + [NIGHT_0100],
    "sn": DAY_3SEG + [NIGHT_0100],
    # === 上期所 - 夜盘到 02:30 ===
    "au": DAY_3SEG + [NIGHT_0230],
    "ag": DAY_3SEG + [NIGHT_0230],
    "ss": DAY_3SEG + [NIGHT_0230],

    # === 能源中心 - 夜盘到 23:00 ===
    "lu": DAY_3SEG + [NIGHT_2300],
    "bc": DAY_3SEG + [NIGHT_2300],
    "nr": DAY_3SEG + [NIGHT_2300],
    "ec": DAY_3SEG + [NIGHT_2300],
    # === 能源中心 - 夜盘到 02:30 ===
    "sc": DAY_3SEG + [NIGHT_0230],

    # === 大商所 - 夜盘到 23:00 ===
    "m": DAY_3SEG + [NIGHT_2300],
    "a": DAY_3SEG + [NIGHT_2300],
    "b": DAY_3SEG + [NIGHT_2300],
    "p": DAY_3SEG + [NIGHT_2300],
    "y": DAY_3SEG + [NIGHT_2300],
    "l": DAY_3SEG + [NIGHT_2300],
    "pp": DAY_3SEG + [NIGHT_2300],
    "v": DAY_3SEG + [NIGHT_2300],
    "eg": DAY_3SEG + [NIGHT_2300],
    "eb": DAY_3SEG + [NIGHT_2300],
    "pg": DAY_3SEG + [NIGHT_2300],
    "rr": DAY_3SEG + [NIGHT_2300],
    "fb": DAY_3SEG + [NIGHT_2300],
    "bb": DAY_3SEG + [NIGHT_2300],
    "lg": DAY_3SEG + [NIGHT_2300],
    # === 大商所 - 夜盘到 01:30 ===
    "i": DAY_3SEG + [NIGHT_0130],
    "j": DAY_3SEG + [NIGHT_0130],
    "jm": DAY_3SEG + [NIGHT_0130],
    "lh": DAY_3SEG + [NIGHT_0130],
    # === 大商所 - 无夜盘 ===
    "c": DAY_3SEG,
    "cs": DAY_3SEG,
    "jd": DAY_3SEG,

    # === 郑商所 - 夜盘到 23:00 ===
    "CF": DAY_3SEG + [NIGHT_2300],
    "RM": DAY_3SEG + [NIGHT_2300],
    "MA": DAY_3SEG + [NIGHT_2300],
    "SR": DAY_3SEG + [NIGHT_2300],
    "TA": DAY_3SEG + [NIGHT_2300],
    "OI": DAY_3SEG + [NIGHT_2300],
    "FG": DAY_3SEG + [NIGHT_2300],
    "SA": DAY_3SEG + [NIGHT_2300],
    "AP": DAY_3SEG + [NIGHT_2300],
    # === 郑商所 - 夜盘到 01:30 ===
    "SM": DAY_3SEG + [NIGHT_0130],
    "SF": DAY_3SEG + [NIGHT_0130],
    "PX": DAY_3SEG + [NIGHT_0130],
    "PR": DAY_3SEG + [NIGHT_0130],
    "PF": DAY_3SEG + [NIGHT_0130],
    "PK": DAY_3SEG + [NIGHT_0130],
    "PL": DAY_3SEG + [NIGHT_0130],
    "SH": DAY_3SEG + [NIGHT_0130],
    "UR": DAY_3SEG + [NIGHT_0130],
    # === 郑商所 - 无夜盘 ===
    "CJ": DAY_3SEG,
    "CY": DAY_3SEG,
    "JR": DAY_3SEG,
    "PM": DAY_3SEG,
    "RS": DAY_3SEG,
    "WH": DAY_3SEG,
    "ZC": DAY_3SEG,

    # === 广期所 - 夜盘到 23:00 ===
    "lc": DAY_3SEG + [NIGHT_2300],
    "si": DAY_3SEG + [NIGHT_2300],
    "ps": DAY_3SEG + [NIGHT_2300],
    "pt": DAY_3SEG + [NIGHT_2300],
    "pd": DAY_3SEG + [NIGHT_2300],
}


# =====================================================================
# 持仓同步管理器
# =====================================================================

class PositionSyncManager(CTdSpiBase):
    """持仓同步管理器：查询持仓 → 对比标准持仓 → 限价加仓/平仓（30分钟冷却） / 首次建仓（30秒超时撤单重发）"""

    # CTP 报单状态码
    _OST_ALL_TRADED = tdapi.THOST_FTDC_OST_AllTraded          # '0' 全部成交
    _OST_PART_TRADED = tdapi.THOST_FTDC_OST_PartTradedQueueing  # '1' 部分成交还在队列
    _OST_NO_TRADE = tdapi.THOST_FTDC_OST_NoTradeQueueing      # '3' 未成交还在队列
    _OST_CANCELED = tdapi.THOST_FTDC_OST_Canceled             # '5' 已撤销

    def __init__(
        self,
        hold_std_path: str,
        main_contracts_path: str,
        conf=None,
        env_name: str = None,
    ):
        self.hold_std_path = hold_std_path
        self.main_contracts_path = main_contracts_path
        self.env_name = env_name

        self._hold_std: List[dict] = []
        self._actual_positions: List[dict] = []
        self._pos_query_event = threading.Event()

        # 订单追踪
        self._orders: Dict[str, dict] = {}
        self._order_lock = threading.Lock()
        self._order_ref_seq = 0

        # 合约信息缓存: InstrumentID -> dict
        self._contract_info: Dict[str, dict] = {}
        self._instrument_exact_case: Dict[str, str] = {}  # UPPER -> exact case

        # 撤单响应事件
        self._cancel_events: Dict[str, threading.Event] = {}

        # 持仓对齐冷却：防止 CTP 持仓查询滞后导致重复下单
        self._last_align_time: Dict[Tuple[str, int, str], float] = {}

        # 完整仓位对齐冷却（30分钟）
        self._last_full_sync_time: float = 0.0

        # 行情查询（线程安全：按 request_id 隔离）
        self._md_lock = threading.Lock()
        self._md_request_id = 0
        self._md_pending: Dict[int, dict] = {}

        # 飞书通知
        self._feishu = FeishuNotifier()

        # CTP 报 1006 不存在的合约，记录下来避免重复尝试
        self._invalid_instruments: set = set()

        # 已处理的 signal.json 报单编号，防止 compare_orders 从 CSV 中反复检测到同一笔委托导致重复提交
        self._processed_signal_ids: set = set()

        super().__init__(conf=conf)

    def _notify_async(self, text: str):
        """异步发送飞书通知，避免 HTTP 阻塞报单流程"""
        threading.Thread(
            target=self._feishu.send_text, args=(text,), daemon=True
        ).start()

    def wait_login(self, timeout: int = 30):
        """覆盖基类的 wait_login，增加超时保护"""
        for _ in range(timeout):
            time.sleep(1)
            if self.is_login:
                return
        raise TimeoutError(f"CTP 登录超时（{timeout}秒）")

    # ------------------------------------------------------------------
    # 数据加载
    # ------------------------------------------------------------------
    def _load_contract_info(self) -> bool:
        """从 main_contracts.json / instruments.json 加载合约信息（交易所、PriceTick、ProductID）"""
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
            self.print(f"[信息] 已加载 {len(self._contract_info)} 个主力合约信息")
        except Exception as e:
            self.print(f"[错误] 加载 main_contracts.json 失败: {e}")
            return False

        # 2) 全部合约（补充大小写映射，特别是 GFEX lowercase 合约）
        inst_path = os.path.join(PROJECT_ROOT, "data", "contracts", "instruments.json")
        if os.path.exists(inst_path):
            try:
                with open(inst_path, "r", encoding="utf-8") as f:
                    instruments = json.load(f)
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
                    if product_id and exchange:
                        self._product_exchange_map[product_id] = exchange
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
        - CZCE 4位年月转3位年月（SA2405→SA405）
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
                exact = self._instrument_exact_case.get(czce_fmt)
                if exact:
                    return exact
                if czce_fmt in self._contract_info:
                    return czce_fmt
                if inst not in self._contract_info:
                    return czce_fmt

        # 3. 通过 ProductID 确定交易所：DCE/GFEX 统一小写
        product_id = inst.rstrip("0123456789")
        exchange = self._product_exchange_map.get(product_id)
        if exchange in ("DCE", "GFEX"):
            lower_inst = inst.lower()
            exact = self._instrument_exact_case.get(lower_inst.upper())
            if exact:
                return exact
            return lower_inst

        # 未知交易所（如 SHFE 部分小写合约），保留原始大小写
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
            self.print(f"[信息] 已加载标准持仓 {len(self._hold_std)} 条")
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
    # 行情查询（获取最新价用于计算限价单价格）
    # ------------------------------------------------------------------
    def query_market_data(self, instrument_id: str, timeout: int = 5) -> Optional[dict]:
        """通过交易API查询合约行情快照，返回最新买卖价（线程安全）"""
        exact_id = self._standardize_contract(instrument_id)
        with self._md_lock:
            self._md_request_id += 1
            req_id = self._md_request_id
            pending = {"event": threading.Event(), "data": None}
            self._md_pending[req_id] = pending
        req = tdapi.CThostFtdcQryDepthMarketDataField()
        req.InstrumentID = exact_id
        self._api.ReqQryDepthMarketData(req, req_id)
        ok = pending["event"].wait(timeout=timeout)
        if not ok:
            self.print(f"[警告] {exact_id} 行情查询超时")
        with self._md_lock:
            data = pending.get("data")
            self._md_pending.pop(req_id, None)
        return data

    def OnRspQryDepthMarketData(
        self,
        pDepthMarketData: tdapi.CThostFtdcDepthMarketDataField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        with self._md_lock:
            pending = self._md_pending.get(nRequestID)
        if pending:
            if pDepthMarketData:
                pending["data"] = {
                    "InstrumentID": pDepthMarketData.InstrumentID or "",
                    "LastPrice": pDepthMarketData.LastPrice,
                    "BidPrice1": pDepthMarketData.BidPrice1,
                    "AskPrice1": pDepthMarketData.AskPrice1,
                    "UpperLimitPrice": pDepthMarketData.UpperLimitPrice,
                    "LowerLimitPrice": pDepthMarketData.LowerLimitPrice,
                }
            # 必须等最后一条数据返回，避免 simu 环境返回多条约行情时拿到错误合约
            if bIsLast:
                pending["event"].set()

    # ------------------------------------------------------------------
    # 持仓查询
    # ------------------------------------------------------------------
    def query_positions(self, timeout: int = 10, retries: int = 1) -> Optional[List[dict]]:
        """查询持仓，超时返回 None（调用者需区分"超时"和"确实无持仓"）"""
        for attempt in range(retries + 1):
            self._pos_query_event.clear()
            self._actual_positions = []
            req = tdapi.CThostFtdcQryInvestorPositionField()
            req.BrokerID = self._broker_id
            req.InvestorID = self._user_id
            self._api.ReqQryInvestorPosition(req, 0)
            ok = self._pos_query_event.wait(timeout=timeout)
            if ok:
                return list(self._actual_positions)
            if attempt < retries:
                self.print(f"[警告] 持仓查询超时，第 {attempt + 1} 次重试...")
                time.sleep(1)
        self.print(f"[错误] 持仓查询连续 {retries + 1} 次超时，返回 None")
        return None

    def OnRspQryInvestorPosition(
        self,
        pInvestorPosition: tdapi.CThostFtdcInvestorPositionField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pInvestorPosition:
            # 总持仓为 0 的不算持仓
            if pInvestorPosition.Position == 0:
                pass
            else:
                self._actual_positions.append({
                    "InstrumentID": (pInvestorPosition.InstrumentID or "").strip().upper(),
                    "PosiDirection": int(pInvestorPosition.PosiDirection)
                    if pInvestorPosition.PosiDirection
                    else 0,
                    "Position": pInvestorPosition.Position,
                    "TodayPosition": pInvestorPosition.TodayPosition,
                    "YdPosition": pInvestorPosition.YdPosition,
                    "ExchangeID": (pInvestorPosition.ExchangeID or "").strip(),
                })
        if bIsLast:
            self._pos_query_event.set()

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
        for key in ("合约", "合约名", "合约代码", "InstrumentID", "instrument_id", "合约名称"):
            val = (row.get(key) or "").strip()
            if val:
                return val.upper()
        return ""

    @staticmethod
    def _extract_volume(row: dict) -> int:
        for key in ("手数", "数量", "总持仓", "Volume", "volume", "持仓量"):
            val = row.get(key, "0")
            if val:
                try:
                    return int(str(val).strip())
                except ValueError:
                    continue
        return 0

    def _parse_hold_std(self) -> Dict[Tuple[str, int], int]:
        result: Dict[Tuple[str, int], int] = {}
        for row in self._hold_std:
            contract = self._standardize_contract(self._extract_contract(row))
            direction_str = self._extract_direction(row)
            volume = self._extract_volume(row)
            if not contract:
                continue
            if volume <= 0:
                continue
            if direction_str in ("买", "多头", "多", "Buy", "BUY", "buy", "B"):
                direction = 2
            elif direction_str in ("卖", "空头", "空", "Sell", "SELL", "sell", "S"):
                direction = 3
            else:
                self.print(f"[警告] 无法解析方向 '{direction_str}'，跳过 {contract}，可用字段: {list(row.keys())}")
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

    def _record_1009_rejection(self, contract: str, direction: int):
        """记录某合约平仓被 CTP 以 1009（持仓不足）拒绝，用于短期冷却避免重复尝试"""
        key = (contract.upper(), direction)
        if not hasattr(self, "_close_1009_cooldown"):
            self._close_1009_cooldown: Dict[Tuple[str, int], float] = {}
        self._close_1009_cooldown[key] = time.time()

    def _is_1009_cooled_down(self, contract: str, direction: int, cooldown: int = 300) -> bool:
        """检查某合约的 1009 拒绝是否仍在冷却期内（默认 5 分钟）"""
        key = (contract.upper(), direction)
        if not hasattr(self, "_close_1009_cooldown"):
            return True
        last = self._close_1009_cooldown.get(key)
        if last is None:
            return True
        return time.time() - last >= cooldown

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
            "EC": "INE", "AP": "ZCE", "CF": "ZCE", "CY": "ZCE",
            "FG": "ZCE", "MA": "ZCE", "OI": "ZCE", "RM": "ZCE",
            "SA": "ZCE", "SF": "ZCE", "SM": "ZCE", "SR": "ZCE",
            "TA": "ZCE", "UR": "ZCE", "PX": "ZCE", "PF": "ZCE",
            "PK": "ZCE", "PR": "ZCE", "PL": "ZCE", "SH": "ZCE",
            "A": "DCE", "B": "DCE", "C": "DCE", "CS": "DCE",
            "EB": "DCE", "EG": "DCE", "I": "DCE", "J": "DCE",
            "JD": "DCE", "JM": "DCE", "L": "DCE", "LH": "DCE",
            "M": "DCE", "P": "DCE", "PG": "DCE", "PP": "DCE",
            "RR": "DCE", "V": "DCE", "Y": "DCE", "FB": "DCE",
            "BB": "DCE", "LG": "DCE", "LC": "GFEX", "SI": "GFEX",
            "PS": "GFEX", "PT": "GFEX", "PD": "GFEX",
        }
        return mapping.get(prefix, "SHFE")

    # ------------------------------------------------------------------
    # 报单 / 撤单 / 成交跟踪
    # ------------------------------------------------------------------
    def _next_order_ref(self) -> str:
        with self._order_lock:
            self._order_ref_seq += 1
            return f"PSM{self._order_ref_seq:09d}"

    def place_limit_order(
        self,
        exchange_id: str,
        instrument_id: str,
        direction: str,
        volume: int,
        limit_price: float,
    ) -> Optional[str]:
        """下限价单，返回 order_ref"""
        exact_id = self._standardize_contract(instrument_id)
        if exact_id.upper() in self._invalid_instruments:
            self.print(f"[跳过] {exact_id} 在无效合约列表中（1006），跳过报单")
            return None
        order_ref = self._next_order_ref()

        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.ExchangeID = exchange_id
        req.InstrumentID = exact_id
        req.OrderRef = order_ref
        req.LimitPrice = limit_price
        req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        req.Direction = (
            tdapi.THOST_FTDC_D_Buy
            if direction == "buy"
            else tdapi.THOST_FTDC_D_Sell
        )
        req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open
        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        req.VolumeTotalOriginal = volume
        req.IsAutoSuspend = 0
        req.IsSwapOrder = 0
        req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose

        fill_event = threading.Event()
        with self._order_lock:
            self._orders[order_ref] = {
                "event": fill_event,
                "status": "submitted",
                "sys_id": "",
                "instr": exact_id,
                "exchange": exchange_id,
                "volume": volume,
                "direction": direction,
                "offset_flag": tdapi.THOST_FTDC_OF_Open,
            }

        ret = self._api.ReqOrderInsert(req, 0)
        if ret != 0:
            self.print(f"[错误] {exact_id} 报单发送失败，返回值={ret}")
            with self._order_lock:
                self._orders[order_ref]["status"] = "send_failed"
            self._notify_async(
                f"❌ 报单发送失败\n合约：{exact_id}\n方向：{direction}\n"
                f"手数：{volume} 手\n限价：{limit_price}\n错误码：{ret}"
            )
            return None

        self.print(
            f"[报单] {exact_id} {direction} {volume}手 "
            f"限价={limit_price} OrderRef={order_ref}"
        )
        self._notify_async(
            f"📤 报单已提交\n合约：{exact_id}\n方向：{direction}\n"
            f"手数：{volume} 手\n限价：{limit_price}\nOrderRef：{order_ref}"
        )
        return order_ref

    def cancel_order(self, order_ref: str) -> bool:
        with self._order_lock:
            info = self._orders.get(order_ref)
            if not info:
                return False

        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.UserID = self._user_id
        req.ExchangeID = info["exchange"]
        req.InstrumentID = info["instr"]
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        if info.get("sys_id"):
            req.OrderSysID = info["sys_id"]
        else:
            req.FrontID = self._front_id or 0
            req.SessionID = self._session_id or 0
            req.OrderRef = order_ref

        cancel_event = threading.Event()
        with self._order_lock:
            self._cancel_events[order_ref] = cancel_event

        self.print(f"[撤单] {info['instr']} OrderRef={order_ref}")
        self._api.ReqOrderAction(req, 0)
        cancel_event.wait(timeout=3)
        return True

    # ------------------------------------------------------------------
    # CTP 回调
    # ------------------------------------------------------------------
    def _handle_order_rejection(self, pInputOrder, pRspInfo, source: str):
        """统一处理报单拒绝/错误回报"""
        if not pRspInfo or pRspInfo.ErrorID == 0:
            return
        ref = (pInputOrder.OrderRef or "").strip() if pInputOrder else ""
        inst = (pInputOrder.InstrumentID or "").strip() if pInputOrder else ""
        err_id = pRspInfo.ErrorID
        err_msg = getattr(pRspInfo, "ErrorMsg", "")
        msg = f"❌ {source}: {inst} Ref={ref} ErrorID={err_id} {err_msg}"
        self.print(msg)
        self._notify_async(msg)

        # ErrorID=1006 合约不存在，记录下来避免后续重复尝试
        if err_id == 1006 and inst:
            self._invalid_instruments.add(inst.upper())
            self.print(f"[警告] 合约 {inst} 被标记为无效（1006），后续将跳过")

        # ErrorID=1009 持仓不足：如果是平仓单，记录冷却避免反复重试
        if err_id == 1009 and inst:
            with self._order_lock:
                info = self._orders.get(ref)
                if info:
                    offset = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)
                    if offset != tdapi.THOST_FTDC_OF_Open:
                        direction = 2 if info.get("direction") == "buy" else 3
                        self._record_1009_rejection(inst, direction)
                        self.print(f"[警告] {inst} 平仓被拒绝（1009），记录 5 分钟冷却")

        with self._order_lock:
            info = self._orders.get(ref)
            if info:
                info["status"] = "rejected"
                info["event"].set()

        self._update_order_file_status(ref, status="rejected")

    def OnRspOrderInsert(
        self,
        pInputOrder: tdapi.CThostFtdcInputOrderField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """报单录入响应：CTP 接受或拒绝报单"""
        self._handle_order_rejection(pInputOrder, pRspInfo, "报单被服务器拒绝")

    def OnErrRtnOrderInsert(
        self,
        pInputOrder: tdapi.CThostFtdcInputOrderField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
    ):
        """报单录入错误回报（异步）"""
        self._handle_order_rejection(pInputOrder, pRspInfo, "报单错误回报")

    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        order_ref = (pOrder.OrderRef or "").strip()
        sys_id = (pOrder.OrderSysID or "").strip()
        status = pOrder.OrderStatus

        with self._order_lock:
            info = self._orders.get(order_ref)
            if not info:
                return
            info["status"] = status
            if sys_id:
                info["sys_id"] = sys_id

        desc = {
            self._OST_ALL_TRADED: "全部成交",
            self._OST_PART_TRADED: "部分成交",
            self._OST_NO_TRADE: "未成交",
            self._OST_CANCELED: "已撤销",
        }.get(status, f"状态={status}")

        self.print(
            f"[回报] {info['instr']} OrderRef={order_ref} "
            f"{desc} SysID={sys_id}"
        )

        if status == self._OST_ALL_TRADED:
            info["event"].set()

        # 同步状态到文件
        session_id = getattr(pOrder, "SessionID", 0)
        self._update_order_file_status(order_ref, status, sys_id, session_id=session_id)

    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        order_ref = (pTrade.OrderRef or "").strip()
        with self._order_lock:
            info = self._orders.get(order_ref)
            if info:
                self.print(
                    f"[成交] {info['instr']} OrderRef={order_ref} "
                    f"Volume={pTrade.Volume} Price={pTrade.Price}"
                )
                self._notify_async(
                    f"✅ 成交回报\n"
                    f"合约：{info['instr']}\n"
                    f"成交手数：{pTrade.Volume} 手\n"
                    f"成交价格：{pTrade.Price}\n"
                    f"OrderRef：{order_ref}"
                )
                # 同步成交手数到文件
                session_id = getattr(pTrade, "SessionID", 0)
                self._update_order_file_status(
                    order_ref, status=None, volume_traded=pTrade.Volume, session_id=session_id
                )

    def OnRspOrderAction(
        self,
        pInputOrderAction: tdapi.CThostFtdcInputOrderActionField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        order_ref = (pInputOrderAction.OrderRef or "").strip()
        if pRspInfo and pRspInfo.ErrorID != 0:
            self.print(
                f"[撤单失败] OrderRef={order_ref} "
                f"ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
            )
        else:
            self.print(f"[撤单响应] OrderRef={order_ref}")

        with self._order_lock:
            ev = self._cancel_events.pop(order_ref, None)
        if ev:
            ev.set()

    def _is_order_pending(self, info: dict) -> bool:
        """判断订单是否仍在途中（未最终完结）"""
        status = info.get("status", "")
        if status in ("send_failed", "rejected"):
            return False
        # 统一转成字符串比较（CTP 返回 bytes 或 str）
        s = status.decode("ascii") if isinstance(status, bytes) else str(status)
        # 0=全部成交, 2=部成部撤, 4=已撤单, 5=已撤销
        return s not in ("0", "2", "4", "5")

    def _get_pending_open_volume(self, instrument_id: str, pos_direction: int) -> int:
        """获取指定合约+持仓方向的未成交开仓委托总量"""
        # pos_direction: 2=多, 3=空
        # 多头开仓 direction_label="buy", 空头开仓 direction_label="sell"
        expected_dir = "buy" if pos_direction == 2 else "sell"
        inst_upper = instrument_id.upper()
        total = 0
        with self._order_lock:
            for info in self._orders.values():
                if not self._is_order_pending(info):
                    continue
                if info.get("instr", "").upper() != inst_upper:
                    continue
                if info.get("direction") != expected_dir:
                    continue
                if info.get("offset_flag", tdapi.THOST_FTDC_OF_Open) == tdapi.THOST_FTDC_OF_Open:
                    total += info.get("volume", 0)
        return total

    def _get_pending_close_volume(self, instrument_id: str, pos_direction: int) -> int:
        """获取指定合约+持仓方向的未成交平仓委托总量"""
        # pos_direction: 2=多, 3=空
        # 多头平仓 direction_label="sell", 空头平仓 direction_label="buy"
        expected_dir = "sell" if pos_direction == 2 else "buy"
        inst_upper = instrument_id.upper()
        total = 0
        with self._order_lock:
            for info in self._orders.values():
                if not self._is_order_pending(info):
                    continue
                if info.get("instr", "").upper() != inst_upper:
                    continue
                if info.get("direction") != expected_dir:
                    continue
                offset = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)
                if offset != tdapi.THOST_FTDC_OF_Open:
                    total += info.get("volume", 0)
        return total

    # ------------------------------------------------------------------
    # 单合约交易（仅用于首次建仓：含行情查询、时段检查、限价单、30秒超时撤单重发）
    # ------------------------------------------------------------------
    def _trade_single(
        self,
        instrument_id: str,
        exchange_id: str,
        direction: str,
        volume: int,
        price_tick: float,
        timeout: int = 30,
        max_retries: int = 1,
    ) -> bool:

        # 1. 检查交易时段
        if not self.is_contract_in_trading_time(instrument_id):
            self.print(f"[跳过] {instrument_id} 当前不在交易时段")
            return False

        # 2. 查询行情快照
        md = self.query_market_data(instrument_id, timeout=5)
        if not md:
            self.print(f"[跳过] {instrument_id} 无法获取行情，跳过")
            return False

        # 3. 计算限价单价格（严格按买一/卖一价，不加减 tick）
        if direction == "buy":
            limit_price = md.get("AskPrice1", 0)
            if limit_price <= 0:
                limit_price = md.get("LastPrice", 0)
            if limit_price <= 0:
                self.print(f"[跳过] {instrument_id} 无有效卖一价")
                return False
            # 安全检查：不超过涨停价
            upper = md.get("UpperLimitPrice", 0)
            if upper > 0 and limit_price > upper:
                limit_price = upper
        else:
            limit_price = md.get("BidPrice1", 0)
            if limit_price <= 0:
                limit_price = md.get("LastPrice", 0)
            if limit_price <= 0:
                self.print(f"[跳过] {instrument_id} 无有效买一价")
                return False
            # 安全检查：不低于跌停价
            lower = md.get("LowerLimitPrice", 0)
            if lower > 0 and limit_price < lower:
                limit_price = lower

        self.print(
            f"[定价] {instrument_id} {direction} 限价={limit_price} "
            f"买一={md.get('BidPrice1')} 卖一={md.get('AskPrice1')} "
            f"涨停={md.get('UpperLimitPrice')} 跌停={md.get('LowerLimitPrice')}"
        )

        # 4. 下单
        order_ref = self.place_limit_order(
            exchange_id, instrument_id, direction, volume, limit_price
        )
        if not order_ref:
            return False

        # 5. 等待成交
        with self._order_lock:
            info = self._orders[order_ref]
        filled = info["event"].wait(timeout=timeout)

        if filled:
            self.print(f"[成功] {instrument_id} 限价单成交")
            return True

        # 6. 超时撤单
        self.print(f"[超时] {instrument_id} {timeout}秒未成交，执行撤单")
        self.cancel_order(order_ref)
        time.sleep(1)

        with self._order_lock:
            final_status = self._orders[order_ref]["status"]
        if final_status == self._OST_ALL_TRADED:
            self.print(f"[成功] {instrument_id} 撤单前已成交")
            return True

        # 7. 重发
        if max_retries > 0:
            self.print(f"[重发] {instrument_id} 重新提交限价单")
            # 重发前重新查行情，价格可能已经变化
            return self._trade_single(
                instrument_id, exchange_id, direction, volume,
                price_tick, timeout=timeout, max_retries=max_retries - 1
            )

        self.print(f"[放弃] {instrument_id} 多次尝试仍未成交")
        return False

    # ------------------------------------------------------------------
    # 持仓对比告警
    # ------------------------------------------------------------------
    def _send_position_mismatch_alert(self, actual: dict, target: dict):
        """发送持仓不一致飞书告警"""
        lines = ["⚠️ 持仓不一致告警"]
        for k in set(target.keys()) - set(actual.keys()):
            lines.append(f"标准有但账户无: {k[0]} 方向={'多' if k[1]==2 else '空'}")
        for k in set(actual.keys()) - set(target.keys()):
            lines.append(f"账户有但标准无: {k[0]} 方向={'多' if k[1]==2 else '空'}")
        for key, t_vol in target.items():
            a_vol = actual.get(key, 0)
            if t_vol != a_vol:
                lines.append(f"手数不一致: {key[0]} 方向={'多' if key[1]==2 else '空'} 标准={t_vol} 实际={a_vol}")
        self._notify_async("\n".join(lines))

    # ------------------------------------------------------------------
    # 核心流程：建仓 + 持仓对比
    # ------------------------------------------------------------------
    def sync_and_trade(
        self,
        trade_volume: int = 1,
        timeout: int = 30,
    ) -> bool:
        self.print("=" * 60)
        self.print("持仓管理开始")
        self.print("=" * 60)

        if not self._load_contract_info():
            return False

        # 先查询一次实际持仓
        positions = self.query_positions(timeout=10)
        if positions is None:
            self.print("[错误] 持仓查询失败，本次同步中止，避免误判空仓导致重复建仓")
            return False
        self.print(f"[信息] 账户实际持仓 {len(positions)} 条原始记录")

        if not self._load_hold_std():
            # 首次运行：没有 hold-std.json，尝试从初始配置读取
            initial_path = os.path.join(PROJECT_ROOT, "data", "initial_positions.json")
            if os.path.exists(initial_path):
                try:
                    with open(initial_path, "r", encoding="utf-8") as f:
                        self._hold_std = json.load(f)
                    self.print(f"[信息] 首次运行，从 initial_positions.json 加载 {len(self._hold_std)} 条标准持仓")
                except Exception as e:
                    self.print(f"[错误] 加载 initial_positions.json 失败: {e}")
                    self._hold_std = []
            else:
                # 没有初始配置，从 CTP 持仓生成
                self.print("[信息] 首次运行，未找到 hold-std.json，从 CTP 持仓生成标准持仓...")
                self._hold_std = self._positions_to_hold_std(positions)

            if not self._hold_std:
                self.print("[错误] 无有效标准持仓来源（initial_positions.json 不存在且账户无持仓）")
                return False
            if not self._save_hold_std():
                return False
            # 重新查询持仓
            positions = self.query_positions(timeout=10)
            if positions is None:
                self.print("[错误] 重新查询持仓失败，中止")
                return False
            self.print(f"[信息] 重新查询持仓 {len(positions)} 条原始记录")

        # 判断是否需要"首次建仓"（账户空但标准持仓有数据）
        actual_agg = self._aggregate_actual_positions()
        target = self._parse_hold_std()

        if not actual_agg and target:
            self.print(f"[结论] 账户空仓，标准持仓有 {len(target)} 个合约，执行首次建仓...")
            return self._build_positions(target, timeout=timeout)

        # 非首次建仓：30 分钟冷却，避免 TTS/线上环境成交不同步导致来回切
        FULL_SYNC_COOLDOWN = 1800  # 30 分钟
        elapsed = time.time() - self._last_full_sync_time
        if elapsed < FULL_SYNC_COOLDOWN:
            self.print(
                f"[冷却] 距离上次仓位对齐仅 {int(elapsed)} 秒（每 {FULL_SYNC_COOLDOWN // 60} 分钟对齐一次），跳过"
            )
            return True
        self._last_full_sync_time = time.time()

        # 持仓对比（扣除在途委托后的有效持仓）
        effective_actual: Dict[Tuple[str, int], int] = {}
        all_keys = set(actual_agg.keys()) | set(target.keys())
        for key in all_keys:
            contract, direction = key
            a_vol = actual_agg.get(key, 0)
            pending_open = self._get_pending_open_volume(contract, direction)
            pending_close = self._get_pending_close_volume(contract, direction)
            effective = a_vol + pending_open - pending_close
            effective_actual[key] = effective
            if pending_open or pending_close:
                self.print(
                    f"[在途] {contract} 实际={a_vol} 开仓委托={pending_open} 平仓委托={pending_close} "
                    f"有效={effective}"
                )

        missing_orders = []
        excess_orders = []
        for key, t_vol in target.items():
            eff_vol = effective_actual.get(key, 0)
            if t_vol > eff_vol:
                diff = t_vol - eff_vol
                contract, direction = key
                missing_orders.append({
                    "contract": contract,
                    "direction": "buy" if direction == 2 else "sell",
                    "volume": diff,
                })

        for key, a_vol in actual_agg.items():
            t_vol = target.get(key, 0)
            eff_vol = effective_actual.get(key, a_vol)
            if eff_vol > t_vol:
                diff = eff_vol - t_vol
                contract, direction = key
                excess_orders.append({
                    "contract": contract,
                    "direction": direction,
                    "volume": diff,
                })

        ALIGN_COOLDOWN = 60  # 同一合约同方向补单/平仓冷却秒数

        if missing_orders:
            self.print(f"[补单] 标准持仓有 {len(missing_orders)} 个合约缺额，自动补齐...")
            for mo in missing_orders:
                cooldown_key = (mo["contract"], 2 if mo["direction"] == "buy" else 3, "open")
                last_time = self._last_align_time.get(cooldown_key, 0)
                if time.time() - last_time < ALIGN_COOLDOWN:
                    self.print(f"[冷却] {mo['contract']} {mo['direction']} 最近 {ALIGN_COOLDOWN}s 内已对齐，跳过")
                    continue

                info = self._get_contract_info(mo["contract"])
                md = self.query_market_data(mo["contract"], timeout=3)
                if md:
                    if mo["direction"] == "buy":
                        limit_price = md.get("AskPrice1", 0)
                        if limit_price <= 0:
                            limit_price = md.get("LastPrice", 0)
                    else:
                        limit_price = md.get("BidPrice1", 0)
                        if limit_price <= 0:
                            limit_price = md.get("LastPrice", 0)
                else:
                    self.print(f"[跳过] {mo['contract']} 无法获取行情，跳过补单")
                    continue

                if limit_price <= 0:
                    self.print(f"[跳过] {mo['contract']} 无有效价格，跳过补单")
                    continue

                self.print(
                    f"[补单] {mo['contract']} {mo['direction']} {mo['volume']}手 "
                    f"限价={limit_price}"
                )
                self._place_order(
                    exchange_id=info["ExchangeID"],
                    instrument_id=mo["contract"],
                    direction=mo["direction"],
                    volume=mo["volume"],
                    limit_price=limit_price,
                    offset_flag=tdapi.THOST_FTDC_OF_Open,
                    wait_fill=False,
                )
                self._last_align_time[cooldown_key] = time.time()
                time.sleep(0.3)

            # 发送补单汇总通知
            if missing_orders:
                lines = ["📋 持仓缺额已自动提交补单"]
                for mo in missing_orders:
                    lines.append(f"  {mo['contract']} {'买' if mo['direction']=='buy' else '卖'} {mo['volume']}手")
                self._notify_async("\n".join(lines))

        if excess_orders:
            self.print(f"[平仓] 发现 {len(excess_orders)} 个合约超额持仓，自动平仓...")
            for eo in excess_orders:
                cooldown_key = (eo["contract"], eo["direction"], "close")
                last_time = self._last_align_time.get(cooldown_key, 0)
                if time.time() - last_time < ALIGN_COOLDOWN:
                    self.print(f"[冷却] {eo['contract']} {'多' if eo['direction']==2 else '空'} 最近 {ALIGN_COOLDOWN}s 内已平仓，跳过")
                    continue

                # 1009 拒绝冷却：近期被 CTP 以持仓不足拒绝过的，暂时跳过
                if not self._is_1009_cooled_down(eo["contract"], eo["direction"]):
                    self.print(f"[冷却] {eo['contract']} {'多' if eo['direction']==2 else '空'} 因近期 1009 拒绝，跳过平仓")
                    continue

                detail = self._get_position_detail(eo["contract"], eo["direction"])
                info = self._get_contract_info(eo["contract"])
                exchange_id = detail["ExchangeID"] or info["ExchangeID"]

                # 防御：实际持仓为 0 或 CTP 数据滞后，跳过
                actual_pos = detail["Position"]
                if actual_pos <= 0:
                    self.print(f"[跳过] {eo['contract']} {'多' if eo['direction']==2 else '空'} 实际持仓为 {actual_pos}，跳过平仓（CTP 数据可能滞后）")
                    continue

                # 限制平仓数量不超过实际持仓
                diff = min(eo["volume"], actual_pos)
                if diff <= 0:
                    continue

                md = self.query_market_data(eo["contract"], timeout=3)
                if md:
                    if eo["direction"] == 2:  # 多头平仓 → 卖出
                        close_direction = "sell"
                        limit_price = md.get("BidPrice1", 0)
                        if limit_price <= 0:
                            limit_price = md.get("LastPrice", 0)
                    else:  # 空头平仓 → 买入
                        close_direction = "buy"
                        limit_price = md.get("AskPrice1", 0)
                        if limit_price <= 0:
                            limit_price = md.get("LastPrice", 0)
                else:
                    self.print(f"[跳过] {eo['contract']} 无法获取行情，跳过平仓")
                    continue

                if limit_price <= 0:
                    self.print(f"[跳过] {eo['contract']} 无有效价格，跳过平仓")
                    continue

                is_shfe = exchange_id in ("SHFE", "INE")
                today = detail["TodayPosition"]
                yd = detail["YdPosition"]

                if is_shfe and today > 0:
                    close_today = min(today, diff)
                    self.print(
                        f"[平仓-平今] {eo['contract']} {'卖' if close_direction=='sell' else '买'} {close_today}手 限价={limit_price}"
                    )
                    self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=eo["contract"],
                        direction=close_direction,
                        volume=close_today,
                        limit_price=limit_price,
                        offset_flag=tdapi.THOST_FTDC_OF_CloseToday,
                        wait_fill=False,
                    )
                    diff -= close_today
                    time.sleep(0.3)

                if diff > 0:
                    offset = tdapi.THOST_FTDC_OF_CloseYesterday if is_shfe else tdapi.THOST_FTDC_OF_Close
                    label = "平昨" if is_shfe else "平仓"
                    self.print(
                        f"[平仓-{label}] {eo['contract']} {'卖' if close_direction=='sell' else '买'} {diff}手 限价={limit_price}"
                    )
                    self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=eo["contract"],
                        direction=close_direction,
                        volume=diff,
                        limit_price=limit_price,
                        offset_flag=offset,
                        wait_fill=False,
                    )
                    diff -= diff
                    time.sleep(0.3)

                self._last_align_time[cooldown_key] = time.time()

            # 发送平仓汇总通知
            lines = ["📉 超额持仓已自动提交平仓"]
            for eo in excess_orders:
                dname = "多" if eo["direction"] == 2 else "空"
                lines.append(f"  {eo['contract']} {dname} {eo['volume']}手")
            self._notify_async("\n".join(lines))

        if missing_orders or excess_orders:
            self.print("[结论] 持仓调整完成（缺额已补 / 超额已平）")
            self.print("=" * 60)
            return True

        self.print("[结论] 持仓一致，无需操作")
        self.print("=" * 60)
        return True

    def _build_positions(self, target: dict, timeout: int = 30) -> bool:
        """首次建仓：账户为空，按 target 买入"""
        # 双重确认：建仓前再次查询，防止 sync_and_trade 判断空仓后到建仓前期间已有持仓变化
        positions = self.query_positions(timeout=10)
        if positions is None:
            self.print("[错误] 建仓前持仓查询失败，中止建仓")
            return False
        if positions:
            self.print(f"[警告] 建仓前检测到账户已有 {len(positions)} 条持仓记录，取消首次建仓（避免重复买入导致超仓）")
            # 把实际持仓写回 hold-std，避免下次继续误判
            self._hold_std = self._positions_to_hold_std(positions)
            if self._hold_std:
                self._save_hold_std()
            self._last_full_sync_time = time.time()
            return False

        success_count = 0
        for (contract, direction), vol in target.items():
            info = self._get_contract_info(contract)
            direction_str = "buy" if direction == 2 else "sell"
            self.print(f"[建仓] {contract} {direction_str} {vol}手")
            ok = self._trade_single(
                instrument_id=contract,
                exchange_id=info["ExchangeID"],
                direction=direction_str,
                volume=vol,
                price_tick=info["PriceTick"],
                timeout=timeout,
                max_retries=1,
            )
            if ok:
                success_count += 1
            time.sleep(0.5)

        # 建仓完成后重新查询并保存为新的标准持仓
        time.sleep(1)
        positions = self.query_positions(timeout=10)
        if positions is None:
            self.print("[警告] 建仓后持仓查询超时，无法更新标准持仓文件")
            positions = []
        self._hold_std = self._positions_to_hold_std(positions)
        if self._hold_std:
            self._save_hold_std()

        # 建仓完成后重置全量同步冷却时间，避免主循环立即再次对比
        self._last_full_sync_time = time.time()

        self.print("=" * 60)
        self.print(f"首次建仓结束: 成功 {success_count}/{len(target)}")
        self.print("=" * 60)
        return success_count == len(target)

    # ------------------------------------------------------------------
    # 委托执行：从 signal.json 读取新增委托并执行
    # ------------------------------------------------------------------
    def execute_orders(self, signal_path: str, timeout: int = 30) -> bool:
        """从 signal.json 读取新增委托，在 CTP 上执行限价单

        signal.json 格式：list[dict]，每条委托至少包含以下字段之一：
            - 合约 / 合约名 / InstrumentID
            - 方向 / 买/卖 / Direction
            - 开平 / 开平标志 / OffsetFlag
            - 手数 / 数量 / Volume
            - 价格 / 委托价 / Price
        """
        if not os.path.exists(signal_path):
            self.print(f"[信息] 无委托文件: {signal_path}")
            return True

        try:
            with open(signal_path, "r", encoding="utf-8") as f:
                orders = json.load(f)
        except Exception as e:
            self.print(f"[错误] 读取 {signal_path} 失败: {e}")
            return False

        if not orders:
            self.print("[信息] 无新增委托")
            return True

        self.print(f"[信息] 待执行委托 {len(orders)} 条")
        success_count = 0
        skipped_count = 0
        for order in orders:
            # 报单编号去重：compare_orders 从 CSV 差集生成 signal.json，同一笔委托可能在多轮 CSV 导出后被反复识别
            signal_id = self._extract_field(order, ["报单编号", "OrderRef", "order_ref", "ID", "id"])
            if signal_id and signal_id in self._processed_signal_ids:
                self.print(f"[跳过] 报单编号 {signal_id} 已处理过，避免重复提交")
                skipped_count += 1
                continue

            contract = self._standardize_contract(self._extract_field(order, ["合约", "合约名", "InstrumentID", "instrument_id"]))
            direction_str = self._extract_field(order, ["方向", "买卖", "买/卖", "Direction", "direction"])
            offset_str = self._extract_field(order, ["开平", "开平标志", "OffsetFlag", "offset_flag", "offset"])
            volume_str = self._extract_field(order, ["手数", "数量", "总报单量", "委托数量", "报单数量", "Volume", "volume"])
            price_str = self._extract_field(order, ["价格", "委托价", "Price", "price", "LimitPrice"])

            if not contract or not direction_str or not volume_str:
                self.print(f"[跳过] 委托字段不全: {order}")
                skipped_count += 1
                continue

            try:
                volume = int(str(volume_str).strip())
                price = float(str(price_str).strip()) if price_str else 0.0
            except ValueError:
                self.print(f"[跳过] 委托数值解析失败: {order}")
                skipped_count += 1
                continue

            # 方向映射
            if direction_str in ("买", "多头", "多", "Buy", "BUY", "buy", "B"):
                direction = tdapi.THOST_FTDC_D_Buy
                direction_label = "buy"
            elif direction_str in ("卖", "空头", "空", "Sell", "SELL", "sell", "S"):
                direction = tdapi.THOST_FTDC_D_Sell
                direction_label = "sell"
            else:
                self.print(f"[跳过] 未知方向: {direction_str}")
                skipped_count += 1
                continue

            # 开平映射
            if offset_str in ("开仓", "Open", "OPEN", "open", "开"):
                offset_flag = tdapi.THOST_FTDC_OF_Open
            elif offset_str in ("平仓", "Close", "CLOSE", "close", "平"):
                offset_flag = tdapi.THOST_FTDC_OF_Close
            elif offset_str in ("平今", "CloseToday", "close_today", "closetoday"):
                offset_flag = tdapi.THOST_FTDC_OF_CloseToday
            elif offset_str in ("平昨", "CloseYesterday", "close_yesterday", "closeyesterday"):
                offset_flag = tdapi.THOST_FTDC_OF_CloseYesterday
            else:
                # 默认按方向推断：买=开仓，卖=平仓（可调整）
                offset_flag = tdapi.THOST_FTDC_OF_Open if direction == tdapi.THOST_FTDC_D_Buy else tdapi.THOST_FTDC_OF_Close
                self.print(f"[警告] 未识别开平标志 '{offset_str}'，默认使用 {'开仓' if offset_flag == tdapi.THOST_FTDC_OF_Open else '平仓'}")

            info = self._get_contract_info(contract)
            exchange_id = info["ExchangeID"]

            env = getattr(self, "env_name", "")
            is_file_price = env in ("online", "simu")
            if is_file_price:
                # online 环境：严格使用委托文件里的价格挂限价单
                if price <= 0:
                    self.print(f"[跳过] {contract} 委托文件未提供有效价格，跳过报单")
                    skipped_count += 1
                    continue
                limit_price = price
            else:
                # 非 online/simu 环境（TTS/simu-vip 等）：查询市场对手价挂限价单
                md = self.query_market_data(contract, timeout=3)
                if direction_label == "buy":
                    market_price = md.get("AskPrice1", 0) if md else 0
                else:
                    market_price = md.get("BidPrice1", 0) if md else 0
                if not market_price:
                    market_price = md.get("LastPrice", 0) if md else 0
                if market_price <= 0:
                    self.print(f"[跳过] {contract} 无法获取市场价格，跳过报单")
                    skipped_count += 1
                    continue
                limit_price = market_price

            ok = self._place_order(
                exchange_id=exchange_id,
                instrument_id=contract,
                direction=direction_label,
                volume=volume,
                limit_price=limit_price,
                offset_flag=offset_flag,
                wait_fill=False,
            )
            if ok:
                success_count += 1
                if signal_id:
                    self._processed_signal_ids.add(signal_id)
            time.sleep(0.3)

        pending = len(orders) - skipped_count
        self.print("=" * 60)
        self.print(f"委托执行结束: 成功 {success_count}/{pending} (跳过 {skipped_count})")
        self.print("=" * 60)
        # 如果所有订单都已处理过（跳过）或成功，返回 True，让外部清空 signal.json
        return success_count == pending

    def _place_order(
        self,
        exchange_id: str,
        instrument_id: str,
        direction: str,
        volume: int,
        limit_price: float,
        offset_flag: int,
        wait_fill: bool = True,
    ) -> bool:
        """下单（通用，支持指定开平标志）

        Args:
            wait_fill: 是否阻塞等待成交。True=等待成交后返回；False=提交后立即返回。
        """
        # 标准化合约代码（CZCE 3位年月 + GFEX 小写恢复）
        exact_id = self._standardize_contract(instrument_id)
        if exact_id.upper() in self._invalid_instruments:
            self.print(f"[跳过] {exact_id} 在无效合约列表中（1006），跳过报单")
            return False

        order_ref = self._next_order_ref()

        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.ExchangeID = exchange_id
        req.InstrumentID = exact_id
        req.OrderRef = order_ref
        req.LimitPrice = limit_price
        req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        req.Direction = tdapi.THOST_FTDC_D_Buy if direction == "buy" else tdapi.THOST_FTDC_D_Sell
        req.CombOffsetFlag = offset_flag
        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        req.VolumeTotalOriginal = volume
        req.IsAutoSuspend = 0
        req.IsSwapOrder = 0
        req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose

        fill_event = threading.Event()
        with self._order_lock:
            self._orders[order_ref] = {
                "event": fill_event,
                "status": "submitted",
                "sys_id": "",
                "instr": exact_id,
                "exchange": exchange_id,
                "volume": volume,
                "direction": direction,
                "offset_flag": offset_flag,
            }

        ret = self._api.ReqOrderInsert(req, 0)
        if ret != 0:
            self.print(f"[错误] {exact_id} 报单发送失败，返回值={ret}")
            with self._order_lock:
                self._orders[order_ref]["status"] = "send_failed"
            self._notify_async(
                f"❌ 委托发送失败\n合约：{exact_id}\n方向：{direction}\n"
                f"手数：{volume} 手\n限价：{limit_price}\n开平：{offset_flag}\n错误码：{ret}"
            )
            return False

        self.print(
            f"[委托] {exact_id} {direction} {volume}手 限价={limit_price} "
            f"开平={offset_flag} OrderRef={order_ref}"
        )
        self._notify_async(
            f"📤 委托已提交\n合约：{exact_id}\n方向：{direction}\n"
            f"手数：{volume} 手\n限价：{limit_price}\n开平：{offset_flag}\nOrderRef：{order_ref}"
        )

        # 持久化到文件，供 UI 显示
        self._save_order_to_file(
            order_ref=order_ref,
            instrument_id=exact_id,
            direction=0 if direction == "buy" else 1,
            comb_offset_flag=chr(offset_flag) if isinstance(offset_flag, int) else str(offset_flag),
            volume_total_original=volume,
            limit_price=limit_price,
            order_status="3",
            volume_traded=0,
            exchange_id=exchange_id,
            insert_time=time.strftime("%H:%M:%S"),
            front_id=getattr(self, "_front_id", 0) or 0,
            session_id=getattr(self, "_session_id", 0) or 0,
        )

        if not wait_fill:
            self.print(f"[委托] {exact_id} 已提交，不等待成交，立即返回")
            return True

        # 等待成交（不撤单重发，委托执行由外部系统控制）
        filled = fill_event.wait(timeout=30)
        return filled

    @staticmethod
    def _extract_field(row: dict, candidates: list) -> str:
        """从字典中提取第一个存在的候选字段值"""
        for key in candidates:
            if key in row:
                val = row[key]
                if val is not None and str(val).strip() != "":
                    return str(val).strip()
        return ""

    # ------------------------------------------------------------------
    # 委托持久化（供 UI 跨进程查看）
    # ------------------------------------------------------------------
    _ORDERS_FILE = os.path.join(PROJECT_ROOT, "order-check", "orders_submitted.json")

    def _save_order_to_file(self, **kwargs):
        """保存或更新委托到共享文件"""
        order_ref = kwargs.get("order_ref", "")
        session_id = kwargs.get("session_id", 0)
        if not order_ref:
            return
        orders = []
        if os.path.exists(self._ORDERS_FILE):
            try:
                with open(self._ORDERS_FILE, "r", encoding="utf-8") as f:
                    orders = json.load(f)
            except Exception:
                orders = []
        # 查找并更新（按 order_ref + session_id 匹配），或追加
        for o in orders:
            if o.get("order_ref") == order_ref and o.get("session_id") == session_id:
                o.update(kwargs)
                break
        else:
            orders.append(kwargs)
        try:
            with open(self._ORDERS_FILE, "w", encoding="utf-8") as f:
                json.dump(orders, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.print(f"[警告] 保存委托文件失败: {e}")

    def _update_order_file_status(self, order_ref: str, status, sys_id: str = "", volume_traded: int = 0, session_id: int = 0):
        """更新委托状态到共享文件（按 order_ref + session_id 匹配）"""
        if not os.path.exists(self._ORDERS_FILE):
            return
        try:
            with open(self._ORDERS_FILE, "r", encoding="utf-8") as f:
                orders = json.load(f)
            for o in orders:
                if o.get("order_ref") == order_ref and o.get("session_id") == session_id:
                    if status is not None:
                        o["order_status"] = status.decode() if isinstance(status, bytes) else status
                    if sys_id:
                        o["order_sys_id"] = sys_id
                    if volume_traded:
                        o["volume_traded"] = volume_traded
                    break
            with open(self._ORDERS_FILE, "w", encoding="utf-8") as f:
                json.dump(orders, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


# ----------------------------------------------------------------------
# 便捷函数
# ----------------------------------------------------------------------
def run_position_sync(
    hold_std_path: str,
    main_contracts_path: str,
    trade_volume: int = 1,
    timeout: int = 30,
    conf=None,
    env_name: str = None,
) -> bool:
    mgr = None
    try:
        mgr = PositionSyncManager(
            hold_std_path=hold_std_path,
            main_contracts_path=main_contracts_path,
            conf=conf,
            env_name=env_name,
        )
        return mgr.sync_and_trade(
            trade_volume=trade_volume, timeout=timeout
        )
    except Exception as e:
        print(f"[异常] 持仓同步过程中出错: {e}")
        return False
    finally:
        if mgr is not None:
            try:
                del mgr
            except Exception:
                pass


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hold_std = os.path.join(base_dir, "order-check", "hold-std.json")
    main_contracts = os.path.join(base_dir, "data", "contracts", "main_contracts.json")
    run_position_sync(hold_std, main_contracts, trade_volume=1, timeout=30)
