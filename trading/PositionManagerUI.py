# -*- coding: utf-8 -*-
"""
持仓管理 UI（tkinter）

功能:
    - 展示当前账户持仓
    - 选中合约时实时显示盘口（买/卖价+量）
    - 委托单列表（含状态），实时刷新
    - 平仓选中合约 / 全部平仓
    - 飞书通知

用法:
    cd /c/projects/low-low-up
    python trading/PositionManagerUI.py [env]
"""

import json
import os
import queue
import re
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import CTdSpiBase, tdapi
from config import config
from utils.feishu_notifier import FeishuNotifier


class PositionManagerUI(CTdSpiBase):

    def __init__(self, conf=None, master=None):
        self.master = master or tk.Tk()
        self.master.title("持仓管理")
        self.master.geometry("1200x750")
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

        # 数据
        self._positions_raw: list = []
        self._positions_agg: list = []
        self._pos_done = threading.Event()

        self._md_lock = threading.Lock()
        self._md_request_id = 0
        self._md_pending: dict = {}  # request_id -> {"event": Event(), "data": None}

        self._orders_raw: list = []
        self._order_done = threading.Event()
        self._orders_lock = threading.Lock()  # 保护 _orders_raw 并发访问

        self._trades_raw: list = []
        self._trade_done = threading.Event()

        self._refresh_lock = threading.Lock()

        self._contract_info: dict = {}
        self._instrument_exact_case: dict = {}  # UPPER -> exact case (e.g. LC2607 -> lc2607)
        self._feishu = FeishuNotifier()
        self._order_ref_seq = 0

        # UI
        self._tree_pos = None
        self._tree_order = None
        self._status_var = tk.StringVar(value="正在连接CTP...")
        self._quote_var = tk.StringVar(value="盘口: --")
        self._pos_summary_var = tk.StringVar(value="总手数: 0 | 今仓: 0 | 昨仓: 0 | 保证金: 0.00")

        # 自动轮询控制
        self._stop_poll = threading.Event()
        self._selected_instrument: str = ""

        # 委托列表变化检测（避免无意义重绘）
        self._last_order_digest: str = ""

        # 资金数据
        self._account_done = threading.Event()
        self._account_data: dict = {}

        self._build_ui()
        super().__init__(conf=conf)
        threading.Thread(target=self._bootstrap, daemon=True).start()

    def _notify_async(self, text: str):
        """异步发送飞书通知，避免 HTTP 阻塞报单流程"""
        threading.Thread(
            target=self._feishu.send_text, args=(text,), daemon=True
        ).start()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self):
        # 顶部状态栏
        top = tk.Frame(self.master)
        top.pack(fill=tk.X, padx=10, pady=5)
        tk.Label(top, textvariable=self._status_var, anchor="w").pack(side=tk.LEFT)
        tk.Label(top, textvariable=self._quote_var, anchor="w", fg="blue").pack(side=tk.LEFT, padx=20)
        tk.Button(top, text="刷新持仓", command=self._on_refresh).pack(side=tk.RIGHT)
        tk.Button(top, text="刷新委托", command=self._on_refresh_orders).pack(side=tk.RIGHT, padx=5)

        # 资金信息栏
        self._acct_var = tk.StringVar(value="资金: 加载中...")
        acct_frame = tk.Frame(self.master, bg="#f0f0f0")
        acct_frame.pack(fill=tk.X, padx=10, pady=2)
        tk.Label(acct_frame, textvariable=self._acct_var, anchor="w", bg="#f0f0f0", font=("Microsoft YaHei", 10, "bold")).pack(side=tk.LEFT, padx=5)

        # 持仓表格
        frame_pos = tk.LabelFrame(self.master, text="持仓", padx=5, pady=5)
        frame_pos.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        cols_pos = (
            "合约", "方向", "总持仓", "今仓", "昨仓",
            "开仓均价", "持仓盈亏", "保证金", "最新价"
        )
        self._tree_pos = ttk.Treeview(frame_pos, columns=cols_pos, show="headings", height=8)
        for c in cols_pos:
            self._tree_pos.heading(c, text=c)
            self._tree_pos.column(c, width=90, anchor="center")
        self._tree_pos.column("合约", width=100)
        self._tree_pos.column("持仓盈亏", width=100)
        self._tree_pos.column("保证金", width=100)
        self._tree_pos.bind("<<TreeviewSelect>>", self._on_position_select)

        vsb1 = ttk.Scrollbar(frame_pos, orient="vertical", command=self._tree_pos.yview)
        self._tree_pos.configure(yscrollcommand=vsb1.set)
        self._tree_pos.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb1.pack(side=tk.RIGHT, fill=tk.Y)

        # 持仓汇总
        pos_summary = tk.Label(frame_pos, textvariable=self._pos_summary_var, anchor="w", fg="#333", font=("Microsoft YaHei", 9, "bold"))
        pos_summary.pack(fill=tk.X, pady=(5, 0))

        # 委托表格
        frame_ord = tk.LabelFrame(self.master, text="委托单", padx=5, pady=5)
        frame_ord.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        cols_ord = (
            "时间", "合约", "方向", "开平", "手数", "价格", "状态", "成交", "OrderRef"
        )
        self._tree_order = ttk.Treeview(frame_ord, columns=cols_ord, show="headings", height=8)
        for c in cols_ord:
            self._tree_order.heading(c, text=c)
            self._tree_order.column(c, width=90, anchor="center")
        self._tree_order.column("合约", width=100)
        self._tree_order.column("状态", width=100)
        self._tree_order.column("OrderRef", width=120)

        vsb2 = ttk.Scrollbar(frame_ord, orient="vertical", command=self._tree_order.yview)
        self._tree_order.configure(yscrollcommand=vsb2.set)
        self._tree_order.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb2.pack(side=tk.RIGHT, fill=tk.Y)

        # 底部按钮
        bot = tk.Frame(self.master)
        bot.pack(fill=tk.X, padx=10, pady=5)
        tk.Button(bot, text="平仓选中", width=12, command=self._on_close_selected).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="全部平仓", width=12, command=self._on_close_all).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="对价平选中", width=12, command=self._on_close_selected_best).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="对价平全部", width=12, command=self._on_close_all_best).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="撤单", width=12, command=self._on_cancel_order).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="撤全部", width=12, command=self._on_cancel_all_orders).pack(side=tk.LEFT, padx=5)
        tk.Button(bot, text="退出", width=12, command=self._on_close).pack(side=tk.RIGHT, padx=5)

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------
    def _bootstrap(self):
        try:
            self._wait_login(timeout=30)
            self._load_contract_info()
            self._refresh_in_thread()
            self._refresh_orders_in_thread()
            try:
                self.query_trading_account(timeout=5)
                self.master.after(0, self._update_account_ui)
            except Exception:
                pass
            # 启动自动轮询
            threading.Thread(target=self._poll_loop, daemon=True).start()
        except Exception as e:
            self.print(f"[_bootstrap] 异常: {e}")
            import traceback
            self.print(traceback.format_exc())
            self._update_status(f"登录失败: {e}")

    def _wait_login(self, timeout=30):
        for _ in range(timeout):
            time.sleep(1)
            if getattr(self, "is_login", False):
                return
        raise TimeoutError("CTP 登录超时")

    def _load_contract_info(self):
        self._product_exchange_map: dict = {}
        # 1) 主力合约配置
        path = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    contracts = json.load(f)
                for c in contracts:
                    cid = c.get("MainContractID", "").strip().upper()
                    exact = c.get("MainContractID", "").strip()
                    product_id = c.get("ProductID", "").strip().upper()
                    exchange = c.get("ExchangeID", "").strip()
                    if cid:
                        self._contract_info[cid] = {
                            "ExchangeID": exchange,
                            "PriceTick": float(c.get("PriceTick", 1)),
                            "VolumeMultiple": int(c.get("VolumeMultiple", 1)),
                            "ProductID": product_id,
                        }
                        if exact:
                            self._instrument_exact_case[cid] = exact
                    if product_id and exchange:
                        self._product_exchange_map[product_id] = exchange
            except Exception:
                pass

        # 2) 全部合约（用于补充大小写映射，特别是 GFEX  lowercase 合约）
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
                        # 若主力合约里没有，补充进 _contract_info
                        if cid not in self._contract_info:
                            self._contract_info[cid] = {
                                "ExchangeID": exchange,
                                "PriceTick": float(ins.get("PriceTick", 1)),
                                "VolumeMultiple": int(ins.get("VolumeMultiple", 1)),
                                "ProductID": product_id,
                            }
                    if product_id and exchange:
                        self._product_exchange_map[product_id] = exchange
            except Exception:
                pass

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

    def _get_contract_info(self, instrument_id: str) -> dict:
        inst = self._standardize_contract(instrument_id)
        info = self._contract_info.get(inst.upper())
        if info:
            return info
        product_id = inst.rstrip("0123456789")
        return {
            "ExchangeID": self._guess_exchange(inst),
            "PriceTick": 1.0,
            "VolumeMultiple": 1,
            "ProductID": product_id,
        }

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
            "EC": "INE",
        }
        return mapping.get(prefix, "SHFE")

    # ------------------------------------------------------------------
    # 持仓查询
    # ------------------------------------------------------------------
    def query_positions(self, timeout=10) -> list:
        self._pos_done.clear()
        self._positions_raw = []
        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        self._api.ReqQryInvestorPosition(req, 0)
        self._pos_done.wait(timeout=timeout)
        return list(self._positions_raw)

    def OnRspQryInvestorPosition(self, p, pRspInfo, nRequestID, bIsLast):
        if p and p.Position > 0:
            pos_dir = int(p.PosiDirection or 0)
            # 过滤净持仓记录(1)，只保留多头(2)/空头(3)，避免重复
            if pos_dir not in (2, 3):
                pass
            else:
                self._positions_raw.append({
                    "InstrumentID": (p.InstrumentID or "").strip().upper(),
                    "PosiDirection": pos_dir,
                    "Position": p.Position,
                    "TodayPosition": p.TodayPosition,
                    "YdPosition": p.YdPosition,
                    "OpenCost": p.OpenCost,
                    "PositionCost": p.PositionCost,
                    "PositionProfit": p.PositionProfit,
                    "UseMargin": p.UseMargin,
                    "ExchangeID": (p.ExchangeID or "").strip(),
                })
        if bIsLast:
            self._pos_done.set()

    # ------------------------------------------------------------------
    # 行情查询
    # ------------------------------------------------------------------
    def query_market_data(self, instrument_id: str, timeout=5) -> dict:
        exact_id = self._standardize_contract(instrument_id)
        with self._md_lock:
            self._md_request_id += 1
            req_id = self._md_request_id
            pending = {"event": threading.Event(), "data": None}
            self._md_pending[req_id] = pending
        req = tdapi.CThostFtdcQryDepthMarketDataField()
        req.InstrumentID = exact_id
        self._api.ReqQryDepthMarketData(req, req_id)
        pending["event"].wait(timeout=timeout)
        with self._md_lock:
            data = pending.get("data")
            self._md_pending.pop(req_id, None)
        return data if data else {}

    def OnRspQryDepthMarketData(self, p, pRspInfo, nRequestID, bIsLast):
        with self._md_lock:
            pending = self._md_pending.get(nRequestID)
        if pending:
            if p:
                pending["data"] = {
                    "LastPrice": p.LastPrice,
                    "BidPrice1": p.BidPrice1,
                    "BidVolume1": p.BidVolume1,
                    "AskPrice1": p.AskPrice1,
                    "AskVolume1": p.AskVolume1,
                    "UpperLimitPrice": p.UpperLimitPrice,
                    "LowerLimitPrice": p.LowerLimitPrice,
                }
                self.print(f"[行情] {p.InstrumentID} 最新={p.LastPrice} 买={p.BidPrice1}×{p.BidVolume1} 卖={p.AskPrice1}×{p.AskVolume1}")
            else:
                err = f" ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}" if pRspInfo else ""
                self.print(f"[行情] 查询返回空{err}")
            # 必须等最后一条数据返回，避免 simu 环境返回多条约行情时拿到错误合约
            if bIsLast:
                pending["event"].set()

    # ------------------------------------------------------------------
    # 委托查询
    # ------------------------------------------------------------------
    def query_orders(self, timeout=10) -> list:
        self._order_done.clear()
        with self._orders_lock:
            self._orders_raw = []
        req = tdapi.CThostFtdcQryOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        ret = self._api.ReqQryOrder(req, 0)
        self.print(f"[QueryOrder] 请求发送 ret={ret}")
        self._order_done.wait(timeout=timeout)
        with self._orders_lock:
            self.print(f"[QueryOrder] 返回 {len(self._orders_raw)} 条")
            return list(self._orders_raw)

    def OnRspQryOrder(self, p, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            self.print(f"[QueryOrder] 响应错误 ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}")
        if p:
            with self._orders_lock:
                self._orders_raw.append({
                    "InsertTime": (p.InsertTime or "").strip(),
                    "InstrumentID": (p.InstrumentID or "").strip().upper(),
                    "Direction": int(p.Direction) if p.Direction else 0,
                    "CombOffsetFlag": (p.CombOffsetFlag or "").strip(),
                    "VolumeTotalOriginal": p.VolumeTotalOriginal,
                    "LimitPrice": p.LimitPrice,
                    "OrderStatus": p.OrderStatus,
                    "VolumeTraded": p.VolumeTraded,
                    "OrderRef": (p.OrderRef or "").strip(),
                    "OrderSysID": (p.OrderSysID or "").strip(),
                    "FrontID": p.FrontID,
                    "SessionID": p.SessionID,
                    "ExchangeID": (p.ExchangeID or "").strip(),
                })
            self.print(f"[QueryOrder] 收到委托 {p.InstrumentID} Ref={p.OrderRef} Status={p.OrderStatus}")
        if bIsLast:
            self._order_done.set()

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        """报单响应：CTP 服务器接受或拒绝报单"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            ref = (pInputOrder.OrderRef or "").strip() if pInputOrder else ""
            inst = (pInputOrder.InstrumentID or "").strip().upper() if pInputOrder else ""
            msg = f"❌ 报单被服务器拒绝: {inst} Ref={ref} ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
            self.print(msg)
            self._update_status(msg)
            self._notify_async(msg)
            # 把拒单记录也加入订单列表，方便用户看到
            if pInputOrder:
                with self._orders_lock:
                    self._orders_raw.append({
                        "InsertTime": time.strftime("%H:%M:%S"),
                        "InstrumentID": inst,
                        "Direction": int(pInputOrder.Direction) if pInputOrder.Direction else 0,
                        "CombOffsetFlag": (pInputOrder.CombOffsetFlag or "").strip(),
                        "VolumeTotalOriginal": pInputOrder.VolumeTotalOriginal,
                        "LimitPrice": pInputOrder.LimitPrice,
                        "OrderStatus": "4",  # 已撤单（表示被拒）
                        "VolumeTraded": 0,
                        "OrderRef": ref,
                        "OrderSysID": "",
                        "FrontID": 0,
                        "SessionID": 0,
                        "ExchangeID": "",
                    })
                self._update_tree_order()

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        """报单错误回报"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            ref = (pInputOrder.OrderRef or "").strip() if pInputOrder else ""
            inst = (pInputOrder.InstrumentID or "").strip().upper() if pInputOrder else ""
            msg = f"❌ 报单错误: {inst} Ref={ref} ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
            self.print(msg)
            self._update_status(msg)
            self._notify_async(msg)

    @staticmethod
    @staticmethod
    def _normalize_status(status):
        """统一状态为 str（0-5）"""
        if isinstance(status, bytes):
            return status.decode("ascii")
        return str(status)

    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        # 实时推送时刷新委托列表
        ref = (pOrder.OrderRef or "").strip()
        inst = (pOrder.InstrumentID or "").strip().upper()
        status = self._normalize_status(pOrder.OrderStatus)
        desc = {"0": "全部成交", "1": "部分成交", "2": "部成部撤", "3": "未成交", "4": "已撤单", "5": "已撤销"}.get(status, f"状态={status}")
        self._update_status(f"委托状态更新: {inst} {ref} {desc}")
        # 更新本地记录的 OrderStatus 等字段
        with self._orders_lock:
            for o in self._orders_raw:
                if o.get("OrderRef") == ref:
                    o["OrderStatus"] = status
                    if pOrder.FrontID:
                        o["FrontID"] = pOrder.FrontID
                    if pOrder.SessionID:
                        o["SessionID"] = pOrder.SessionID
                    if pOrder.ExchangeID:
                        o["ExchangeID"] = (pOrder.ExchangeID or "").strip()
                    if pOrder.OrderSysID:
                        o["OrderSysID"] = (pOrder.OrderSysID or "").strip()
                    break
        self._update_tree_order()

    # ------------------------------------------------------------------
    # 资金查询
    # ------------------------------------------------------------------
    def query_trading_account(self, timeout=10) -> dict:
        self._account_done.clear()
        self._account_data = {}
        req = tdapi.CThostFtdcQryTradingAccountField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        self._api.ReqQryTradingAccount(req, 0)
        ok = self._account_done.wait(timeout=timeout)
        if not ok:
            self.print("[警告] 资金查询超时")
        return dict(self._account_data)

    def OnRspQryTradingAccount(self, pTradingAccount: tdapi.CThostFtdcTradingAccountField,
                                pRspInfo: tdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        if pTradingAccount:
            self._account_data = {
                "Balance": pTradingAccount.Balance,
                "Available": pTradingAccount.Available,
                "CurrMargin": pTradingAccount.CurrMargin,
                "PositionProfit": pTradingAccount.PositionProfit,
                "CloseProfit": pTradingAccount.CloseProfit,
            }
        if bIsLast:
            self._account_done.set()

    def _update_account_ui(self):
        d = self._account_data
        if not d:
            return
        bal = d.get("Balance", 0)
        avail = d.get("Available", 0)
        margin = d.get("CurrMargin", 0)
        pos_profit = d.get("PositionProfit", 0)
        close_profit = d.get("CloseProfit", 0)
        total_profit = pos_profit + close_profit
        text = (
            f"权益 {bal:,.2f}   "
            f"可用 {avail:,.2f}   "
            f"保证金 {margin:,.2f}   "
            f"持仓盈亏 {pos_profit:+.2f}   "
            f"平仓盈亏 {close_profit:+.2f}   "
            f"总盈亏 {total_profit:+.2f}"
        )
        self._acct_var.set(text)

    # ------------------------------------------------------------------
    # 成交查询
    # ------------------------------------------------------------------
    def query_trades(self, timeout=10) -> list:
        self._trade_done.clear()
        self._trades_raw = []
        req = tdapi.CThostFtdcQryTradeField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        # 只查当天成交，避免跨会话 OrderRef 冲突
        if getattr(self, "_trading_day", ""):
            req.TradeDate = self._trading_day
        self._api.ReqQryTrade(req, 0)
        self._trade_done.wait(timeout=timeout)
        return list(self._trades_raw)

    def OnRspQryTrade(self, p, pRspInfo, nRequestID, bIsLast):
        if p:
            self._trades_raw.append({
                "TradeTime": (p.TradeTime or "").strip(),
                "InstrumentID": (p.InstrumentID or "").strip().upper(),
                "Direction": int(p.Direction) if p.Direction else 0,
                "OffsetFlag": (p.OffsetFlag or "").strip(),
                "Volume": p.Volume,
                "Price": p.Price,
                "OrderRef": (p.OrderRef or "").strip(),
            })
        if bIsLast:
            self._trade_done.set()

    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        order_ref = (pTrade.OrderRef or "").strip()
        self._update_status(
            f"成交回报: {pTrade.InstrumentID} {pTrade.Volume}手 价={pTrade.Price} Ref={order_ref}"
        )
        self._notify_async(
            f"✅ 成交回报\n合约：{pTrade.InstrumentID}\n"
            f"成交手数：{pTrade.Volume} 手\n成交价格：{pTrade.Price}\nOrderRef：{order_ref}"
        )
        self.master.after(0, self._on_refresh_orders)

    # ------------------------------------------------------------------
    # 数据聚合与 UI 刷新
    # ------------------------------------------------------------------
    def _aggregate_positions(self) -> list:
        self.print(f"[持仓聚合] 原始记录共 {len(self._positions_raw)} 条:")
        for p in self._positions_raw:
            self.print(f"  原始: {p['InstrumentID']} 方向={p['PosiDirection']} 总={p['Position']} 今={p['TodayPosition']} 昨={p['YdPosition']}")
        agg = {}
        for p in self._positions_raw:
            inst = p["InstrumentID"]
            direction = p["PosiDirection"]
            key = (inst, direction)
            if key not in agg:
                agg[key] = {
                    "InstrumentID": inst, "PosiDirection": direction,
                    "Position": 0, "TodayPosition": 0, "YdPosition": 0,
                    "OpenCost": 0.0, "PositionCost": 0.0,
                    "PositionProfit": 0.0, "UseMargin": 0.0,
                }
            for k in ("Position", "TodayPosition", "YdPosition"):
                agg[key][k] += p[k]
            for k in ("OpenCost", "PositionCost", "PositionProfit", "UseMargin"):
                agg[key][k] += p[k]
        self.print(f"[持仓聚合] 聚合后共 {len(agg)} 个合约:")
        for key, item in agg.items():
            self.print(f"  聚合: {key[0]} 方向={key[1]} 总={item['Position']} 今={item['TodayPosition']} 昨={item['YdPosition']}")

        result = []
        for item in agg.values():
            inst_upper = item["InstrumentID"]
            exact_id = self._standardize_contract(inst_upper)
            info = self._get_contract_info(inst_upper)
            vm = info["VolumeMultiple"]
            pos = item["Position"]
            avg_price = item["PositionCost"] / (pos * vm) if pos > 0 and vm > 0 else 0.0
            md = self.query_market_data(exact_id, timeout=3)
            result.append({
                **item,
                "InstrumentID": exact_id,
                "AvgPrice": round(avg_price, 2),
                "LastPrice": md.get("LastPrice", 0.0),
                "ExchangeID": info["ExchangeID"],
                "VolumeMultiple": vm,
            })
        return result

    def _refresh_in_thread(self):
        try:
            self._update_status("正在查询持仓...")
            self.query_positions(timeout=10)
            if not self._positions_raw:
                self._update_status("当前无持仓")
                self._clear_tree(self._tree_pos)
                self._pos_summary_var.set("总手数: 0 | 今仓: 0 | 昨仓: 0 | 保证金: 0.00")
                return
            self._positions_agg = self._aggregate_positions()
            self._update_tree_pos()
            total_pos = sum(p["Position"] for p in self._positions_agg)
            total_today = sum(p["TodayPosition"] for p in self._positions_agg)
            total_yd = sum(p["YdPosition"] for p in self._positions_agg)
            total_profit = sum(p["PositionProfit"] for p in self._positions_agg)
            total_margin = sum(p.get("UseMargin", 0) for p in self._positions_agg)
            self._pos_summary_var.set(
                f"总手数: {total_pos} | 今仓: {total_today} | 昨仓: {total_yd} | 保证金: {total_margin:,.2f}"
            )
            self._update_status(f"持仓查询完成 | 合约数: {len(self._positions_agg)} | 总盈亏: {total_profit:,.2f}")
        except Exception as e:
            self.print(f"[_refresh_in_thread] 异常: {e}")
            import traceback
            self.print(traceback.format_exc())
            self._update_status(f"持仓查询出错: {e}")

    def _refresh_orders_in_thread(self):
        with self._refresh_lock:
            self.query_orders(timeout=10)
            self.query_trades(timeout=10)
            self._merge_submitted_orders()
            self._update_tree_order()

    def _merge_submitted_orders(self):
        """合并 PositionSyncManager 写入的委托文件（跨进程共享）"""
        path = os.path.join(PROJECT_ROOT, "order-check", "orders_submitted.json")
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                orders = json.load(f)
            with self._orders_lock:
                existing_refs = {str(o.get("OrderRef", "")).strip() for o in self._orders_raw}
                for o in orders:
                    ref = str(o.get("order_ref", "")).strip()
                    if not ref or ref in existing_refs:
                        continue
                    # 统一字段名（文件里用 snake_case，内部用 CamelCase）
                    self._orders_raw.append({
                        "InsertTime": o.get("insert_time", ""),
                        "InstrumentID": (o.get("instrument_id", "") or "").strip(),
                        "Direction": o.get("direction", 0),
                        "CombOffsetFlag": o.get("comb_offset_flag", ""),
                        "VolumeTotalOriginal": o.get("volume_total_original", 0),
                        "LimitPrice": o.get("limit_price", 0),
                        "OrderStatus": self._normalize_status(o.get("order_status", "3")),
                        "VolumeTraded": o.get("volume_traded", 0),
                        "OrderRef": ref,
                        "OrderSysID": o.get("order_sys_id", ""),
                        "FrontID": o.get("front_id", 0),
                        "SessionID": o.get("session_id", 0),
                        "ExchangeID": o.get("exchange_id", ""),
                    })
        except Exception:
            pass

    def _update_tree_pos(self):
        self.master.after(0, self._render_tree_pos)

    def _render_tree_pos(self):
        # 保存当前选中的合约
        sel = self._tree_pos.selection()
        selected_inst = ""
        if sel:
            values = self._tree_pos.item(sel[0])["values"]
            if values:
                selected_inst = values[0]

        self._clear_tree(self._tree_pos)
        for p in self._positions_agg:
            direction = "多" if p["PosiDirection"] == 2 else "空" if p["PosiDirection"] == 3 else "-"
            item_id = self._tree_pos.insert("", "end", values=(
                p["InstrumentID"], direction, p["Position"],
                p["TodayPosition"], p["YdPosition"], p["AvgPrice"],
                f"{p['PositionProfit']:,.2f}", f"{p['UseMargin']:,.2f}", p["LastPrice"],
            ))
            if p["InstrumentID"] == selected_inst:
                self._tree_pos.selection_set(item_id)
                self._tree_pos.see(item_id)

        # 恢复选中后更新盘口显示
        if selected_inst:
            self.master.after(0, lambda: self._on_position_select())

    def _update_tree_order(self):
        self.master.after(0, self._render_tree_order)

    def _render_tree_order(self):
        try:
            # 变化检测：如果订单+成交数据和上次一样，跳过重绘
            digest_parts = []
            for o in sorted(self._orders_raw, key=lambda x: x.get("OrderRef", "")):
                digest_parts.append(f"{o.get('OrderRef')}:{self._normalize_status(o.get('OrderStatus',''))}:{o.get('VolumeTraded',0)}")
            for t in sorted(self._trades_raw, key=lambda x: x.get("OrderRef", "")):
                digest_parts.append(f"T{t.get('OrderRef')}:{t.get('Volume',0)}")
            current_digest = "|".join(digest_parts)
            if current_digest == self._last_order_digest:
                return
            self._last_order_digest = current_digest

            # 保存当前选中的 OrderRef
            sel = self._tree_order.selection()
            selected_ref = ""
            if sel:
                values = self._tree_order.item(sel[0])["values"]
                if values:
                    selected_ref = values[-1] if len(values) > 0 else ""

            self._clear_tree(self._tree_order)
            # 以 OrderRef 为键聚合成交（query_trades 已限制为当天，避免跨天冲突）
            trade_map = {}
            for t in self._trades_raw:
                ref = t["OrderRef"]
                trade_map[ref] = trade_map.get(ref, 0) + t["Volume"]

            status_map = {
                "0": "全部成交", "1": "部分成交", "2": "部成部撤",
                "3": "未成交", "4": "已撤单", "5": "已撤销",
            }
            offset_map = {
                "0": "开仓", "1": "平仓", "2": "强平",
                "3": "平今", "4": "平昨", "5": "强减",
            }
            dir_map = {0: "买", 1: "卖"}

            for o in self._orders_raw:
                try:
                    status = status_map.get(self._normalize_status(o["OrderStatus"]), str(o["OrderStatus"]))
                    offset = offset_map.get(o["CombOffsetFlag"], o["CombOffsetFlag"])
                    direction = dir_map.get(o["Direction"], str(o["Direction"]))
                    traded = trade_map.get(o["OrderRef"], o["VolumeTraded"])
                    item_id = self._tree_order.insert("", "end", values=(
                        o["InsertTime"], o["InstrumentID"], direction, offset,
                        o["VolumeTotalOriginal"], o["LimitPrice"], status, traded, o["OrderRef"],
                    ))
                    if o["OrderRef"] == selected_ref:
                        self._tree_order.selection_set(item_id)
                        self._tree_order.see(item_id)
                except Exception as e:
                    self.print(f"[_render_tree_order] 单条渲染错误: {e} data={o}")
        except Exception as e:
            self.print(f"[_render_tree_order] 渲染错误: {e}")

    @staticmethod
    def _clear_tree(tree):
        for item in tree.get_children():
            tree.delete(item)

    def _update_status(self, text: str):
        self.master.after(0, lambda: self._status_var.set(text))

    # ------------------------------------------------------------------
    # 选中持仓 → 显示盘口
    # ------------------------------------------------------------------
    def _poll_loop(self):
        """后台定时轮询：持仓、委托、盘口"""
        poll_count = 0
        while not self._stop_poll.is_set():
            time.sleep(2)
            if not getattr(self, "is_login", False):
                continue
            poll_count += 1
            # 每 6 秒刷新持仓 + 资金
            if poll_count % 3 == 0:
                try:
                    self.query_positions(timeout=10)
                    if self._positions_raw:
                        self._positions_agg = self._aggregate_positions()
                        self._update_tree_pos()
                except Exception as e:
                    self.print(f"[_poll_loop] 持仓刷新异常: {e}")
                    import traceback
                    self.print(traceback.format_exc())
                try:
                    self.query_trading_account(timeout=5)
                    self.master.after(0, self._update_account_ui)
                except Exception as e:
                    self.print(f"[_poll_loop] 资金刷新异常: {e}")
            # 每 6 秒刷新委托（避免界面频繁闪烁）
            if poll_count % 3 == 0:
                try:
                    with self._refresh_lock:
                        self.query_orders(timeout=10)
                        self.query_trades(timeout=10)
                        self._merge_submitted_orders()
                        self._update_tree_order()
                except Exception as e:
                    self.print(f"[_poll_loop] 委托刷新异常: {e}")
                    import traceback
                    self.print(traceback.format_exc())
            # 每 2 秒刷新盘口（如有选中合约）
            if self._selected_instrument:
                try:
                    md = self.query_market_data(self._selected_instrument, timeout=3)
                    if md:
                        self._quote_var.set(
                            f"{self._selected_instrument} | 最新 {md.get('LastPrice', 0)} "
                            f"| 买 {md.get('BidPrice1', 0)}×{md.get('BidVolume1', 0)} "
                            f"| 卖 {md.get('AskPrice1', 0)}×{md.get('AskVolume1', 0)}"
                        )
                except Exception:
                    pass

    def _on_position_select(self, event=None):
        sel = self._tree_pos.selection()
        if not sel:
            self._selected_instrument = ""
            self._quote_var.set("盘口: --")
            return
        values = self._tree_pos.item(sel[0])["values"]
        if not values:
            self._selected_instrument = ""
            return
        self._selected_instrument = values[0]
        threading.Thread(target=self._show_quote, args=(self._selected_instrument,), daemon=True).start()

    def _show_quote(self, instrument_id: str):
        md = self.query_market_data(instrument_id, timeout=3)
        if not md:
            self._quote_var.set(f"{instrument_id}: 无法获取行情")
            return
        bid = md.get("BidPrice1", 0)
        bid_vol = md.get("BidVolume1", 0)
        ask = md.get("AskPrice1", 0)
        ask_vol = md.get("AskVolume1", 0)
        last = md.get("LastPrice", 0)
        self._quote_var.set(
            f"{instrument_id} | 最新 {last} | 买 {bid}×{bid_vol} | 卖 {ask}×{ask_vol}"
        )

    # ------------------------------------------------------------------
    # 按钮事件
    # ------------------------------------------------------------------
    def _has_pending_close_order(self, instrument_id: str, pos_direction: int) -> bool:
        """检查指定合约+持仓方向是否有在途平仓委托"""
        # pos_direction: 2=多, 3=空
        # 多头平仓方向=卖(1), 空头平仓方向=买(0)
        expected_close_dir = 1 if pos_direction == 2 else 0
        inst_upper = instrument_id.upper()
        for o in self._orders_raw:
            status = self._normalize_status(o.get("OrderStatus", ""))
            if status in ("0", "4", "5"):  # 全部成交/已撤单/已撤销
                continue
            if o.get("InstrumentID", "").upper() != inst_upper:
                continue
            if o.get("Direction") != expected_close_dir:
                continue
            offset = o.get("CombOffsetFlag", "")
            if offset in ("1", "3", "4"):  # 平仓/平今/平昨
                return True
        return False

    def _on_refresh(self):
        if not getattr(self, "is_login", False):
            messagebox.showwarning("提示", "尚未登录")
            return
        threading.Thread(target=self._refresh_in_thread, daemon=True).start()

    def _on_refresh_orders(self):
        if not getattr(self, "is_login", False):
            return
        threading.Thread(target=self._refresh_orders_in_thread, daemon=True).start()

    def _on_close_selected(self):
        sel = self._tree_pos.selection()
        if not sel:
            messagebox.showwarning("提示", "请先选中一行")
            return
        values = self._tree_pos.item(sel[0])["values"]
        if not values:
            return
        instrument_id = values[0]
        direction_str = values[1]
        volume = int(values[2])

        pos = next((p for p in self._positions_agg if p["InstrumentID"] == instrument_id), None)
        if not pos:
            messagebox.showerror("错误", "未找到持仓数据")
            return

        action = "卖出平仓" if direction_str == "多" else "买入平仓"
        if self._has_pending_close_order(instrument_id, pos["PosiDirection"]):
            messagebox.showwarning("提示", f"{instrument_id} 已有在途平仓委托，请等待成交或撤单后再操作")
            return
        if not messagebox.askyesno("确认平仓", f"合约: {instrument_id}\n方向: {direction_str}\n手数: {volume}\n操作: {action}\n\n确认执行?"):
            return
        threading.Thread(target=self._do_close_position, args=(pos,), daemon=True).start()

    def _on_close_all(self):
        if not self._positions_agg:
            messagebox.showwarning("提示", "当前无持仓")
            return
        if not messagebox.askyesno("确认平仓", f"确认全部平仓？共 {len(self._positions_agg)} 个合约"):
            return

        def _close_all():
            for pos in self._positions_agg:
                if self._has_pending_close_order(pos["InstrumentID"], pos["PosiDirection"]):
                    self._update_status(f"{pos['InstrumentID']} 已有在途平仓委托，跳过")
                    continue
                self._do_close_position(pos, allow_prompt=False)
                time.sleep(0.3)

        threading.Thread(target=_close_all, daemon=True).start()

    def _on_close_selected_best(self):
        sel = self._tree_pos.selection()
        if not sel:
            messagebox.showwarning("提示", "请先选中一行")
            return
        values = self._tree_pos.item(sel[0])["values"]
        if not values:
            return
        instrument_id = values[0]
        direction_str = values[1]
        volume = int(values[2])
        pos = next((p for p in self._positions_agg if p["InstrumentID"] == instrument_id), None)
        if not pos:
            messagebox.showerror("错误", "未找到持仓数据")
            return
        if self._has_pending_close_order(instrument_id, pos["PosiDirection"]):
            messagebox.showwarning("提示", f"{instrument_id} 已有在途平仓委托，请等待成交或撤单后再操作")
            return
        action = "卖出平仓（对价）" if direction_str == "多" else "买入平仓（对价）"
        if not messagebox.askyesno("确认平仓", f"合约: {instrument_id}\n方向: {direction_str}\n手数: {volume}\n操作: {action}\n\n确认执行?"):
            return
        threading.Thread(target=self._do_close_position, args=(pos, True), daemon=True).start()

    def _on_close_all_best(self):
        if not self._positions_agg:
            messagebox.showwarning("提示", "当前无持仓")
            return
        if not messagebox.askyesno("确认平仓", f"确认全部对价平仓？共 {len(self._positions_agg)} 个合约"):
            return

        def _close_all_best():
            for pos in self._positions_agg:
                if self._has_pending_close_order(pos["InstrumentID"], pos["PosiDirection"]):
                    self._update_status(f"{pos['InstrumentID']} 已有在途平仓委托，跳过")
                    continue
                self._do_close_position(pos, True, allow_prompt=False)
                time.sleep(0.3)

        threading.Thread(target=_close_all_best, daemon=True).start()

    def _on_cancel_order(self):
        sel = self._tree_order.selection()
        if not sel:
            messagebox.showwarning("提示", "请先选中委托单")
            return
        values = self._tree_order.item(sel[0])["values"]
        if not values:
            return
        order_ref = str(values[-1]).strip() if len(values) > 0 else ""
        if not order_ref:
            messagebox.showerror("错误", "未找到 OrderRef")
            return
        order = next((o for o in self._orders_raw if str(o.get("OrderRef", "")).strip() == order_ref), None)
        if not order:
            messagebox.showerror("错误", "未找到委托记录")
            return
        status = self._normalize_status(order.get("OrderStatus", ""))
        if status in ("0", "4", "5"):
            messagebox.showwarning("提示", "该委托已成交或已撤销，无需撤单")
            return
        inst = order.get("InstrumentID", "")
        if not messagebox.askyesno("确认撤单", f"合约: {inst}\nOrderRef: {order_ref}\n\n确认撤单?"):
            return
        threading.Thread(target=self._cancel_order, args=(order,), daemon=True).start()

    # ------------------------------------------------------------------
    # 平仓执行
    # ------------------------------------------------------------------
    def _ask_manual_price(self, instrument_id: str) -> float | None:
        """后台线程安全地弹出价格输入对话框"""
        result_queue = queue.Queue(maxsize=1)

        def _dialog():
            try:
                price = simpledialog.askfloat(
                    "输入平仓价格",
                    f"{instrument_id}\n无法获取对手价行情，请输入平仓价格：",
                    parent=self.master,
                )
                result_queue.put(price)
            except Exception as e:
                self.print(f"[错误] 价格输入对话框异常: {e}")
                result_queue.put(None)

        self.master.after(0, _dialog)
        try:
            return result_queue.get(timeout=30)
        except queue.Empty:
            self.print(f"[超时] {instrument_id} 价格输入超时")
            return None

    def _do_close_position(self, pos: dict, best_price: bool = False, allow_prompt: bool = True):
        instrument_id = pos["InstrumentID"]
        direction = pos["PosiDirection"]
        today = pos["TodayPosition"]
        yd = pos["YdPosition"]
        exchange = pos["ExchangeID"]

        close_direction = tdapi.THOST_FTDC_D_Sell if direction == 2 else tdapi.THOST_FTDC_D_Buy
        md = self.query_market_data(instrument_id, timeout=5)
        if direction == 2:
            price = md.get("BidPrice1", 0) if md else 0
        else:
            price = md.get("AskPrice1", 0) if md else 0

        # 对手价为0时，尝试用最新价兜底
        if price <= 0:
            price = md.get("LastPrice", 0) if md else 0

        # 若仍无有效价格
        if price <= 0:
            if best_price and allow_prompt:
                manual_price = self._ask_manual_price(instrument_id)
                if manual_price is None or manual_price <= 0:
                    self._update_status(f"{instrument_id} 未输入有效价格，跳过平仓")
                    return
                price = manual_price
            else:
                self._update_status(f"{instrument_id} 无法获取行情，跳过平仓")
                return

        is_shfe = exchange in ("SHFE", "INE")

        if is_shfe and today > 0:
            self._send_close_order(exchange, instrument_id, close_direction, today, price, tdapi.THOST_FTDC_OF_CloseToday, "平今", best_price=best_price)
            time.sleep(0.5)

        # 上期所：总持仓 - 已平今仓 = 剩余需平数量（昨仓或其他）
        remaining = pos["Position"] - today if is_shfe else pos["Position"]

        if remaining > 0:
            offset = tdapi.THOST_FTDC_OF_CloseYesterday if is_shfe else tdapi.THOST_FTDC_OF_Close
            label = "平昨" if is_shfe else "平仓"
            self._send_close_order(exchange, instrument_id, close_direction, remaining, price, offset, label, best_price=best_price)

        # 自动刷新委托列表
        time.sleep(1)
        self._refresh_orders_in_thread()

    def _send_close_order(self, exchange_id, instrument_id, direction, volume, price, offset_flag, label, best_price: bool = False):
        """平仓下单（仿 ArbitrageTrading.py 的 insert_order 格式）"""
        self._order_ref_seq += 1
        order_ref = str(self._order_ref_seq)

        # 标准化合约代码（CZCE 3位年月 + GFEX 小写恢复）
        exact_id = self._standardize_contract(instrument_id)

        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.ExchangeID = exchange_id
        req.InstrumentID = exact_id
        req.Direction = direction
        req.CombOffsetFlag = offset_flag
        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        req.VolumeTotalOriginal = volume
        req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        req.IsAutoSuspend = 0
        req.IsSwapOrder = 0
        req.UserForceClose = 0
        req.OrderRef = order_ref

        # 统一用 LimitPrice；BestPrice 在部分柜台不支持，彻底弃用
        if price <= 0:
            self.print(f"[跳过] {exact_id} 价格无效({price})，不下单")
            return
        req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        req.LimitPrice = price
        price_str = str(price)

        dname = "买" if direction == tdapi.THOST_FTDC_D_Buy else "卖"
        offset_str = {tdapi.THOST_FTDC_OF_Open: "开仓", tdapi.THOST_FTDC_OF_Close: "平仓",
                      tdapi.THOST_FTDC_OF_CloseToday: "平今", tdapi.THOST_FTDC_OF_CloseYesterday: "平昨"}.get(offset_flag, "未知")
        self.print(f"[下单] {exact_id} {dname} {offset_str} 价格={price_str} 数量={volume} Ref={order_ref}")

        # 先把订单加入本地列表（用户能立即看到"已提交"）
        self._orders_raw.append({
            "InsertTime": time.strftime("%H:%M:%S"),
            "InstrumentID": exact_id,
            "Direction": 0 if direction == tdapi.THOST_FTDC_D_Buy else 1,
            "CombOffsetFlag": chr(offset_flag) if isinstance(offset_flag, int) else str(offset_flag),
            "VolumeTotalOriginal": volume,
            "LimitPrice": price if price > 0 else 0,
            "OrderStatus": b"3",  # 未成交
            "VolumeTraded": 0,
            "OrderRef": order_ref,
            "OrderSysID": "",
            "FrontID": getattr(self, "_front_id", 0) or 0,
            "SessionID": getattr(self, "_session_id", 0) or 0,
            "ExchangeID": exchange_id or self._get_contract_info(exact_id).get("ExchangeID", ""),
        })
        self._update_tree_order()

        ret = self._api.ReqOrderInsert(req, 0)
        if ret == 0:
            price_str = str(price) if (best_price and price > 0) or not best_price else "对价"
            msg = f"{label}已提交: {exact_id} {dname} {volume}手 价={price_str} Ref={order_ref}"
            self._update_status(msg)
            self._notify_async(f"📤 {msg}")
        else:
            msg = f"{label}提交失败: {exact_id} 返回值={ret}"
            self._update_status(msg)
            self._notify_async(f"❌ {msg}")
            for o in self._orders_raw:
                if o["OrderRef"] == order_ref:
                    o["OrderStatus"] = b"4"
            self._update_tree_order()

    def _cancel_order(self, order: dict):
        """撤单：使用 FrontID + SessionID + OrderRef"""
        inst = order.get("InstrumentID", "")
        ref = order.get("OrderRef", "")
        front_id = order.get("FrontID", 0)
        session_id = order.get("SessionID", 0)
        exchange_id = order.get("ExchangeID", "")

        exact_id = self._standardize_contract(inst)
        if not exchange_id:
            exchange_id = self._get_contract_info(exact_id).get("ExchangeID", "")

        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.UserID = self._user_id
        req.ExchangeID = exchange_id
        req.InstrumentID = exact_id
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete
        req.FrontID = front_id or getattr(self, "_front_id", 0) or 0
        req.SessionID = session_id or getattr(self, "_session_id", 0) or 0
        req.OrderRef = ref

        self.print(f"[撤单] {inst} Ref={ref} FrontID={req.FrontID} SessionID={req.SessionID}")
        ret = self._api.ReqOrderAction(req, 0)
        if ret == 0:
            self._update_status(f"撤单已提交: {inst} Ref={ref}")
        else:
            self._update_status(f"撤单提交失败: {inst} Ref={ref} 返回值={ret}")
            self._notify_async(f"❌ 撤单提交失败: {inst} Ref={ref} 返回值={ret}")

    def _on_cancel_all_orders(self):
        """撤销所有未成交/部分成交的委托单"""
        if not getattr(self, "is_login", False):
            messagebox.showwarning("提示", "尚未登录")
            return
        # 收集可撤委托
        cancel_candidates = []
        for o in self._orders_raw:
            status = self._normalize_status(o.get("OrderStatus", ""))
            if status in ("0", "4", "5"):  # 全部成交/已撤单/已撤销
                continue
            cancel_candidates.append(o)
        if not cancel_candidates:
            messagebox.showinfo("提示", "当前没有在途委托")
            return
        if not messagebox.askyesno("确认撤全部", f"共 {len(cancel_candidates)} 条在途委托，确认全部撤销？"):
            return
        threading.Thread(target=self._cancel_all_orders_in_thread, args=(cancel_candidates,), daemon=True).start()

    def _cancel_all_orders_in_thread(self, orders: list):
        """后台线程批量撤单"""
        success = 0
        for o in orders:
            try:
                self._cancel_order(o)
                success += 1
                time.sleep(0.2)
            except Exception as e:
                self.print(f"[批量撤单] 失败: {o.get('OrderRef')} {e}")
        self._update_status(f"批量撤单完成: 提交 {success}/{len(orders)} 条")
        # 刷新委托列表
        self.master.after(1000, self._on_refresh_orders)

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        """撤单响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            inst = (pInputOrderAction.InstrumentID or "").strip() if pInputOrderAction else ""
            ref = (pInputOrderAction.OrderRef or "").strip() if pInputOrderAction else ""
            msg = f"❌ 撤单失败: {inst} Ref={ref} ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
            self.print(msg)
            self._update_status(msg)
            self._notify_async(msg)
        else:
            inst = (pInputOrderAction.InstrumentID or "").strip() if pInputOrderAction else ""
            ref = (pInputOrderAction.OrderRef or "").strip() if pInputOrderAction else ""
            self._update_status(f"撤单成功: {inst} Ref={ref}")
            # 刷新委托列表
            self.master.after(500, self._on_refresh_orders)

    def _next_order_ref(self) -> str:
        return f"PMU{int(time.time() * 1000) % 1000000000:09d}"

    # ------------------------------------------------------------------
    # 退出
    # ------------------------------------------------------------------
    def _on_close(self):
        if hasattr(self, "_stop_poll"):
            self._stop_poll.set()
        try:
            self.master.destroy()
        except Exception:
            pass
        try:
            del self
        except Exception:
            pass
        os._exit(0)

    def run(self):
        self.master.mainloop()


def main():
    env_name = sys.argv[1].lower() if len(sys.argv) > 1 else "7x24"
    conf = config.envs.get(env_name)
    if not conf:
        print(f"未知环境: {env_name}，可用: {list(config.envs.keys())}")
        return
    root = tk.Tk()
    app = PositionManagerUI(conf=conf, master=root)
    app.run()


if __name__ == "__main__":
    main()
