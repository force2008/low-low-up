# -*- coding: utf-8 -*-
"""
PositionSyncManager 基类模块

包含：
- 所有必要的 imports
- PositionSyncManager 类的 __init__ 方法
- 异步通知、交易日管理、持久化状态
- 所有 CTP 回调方法
"""

import json
import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .constants import PRODUCT_TRADING_SESSIONS, DAY_3SEG

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__('sys').path:
    __import__('sys').path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import CTdSpiBase, tdapi

# 飞书通知
from utils.feishu_notifier import FeishuNotifier


class PositionSyncManagerBase(CTdSpiBase):
    """持仓同步管理器基类：包含初始化、持久化、CTP回调"""

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
        position_ratio: float = 1.0,
    ):
        if position_ratio <= 0:
            raise ValueError(f"position_ratio 必须大于 0，当前值: {position_ratio}")
        self._position_ratio = float(position_ratio)

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
        self._product_exchange_map: Dict[str, str] = {}  # ProductID -> ExchangeID

        # 撤单响应事件
        self._cancel_events: Dict[str, threading.Event] = {}
        self._cancel_result: Optional[List] = None

        # 持仓对齐冷却：防止 CTP 持仓查询滞后导致重复下单
        self._last_align_time: Dict[Tuple[str, int, str], float] = {}

        # 完整仓位对齐冷却（2分钟）
        self._last_full_sync_time: float = 0.0

        # 首次运行标志：只在第一次执行同步时为 True
        self._is_first_run: bool = True

        # 持仓查询锁：防止后台线程和用户操作并发查询导致数据错乱
        self._pos_query_lock = threading.Lock()

        # 同步互斥锁：防止并发调用 sync_and_trade
        # 使用阻塞模式，5秒超时，避免重复同步
        self._sync_lock = threading.Lock()

        # 标记当前实例是否正在执行同步（用于进程内快速检查）
        self._is_syncing = False

        # 行情查询（线程安全：按 request_id 隔离，使用递归锁支持重入）
        self._md_lock = threading.RLock()
        self._md_request_id = 0
        self._md_pending: Dict[int, dict] = {}

        # 委托查询（线程安全）
        self._orders_query_event: Optional[threading.Event] = None
        self._orders_raw_query: List[dict] = []

        # 飞书通知
        self._feishu = FeishuNotifier()

        # CTP 报 1006 不存在的合约，记录下来避免重复尝试
        self._invalid_instruments: set = set()

        # 持久化状态文件（程序重启后不丢失）
        # 格式: {trading_day: [id1, id2, ...]}，按交易日隔离，避免跨天 OrderSysID 复用导致漏单
        base_state_dir = os.path.dirname(os.path.abspath(__file__))
        self._PROCESSED_IDS_FILE = os.path.join(base_state_dir, '.processed_ids.json')
        self._PROCESSED_IDS_RETENTION_DAYS = 7

        # 日志记录器（可选，由调用方注入）
        self._logger = None

        # 已处理的 signal.json 报单编号，按交易日分组
        self._processed_signal_data: dict = {}
        self._load_processed_ids()

        # 成交回报去重：记录已处理的成交（用于去重，避免一个委托多个成交回报时重复更新hold.json）
        # 格式: {(OrderRef, TradeTime): timestamp}
        self._processed_trades: Dict[Tuple[str, str], float] = {}
        self._processed_trades_lock = threading.Lock()

        # 持仓更新防抖：确保同一个合约方向的成交，只更新一次hold.json
        self._last_hold_update_time: Dict[str, float] = {}
        self._HOLD_UPDATE_DEBOUNCE_SEC = 2.0  # 2秒内只更新一次

        # 异步持仓更新标志（避免重复调度）
        self._hold_update_scheduled = False

        # 先初始化监控线程相关变量（必须在 super().__init__ 之前，因为后者可能阻塞）
        self.ORDER_TIMEOUT_SECONDS = 60
        self.MAX_REPLACE_COUNT = 3
        self._replace_monitor_thread: Optional[threading.Thread] = None
        self._replace_stop_event = threading.Event()

        print("[__init__] 准备调用 super().__init__...")
        super().__init__(conf=conf)
        print("[__init__] super().__init__ 完成，start_monitor")
        self.print(f"[配置] 持仓同步比例: {self._position_ratio}")

        # 自动撤单重挂监控（未成交开仓委托超时后自动撤单并用最新对手价重挂）
        # 注意：必须在 super().__init__ 之后调用，因为后者会阻塞直到登录成功
        self._start_replace_monitor()
        print("[__init__] 监控线程启动完成")

    def _notify_async(self, text: str):
        """异步发送飞书通知，完全不阻塞"""
        def _send():
            try:
                self._feishu.send_text(text)
            except Exception:
                pass  # 完全忽略异常
        threading.Thread(target=_send, daemon=True).start()

    def set_logger(self, logger):
        """注入日志记录器，使 print() 输出到日志文件（供 run_pipeline.py 使用）"""
        self._logger = logger

    def print(self, *args, **kwargs):
        """统一打印：同时输出到 stdout 和日志记录器"""
        # 先输出到 stdout
        msg = " ".join(str(a) for a in args)
        print(msg, **kwargs)
        # 如果有日志记录器，也写入日志
        if self._logger:
            self._logger.info(msg)

    # ------------------------------------------------------------------
    # 持久化状态（重启后不丢失）
    # ------------------------------------------------------------------
    def _get_trading_day(self) -> str:
        """获取当前交易日，优先用 CTP 返回的交易日，否则用系统日期"""
        td = getattr(self, '_trading_day', '')
        if td and len(td) >= 8:
            # CTP 格式: 20260508 -> 2026-05-08
            return f"{td[:4]}-{td[4:6]}-{td[6:8]}"
        return datetime.date.today().isoformat()

    def _load_processed_ids(self):
        """从磁盘加载已处理的 signal ids，按交易日分组
        兼容旧格式（纯 list）自动迁移
        """
        if os.path.exists(self._PROCESSED_IDS_FILE):
            try:
                with open(self._PROCESSED_IDS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self._processed_signal_data = data
                    elif isinstance(data, list):
                        # 旧格式迁移：按当前交易日归入 dict
                        self._processed_signal_data = {self._get_trading_day(): data}
            except Exception:
                self._processed_signal_data = {}

    def _save_processed_ids(self):
        """保存已处理的 signal ids 到磁盘，清理过期数据"""
        if not isinstance(self._processed_signal_data, dict):
            self._processed_signal_data = {}
        # 清理超过保留天数的旧数据
        cutoff = (datetime.date.today() - datetime.timedelta(days=self._PROCESSED_IDS_RETENTION_DAYS)).isoformat()
        self._processed_signal_data = {
            k: v for k, v in self._processed_signal_data.items() if k >= cutoff
        }
        try:
            with open(self._PROCESSED_IDS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._processed_signal_data, f, ensure_ascii=False)
        except Exception:
            pass

    def wait_login(self, timeout: int = 30):
        """覆盖基类的 wait_login，增加超时保护"""
        for _ in range(timeout):
            time.sleep(1)
            if self.is_login:
                return
        raise TimeoutError(f"CTP 登录超时（{timeout}秒）")

    # ------------------------------------------------------------------
    # CTP 回调
    # ------------------------------------------------------------------
    def _handle_order_rejection(self, pInputOrder, pRspInfo, source: str):
        """统一处理报单拒绝/错误回报"""
        if not pRspInfo or pRspInfo.ErrorID == 0:
            return
        # 尝试多种方式匹配订单
        ref = ""
        inst = (pInputOrder.InstrumentID or "").strip() if pInputOrder else ""
        if pInputOrder:
            ref_raw = str(pInputOrder.OrderRef or "").strip('\x00')
            ref = ref_raw.strip()
            if not ref:
                ref = ref_raw
        err_id = pRspInfo.ErrorID
        err_msg = getattr(pRspInfo, "ErrorMsg", "")
        msg = f"❌ {source}: {inst} Ref={ref} ErrorID={err_id} {err_msg}"
        self.print(msg)
        self._notify_async(msg)

        # ErrorID=1006 合约不存在，记录下来避免后续重复尝试
        if err_id == 1006 and inst:
            self._invalid_instruments.add(inst.upper())
            self.print(f"[警告] 合约 {inst} 被标记为无效（1006），后续将跳过")

        # ErrorID=1009 持仓不足：如果是平仓单，撤掉该合约所有未成交平仓委托，下次重新计算后重试
        if err_id == 1009 and inst:
            with self._order_lock:
                info = None
                for key in (ref, ref_raw if ref_raw else "", ref.lstrip() if ref else ""):
                    if key and key in self._orders:
                        info = self._orders.get(key)
                        break
                if info:
                    offset = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)
                    if offset != tdapi.THOST_FTDC_OF_Open:
                        dir_label = info.get("direction", "")
                        # 撤掉该合约所有未成交平仓单
                        pending_refs = self._get_pending_order_refs(inst, dir_label, offset)
                        if pending_refs:
                            self.print(f"[1009处理] {inst} 平仓被拒绝，撤掉 {len(pending_refs)} 笔未成交平仓委托，下次重新计算后重试")
                            for pending_ref in pending_refs:
                                self.cancel_order(pending_ref)
                                time.sleep(0.2)
                        else:
                            self.print(f"[1009处理] {inst} 平仓被拒绝，无在途平仓单可撤，下次重新计算后重试")

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
        order_ref_raw = str(pOrder.OrderRef or "").strip('\x00')
        order_ref = order_ref_raw.strip()
        sys_id = str(pOrder.OrderSysID or "").strip().strip('\x00')
        status = str(pOrder.OrderStatus or "").strip().strip('\x00')

        with self._order_lock:
            # 精确匹配
            info = self._orders.get(order_ref)
            if not info:
                # 尝试用原始值（未 strip）匹配
                info = self._orders.get(order_ref_raw)
            if not info:
                # 尝试模糊匹配：去掉前导空格后匹配
                order_ref_lstrip = order_ref_raw.lstrip()
                info = self._orders.get(order_ref_lstrip)
            if not info:
                # 最后尝试：用 sys_id + exchange 组合匹配
                for ref, oi in self._orders.items():
                    if oi.get("sys_id") == sys_id and oi.get("exchange") == str(pOrder.ExchangeID or "").strip():
                        info = oi
                        break
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
        order_ref_raw = str(getattr(pTrade, "OrderRef", "") or "").strip('\x00')
        order_ref = order_ref_raw.strip()
        # 安全读取价格/数量（防御性转换，兼容不同 CTP 绑定版本）
        raw_price = getattr(pTrade, "Price", 0.0)
        try:
            trade_price = float(raw_price) if raw_price is not None else 0.0
        except Exception:
            trade_price = 0.0
        raw_volume = getattr(pTrade, "Volume", 0)
        try:
            trade_volume = int(raw_volume) if raw_volume is not None else 0
        except Exception:
            trade_volume = 0
        trade_instr = str(getattr(pTrade, "InstrumentID", "") or "").strip().strip('\x00')
        trade_direction = str(getattr(pTrade, "Direction", "") or "").strip().strip('\x00')
        trade_offset = str(getattr(pTrade, "OffsetFlag", "") or "").strip().strip('\x00')

        # 获取成交时间用于去重
        trade_time = str(getattr(pTrade, "TradeTime", "") or "").strip()
        trade_date = str(getattr(pTrade, "TradeDate", "") or "").strip()
        trade_key = (order_ref, trade_date, trade_time)

        # ========== 成交去重逻辑 ==========
        current_time = time.time()
        should_update_hold = False

        with self._processed_trades_lock:
            if trade_key in self._processed_trades:
                # 重复的成交回报，只打印日志不更新hold.json
                self.print(
                    f"[成交-去重] {trade_instr} OrderRef={order_ref} "
                    f"Volume={trade_volume} (重复回报，已忽略)"
                )
            else:
                # 新的成交
                self._processed_trades[trade_key] = current_time
                should_update_hold = True

                # 清理过期的去重记录（保留最近5分钟）
                expired_keys = [
                    k for k, ts in self._processed_trades.items()
                    if current_time - ts > 300
                ]
                for k in expired_keys:
                    del self._processed_trades[k]

        with self._order_lock:
            # 精确匹配
            info = self._orders.get(order_ref)
            if not info:
                # 尝试用原始值匹配
                info = self._orders.get(order_ref_raw)
            if not info:
                # 尝试模糊匹配
                order_ref_lstrip = order_ref_raw.lstrip()
                info = self._orders.get(order_ref_lstrip)
            if not info:
                # 用 SysID + Exchange 匹配
                trade_sys_id = str(getattr(pTrade, "OrderSysID", "") or "").strip()
                exchange_id = str(getattr(pTrade, "ExchangeID", "") or "").strip()
                for ref, oi in self._orders.items():
                    if oi.get("sys_id") == trade_sys_id and oi.get("exchange") == exchange_id:
                        info = oi
                        break

        instr = info["instr"] if info else trade_instr
        if not instr:
            instr = "未知合约"

        self.print(
            f"[成交] {instr} OrderRef={order_ref} "
            f"Volume={trade_volume} Price={trade_price} "
            f"Dir={trade_direction} Offset={trade_offset}"
        )

        # 合并同合约同方向的成交通知，减少飞书通知数量
        # 使用防抖机制：2秒内同一合约同方向只通知一次
        notify_key = f"{trade_instr}_{trade_direction}"
        last_notify = self._last_hold_update_time.get(notify_key, 0)
        if current_time - last_notify >= self._HOLD_UPDATE_DEBOUNCE_SEC:
            self._last_hold_update_time[notify_key] = current_time
            self._notify_async(
                f"✅ 成交回报\n"
                f"合约：{instr}\n"
                f"成交手数：{trade_volume} 手\n"
                f"成交价格：{trade_price}\n"
                f"OrderRef：{order_ref}"
            )

        # ========== 异步更新 hold.json ==========
        # 移除同步持仓查询，避免阻塞成交回报处理
        # PositionManagerUI 就是这样处理的，批量成交会更快
        # 如果需要更新持仓，应该由单独的线程定时执行，而不是在 OnRtnTrade 中同步查询
        if should_update_hold:
            self._schedule_hold_update()

        # 同步成交手数到文件
        if info:
            session_id = getattr(pTrade, "SessionID", 0)
            self._update_order_file_status(
                order_ref, status=None, volume_traded=trade_volume, session_id=session_id
            )

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
                # 统一 PosiDirection 类型（CTP 可能返回 str / bytes / int，且常带 \0 结尾）
                # openctp-ctp 中 THOST_FTDC_PD_Long='2', Short='3' 为 str
                posi_dir = pInvestorPosition.PosiDirection
                if isinstance(posi_dir, bytes):
                    posi_dir = posi_dir.decode().strip().strip('\x00')
                elif not isinstance(posi_dir, str):
                    posi_dir = str(posi_dir) if posi_dir is not None else ""
                posi_dir = posi_dir.strip().strip('\x00')
                try:
                    posi_dir_val = int(posi_dir) if posi_dir else 0
                except (ValueError, TypeError):
                    posi_dir_val = 0

                # 过滤净持仓记录(1)，只保留多头(2)/空头(3)明细
                # 避免净持仓与明细重复计数，或方向不匹配导致误判空仓
                if posi_dir_val in (2, 3):
                    self._actual_positions.append({
                        "InstrumentID": (pInvestorPosition.InstrumentID or "").strip().upper(),
                        "PosiDirection": posi_dir_val,
                        "Position": pInvestorPosition.Position,
                        "TodayPosition": pInvestorPosition.TodayPosition,
                        "YdPosition": pInvestorPosition.YdPosition,
                        "ExchangeID": (pInvestorPosition.ExchangeID or "").strip(),
                    })
        if bIsLast:
            self._pos_query_event.set()

    def OnRspQryOrder(
        self,
        pOrder: tdapi.CThostFtdcOrderField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pOrder:
            # 统一 Direction 类型（int / bytes / str -> str，兼容 \0 结尾）
            # openctp-ctp 常量如 THOST_FTDC_D_Buy = '0' 为 str，保持与常量类型一致
            direction = pOrder.Direction
            if isinstance(direction, bytes):
                direction_val = direction.decode().strip().strip('\x00')
            elif isinstance(direction, str):
                direction_val = direction.strip().strip('\x00')
            else:
                direction_val = str(direction) if direction is not None else "0"

            # 统一 CombOffsetFlag 类型（int / bytes / str -> str，兼容 \0 结尾）
            # openctp-ctp 常量如 THOST_FTDC_OF_Open = '0' 为 str，保持与常量类型一致
            offset = pOrder.CombOffsetFlag
            if isinstance(offset, bytes):
                offset_val = offset.decode().strip().strip('\x00')
            elif isinstance(offset, str):
                offset_val = offset.strip().strip('\x00')
            else:
                offset_val = str(offset) if offset is not None else "0"

            # 统一 OrderStatus 类型（int / bytes / str -> str，兼容 \0 结尾）
            # CTP OrderStatus 为单字符：'0'~'5'，后续统一按字符串比较
            status = pOrder.OrderStatus
            if isinstance(status, bytes):
                status_val = status.decode().strip().strip('\x00')
            elif isinstance(status, str):
                status_val = status.strip().strip('\x00')
            else:
                status_val = str(status) if status is not None else ""

            self._orders_raw_query.append({
                "InstrumentID": (pOrder.InstrumentID or "").strip(),
                "ExchangeID": (pOrder.ExchangeID or "").strip(),
                "Direction": direction_val,
                "CombOffsetFlag": offset_val,
                "OrderStatus": status_val,
                "VolumeTotalOriginal": pOrder.VolumeTotalOriginal,
                "VolumeTraded": pOrder.VolumeTraded,
                "OrderRef": (pOrder.OrderRef or "").strip(),
                "OrderSysID": (pOrder.OrderSysID or "").strip(),
                "InsertDate": (pOrder.InsertDate or "").strip(),
            })
        if bIsLast:
            if self._orders_query_event:
                self._orders_query_event.set()

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

    def OnRspOrderAction(
        self,
        pInputOrderAction: tdapi.CThostFtdcInputOrderActionField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        order_ref_raw = str(pInputOrderAction.OrderRef or "").strip('\x00') if pInputOrderAction else ""
        order_ref = order_ref_raw.strip() if order_ref_raw else ""

        # 尝试多种方式匹配订单和撤单事件
        cancel_key = None
        with self._order_lock:
            for key in (order_ref, order_ref_raw, order_ref_raw.lstrip() if order_ref_raw else ""):
                if key and key in self._cancel_events:
                    cancel_key = key
                    break
            info = None
            for key in (order_ref, order_ref_raw, order_ref_raw.lstrip() if order_ref_raw else ""):
                if key and key in self._orders:
                    info = self._orders.get(key)
                    break

        if pRspInfo and pRspInfo.ErrorID != 0:
            display_ref = order_ref if order_ref else "未知"
            self.print(
                f"[撤单失败] OrderRef={display_ref} "
                f"ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
            )
            if info:
                with self._order_lock:
                    cancel_result = getattr(self, '_cancel_result', None)
                    if cancel_result is not None:
                        cancel_result[0] = False
        else:
            display_ref = order_ref if order_ref else "未知"
            self.print(f"[撤单响应] OrderRef={display_ref}")
            # 撤单成功，更新订单状态为已撤销
            if info:
                info["status"] = self._OST_CANCELED
                info["event"].set()
                with self._order_lock:
                    cancel_result = getattr(self, '_cancel_result', None)
                    if cancel_result is not None:
                        cancel_result[0] = True

        with self._order_lock:
            if cancel_key:
                ev = self._cancel_events.pop(cancel_key, None)
                if ev:
                    ev.set()

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------
    def _is_order_pending(self, info: dict) -> bool:
        """判断订单是否仍在途中（未最终完结）"""
        status = info.get("status", "")
        if status in ("send_failed", "rejected"):
            return False
        # 统一转成字符串比较（CTP 返回 bytes 或 str）
        s = status.decode("ascii") if isinstance(status, bytes) else str(status)
        # 0=全部成交, 2=部成部撤, 4=已撤单, 5=已撤销
        return s not in ("0", "2", "4", "5")

    def _get_pending_order_refs(
        self, instrument_id: str, direction: str, offset_flag: int
    ) -> List[str]:
        """获取指定合约+方向+开平的未完结委托 OrderRef 列表"""
        inst_upper = instrument_id.upper()
        refs = []
        with self._order_lock:
            for ref, info in self._orders.items():
                # 过滤掉空 ref
                if not ref:
                    continue
                if not self._is_order_pending(info):
                    continue
                if info.get("instr", "").upper() != inst_upper:
                    continue
                if info.get("direction") != direction:
                    continue
                if info.get("offset_flag", tdapi.THOST_FTDC_OF_Open) != offset_flag:
                    continue
                refs.append(ref)
        return refs

    @staticmethod
    def _offset_flag_label(offset_flag: int) -> str:
        return {
            tdapi.THOST_FTDC_OF_Open: "开仓",
            tdapi.THOST_FTDC_OF_Close: "平仓",
            tdapi.THOST_FTDC_OF_CloseToday: "平今",
            tdapi.THOST_FTDC_OF_CloseYesterday: "平昨",
            tdapi.THOST_FTDC_OF_ForceClose: "强平",
        }.get(offset_flag, f"开平={offset_flag}")

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
    # 按环境+日期分文件，避免不同环境/日期的旧委托混在一起
    # ------------------------------------------------------------------
    def _get_orders_file(self) -> str:
        env = getattr(self, "env_name", "unknown")
        date_str = time.strftime("%Y-%m-%d")
        return os.path.join(PROJECT_ROOT, "order-check", f"orders_submitted_{env}_{date_str}.json")

    def _save_order_to_file(self, **kwargs):
        """保存或更新委托到共享文件"""
        order_ref = kwargs.get("order_ref", "")
        session_id = kwargs.get("session_id", 0)
        if not order_ref:
            return
        orders = []
        if os.path.exists(self._get_orders_file()):
            try:
                with open(self._get_orders_file(), "r", encoding="utf-8") as f:
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
            with open(self._get_orders_file(), "w", encoding="utf-8") as f:
                json.dump(orders, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.print(f"[警告] 保存委托文件失败: {e}")

    def _update_order_file_status(self, order_ref: str, status, sys_id: str = "", volume_traded: int = 0, session_id: int = 0):
        """更新委托状态到共享文件（按 order_ref + session_id 匹配）"""
        if not os.path.exists(self._get_orders_file()):
            return
        try:
            with open(self._get_orders_file(), "r", encoding="utf-8") as f:
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
            with open(self._get_orders_file(), "w", encoding="utf-8") as f:
                json.dump(orders, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 自动撤单重挂监控（未成交开仓委托超时后自动撤单并用最新对手价重挂）
    # ------------------------------------------------------------------
    def shutdown(self):
        """关闭管理器，停止后台监控线程"""
        self._stop_replace_monitor()

    def _start_replace_monitor(self):
        """启动自动撤单重挂监控线程"""
        self.print(f"[监控] 尝试启动监控线程, 当前状态: thread={self._replace_monitor_thread}")
        if self._replace_monitor_thread and self._replace_monitor_thread.is_alive():
            self.print("[监控] 监控线程已在运行，跳过")
            return
        self._replace_stop_event.clear()
        self._replace_monitor_thread = threading.Thread(
            target=self._replace_monitor_loop, daemon=True,
            name="PositionSyncMonitor"
        )
        self._replace_monitor_thread.start()
        self.print("[监控] 自动撤单重挂监控线程已启动")

    def _stop_replace_monitor(self):
        """停止自动撤单重挂监控线程"""
        self._replace_stop_event.set()
        if self._replace_monitor_thread and self._replace_monitor_thread.is_alive():
            self._replace_monitor_thread.join(timeout=3)
        self.print("[监控] 自动撤单重挂监控线程已停止")

    def _replace_monitor_loop(self):
        """监控循环：每 45 秒做一次仓位对比，发现差异则同步"""
        self.print("[监控] 监控线程启动，等待首次检查...")
        CHECK_INTERVAL = 45  # 检查间隔
        while not self._replace_stop_event.is_set():
            self._replace_stop_event.wait(CHECK_INTERVAL)
            if self._replace_stop_event.is_set():
                self.print("[监控] 收到停止信号，退出循环")
                break
            try:
                self.print(f"[监控] 开始第 N 次检查 (间隔 {CHECK_INTERVAL} 秒)")
                # 1. 先检查未成交委托是否需要撤单重挂
                self._check_and_replace_pending_orders()

                # 2. 做一次仓位对比
                self._check_position_diff()
                self.print("[监控] 本次检查完成")
            except Exception as e:
                import traceback
                self.print(f"[监控异常] 监控检查出错: {e}")
                self.print(traceback.format_exc())

    def _check_position_diff(self):
        """检查仓位差异，有差异则同步"""
        # 获取锁，防止与 sync_and_trade 并发
        if not self._sync_lock.acquire(blocking=True, timeout=5):
            self.print("[监控] 锁被占用，跳过本次检查")
            return

        try:
            # 查询 CTP 实际持仓
            positions = self.query_positions(timeout=10)
            if positions is None:
                self.print("[监控] 持仓查询失败")
                return

            # 加载标准持仓
            if not self._load_hold_std():
                self.print("[监控] 加载标准持仓失败")
                return

            # 聚合持仓
            actual_agg = self._aggregate_actual_positions()
            target = self._parse_hold_std()

            # 计算差异
            missing = []
            excess = []
            for key, t_vol in target.items():
                a_vol = actual_agg.get(key, 0)
                if t_vol > a_vol:
                    contract, direction = key
                    missing.append({
                        "contract": contract,
                        "direction": "buy" if direction == 2 else "sell",
                        "volume": t_vol - a_vol,
                    })

            for key, a_vol in actual_agg.items():
                t_vol = target.get(key, 0)
                if a_vol > t_vol:
                    contract, direction = key
                    excess.append({
                        "contract": contract,
                        "direction": direction,
                        "volume": a_vol - t_vol,
                    })

            if missing or excess:
                # 有差异，发送通知并执行同步
                lines = ["🔄 45秒检测到仓位差异，准备同步："]
                if missing:
                    total_missing = sum(mo["volume"] for mo in missing)
                    lines.append(f"📈 缺额开仓 ({len(missing)} 个合约，共 {total_missing} 手):")
                    for mo in missing[:5]:
                        d = "买" if mo["direction"] == "buy" else "卖"
                        lines.append(f"  {mo['contract']} {d} {mo['volume']}手")
                    if len(missing) > 5:
                        lines.append(f"  ... 等共 {len(missing)} 个")
                if excess:
                    total_excess = sum(eo["volume"] for eo in excess)
                    lines.append(f"📉 超额平仓 ({len(excess)} 个合约，共 {total_excess} 手):")
                    for eo in excess[:5]:
                        d = "多" if eo["direction"] == 2 else "空"
                        lines.append(f"  {eo['contract']} {d} {eo['volume']}手")
                    if len(excess) > 5:
                        lines.append(f"  ... 等共 {len(excess)} 个")

                self._notify_async("\n".join(lines))
                self.print(f"[监控] 检测到仓位差异: 缺额 {len(missing)} 个，超额 {len(excess)} 个")

                # 执行同步（已持有锁，传入 lock_held=True）
                self._do_sync(trade_volume=1, lock_held=True)
            else:
                # 无差异也要发送定期通知，让用户知道系统一直在检查
                total_target = sum(t for t in target.values())
                total_actual = sum(a for a in actual_agg.values())
                self._notify_async(
                    f"✅ 45秒持仓检测\n"
                    f"标准持仓: {len(target)} 个合约, {total_target} 手\n"
                    f"实际持仓: {len(actual_agg)} 个合约, {total_actual} 手\n"
                    f"状态: 仓位一致 ✓"
                )
                self.print("[监控] 45秒检测: 仓位一致")

        except Exception as e:
            import traceback
            self.print(f"[监控] 检查仓位差异异常: {e}")
            traceback.print_exc()
        finally:
            self._sync_lock.release()

    def _next_order_ref(self) -> str:
        with self._order_lock:
            self._order_ref_seq += 1
            return f"PSM{self._order_ref_seq:09d}"

    # Placeholder methods to be implemented by subclasses
    def _check_and_replace_pending_orders(self):
        """扫描未成交委托（含开平仓），超时则撤单并用最新对手价重挂"""
        pass

    def _update_hold_json_file(self):
        """使用已查询的持仓数据更新 hold.json 文件"""
        pass

    def _schedule_hold_update(self):
        """安排异步持仓更新（避免阻塞成交回报）"""
        # 使用定时器，延迟 2 秒后执行，给批量成交留出时间
        def _delayed_update():
            try:
                self._update_hold_json_from_ctp_async()
            except Exception:
                pass
        # 只在还没有待执行的更新时安排新任务
        if not getattr(self, '_hold_update_scheduled', False):
            self._hold_update_scheduled = True
            threading.Timer(2.0, _delayed_update).start()

    def _update_hold_json_from_ctp_async(self):
        """从 CTP 异步查询持仓并更新 hold.json（供成交回报调用）"""
        try:
            positions = self.query_positions(timeout=5)
            if positions is None:
                self.print("[异步更新hold] 持仓查询失败")
                return

                # 聚合持仓
                aggregated = {}
                for pos in positions:
                    contract = pos.get("InstrumentID", "")
                    if not contract:
                        continue

                    today_vol = int(pos.get("TodayPosition", 0) or 0)
                    yd_vol = int(pos.get("YdPosition", 0) or 0)
                    total_vol = today_vol + yd_vol
                    if total_vol == 0:
                        continue

                    pos_dir = pos.get("PosiDirection", 0)
                    if pos_dir == 2:
                        direction = "买"
                    elif pos_dir == 3:
                        direction = "卖"
                    else:
                        continue

                    key = (contract, direction)
                    aggregated[key] = aggregated.get(key, 0) + total_vol

                # 转换为 hold.json 格式
                hold_rows = []
                for (contract, direction), volume in aggregated.items():
                    hold_rows.append({
                        "合约ID": contract,
                        "买/卖": direction,
                        "手数": str(volume),
                        "来源": "CTP成交回报"
                    })

                # 写入 hold.json
                hold_json_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    '..', 'order-check', 'hold.json'
                )
                with open(hold_json_path, 'w', encoding='utf-8') as f:
                    json.dump(hold_rows, f, ensure_ascii=False, indent=2)
                self.print(f"[异步更新hold] 已更新（共 {len(hold_rows)} 条）")
        except Exception as e:
            self.print(f"[异步更新hold] 异常: {e}")
        finally:
            self._hold_update_scheduled = False

    def _update_hold_json_from_ctp(self):
        """从 CTP 查询持仓并更新 hold.json（保留用于其他场景）"""
        pass

    def cancel_order(self, order_ref: str) -> bool:
        """撤单 - 子类实现"""
        return False

    def query_market_data(self, instrument_id: str, timeout: int = 5) -> Optional[dict]:
        """查询行情数据 - 子类实现"""
        return None

    def query_positions(self, timeout: int = 10, retries: int = 1, blocking: bool = True) -> Optional[List[dict]]:
        """查询持仓 - 子类实现"""
        return None

    def query_orders(
        self, timeout: int = 10, only_pending: bool = False, today_only: bool = True
    ) -> Optional[List[dict]]:
        """查询委托 - 子类实现"""
        return None