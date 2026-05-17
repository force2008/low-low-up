# -*- coding: utf-8 -*-
"""
订单操作模块

包含：
- place_limit_order, cancel_order, _cancel_order_by_sysid
- _is_order_pending, _get_pending_open_volume, _get_pending_close_volume
- _get_pending_order_refs, _get_pending_orders_by_sysid
- _calc_pending_from_ctp, _sync_ctp_orders_to_memory, _build_pending_map
"""

import os
import threading
import time
from typing import Dict, List, Optional, Tuple

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__('sys').path:
    __import__('sys').path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import tdapi


class PositionSyncManagerOrderOps:
    """持仓同步管理器 - 订单操作部分"""

    # ------------------------------------------------------------------
    # 报单 / 撤单
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
                "submit_time": time.time(),
                "replace_count": 0,
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

    def _place_order(
        self,
        exchange_id: str,
        instrument_id: str,
        direction: str,
        volume: int,
        limit_price: float,
        offset_flag: int,
    ) -> bool:
        """通用下单方法，支持指定开平标志，返回 True/False"""
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
                "submit_time": time.time(),
                "replace_count": 0,
            }

        ret = self._api.ReqOrderInsert(req, 0)
        if ret != 0:
            self.print(f"[错误] {exact_id} 报单发送失败，返回值={ret}")
            with self._order_lock:
                self._orders[order_ref]["status"] = "send_failed"
            self._notify_async(
                f"❌ 报单发送失败\n合约：{exact_id}\n方向：{direction}\n"
                f"手数：{volume} 手\n限价：{limit_price}\n开平：{offset_flag}\n错误码：{ret}"
            )
            return False

        self.print(
            f"[报单] {exact_id} {direction} {volume}手 "
            f"限价={limit_price} OrderRef={order_ref}"
        )
        return True

    def cancel_order(self, order_ref: str) -> bool:
        # 模糊匹配：尝试多种方式找到订单
        info = None
        matched_ref = None
        ref_raw = str(order_ref or "").strip('\x00') if order_ref else ""
        ref_stripped = ref_raw.strip()
        ref_lstrip = ref_raw.lstrip() if ref_raw else ""

        with self._order_lock:
            for key in (order_ref, ref_raw, ref_stripped, ref_lstrip):
                if key and key in self._orders:
                    info = self._orders[key]
                    matched_ref = key
                    break
            # 如果还没找到，尝试用 sys_id + exchange 匹配
            if not info:
                for key, oi in self._orders.items():
                    if oi.get("sys_id") == order_ref:
                        info = oi
                        matched_ref = key
                        break

        if not info:
            self.print(f"[撤单] OrderRef={order_ref} 不在订单列表中，尝试从 CTP 查询...")
            # 如果找不到，尝试通过 CTP 查询找到订单信息
            return False

        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.UserID = self._user_id
        req.ExchangeID = info["exchange"]
        req.InstrumentID = info["instr"]
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        # 优先使用 OrderSysID 撤单（更可靠）
        if info.get("sys_id"):
            req.OrderSysID = info["sys_id"]
            self.print(f"[撤单] {info['instr']} OrderSysID={info['sys_id']}")
        else:
            req.FrontID = self._front_id or 0
            req.SessionID = self._session_id or 0
            # 使用规范化后的 ref
            req.OrderRef = matched_ref if matched_ref else order_ref
            self.print(f"[撤单] {info['instr']} OrderRef={matched_ref}")

        cancel_event = threading.Event()
        cancel_result = [None]  # None=未返回, True=成功, False=失败
        with self._order_lock:
            self._cancel_events[matched_ref if matched_ref else order_ref] = cancel_event
            self._cancel_result = cancel_result

        self._api.ReqOrderAction(req, 0)

        # 等待最多 3 秒，同时检查撤单结果
        timeout = 3
        waited = 0
        while waited < timeout:
            if cancel_event.wait(timeout=0.5):
                break
            waited += 0.5
            # 检查结果是否已知
            if cancel_result[0] is not None:
                return cancel_result[0]

        # 超时或结果已知
        if cancel_result[0] is None:
            # 超时，保守返回 True 让调用者继续
            self.print(f"[撤单] OrderRef={matched_ref} 等待响应超时，保守处理")
            return True
        return cancel_result[0]

    def _cancel_order_by_sysid(self, order_sys_id: str, exchange_id: str, instrument_id: str) -> bool:
        """通过 OrderSysID 撤单（不依赖 _orders 内存缓存）"""
        self.print(f"[撤单开始] {instrument_id} OrderSysID={order_sys_id} Exchange={exchange_id}")
        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.UserID = self._user_id
        req.ExchangeID = exchange_id
        req.InstrumentID = instrument_id
        req.OrderSysID = order_sys_id
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        cancel_event = threading.Event()
        self._cancel_events[order_sys_id] = cancel_event
        self._cancel_result = [None]

        self.print(f"[撤单] 发送请求: {instrument_id} OrderSysID={order_sys_id}")
        ret = self._api.ReqOrderAction(req, 0)
        self.print(f"[撤单] ReqOrderAction 返回值: {ret}")

        timeout = 3
        waited = 0
        while waited < timeout:
            if cancel_event.wait(timeout=0.5):
                break
            waited += 0.5

        result = self._cancel_result[0]
        self.print(f"[撤单结果] OrderSysID={order_sys_id} result={result} waited={waited}s")
        if result is None:
            self.print(f"[撤单] OrderSysID={order_sys_id} 等待响应超时，保守处理")
            return True
        return result

    # ------------------------------------------------------------------
    # 订单状态查询
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

    def _get_pending_orders_by_sysid(
        self, instrument_id: str, direction: str, offset_flag: int, ctp_orders: List[dict]
    ) -> List[dict]:
        """从 CTP 查询结果中直接获取指定合约+方向+开平的未成交委托列表（按 OrderSysID 匹配）"""
        if not ctp_orders:
            return []
        inst_upper = instrument_id.upper()
        expected_dir = tdapi.THOST_FTDC_D_Buy if direction == "buy" else tdapi.THOST_FTDC_D_Sell
        pending = []
        for o in ctp_orders:
            status = str(o.get("OrderStatus", "")).strip()
            if status not in ("1", "3"):
                continue
            if o.get("InstrumentID", "").upper() != inst_upper:
                continue
            if str(o.get("Direction", "")).strip() != str(expected_dir).strip():
                continue
            offset_val = str(o.get("CombOffsetFlag", "")).strip()
            if offset_val != str(offset_flag).strip():
                continue
            pending.append(o)
        return pending

    def _calc_pending_from_ctp(
        self, ctp_orders: List[dict], contract: str, pos_direction: int, is_open: bool
    ) -> int:
        """从 CTP 委托查询结果计算指定合约+方向的未成交委托量

        pos_direction: 2=多, 3=空
        is_open=True:  多开=Buy(0), 空开=Sell(1)
        is_open=False: 多平=Sell(1), 空平=Buy(0)
        只统计状态为 1(部分成交还在队列) 或 3(未成交还在队列) 的委托
        """
        if not ctp_orders:
            return 0
        inst_upper = contract.upper()
        if is_open:
            expected_dir = (
                tdapi.THOST_FTDC_D_Buy if pos_direction == 2 else tdapi.THOST_FTDC_D_Sell
            )
        else:
            expected_dir = (
                tdapi.THOST_FTDC_D_Sell if pos_direction == 2 else tdapi.THOST_FTDC_D_Buy
            )

        total = 0
        for o in ctp_orders:
            status = str(o.get("OrderStatus", "")).strip()
            if status not in ("1", "3"):
                continue
            if o.get("InstrumentID", "").upper() != inst_upper:
                continue
            if str(o.get("Direction", "")).strip() != str(expected_dir).strip():
                continue

            offset_val = str(o.get("CombOffsetFlag", "")).strip()
            if is_open:
                if offset_val != str(tdapi.THOST_FTDC_OF_Open).strip():
                    continue
            else:
                if offset_val == tdapi.THOST_FTDC_OF_Open:
                    continue

            vol_total = o.get("VolumeTotalOriginal", 0)
            vol_traded = o.get("VolumeTraded", 0)
            total += max(0, vol_total - vol_traded)
        return total

    def _sync_ctp_orders_to_memory(self, ctp_orders: List[dict]):
        """把 CTP 查询到的未成交委托同步到 _orders，使自动撤单重挂能处理历史/外部委托"""
        if not ctp_orders:
            return
        synced = 0
        with self._order_lock:
            existing_refs = set(self._orders.keys())
            for o in ctp_orders:
                status = str(o.get("OrderStatus", "")).strip()
                if status not in ("1", "3"):
                    continue
                ref = o.get("OrderRef", "")
                ref_raw = str(ref or "").strip('\x00') if ref else ""
                ref_stripped = ref_raw.strip()
                ref_lstrip = ref_raw.lstrip() if ref_raw else ""

                # 模糊匹配：尝试多种方式找到已存在的 ref
                actual_ref = None
                if ref and ref in existing_refs:
                    actual_ref = ref
                elif ref_raw and ref_raw in existing_refs:
                    actual_ref = ref_raw
                elif ref_stripped and ref_stripped in existing_refs:
                    actual_ref = ref_stripped
                elif ref_lstrip and ref_lstrip in existing_refs:
                    actual_ref = ref_lstrip
                else:
                    # 用 OrderSysID + ExchangeID 组合匹配
                    sys_id = o.get("OrderSysID", "")
                    exchange_id = o.get("ExchangeID", "") or self._guess_exchange(o.get("InstrumentID", ""))
                    for existing_ref in existing_refs:
                        existing_info = self._orders.get(existing_ref, {})
                        if existing_info.get("sys_id") == sys_id and existing_info.get("exchange") == exchange_id:
                            actual_ref = existing_ref
                            break

                if actual_ref is not None:
                    continue
                # 不存在，添加到 _orders
                instrument_id = o.get("InstrumentID", "")
                exchange_id = o.get("ExchangeID", "") or self._guess_exchange(instrument_id)
                direction_val = str(o.get("Direction", "")).strip()
                direction_label = "buy" if direction_val == str(tdapi.THOST_FTDC_D_Buy).strip() else "sell"
                vol_total = o.get("VolumeTotalOriginal", 0)
                vol_traded = o.get("VolumeTraded", 0)
                remain = max(0, vol_total - vol_traded)
                if remain <= 0:
                    continue
                offset_flag = str(o.get("CombOffsetFlag", "")).strip()
                # 设置 submit_time 为较早时间，让监控线程尽快触发撤单重挂
                fake_submit_time = time.time() - self.ORDER_TIMEOUT_SECONDS - 10
                key_ref = ref_stripped if ref_stripped else ref_lstrip if ref_lstrip else ref_raw if ref_raw else "ext"
                self._orders[key_ref] = {
                    "event": threading.Event(),
                    "status": status,
                    "sys_id": o.get("OrderSysID", ""),
                    "instr": instrument_id,
                    "exchange": exchange_id,
                    "volume": remain,
                    "direction": direction_label,
                    "offset_flag": offset_flag,
                    "submit_time": fake_submit_time,
                    "replace_count": 0,
                    "from_ctp_sync": True,
                }
                existing_refs.add(key_ref)
                synced += 1
        if synced:
            self.print(f"[同步] 已将 {synced} 条 CTP 未成交委托同步到内存，等待自动撤单重挂")

    def _build_pending_map(self, ctp_orders: List[dict]) -> Dict[Tuple[str, int, bool], int]:
        """从 CTP 委托列表一次性构建 (合约, 持仓方向, 是否开仓) -> 未成交量 映射，避免 O(N×M) 遍历"""
        result: Dict[Tuple[str, int, bool], int] = {}
        OF_OPEN = str(tdapi.THOST_FTDC_OF_Open).strip()
        D_BUY = str(tdapi.THOST_FTDC_D_Buy).strip()
        for o in ctp_orders:
            status = str(o.get("OrderStatus", "")).strip()
            if status not in ("1", "3"):
                continue
            instrument_id = o.get("InstrumentID", "").upper()
            direction = str(o.get("Direction", "")).strip()
            offset = str(o.get("CombOffsetFlag", "")).strip()
            is_open = offset == OF_OPEN
            if is_open:
                pos_dir = 2 if direction == D_BUY else 3
            else:
                # 平仓方向同方向：空头平仓(买平)→空头方向(3)，多头平仓(卖平)→多头方向(2)
                pos_dir = 3 if direction == D_BUY else 2
            vol_total = o.get("VolumeTotalOriginal", 0)
            vol_traded = o.get("VolumeTraded", 0)
            remain = max(0, vol_total - vol_traded)
            if remain <= 0:
                continue
            key = (instrument_id, pos_dir, is_open)
            result[key] = result.get(key, 0) + remain
        return result

    # ------------------------------------------------------------------
    # 自动撤单重挂监控（每 45 秒检查一次）
    # ------------------------------------------------------------------
    def _check_and_replace_pending_orders(self):
        """扫描未成交委托，价格变化超过 1 个 tick 则撤单重挂"""
        # 获取所有未成交的委托
        pending_orders = []
        with self._order_lock:
            for ref, info in self._orders.items():
                if not self._is_order_pending(info):
                    continue
                pending_orders.append((ref, info))

        if not pending_orders:
            return

        self.print(f"[监控] 检查 {len(pending_orders)} 个未成交委托...")

        # 按合约分组查询行情
        contracts_to_query = set()
        for ref, info in pending_orders:
            contracts_to_query.add(info.get("instr", "").upper())

        # 批量查询行情
        market_data_map = {}
        for contract in contracts_to_query:
            md = self.query_market_data(contract, timeout=3, max_retries=2)
            if md:
                market_data_map[contract.upper()] = md

        # 检查每个委托
        cancel_list = []
        for ref, info in pending_orders:
            contract = info.get("instr", "").upper()
            md = market_data_map.get(contract)
            if not md:
                continue

            last_price = info.get("limit_price", 0) or info.get("last_md_price", 0)
            direction = info.get("direction", "")
            offset_flag = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)

            if direction == "buy":
                current_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
            else:
                current_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)

            if not current_price or current_price <= 0:
                continue

            # 获取 price tick
            info_obj = self._get_contract_info(contract.lower())
            price_tick = info_obj.get("PriceTick", 1.0) if info_obj else 1.0

            # 检查价格变化
            if last_price > 0 and abs(current_price - last_price) > price_tick:
                replace_count = info.get("replace_count", 0)
                if replace_count >= self.MAX_REPLACE_COUNT:
                    self.print(f"[监控] {contract} 已达到最大重挂次数 {self.MAX_REPLACE_COUNT}，发送告警")
                    # 发送飞书告警通知
                    d = "买" if direction == "buy" else "卖"
                    vol = info.get("volume", 0)
                    self._notify_async(
                        f"⚠️ 委托未能成交告警\n"
                        f"合约: {contract}\n"
                        f"方向: {d}\n"
                        f"手数: {vol}\n"
                        f"委托价: {last_price}\n"
                        f"当前价: {current_price}\n"
                        f"已重挂 {replace_count} 次，请人工处理"
                    )
                    continue

                cancel_list.append({
                    "ref": ref,
                    "info": info,
                    "old_price": last_price,
                    "new_price": current_price,
                })

        # 执行撤单和重挂
        if cancel_list:
            self.print(f"[监控] 发现 {len(cancel_list)} 个委托价格变化，需要撤单重挂")
            for item in cancel_list:
                self._cancel_and_replace(item, market_data_map)

    def _cancel_and_replace(self, item: dict, market_data_map: dict):
        """撤单并重挂"""
        ref = item["ref"]
        info = item["info"]
        contract = info.get("instr", "")
        direction = info.get("direction", "")
        offset_flag = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)
        volume = info.get("volume", 0)
        old_price = item["old_price"]
        new_price = item["new_price"]

        # 撤单
        order_sys_id = info.get("sys_id", "")
        exchange_id = info.get("exchange", "")
        if order_sys_id:
            self._cancel_order_by_sysid(order_sys_id, exchange_id, contract)
        else:
            self.cancel_order(ref)
        time.sleep(0.5)

        # 重新获取行情
        md = market_data_map.get(contract.upper())
        if md:
            if direction == "buy":
                current_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
            else:
                current_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
        else:
            current_price = new_price

        # 获取交易所
        info_obj = self._get_contract_info(contract.lower())
        if info_obj:
            exchange_id = info_obj.get("ExchangeID", exchange_id)

        # 重挂
        ok = self._place_order(
            exchange_id=exchange_id,
            instrument_id=contract,
            direction=direction,
            volume=volume,
            limit_price=current_price,
            offset_flag=offset_flag,
        )

        if ok:
            # 更新计数
            with self._order_lock:
                if ref in self._orders:
                    self._orders[ref]["replace_count"] = info.get("replace_count", 0) + 1

            # 发送通知
            d = "买" if direction == "buy" else "卖"
            self._notify_async(
                f"🔄 撤单重挂\n"
                f"合约: {contract}\n"
                f"方向: {d}\n"
                f"原价格: {old_price}\n"
                f"新价格: {current_price}"
            )
            self.print(f"[监控] {contract} 撤单重挂 @{current_price}")
        else:
            self.print(f"[监控] {contract} 撤单重挂失败")