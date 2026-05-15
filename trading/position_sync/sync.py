# -*- coding: utf-8 -*-
"""
同步逻辑模块

包含：
- _trade_single
- _send_position_mismatch_alert
- sync_and_trade (主方法)
- _send_sync_notification
- _build_positions
- execute_orders
- _place_order
"""

import json
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__('sys').path:
    __import__('sys').path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import tdapi


class PositionSyncManagerSync:
    """持仓同步管理器 - 同步逻辑部分"""

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

        self.print("[步骤1] 加载合约信息...")
        if not self._load_contract_info():
            self.print("[错误] _load_contract_info() 返回 False")
            return False
        self.print("[步骤1] 合约信息加载成功")

        # 先查询一次实际持仓
        self.print("[步骤2] 查询持仓...")
        positions = self.query_positions(timeout=10)
        if positions is None:
            self.print("[错误] 持仓查询失败，本次同步中止，避免误判空仓导致重复建仓")
            return False
        self.print(f"[步骤2] 账户实际持仓 {len(positions)} 条原始记录")

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

        # 持仓对比（扣除在途委托后的有效持仓）
        actual_agg = self._aggregate_actual_positions()
        target = self._parse_hold_std()

        # 调试：打印解析结果，帮助排查"持仓一致"误判
        self.print(f"[调试] 实际持仓聚合: {actual_agg}")
        self.print(f"[调试] 标准持仓解析: {target}")

        # 如果查询返回空持仓，二次确认避免误判空仓导致重复建仓
        if not actual_agg and target:
            self.print("[警告] 首次查询返回空持仓，进行二次确认...")
            time.sleep(1)
            positions2 = self.query_positions(timeout=10)
            if positions2 is not None and positions2:
                actual_agg = self._aggregate_actual_positions()
                self.print(f"[信息] 二次确认后实际持仓 {len(positions2)} 条记录")

        # 查询 CTP 当前委托（程序重启后 _orders 可能为空，必须查 CTP 才能知道真实在途委托）
        ctp_orders = self.query_orders(timeout=5, only_pending=True, today_only=True)
        if ctp_orders is None:
            self.print("[警告] 委托查询超时，继续执行同步（使用空委托列表）")
            ctp_orders = []  # 使用空列表，继续执行同步
        if ctp_orders:
            self.print(f"[信息] CTP 当前委托 {len(ctp_orders)} 条")
            # 把 CTP 上的未成交委托同步到 _orders，让自动撤单重挂统一处理
            self._sync_ctp_orders_to_memory(ctp_orders)

        # 一次性构建 pending 映射，避免对每个合约重复遍历大量委托
        pending_map = self._build_pending_map(ctp_orders)
        self.print(f"[对比] ctp_orders 共 {len(ctp_orders)} 条, pending_map: {dict(pending_map)}")

        # 持仓对比（扣除在途委托后的有效持仓）
        effective_actual: Dict[Tuple[str, int], int] = {}

        # 更新 hold.json（当前持仓快照）
        self._update_hold_json_file()
        all_keys = set(actual_agg.keys()) | set(target.keys())
        for key in all_keys:
            contract, direction = key
            a_vol = actual_agg.get(key, 0)
            contract_upper = contract.upper()
            pending_open = pending_map.get((contract_upper, direction, True), 0)
            pending_close = pending_map.get((contract_upper, direction, False), 0)
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

        # 操作日志：记录实际执行的操作
        action_log = []

        # 调试：打印对比结果
        self.print(f"[对比] 标准持仓汇总: {dict(target)}")
        self.print(f"[对比] 实际持仓汇总: {dict(actual_agg)}")
        self.print(f"[对比] 有效持仓汇总: {dict(effective_actual)}")
        self.print(f"[对比] pending_map: {dict(pending_map)}")
        self.print(f"[对比] 缺额: {missing_orders}")
        self.print(f"[对比] 超额: {excess_orders}")
        self.print(f"[对比] pending_orders={len(ctp_orders)}, missing_orders={len(missing_orders)}, excess_orders={len(excess_orders)}")

        # ========== 处理只在 pending_map 中但不在 target 的合约 ==========
        # 这些是标准不想要的开仓委托，必须撤销
        std_keys = set(target.keys())
        pending_only_orders = []  # 只在 pending 中的开仓委托
        for (contract, pos_dir, is_open), vol in pending_map.items():
            if not is_open:  # 只处理开仓委托
                continue
            pos_key = (contract, pos_dir)  # (合约, 持仓方向)
            if pos_key not in std_keys and vol > 0:
                # 标准中没有这个持仓方向，应该撤销这个开仓委托
                dir_label = "buy" if pos_dir == 2 else "sell"
                pending_only_orders.append({
                    "contract": contract,
                    "pos_dir": pos_dir,
                    "direction_label": dir_label,
                    "volume": vol,
                })
                self.print(f"[撤销在途] {contract} {'买' if pos_dir == 2 else '卖'}开 {vol} 手（标准中无此持仓方向）")

        # 撤销只在 pending 中的开仓委托
        if pending_only_orders:
            self.print(f"[撤销在途] 共 {len(pending_only_orders)} 个合约需要撤销开仓委托...")
            for po in pending_only_orders:
                # 找对应的委托并撤销
                po_contract_upper = po["contract"].upper()
                expected_dir = tdapi.THOST_FTDC_D_Buy if po["pos_dir"] == 2 else tdapi.THOST_FTDC_D_Sell

                pending_to_cancel = [
                    o for o in ctp_orders
                    if o.get("InstrumentID", "").upper() == po_contract_upper
                    and str(o.get("Direction", "")).strip() == str(expected_dir).strip()
                    and str(o.get("CombOffsetFlag", "")).strip() == str(tdapi.THOST_FTDC_OF_Open).strip()
                    and str(o.get("OrderStatus", "")).strip() in ("1", "3")
                ]
                if pending_to_cancel:
                    self.print(f"[撤销在途] {po['contract']} 找到 {len(pending_to_cancel)} 笔委托，撤销")
                    action_log.append(f"🔄 撤销在途: {po['contract']} {'买' if po['pos_dir']==2 else '卖'}开 {po['volume']} 手")
                    for o in pending_to_cancel:
                        order_sys_id = o.get("OrderSysID", "")
                        exchange_id = o.get("ExchangeID", "")
                        if order_sys_id:
                            self._cancel_order_by_sysid(order_sys_id, exchange_id, po["contract"])
                        else:
                            self.cancel_order(o.get("OrderRef", ""))
                    time.sleep(0.5)

        # 检查 RB2610/rb2610 的状态
        for key in list(target.keys()) + list(actual_agg.keys()) + list(effective_actual.keys()):
            key_str = str(key).lower()
            if "rb" in key_str or "2610" in key_str:
                self.print(f"[对比-debug] {key} -> target={target.get(key, 'N/A')}, actual={actual_agg.get(key, 'N/A')}, effective={effective_actual.get(key, 'N/A')}")

        # 兜底检查：如果有合约只在 actual_agg 中但不在 excess_orders 中，强制加入
        std_keys = set(target.keys())
        actual_keys = set(actual_agg.keys())
        only_in_actual = actual_keys - std_keys
        for key in only_in_actual:
            contract, direction = key
            vol = actual_agg.get(key, 0)
            # 检查是否已经在 excess_orders 中
            already_added = any(eo["contract"] == contract and eo["direction"] == direction for eo in excess_orders)
            if not already_added and vol > 0:
                self.print(f"[补检] {contract} {direction} {vol}手 只在实际中，强制加入超额平仓")
                excess_orders.append({
                    "contract": contract,
                    "direction": direction,
                    "volume": vol,
                })

        if missing_orders:
            self.print(f"[补单] 标准持仓有 {len(missing_orders)} 个合约缺额，自动补齐...")
            for mo in missing_orders:
                self.print(f"[补单执行] 处理 {mo['contract']} {mo['direction']} {mo['volume']}手")
                # 直接从 ctp_orders 中查找需要撤掉的同方向开仓委托
                mo_upper = mo['contract'].upper()
                expected_dir = tdapi.THOST_FTDC_D_Buy if mo["direction"] == "buy" else tdapi.THOST_FTDC_D_Sell
                expected_dir_str = str(expected_dir).strip()
                OF_OPEN_STR = str(tdapi.THOST_FTDC_OF_Open).strip()

                # 打印所有 ctp_orders 中的合约，用于调试
                self.print(f"[调试] ctp_orders 共 {len(ctp_orders)} 条, 查找 {mo_upper} dir={expected_dir_str} offset={OF_OPEN_STR}:")
                for o in ctp_orders:
                    instr = o.get("InstrumentID", "")
                    dir_val = str(o.get("Direction", "")).strip()
                    offset_val = str(o.get("CombOffsetFlag", "")).strip()
                    status_val = str(o.get("OrderStatus", "")).strip()
                    match = "✓" if (instr.upper() == mo_upper and dir_val == expected_dir_str and offset_val == OF_OPEN_STR and status_val in ("1", "3")) else "✗"
                    self.print(f"  [{match}] {instr} dir={repr(dir_val)} offset={repr(offset_val)} status={repr(status_val)}")

                pending_to_cancel = [
                    o for o in ctp_orders
                    if o.get("InstrumentID", "").upper() == mo_upper
                    and str(o.get("Direction", "")).strip() == expected_dir_str
                    and str(o.get("CombOffsetFlag", "")).strip() == OF_OPEN_STR
                    and str(o.get("OrderStatus", "")).strip() in ("1", "3")
                ]
                if pending_to_cancel:
                    self.print(f"[撤旧单] {mo['contract']} {mo['direction']} 存在 {len(pending_to_cancel)} 笔未成交开仓委托，先撤单")
                    action_log.append(f"🔄 撤单: {mo['contract']} {mo['direction']} {len(pending_to_cancel)} 笔")
                    for o in pending_to_cancel:
                        order_sys_id = o.get("OrderSysID", "")
                        exchange_id = o.get("ExchangeID", "")
                        if order_sys_id:
                            self._cancel_order_by_sysid(order_sys_id, exchange_id, mo["contract"])
                        else:
                            self.cancel_order(o.get("OrderRef", ""))
                    time.sleep(1)  # 等待撤单生效
                else:
                    self.print(f"[补单] {mo['contract']} 无需撤单，直接提交新委托")

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
                ok = self._place_order(
                    exchange_id=info["ExchangeID"],
                    instrument_id=mo["contract"],
                    direction=mo["direction"],
                    volume=mo["volume"],
                    limit_price=limit_price,
                    offset_flag=tdapi.THOST_FTDC_OF_Open,
                    wait_fill=False,
                )
                if ok:
                    action_log.append(f"🆕 补单成功: {mo['contract']} {mo['direction']} {mo['volume']}手 @{limit_price}")
                    self.print(f"[补单成功] {mo['contract']} 已提交")
                else:
                    action_log.append(f"❌ 补单失败: {mo['contract']} {mo['direction']} {mo['volume']}手")
                    self.print(f"[补单失败] {mo['contract']}")
                time.sleep(0.3)

        else:
            self.print(f"[补单] 无缺额订单")

        if excess_orders:
            self.print(f"[平仓] 发现 {len(excess_orders)} 个合约超额持仓，自动平仓...")
            self.print(f"[平仓-debug] excess_orders 详情: {excess_orders}")

            # ========== 第一步：撤销所有与 excess 相关的在途开仓委托 ==========
            # 平仓前必须先撤销相反方向的开仓委托（如平空头时撤销买开）
            self.print("[平仓] 检查是否有需要撤销的开仓委托...")
            for eo in excess_orders:
                eo_contract = eo["contract"].upper()
                eo_dir = eo["direction"]  # 2=多头, 3=空头
                # 相反方向的开仓委托需要撤销
                opposite_open_dir = tdapi.THOST_FTDC_D_Sell if eo_dir == 2 else tdapi.THOST_FTDC_D_Buy

                # 从 ctp_orders 中找相反方向的开仓委托
                opposite_pending = [
                    o for o in ctp_orders
                    if o.get("InstrumentID", "").upper() == eo_contract
                    and str(o.get("Direction", "")).strip() == str(opposite_open_dir).strip()
                    and str(o.get("CombOffsetFlag", "")).strip() == str(tdapi.THOST_FTDC_OF_Open).strip()
                    and str(o.get("OrderStatus", "")).strip() in ("1", "3")
                ]
                if opposite_pending:
                    self.print(f"[撤旧开仓] {eo['contract']} 方向相反的开仓委托 {len(opposite_pending)} 笔，撤销")
                    action_log.append(f"🔄 撤旧开仓: {eo['contract']} {len(opposite_pending)} 笔")
                    for o in opposite_pending:
                        order_sys_id = o.get("OrderSysID", "")
                        exchange_id = o.get("ExchangeID", "")
                        if order_sys_id:
                            self._cancel_order_by_sysid(order_sys_id, exchange_id, eo["contract"])
                        else:
                            self.cancel_order(o.get("OrderRef", ""))
                    time.sleep(0.5)

            # ========== 第二步：平仓 ==========
            for eo in excess_orders:
                # 先检查是否有未成交的同方向平仓委托需要撤掉（直接从 CTP 查询结果中查找）
                close_dir_label = "sell" if eo["direction"] == 2 else "buy"
                expected_dir = tdapi.THOST_FTDC_D_Sell if eo["direction"] == 2 else tdapi.THOST_FTDC_D_Buy
                self.print(f"[平仓] 检查 {eo['contract']} 方向={close_dir_label} 是否需要撤单...")

                pending_from_ctp = [
                    o for o in ctp_orders
                    if o.get("InstrumentID", "").upper() == eo["contract"].upper()
                    and str(o.get("Direction", "")).strip() == str(expected_dir).strip()
                    and str(o.get("CombOffsetFlag", "")).strip() == str(tdapi.THOST_FTDC_OF_Close).strip()
                    and str(o.get("OrderStatus", "")).strip() in ("1", "3")
                ]
                if pending_from_ctp:
                    self.print(f"[撤旧单] {eo['contract']} {close_dir_label} 存在 {len(pending_from_ctp)} 笔未成交平仓委托，先撤单")
                    action_log.append(f"🔄 撤单: {eo['contract']} {close_dir_label} {len(pending_from_ctp)} 笔")
                    for o in pending_from_ctp:
                        order_ref = o.get("OrderRef", "")
                        order_sys_id = o.get("OrderSysID", "")
                        if order_sys_id:
                            self._cancel_order_by_sysid(order_sys_id, o.get("ExchangeID", ""), eo["contract"])
                        elif order_ref:
                            self.cancel_order(order_ref)
                    time.sleep(1)  # 等待撤单生效
                    # 重新查询持仓
                    self.query_positions(timeout=5)
                    actual_agg = self._aggregate_actual_positions()

                detail = self._get_position_detail(eo["contract"], eo["direction"])
                info = self._get_contract_info(eo["contract"])
                exchange_id = detail["ExchangeID"] or info["ExchangeID"]

                # 防御：实际持仓为 0 或 CTP 数据滞后，跳过
                actual_pos = detail["Position"]
                if actual_pos <= 0:
                    # 打印调试信息，帮助诊断问题
                    contract_upper = eo["contract"].upper()
                    matching_positions = [p for p in self._actual_positions if p.get("InstrumentID", "").upper() == contract_upper]
                    if matching_positions:
                        self.print(f"[调试] {eo['contract']} 在 _actual_positions 中有 {len(matching_positions)} 条记录: {matching_positions}")
                        self.print(f"[调试] 查询方向={eo['direction']}，但记录中的 PosiDirection: {[p.get('PosiDirection') for p in matching_positions]}")
                    self.print(f"[跳过] {eo['contract']} {'多' if eo['direction']==2 else '空'} 实际持仓为 {actual_pos}，跳过平仓（CTP 数据可能滞后或方向不匹配）")
                    continue

                # 防御：扣除已挂未成交平仓委托后的可用可平量
                pending_close_vol = self._get_pending_close_volume(eo["contract"], eo["direction"])
                available = actual_pos - pending_close_vol
                if available <= 0:
                    self.print(f"[跳过] {eo['contract']} {'多' if eo['direction']==2 else '空'} 实际持仓 {actual_pos}，已有 {pending_close_vol} 手未成交平仓委托，无可平量，跳过")
                    continue

                # 限制平仓数量不超过可用可平量
                diff = min(eo["volume"], available)
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
                    ok = self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=eo["contract"],
                        direction=close_direction,
                        volume=close_today,
                        limit_price=limit_price,
                        offset_flag=tdapi.THOST_FTDC_OF_CloseToday,
                        wait_fill=False,
                    )
                    if ok:
                        action_log.append(f"🆕 平仓成功: {eo['contract']} {'卖' if close_direction=='sell' else '买'} {close_today}手 @{limit_price} (平今)")
                    else:
                        action_log.append(f"❌ 平仓失败: {eo['contract']} {'卖' if close_direction=='sell' else '买'} {close_today}手 (平今)")
                    diff -= close_today
                    time.sleep(0.3)

                if diff > 0:
                    offset = tdapi.THOST_FTDC_OF_CloseYesterday if is_shfe else tdapi.THOST_FTDC_OF_Close
                    label = "平昨" if is_shfe else "平仓"
                    self.print(
                        f"[平仓-{label}] {eo['contract']} {'卖' if close_direction=='sell' else '买'} {diff}手 限价={limit_price}"
                    )
                    ok = self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=eo["contract"],
                        direction=close_direction,
                        volume=diff,
                        limit_price=limit_price,
                        offset_flag=offset,
                        wait_fill=False,
                    )
                    if ok:
                        action_log.append(f"🆕 平仓成功: {eo['contract']} {'卖' if close_direction=='sell' else '买'} {diff}手 @{limit_price} ({label})")
                    else:
                        action_log.append(f"❌ 平仓失败: {eo['contract']} {'卖' if close_direction=='sell' else '买'} {diff}手 ({label})")
                    diff -= diff
                    time.sleep(0.3)
        else:
            self.print(f"[平仓] 无超额订单")

        # ========== 生成详细的同步结果通知 ==========
        self._send_sync_notification(target, actual_agg, effective_actual, pending_map, missing_orders, excess_orders, action_log)

        if missing_orders or excess_orders:
            self.print("[结论] 持仓调整完成（缺额已补 / 超额已平）")
            self.print("=" * 60)
            self._is_first_run = False
            return True

        self.print("[结论] 持仓一致，无需操作")
        self.print("=" * 60)
        self._is_first_run = False
        return True

    def _send_sync_notification(self, target: dict, actual_agg: dict, effective_actual: dict,
                                 pending_map: dict, missing_orders: list, excess_orders: list,
                                 action_log: list = None):
        """发送详细的持仓同步结果飞书通知"""
        lines = []
        action_log = action_log or []

        # 统计汇总
        total_std_vol = sum(target.values())
        total_actual_vol = sum(actual_agg.values())
        total_effective_vol = sum(effective_actual.values())

        # 检查总手数是否一致
        has_vol_diff = (total_std_vol != total_actual_vol)

        # 检查是否有具体的合约差异
        has_missing = len(missing_orders) > 0
        has_excess = len(excess_orders) > 0

        if not has_missing and not has_excess and not has_vol_diff:
            # 持仓完全一致
            lines.append("✅ 持仓同步完成：一致")
            lines.append(f"📊 标准持仓: {total_std_vol} 手 ({len(target)} 个合约)")
            lines.append(f"📊 实际持仓: {total_actual_vol} 手 ({len(actual_agg)} 个合约)")
            lines.append("无差异，无需操作")
        else:
            # 有差异
            lines.append("🔄 持仓同步完成：有差异")
            lines.append("=" * 25)

            # 显示对比汇总
            lines.append(f"📊 标准持仓: {total_std_vol} 手 ({len(target)} 个合约)")
            lines.append(f"📊 实际持仓: {total_actual_vol} 手 ({len(actual_agg)} 个合约)")
            lines.append(f"📊 有效持仓: {total_effective_vol} 手 (扣除在途委托)")

            if has_vol_diff:
                diff = total_actual_vol - total_std_vol
                sign = "+" if diff > 0 else ""
                lines.append(f"⚠️ 总手数差: {sign}{diff} 手")

            # 在途委托明细
            if pending_map:
                pending_items = [(k, v) for k, v in pending_map.items() if v > 0]
                if pending_items:
                    lines.append("-" * 25)
                    lines.append(f"📋 在途委托 ({len(pending_items)} 笔)：")
                    for (contract, direction, is_open), vol in sorted(pending_items):
                        dname = "买开" if (direction == 2 and is_open) else \
                                "卖开" if (direction == 3 and is_open) else \
                                "买平" if (direction == 2 and not is_open) else "卖平"
                        lines.append(f"  {contract} {dname} {vol}手")
                    lines.append("  (处理超额时会先撤掉这些委托)")

            lines.append("=" * 25)

            # 缺额（需要开仓）
            if missing_orders:
                lines.append(f"📈 缺额开仓 ({len(missing_orders)} 个)：")
                total_missing = 0
                for mo in missing_orders:
                    dname = "买" if mo["direction"] == "buy" else "卖"
                    lines.append(f"  {mo['contract']} {dname} {mo['volume']}手")
                    total_missing += mo["volume"]
                lines.append(f"  共计: {total_missing} 手")
                lines.append("-" * 15)

            # 超额（需要平仓）
            if excess_orders:
                lines.append(f"📉 超额平仓 ({len(excess_orders)} 个)：")
                total_excess = 0
                for eo in excess_orders:
                    dname = "多" if eo["direction"] == 2 else "空"
                    lines.append(f"  {eo['contract']} {dname} {eo['volume']}手")
                    total_excess += eo["volume"]
                lines.append(f"  共计: {total_excess} 手")
                lines.append("-" * 15)

            # 显示合约差异（只在标准持仓和实际持仓的合约不完全相同时）
            std_contracts = set(target.keys())
            actual_contracts = set(actual_agg.keys())
            only_in_std = std_contracts - actual_contracts
            only_in_actual = actual_contracts - std_contracts

            if only_in_std:
                lines.append("-" * 25)
                lines.append(f"⚠️ 标准中有但实际中没有 ({len(only_in_std)} 个)：")
                lines.append("  (可能是已平仓但未同步)")
                for c, d in only_in_std:
                    dname = "买" if d == 2 else "卖"
                    vol = target[(c, d)]
                    lines.append(f"  {c} {dname} {vol}手")

            if only_in_actual:
                lines.append("-" * 25)
                lines.append(f"⚠️ 实际中有但标准中没有 ({len(only_in_actual)} 个)：")
                lines.append("  (可能是新开仓但未写入标准持仓)")
                for c, d in only_in_actual:
                    dname = "买" if d == 2 else "卖"
                    vol = actual_agg[(c, d)]
                    lines.append(f"  {c} {dname} {vol}手")

            lines.append("=" * 25)

            if action_log:
                lines.append("-" * 25)
                lines.append("📝 实际操作记录：")
                for log in action_log:
                    lines.append(f"  {log}")
            elif has_missing or has_excess:
                lines.append("⚠️ 已处理，但无操作记录（可能被冷却跳过）")

        self._notify_async("\n".join(lines))

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
            exchange_id = info["ExchangeID"]

            # 快速获取行情（3秒超时）
            md = self.query_market_data(contract, timeout=3)
            if not md:
                self.print(f"[建仓-跳过] {contract} 无法获取行情")
                continue

            # 计算价格
            if direction == 2:  # 多头 → 买入
                limit_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
            else:  # 空头 → 卖出
                limit_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)

            if limit_price <= 0:
                self.print(f"[建仓-跳过] {contract} 无有效价格")
                continue

            direction_str = "buy" if direction == 2 else "sell"
            self.print(f"[建仓-快速提交] {contract} {direction_str} {vol}手 @{limit_price}")

            # 快速提交，不等待成交
            ok = self._place_order(
                exchange_id=exchange_id,
                instrument_id=contract,
                direction=direction_str,
                volume=vol,
                limit_price=limit_price,
                offset_flag=tdapi.THOST_FTDC_OF_Open,
                wait_fill=False,
            )
            if ok:
                success_count += 1
            time.sleep(0.2)  # 减少等待时间

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
        return success_count >= len(target) * 0.8  # 80%成功就认为完成

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
        # 确保合约信息已加载（execute_orders 可能在 sync_and_trade 之前被调用）
        self._load_contract_info()

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

        self.print(f"[信息] 持仓差异订单 {len(orders)} 条待执行")
        success_count = 0
        skipped_count = 0
        notify_lines = [f"📋 持仓差异订单，待执行 {len(orders)} 条"]

        def _offset_label(flag):
            return {
                tdapi.THOST_FTDC_OF_Open: "开",
                tdapi.THOST_FTDC_OF_Close: "平",
                tdapi.THOST_FTDC_OF_CloseToday: "平今",
                tdapi.THOST_FTDC_OF_CloseYesterday: "平昨",
            }.get(flag, "未知")

        def _dir_label(label):
            return "买" if label == "buy" else "卖"

        # 预查持仓：如果有平仓单，先查询一次持仓供后续检查
        has_close = False
        for order in orders:
            off = self._extract_field(order, ["开平", "开平标志", "OffsetFlag", "offset_flag", "offset"])
            if off and off not in ("开仓", "Open", "OPEN", "open", "开"):
                has_close = True
                break
        current_positions = None
        if has_close:
            current_positions = self.query_positions(timeout=5)
            if current_positions is None:
                self.print("[警告] 持仓查询超时，平仓单将保守跳过")
                notify_lines.append("⚠️ 持仓查询超时，平仓单将保守跳过")

        # 按交易日获取已处理的 signal ids
        trading_day = self._get_trading_day()
        processed_ids = set(self._processed_signal_data.get(trading_day, []))

        for order in orders:
            try:
                # 报单编号去重：compare_orders 从 CSV 差集生成 signal.json，同一笔委托可能在多轮 CSV 导出后被反复识别
                signal_id = self._extract_field(order, ["报单编号", "OrderRef", "order_ref", "ID", "id"])
                contract = self._standardize_contract(self._extract_field(order, ["合约", "合约名", "InstrumentID", "instrument_id"]))
                direction_str = self._extract_field(order, ["方向", "买卖", "买/卖", "Direction", "direction"])
                offset_str = self._extract_field(order, ["开平", "开平标志", "OffsetFlag", "offset_flag", "offset"])
                volume_str = self._extract_field(order, ["手数", "数量", "总报单量", "委托数量", "报单数量", "Volume", "volume"])
                price_str = self._extract_field(order, ["价格", "委托价", "Price", "price", "LimitPrice"])

                # 预解析字段，用于通知
                _vol = 0
                try:
                    _vol = int(str(volume_str).strip()) if volume_str else 0
                except ValueError:
                    pass
                _sig = signal_id or "无编号"

                if signal_id and signal_id in processed_ids:
                    msg = f"⏭️ {contract} {direction_str or '?'} {_vol}手（编号{_sig}）：已处理过，跳过"
                    self.print(f"[跳过] 报单编号 {signal_id} 已处理过，避免重复提交")
                    notify_lines.append(msg)
                    skipped_count += 1
                    continue

                if not contract or not direction_str or not volume_str:
                    self.print(f"[跳过] 委托字段不全: {order}")
                    notify_lines.append(f"⏭️ {contract or '?'} {direction_str or '?'} {_vol}手：字段不全，跳过")
                    skipped_count += 1
                    continue

                try:
                    volume = int(str(volume_str).strip())
                    price = float(str(price_str).strip()) if price_str else 0.0
                except ValueError:
                    self.print(f"[跳过] 委托数值解析失败: {order}")
                    notify_lines.append(f"⏭️ {contract} {direction_str} {_vol}手：数值解析失败，跳过")
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
                    notify_lines.append(f"⏭️ {contract} {direction_str} {volume}手：未知方向，跳过")
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
                    # 默认按方向推断：买=开仓，卖=平仓
                    offset_flag = tdapi.THOST_FTDC_OF_Open if direction == tdapi.THOST_FTDC_D_Buy else tdapi.THOST_FTDC_OF_Close
                    self.print(f"[警告] 未识别开平标志 '{offset_str}'，默认使用 {'开仓' if offset_flag == tdapi.THOST_FTDC_OF_Open else '平仓'}")

                off_label = _offset_label(offset_flag)
                dir_label = _dir_label(direction_label)

                # --------------------------------------------------------------
                # 平仓单防御：检查持仓是否足够
                # --------------------------------------------------------------
                if offset_flag != tdapi.THOST_FTDC_OF_Open:
                    # 买平 → 需要有空仓 (PosiDirection=3)
                    # 卖平 → 需要有多仓 (PosiDirection=2)
                    required_pos_dir = 3 if direction == tdapi.THOST_FTDC_D_Buy else 2
                    actual_pos = 0
                    if current_positions:
                        for pos in current_positions:
                            if pos.get("InstrumentID", "").upper() == contract.upper() and pos.get("PosiDirection") == required_pos_dir:
                                actual_pos += pos.get("Position", 0)
                    if actual_pos <= 0:
                        self.print(f"[跳过] {contract} 平仓单：当前无对应持仓，跳过")
                        notify_lines.append(f"⏭️ {contract} {dir_label}{off_label} {volume}手：当前无对应持仓，跳过")
                        skipped_count += 1
                        continue
                    if volume > actual_pos:
                        self.print(f"[警告] {contract} 平仓手数 {volume} 超过持仓 {actual_pos}，调整为 {actual_pos}")
                        volume = actual_pos

                info = self._get_contract_info(contract)
                exchange_id = info["ExchangeID"]

                env = getattr(self, "env_name", "")
                is_file_price = env in ("online", "simu")
                if is_file_price:
                    # online/simu 环境：严格使用委托文件里的价格挂限价单
                    if price <= 0:
                        self.print(f"[跳过] {contract} 委托文件未提供有效价格，跳过报单")
                        notify_lines.append(f"⏭️ {contract} {dir_label}{off_label} {volume}手：无有效价格，跳过")
                        skipped_count += 1
                        continue
                    limit_price = price
                else:
                    # 非 online/simu 环境：查询市场对手价挂限价单
                    md = self.query_market_data(contract, timeout=3)
                    if direction_label == "buy":
                        market_price = md.get("AskPrice1", 0) if md else 0
                    else:
                        market_price = md.get("BidPrice1", 0) if md else 0
                    if not market_price:
                        market_price = md.get("LastPrice", 0) if md else 0
                    if market_price <= 0:
                        self.print(f"[跳过] {contract} 无法获取市场价格，跳过报单")
                        notify_lines.append(f"⏭️ {contract} {dir_label}{off_label} {volume}手：无法获取市场价格，跳过")
                        skipped_count += 1
                        continue

                    limit_price = market_price

                # 检查是否有相同合约+方向+开平的未成交委托，有则先撤旧单再重挂
                pending_refs = self._get_pending_order_refs(contract, direction_label, offset_flag)
                if pending_refs:
                    self.print(f"[撤旧单] {contract} 存在 {len(pending_refs)} 笔未成交委托，先撤单再重挂")
                    notify_lines.append(f"🔄 {contract} {dir_label}{off_label} {volume}手：撤掉 {len(pending_refs)} 笔旧单，重新挂单")
                    for ref in pending_refs:
                        self.cancel_order(ref)
                        time.sleep(0.2)
                else:
                    notify_lines.append(f"🆕 {contract} {dir_label}{off_label} {volume}手 @{limit_price}：新挂单")

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
                        processed_ids.add(signal_id)
                        self._processed_signal_data[trading_day] = list(processed_ids)
                        self._save_processed_ids()
                    # 更新通知行（替换为新挂详情）
                    if pending_refs:
                        notify_lines[-1] = f"🔄 {contract} {dir_label}{off_label} {volume}手 @{limit_price}：撤旧单后新挂 (Ref={ok})"
                    else:
                        notify_lines[-1] = f"✅ {contract} {dir_label}{off_label} {volume}手 @{limit_price}：新挂成功 (Ref={ok})"
                else:
                    if pending_refs:
                        notify_lines[-1] = f"❌ {contract} {dir_label}{off_label} {volume}手：撤旧单后新挂失败"
                    else:
                        notify_lines[-1] = f"❌ {contract} {dir_label}{off_label} {volume}手 @{limit_price}：新挂失败"
                time.sleep(0.3)
            except Exception as e:
                self.print(f"[错误] 处理委托时异常: {e}")
                import traceback
                traceback.print_exc()
                skipped_count += 1
                continue

        pending = len(orders) - skipped_count
        self.print("=" * 60)
        self.print(f"委托执行结束: 成功 {success_count}/{pending} (跳过 {skipped_count})")
        self.print("=" * 60)
        notify_lines.append(f"\n汇总: 成功 {success_count} / 待执行 {pending} / 跳过 {skipped_count}")
        self._notify_async("\n".join(notify_lines))
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
                "submit_time": time.time(),
                "replace_count": 0,
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