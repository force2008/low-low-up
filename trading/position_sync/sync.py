# -*- coding: utf-8 -*-
"""
同步逻辑模块 - 快速同步版本

核心设计：2-3分钟内完成所有委托提交
- 第一阶段：并行查询所有行情（无等待）
- 第二阶段：串行提交开仓委托（0.2秒间隔，与 PositionManagerUI.py 一致）
- 第三阶段：串行提交平仓委托（0.2秒间隔，与 PositionManagerUI.py 一致）
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
    # 核心流程：持仓对比 + 快速同步
    # ------------------------------------------------------------------
    def sync_and_trade(
        self,
        trade_volume: int = 1,
        timeout: int = 30,
        position_ratio: float = 1.0,
    ) -> bool:
        # 获取线程锁，防止并发调用
        if not self._sync_lock.acquire(blocking=True, timeout=5):
            self.print("[跳过] 同步被锁定，跳过本次同步")
            return False

        self._is_syncing = True
        try:
            return self._do_sync(trade_volume, timeout, position_ratio=position_ratio, lock_held=True)
        finally:
            self._is_syncing = False
            self._sync_lock.release()

    def _do_sync(self, trade_volume: int = 1, timeout: int = 30, position_ratio: float = None, lock_held: bool = False) -> bool:
        """执行同步：加载数据 -> 对比 -> 快速同步"""
        if position_ratio is not None:
            if position_ratio <= 0:
                self.print(f"[错误] position_ratio 必须大于 0，当前值: {position_ratio}")
                return False
            self._position_ratio = float(position_ratio)

        self.print("=" * 60)
        self.print("【持仓同步开始】")
        self.print(f"【持仓比例】{self._position_ratio}")
        self.print("=" * 60)

        # 检查冷却期：上次同步后60秒内不再同步（避免CTP持仓数据滞后导致重复下单）
        current_time = time.time()
        last_sync = getattr(self, '_last_sync_time', 0)
        SYNC_COOLDOWN = 60  # 60秒冷却
        if current_time - last_sync < SYNC_COOLDOWN:
            self.print(f"[跳过] 距离上次同步仅 {current_time - last_sync:.0f} 秒，冷却中（{SYNC_COOLDOWN}秒）")
            return False
        self._last_sync_time = current_time

        try:
            # 1. 加载合约信息
            if not self._load_contract_info():
                self.print("[错误] 加载合约信息失败")
                return False

            # 2. 查询在途委托并撤销所有（避免"已有委托在途"导致跳过）
            ctp_orders = self.query_orders(timeout=10, only_pending=True, today_only=True) or []
            if ctp_orders:
                self.print(f"[撤销] 发现 {len(ctp_orders)} 条在途委托，先全部撤销...")
                cancel_success = 0
                for o in ctp_orders:
                    order_sys_id = str(o.get("OrderSysID", "")).strip()
                    exchange_id = str(o.get("ExchangeID", "")).strip()
                    instrument_id = str(o.get("InstrumentID", "")).strip()
                    if order_sys_id:
                        if self._cancel_order_by_sysid(order_sys_id, exchange_id, instrument_id):
                            cancel_success += 1
                    else:
                        order_ref = str(o.get("OrderRef", "")).strip()
                        if self.cancel_order(order_ref):
                            cancel_success += 1
                    time.sleep(0.3)
                self.print(f"[撤销] 已撤销 {cancel_success}/{len(ctp_orders)} 条委托")
                # 等待撤单确认
                time.sleep(2)
            else:
                self.print("[撤销] 无在途委托需要撤销")

            # 3. 查询持仓（同步前再次确认，基于最新数据）
            time.sleep(1)  # 等待 CTP 更新
            positions = self.query_positions(timeout=15)
            if positions is None:
                self.print("[错误] 持仓查询失败")
                return False

            # 4. 加载标准持仓
            if not self._load_hold_std():
                initial_path = os.path.join(PROJECT_ROOT, "data", "initial_positions.json")
                if os.path.exists(initial_path):
                    try:
                        with open(initial_path, "r", encoding="utf-8") as f:
                            self._hold_std = json.load(f)
                        self.print(f"[首次] 从 initial_positions.json 加载 {len(self._hold_std)} 条")
                    except Exception as e:
                        self.print(f"[错误] 加载 initial_positions.json 失败: {e}")
                        self._hold_std = []
                else:
                    self._hold_std = self._positions_to_hold_std(positions)

                if not self._hold_std:
                    self.print("[错误] 无有效标准持仓")
                    return False
                self._save_hold_std()

            # 5. 聚合持仓
            actual_agg = self._aggregate_actual_positions()
            target = self._parse_hold_std()

            # 6. 再次查询在途委托（撤销后的状态）
            ctp_orders = self.query_orders(timeout=10, only_pending=True, today_only=True) or []
            if ctp_orders:
                self.print(f"[委托] 撤销后剩余 CTP 在途 {len(ctp_orders)} 条")
                self._sync_ctp_orders_to_memory(ctp_orders)
            else:
                self.print("[委托] 撤销后无在途委托")

            # 7. 构建在途映射
            pending_map = self._build_pending_map(ctp_orders)

            # 8. 计算有效持仓
            # 修复：只在开仓方向扣减在途委托，不在平仓方向扣减
            # 因为未成交的平仓委托还没减少实际持仓，不应该提前扣减
            effective_actual = {}
            for key in set(actual_agg.keys()) | set(target.keys()):
                contract, direction = key
                a_vol = actual_agg.get(key, 0)
                pending_open = pending_map.get((contract.upper(), direction, True), 0)
                # 不再扣减 pending_close（未成交的平仓委托不应该提前减少有效持仓）
                pending_close = 0  # pending_map.get((contract.upper(), direction, False), 0)
                effective_actual[key] = a_vol + pending_open - pending_close
                if pending_open > 0:
                    self.print(f"[有效持仓] {contract} {'多' if direction == 2 else '空'}: 实际{a_vol} + 在途开仓{pending_open} = {effective_actual[key]}")

            # 调试：打印 pending_map 中所有开仓委托
            for pm_key, pm_vol in pending_map.items():
                if pm_vol > 0 and len(pm_key) >= 3 and pm_key[2]:  # is_open = True
                    self.print(f"[pending_map] {pm_key[0]} {'多' if pm_key[1] == 2 else '空'} 开仓 {pm_vol} 手")

            # 9. 计算缺额/超额
            missing_orders = []
            excess_orders = []

            # 检查是否有合约在1009冷却期
            current_time = time.time()
            cooling_contracts = []
            if hasattr(self, '_last_1009_reject'):
                for contract_upper, reject_time in list(self._last_1009_reject.items()):
                    if current_time - reject_time < 30:
                        cooling_contracts.append(contract_upper)

            # 计算缺额（使用 effective_actual = actual_agg + pending_open）
            # 避免在途开仓委托被错误判断为缺额
            for key, t_vol in target.items():
                effective_vol = effective_actual.get(key, 0)
                if t_vol > effective_vol:
                    contract, direction = key
                    self.print(f"[缺额计算] {contract} {'多' if direction == 2 else '空'}: 标准{t_vol} vs 有效{effective_vol}, 缺额{t_vol - effective_vol}")
                    missing_orders.append({
                        "contract": contract,
                        "direction": "buy" if direction == 2 else "sell",
                        "volume": t_vol - effective_vol,
                    })

            # 计算超额（使用 effective_actual = actual_agg + pending_open）
            # 避免在途开仓委托被错误判断为超额（平掉还没成交的持仓）
            for key, effective_vol in effective_actual.items():
                contract, direction = key
                t_vol = target.get(key, 0)
                vol_to_close = effective_vol - t_vol
                if vol_to_close > 0:
                    # 跳过1009冷却期内的合约
                    if contract.upper() in cooling_contracts:
                        self.print(f"[平] {contract} 在1009冷却期内（30秒），跳过本次平仓")
                        continue
                    excess_orders.append({
                        "contract": contract,
                        "direction": direction,
                        "volume": vol_to_close,
                    })

            # 10. 更新 hold.json
            self._update_hold_json_file()

            # 11. 输出对比摘要
            self.print(f"[对比] 标准:{len(target)} 有效:{len(effective_actual)} 缺额:{len(missing_orders)} 超额:{len(excess_orders)}")
            if missing_orders:
                self.print(f"[缺额] {[mo['contract'] for mo in missing_orders]}")
            if excess_orders:
                self.print(f"[超额] {[eo['contract'] for eo in excess_orders]}")


            # 12. 发送持仓差异通知
            if missing_orders or excess_orders:
                diff_lines = [f"🔄 持仓差异检测到（比例={self._position_ratio}），准备同步："]

                if missing_orders:
                    total_missing = sum(mo["volume"] for mo in missing_orders)
                    diff_lines.append(f"📈 缺额开仓 ({len(missing_orders)} 个合约，共 {total_missing} 手):")
                    for mo in missing_orders:
                        d = "买" if mo["direction"] == "buy" else "卖"
                        diff_lines.append(f"  {mo['contract']} {d} {mo['volume']}手")
                if excess_orders:
                    total_excess = sum(eo["volume"] for eo in excess_orders)
                    diff_lines.append(f"📉 超额平仓 ({len(excess_orders)} 个合约，共 {total_excess} 手):")
                    for eo in excess_orders:
                        d = "多" if eo["direction"] == 2 else "空"
                        diff_lines.append(f"  {eo['contract']} {d} {eo['volume']}手")

                # 13. 执行快速同步
                total_target = sum(t for t in target.values())
                total_actual = sum(a for a in actual_agg.values())

                # 判断是否有差异
                has_diff = bool(missing_orders) or bool(excess_orders)

                if diff_lines:
                    self._notify_async("🔄 持仓差异检测到，准备同步：\n" + "\n".join(diff_lines))

                if has_diff:
                    success = self._fast_sync(missing_orders, excess_orders, ctp_orders, target, actual_agg, pending_map)
                    self.print("[结论] 同步完成（委托已提交）")
                    self._is_first_run = False
                    return success
                else:
                    self.print("[结论] 持仓一致，无需操作")
                    self._notify_async(
                        f"✅ 启动持仓检测\n"
                        f"标准持仓: {len(target)} 个合约, {total_target} 手\n"
                        f"实际持仓: {len(actual_agg)} 个合约, {total_actual} 手\n"
                        f"状态: 仓位一致 ✓"
                    )
                    self._is_first_run = False
                    return True
        except Exception as e:
            import traceback
            self.print(f"[异常] _do_sync 出错: {e}")
            traceback.print_exc()
            return False

    def _fast_sync(self, missing_orders: list, excess_orders: list, ctp_orders: list, target: dict = None, actual_agg: dict = None, pending_map: dict = None) -> bool:
        """快速同步：并行查询 + 批量提交"""
        if target is None:
            target = {}
        if actual_agg is None:
            actual_agg = {}
        if pending_map is None:
            pending_map = {}

        self.print("=" * 50)
        self.print("【快速同步模式】")
        self.print("=" * 50)

        # ============================================================
        # 第一阶段：并行查询所有行情
        # ============================================================
        self.print("[快速] 第一阶段：并行查询行情...")
        all_contracts = set([mo["contract"] for mo in missing_orders] + [eo["contract"] for eo in excess_orders])
        market_data_map = {}
        md_lock = threading.Lock()

        def _query_batch(contracts):
            for contract in contracts:
                md = self.query_market_data(contract, timeout=5, max_retries=3)
                if md:
                    with md_lock:
                        market_data_map[contract] = md

        # 并行查询（每批8个）
        MAX_WORKERS = 8
        contract_list = list(all_contracts)
        threads = []
        for i in range(0, len(contract_list), MAX_WORKERS):
            batch = contract_list[i:i + MAX_WORKERS]
            t = threading.Thread(target=_query_batch, args=(batch,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        self.print(f"[快速] 行情查询完成: {len(market_data_map)}/{len(all_contracts)} 个")

        # ============================================================
        # 第二阶段：串行提交开仓委托（与 PositionManagerUI.py 一致，避免并发压垮 CTP API）
        # ============================================================
        submitted_open = [0]
        skip_open = [0]
        open_orders = []  # 记录已提交的委托

        def _submit_open_serial():
            """串行提交开仓委托（与 PositionManagerUI.py 保持一致）"""
            for mo in missing_orders:
                contract = mo["contract"]
                md = market_data_map.get(contract)
                if not md:
                    exact = self._standardize_contract(contract)
                    self.print(f"[开] {contract}({exact}) 无行情，跳过")
                    skip_open[0] += 1
                    continue

                # 检查在途委托
                mo_upper = contract.upper()
                need_new_order = True
                # 调试：打印所有 ctp_orders 中该合约的委托
                for o in ctp_orders:
                    if o.get("InstrumentID", "").upper() == mo_upper:
                        self.print(f"[开调试] 找到委托: {o.get('InstrumentID')} Dir={o.get('Direction')} Offset={o.get('CombOffsetFlag')} Status={o.get('OrderStatus')}")
                for o in ctp_orders:
                    if (o.get("InstrumentID", "").upper() == mo_upper
                        and str(o.get("Direction", "")).strip() == (tdapi.THOST_FTDC_D_Buy if mo["direction"] == "buy" else tdapi.THOST_FTDC_D_Sell).strip()
                        and str(o.get("CombOffsetFlag", "")).strip() == str(tdapi.THOST_FTDC_OF_Open).strip()
                        and str(o.get("OrderStatus", "")).strip() in ("1", "3")):
                        last_price = o.get("LimitPrice", 0)
                        if mo["direction"] == "buy":
                            current_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
                        else:
                            current_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
                        info = self._get_contract_info(contract)
                        price_tick = info.get("PriceTick", 1.0)

                        # 检查是否需要撤单重挂：价格变化超过tick
                        price_changed = last_price > 0 and current_price > 0 and abs(current_price - last_price) >= price_tick

                        if price_changed:
                            self.print(f"[开] {contract} 价格变化 {last_price}->{current_price}，撤单重挂")
                            order_sys_id = o.get("OrderSysID", "")
                            exchange_id = o.get("ExchangeID", "")
                            if order_sys_id:
                                self._cancel_order_by_sysid(order_sys_id, exchange_id, contract)
                            else:
                                self.cancel_order(o.get("OrderRef", ""))
                            time.sleep(0.5)  # 等待撤单完成
                        else:
                            # 价格没变化，保持等待
                            self.print(f"[开] {contract} 在途足够且价格未变，保持等待")
                            need_new_order = False
                            skip_open[0] += 1
                        break

                if not need_new_order:
                    time.sleep(0.2)
                    continue

                # 下单
                if mo["direction"] == "buy":
                    limit_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
                else:
                    limit_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)

                if limit_price <= 0:
                    self.print(f"[开] {contract} 无有效价格")
                    skip_open[0] += 1
                    time.sleep(0.2)
                    continue

                info = self._get_contract_info(contract)
                if not info:
                    self.print(f"[开] {contract} 获取合约信息失败，跳过")
                    skip_open[0] += 1
                    time.sleep(0.2)
                    continue

                ok = self._place_order(
                    exchange_id=info["ExchangeID"],
                    instrument_id=contract,
                    direction=mo["direction"],
                    volume=mo["volume"],
                    limit_price=limit_price,
                    offset_flag=tdapi.THOST_FTDC_OF_Open,
                )
                if ok:
                    self.print(f"[开] {contract} 提交成功 @{limit_price}")
                    submitted_open[0] += 1
                    open_orders.append({
                        "contract": contract,
                        "direction": mo["direction"],
                        "volume": mo["volume"],
                        "price": limit_price,
                    })
                else:
                    skip_open[0] += 1

                # 串行执行，每笔间隔 0.2 秒（与 PositionManagerUI.py 一致）
                time.sleep(0.2)

        if missing_orders:
            self.print(f"[快速] 第二阶段：串行提交 {len(missing_orders)} 个开仓委托...")
            _submit_open_serial()
            self.print(f"[快速] 开仓完成: 提交 {submitted_open[0]} / 跳过 {skip_open[0]}")

        # ============================================================
        # 第三阶段：串行提交平仓委托（与 PositionManagerUI.py 一致）
        # ============================================================
        submitted_close = [0]
        skip_close = [0]
        close_orders = []  # 记录已提交的平仓委托

        def _submit_close_serial():
            """串行提交平仓委托（与 PositionManagerUI.py 的 _do_close_all_batch 保持一致）"""
            for eo in excess_orders:
                contract = eo["contract"]
                pos_dir = eo["direction"]
                eo_volume = eo["volume"]  # 保存原始计划数量

                # 检查该合约+方向是否已有成功的平仓委托在处理中
                # （避免重复提交导致 1009）
                close_dir = "sell" if pos_dir == 2 else "buy"
                pending_close_ref = None
                pending_close_info = None
                with self._order_lock:
                    for ref, info in self._orders.items():
                        if info.get("instr", "").upper() == contract.upper():
                            if info.get("direction") == close_dir:
                                offset = info.get("offset_flag", tdapi.THOST_FTDC_OF_Open)
                                if offset != tdapi.THOST_FTDC_OF_Open:  # 是平仓委托
                                    if self._is_order_pending(info):
                                        pending_close_ref = ref
                                        pending_close_info = info
                                        break
                if pending_close_ref:
                    # 有平仓委托在途，等待30秒检查循环处理
                    self.print(f"[平] {contract} 已有平仓委托在途，等待30秒检查循环处理")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                md = market_data_map.get(contract)
                if not md:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                detail = self._get_position_detail(contract, pos_dir)
                if detail.get("Position", 0) <= 0:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                info = self._get_contract_info(contract)
                if not info:
                    self.print(f"[平] {contract} 获取合约信息失败")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                exchange_id = detail.get("ExchangeID", "") or info["ExchangeID"]
                actual_pos = detail.get("Position", 0)

                # ========== 关键修复：平仓前先撤销所有相反方向的委托 ==========
                # 如果有多头超额（需要平多），先撤销所有空头委托
                # 如果有空头超额（需要平空），先撤销所有多头委托
                opposite_dir = "sell" if pos_dir == 2 else "buy"

                # 检查该合约是否之前被 1009 拒绝过（冷却机制）
                current_time = time.time()
                last_rejected = getattr(self, '_last_1009_reject', {}).get(contract.upper(), 0)
                if current_time - last_rejected < 30:  # 30秒内不重复尝试同一合约
                    self.print(f"[平] {contract} 30秒内被1009拒绝过，跳过，等待下次同步")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                # ========== 跳过有在途开仓委托的合约 ==========
                # 如果该合约+方向有在途开仓委托，说明持仓正在变化中
                # 不应该在这个时间点平仓，避免"开仓未成交但持仓已平"的错误
                pending_open_vol = pending_map.get((contract.upper(), pos_dir, True), 0)
                if pending_open_vol > 0:
                    self.print(f"[平] {contract} 有在途开仓委托 {pending_open_vol} 手，跳过平仓（等待成交确认）")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                # ========== 跳过有在途开仓委托的合约 ==========

                # 调试：检查所有未成交委托
                with self._order_lock:
                    all_pending = [
                        (ref, info.get("instr"), info.get("direction"), info.get("offset_flag"))
                        for ref, info in self._orders.items()
                        if self._is_order_pending(info)
                    ]
                    self.print(f"[平调试] {contract} 需要平{'多' if pos_dir == 2 else '空'}，查找相反方向={opposite_dir}，当前未成交委托: {len(all_pending)} 个")
                    for ref, instr, direction, offset in all_pending:
                        if instr and instr.upper() == contract.upper():
                            self.print(f"  -> {ref}: instr={instr}, dir={direction}, offset={offset}")

                # 撤销所有相反方向的委托（不管是开仓还是平仓）
                opposite_orders_to_cancel = []
                with self._order_lock:
                    for ref, info in self._orders.items():
                        if not self._is_order_pending(info):
                            continue
                        if info.get("instr", "").upper() != contract.upper():
                            continue
                        if info.get("direction") == opposite_dir:
                            opposite_orders_to_cancel.append(ref)

                if opposite_orders_to_cancel:
                    self.print(f"[平] {contract} 有 {len(opposite_orders_to_cancel)} 笔相反方向委托，先全部撤销")
                    for pending_ref in opposite_orders_to_cancel:
                        self.cancel_order(pending_ref)
                        time.sleep(0.5)  # 增加等待时间，确保撤单完成
                    # 重要：撤销后重新查询持仓，确保平仓量基于最新数据
                    self.print(f"[平] {contract} 撤销完成，重新查询持仓...")
                    time.sleep(1)  # 等待 CTP 更新持仓数据
                    # 重新查询持仓（这是关键！）
                    new_positions = self.query_positions(timeout=5)
                    if new_positions:
                        self._actual_positions = new_positions
                    detail = self._get_position_detail(contract, pos_dir)
                    actual_pos = detail.get("Position", 0)
                    self.print(f"[平] {contract} 重新查询后持仓: {actual_pos} 手")
                else:
                    self.print(f"[平] {contract} 无相反方向在途委托")
                # ========== 撤销完成 ==========

                pending_close_vol = self._get_pending_close_volume(contract, pos_dir)
                # 修复：不要双重扣减！excess_orders 的 volume 已经扣除了 pending_close
                # 所以这里直接用 excess_orders 的 volume，不要再减去 pending_close_vol
                available = actual_pos  # 直接用实际持仓，不扣 pending_close_vol
                self.print(f"[平调试] {contract} excess_orders.volume={eo['volume']}, actual_pos={actual_pos}, pending_close_vol={pending_close_vol}, available={available}")
                if available <= 0:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                diff = min(eo["volume"], available)
                self.print(f"[平调试] {contract} diff初始值={diff}")
                if diff <= 0:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                if pos_dir == 2:  # 多头 → 卖出
                    close_direction = "sell"
                    limit_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
                else:  # 空头 → 买入
                    close_direction = "buy"
                    limit_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)

                if limit_price <= 0:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                # 提交平仓委托前，再次查询持仓确认（避免持仓已变化导致 1009）
                latest_positions = self.query_positions(timeout=5)
                if latest_positions:
                    self._actual_positions = latest_positions
                latest_detail = self._get_position_detail(contract, pos_dir)
                latest_pos = latest_detail.get("Position", 0)
                if latest_pos <= 0:
                    self.print(f"[平] {contract} 最新查询持仓为 0，无需平仓，跳过")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                # 如果最新持仓小于计划平仓量，以最新持仓为准
                if latest_pos < diff:
                    self.print(f"[平] {contract} 持仓变化: {diff} -> {latest_pos}")
                    diff = latest_pos

                is_shfe = exchange_id in ("SHFE", "INE")
                today = latest_detail.get("TodayPosition", 0)

                # 记录本次平仓的数量（避免后续 diff 被修改）
                close_vol_submitted = 0

                # 平今
                if is_shfe and today > 0 and diff > 0:
                    close_today = min(today, diff)
                    ok = self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=contract,
                        direction=close_direction,
                        volume=close_today,
                        limit_price=limit_price,
                        offset_flag=tdapi.THOST_FTDC_OF_CloseToday,
                    )
                    if not ok:
                        # 报单被拒绝（如1009持仓不足），跳过该合约继续下一个
                        self.print(f"[平] {contract} 平今报单被拒绝，跳过")
                        skip_close[0] += 1
                        # 记录 1009 拒绝时间，用于冷却
                        if not hasattr(self, '_last_1009_reject'):
                            self._last_1009_reject = {}
                        self._last_1009_reject[contract.upper()] = time.time()
                        time.sleep(0.2)
                        continue
                    close_vol_submitted += close_today
                    diff -= close_today
                    time.sleep(0.2)  # 与 PositionManagerUI.py 一致

                # 平昨（只有 diff > 0 时才提交）
                if diff > 0:
                    offset = tdapi.THOST_FTDC_OF_CloseYesterday if is_shfe else tdapi.THOST_FTDC_OF_Close
                    ok = self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=contract,
                        direction=close_direction,
                        volume=diff,
                        limit_price=limit_price,
                        offset_flag=offset,
                    )
                    if not ok:
                        # 报单被拒绝（如1009持仓不足），跳过该合约继续下一个
                        self.print(f"[平] {contract} 平昨报单被拒绝，跳过")
                        skip_close[0] += 1
                        # 记录 1009 拒绝时间，用于冷却
                        if not hasattr(self, '_last_1009_reject'):
                            self._last_1009_reject = {}
                        self._last_1009_reject[contract.upper()] = time.time()
                        time.sleep(0.2)
                        continue
                    close_vol_submitted += diff
                    time.sleep(0.2)  # 与 PositionManagerUI.py 一致

                # 只有实际提交了才记录
                if close_vol_submitted > 0:
                    self.print(f"[平] {contract} 提交成功 @{limit_price} ({close_vol_submitted}手)")
                    submitted_close[0] += 1
                    close_orders.append({
                        "contract": contract,
                        "direction": "sell" if pos_dir == 2 else "buy",
                        "volume": close_vol_submitted,
                        "price": limit_price,
                    })
                else:
                    self.print(f"[平] {contract} 无需平仓（diff={diff}），跳过记录")

        if excess_orders:
            self.print(f"[快速] 第三阶段：串行提交 {len(excess_orders)} 个平仓委托...")
            _submit_close_serial()
            self.print(f"[快速] 平仓完成: 提交 {submitted_close[0]} / 跳过 {skip_close[0]}")

        # 发送详细通知
        total_submit = submitted_open[0] + submitted_close[0]
        total_skip = skip_open[0] + skip_close[0]

        # 检查是否有平仓被拒绝（1009），如果有则发送警告并重置跳过计数
        # 这可以防止程序"锁住"在不断提交被拒绝的委托上
        if skip_close[0] > 0 and excess_orders:
            self.print(f"[警告] 平仓跳过 {skip_close[0]} 个（可能是 1009 拒绝），下次同步将重新计算")

        # 计算标准仓和CTP持仓手数
        total_target = sum(t for t in target.values())
        total_actual = sum(a for a in actual_agg.values())

        if total_submit > 0:
            lines = [
                f"⚡ 同步完成（标准仓 {total_target} 手 vs CTP {total_actual} 手，共提交 {total_submit} 个委托）"
            ]
            if open_orders:
                lines.append("📈 开仓委托:")
                for o in open_orders:
                    if o["volume"] <= 0:  # 跳过 volume=0 的无效记录
                        continue
                    d = "买" if o["direction"] == "buy" else "卖"
                    lines.append(f"  {o['contract']} {d} {o['volume']}手 @{o['price']}")
            if close_orders:
                lines.append("📉 平仓委托:")
                for o in close_orders:
                    if o["volume"] <= 0:  # 跳过 volume=0 的无效记录
                        continue
                    d = "卖" if o["direction"] == "sell" else "买"
                    lines.append(f"  {o['contract']} {d} {o['volume']}手 @{o['price']}")
            self._notify_async("\n".join(lines))
        else:
            # 没有成功提交的委托
            lines = [
                f"⚠️ 同步完成但无委托提交（标准仓 {total_target} 手 vs CTP {total_actual} 手）",
                f"缺额开仓: {len(missing_orders)} 个, 超额平仓: {len(excess_orders)} 个"
            ]
            # 如果有差异但没有委托，说明跳过了
            if missing_orders or excess_orders:
                lines.append(f"开仓跳过: {skip_open[0]}, 平仓跳过: {skip_close[0]}")
                lines.append("⚠️ 请检查日志查看跳过原因（可能：查不到行情/合约信息/持仓已为0）")
            self._notify_async("\n".join(lines))

        self.print(f"[快速] 完成: 开仓 {submitted_open[0]}/{len(missing_orders)} 平仓 {submitted_close[0]}/{len(excess_orders)}")
        print("=" * 50)
        return True

    # ------------------------------------------------------------------
    # 以下方法暂时保留，用于兼容旧代码
    # ------------------------------------------------------------------
    def _trade_single(self, instrument_id, exchange_id, direction, volume, price_tick, timeout=30, max_retries=1):
        """单合约交易（保留用于特殊场景）"""
        md = self.query_market_data(instrument_id, timeout=5)
        if not md:
            return False
        if direction == "buy":
            limit_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
        else:
            limit_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
        if limit_price <= 0:
            return False
        order_ref = self.place_limit_order(exchange_id, instrument_id, direction, volume, limit_price)
        if not order_ref:
            return False
        with self._order_lock:
            info = self._orders[order_ref]
        filled = info["event"].wait(timeout=timeout)
        if filled:
            return True
        self.cancel_order(order_ref)
        time.sleep(1)
        with self._order_lock:
            final_status = self._orders[order_ref]["status"]
        if final_status == self._OST_ALL_TRADED:
            return True
        if max_retries > 0:
            return self._trade_single(instrument_id, exchange_id, direction, volume, price_tick, timeout, max_retries - 1)
        return False

    def _send_position_mismatch_alert(self, actual, target):
        """持仓不一致告警"""
        lines = ["⚠️ 持仓不一致告警"]
        for k in set(target.keys()) - set(actual.keys()):
            lines.append(f"标准有但账户无: {k[0]} 方向={'多' if k[1]==2 else '空'}")
        for k in set(actual.keys()) - set(target.keys()):
            lines.append(f"账户有但标准无: {k[0]} 方向={'多' if k[1]==2 else '空'}")
        self._notify_async("\n".join(lines))

    def _send_sync_notification(self, *args, **kwargs):
        """发送同步通知（新版使用内联通知）"""
        pass

    def _build_positions(self, target, timeout=30):
        """首次建仓"""
        positions = self.query_positions(timeout=10)
        if positions is None:
            return False
        if positions:
            self._hold_std = self._positions_to_hold_std(positions)
            if self._hold_std:
                self._save_hold_std()
            return False

        success_count = 0
        for (contract, direction), vol in target.items():
            info = self._get_contract_info(contract)
            exchange_id = info["ExchangeID"]
            md = self.query_market_data(contract, timeout=3)
            if not md:
                continue
            if direction == 2:
                limit_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
            else:
                limit_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
            if limit_price <= 0:
                continue
            direction_str = "buy" if direction == 2 else "sell"
            ok = self._place_order(
                exchange_id=exchange_id,
                instrument_id=contract,
                direction=direction_str,
                volume=vol,
                limit_price=limit_price,
                offset_flag=tdapi.THOST_FTDC_OF_Open,
            )
            if ok:
                success_count += 1
            time.sleep(0.1)

        time.sleep(1)
        positions = self.query_positions(timeout=10)
        self._hold_std = self._positions_to_hold_std(positions) if positions else []
        if self._hold_std:
            self._save_hold_std()
        return success_count >= len(target) * 0.8

    def execute_orders(self, signal_path, timeout=30):
        """从 signal.json 读取委托并执行（新版不再使用）"""
        self.print("[信息] execute_orders 在新版中不再使用")
        return True