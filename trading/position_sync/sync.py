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

        # 1. 加载合约信息
        if not self._load_contract_info():
            self.print("[错误] 加载合约信息失败")
            return False

        # 2. 查询持仓
        positions = self.query_positions(timeout=15)
        if positions is None:
            self.print("[错误] 持仓查询失败")
            return False

        # 3. 加载标准持仓
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

        # 4. 聚合持仓
        actual_agg = self._aggregate_actual_positions()
        target = self._parse_hold_std()

        # 5. 查询在途委托
        ctp_orders = self.query_orders(timeout=10, only_pending=True, today_only=True) or []
        if ctp_orders:
            self.print(f"[委托] CTP 在途 {len(ctp_orders)} 条")
            self._sync_ctp_orders_to_memory(ctp_orders)
        else:
            self.print("[委托] 无在途委托")

        # 6. 构建在途映射
        pending_map = self._build_pending_map(ctp_orders)

        # 7. 计算有效持仓
        effective_actual = {}
        for key in set(actual_agg.keys()) | set(target.keys()):
            contract, direction = key
            a_vol = actual_agg.get(key, 0)
            pending_open = pending_map.get((contract.upper(), direction, True), 0)
            pending_close = pending_map.get((contract.upper(), direction, False), 0)
            effective_actual[key] = a_vol + pending_open - pending_close

        # 8. 计算缺额/超额
        missing_orders = []
        excess_orders = []
        for key, t_vol in target.items():
            eff_vol = effective_actual.get(key, 0)
            if t_vol > eff_vol:
                contract, direction = key
                missing_orders.append({
                    "contract": contract,
                    "direction": "buy" if direction == 2 else "sell",
                    "volume": t_vol - eff_vol,
                })

        for key, a_vol in actual_agg.items():
            t_vol = target.get(key, 0)
            eff_vol = effective_actual.get(key, a_vol)
            if eff_vol > t_vol:
                contract, direction = key
                excess_orders.append({
                    "contract": contract,
                    "direction": direction,
                    "volume": eff_vol - t_vol,
                })

        # 9. 更新 hold.json
        self._update_hold_json_file()

        # 10. 输出对比摘要
        self.print(f"[对比] 标准:{len(target)} 实际:{len(actual_agg)} 缺额:{len(missing_orders)} 超额:{len(excess_orders)}")
        if missing_orders:
            self.print(f"[缺额] {[mo['contract'] for mo in missing_orders]}")
        if excess_orders:
            self.print(f"[超额] {[eo['contract'] for eo in excess_orders]}")

        # 11. 发送持仓差异通知
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
            self._notify_async("\n".join(diff_lines))

        # 12. 执行快速同步
        if missing_orders or excess_orders:
            success = self._fast_sync(missing_orders, excess_orders, ctp_orders)
            self.print("[结论] 同步完成（委托已提交）")
            self._is_first_run = False
            return success
        else:
            self.print("[结论] 持仓一致，无需操作")
            self._is_first_run = False
            return True

    def _fast_sync(self, missing_orders: list, excess_orders: list, ctp_orders: list) -> bool:
        """快速同步：并行查询 + 批量提交，2-3分钟内完成"""
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
                    self.print(f"[开] {contract} 无行情，跳过")
                    skip_open[0] += 1
                    continue

                # 检查在途委托
                mo_upper = contract.upper()
                need_new_order = True
                for o in ctp_orders:
                    if (o.get("InstrumentID", "").upper() == mo_upper
                        and str(o.get("Direction", "")).strip() == (tdapi.THOST_FTDC_D_Buy if mo["direction"] == "buy" else tdapi.THOST_FTDC_D_Sell).strip()
                        and str(o.get("CombOffsetFlag", "")).strip() == str(tdapi.THOST_FTDC_OF_Open).strip()
                        and str(o.get("OrderStatus", "")).strip() in ("1", "3")):
                        pending_vol = o.get("VolumeTotalOriginal", 0) - o.get("VolumeTraded", 0)
                        if pending_vol >= mo["volume"]:
                            self.print(f"[开] {contract} 在途足够，保持等待")
                            need_new_order = False
                            skip_open[0] += 1
                            break
                        else:
                            last_price = o.get("LimitPrice", 0)
                            if mo["direction"] == "buy":
                                current_price = md.get("AskPrice1", 0) or md.get("LastPrice", 0)
                            else:
                                current_price = md.get("BidPrice1", 0) or md.get("LastPrice", 0)
                            info = self._get_contract_info(contract)
                            price_tick = info.get("PriceTick", 1.0)
                            if last_price > 0 and current_price > 0 and abs(current_price - last_price) < price_tick:
                                self.print(f"[开] {contract} 手数不足，撤单重挂")
                            else:
                                self.print(f"[开] {contract} 价格变化，撤单重挂")
                            order_sys_id = o.get("OrderSysID", "")
                            exchange_id = o.get("ExchangeID", "")
                            if order_sys_id:
                                self._cancel_order_by_sysid(order_sys_id, exchange_id, contract)
                            else:
                                self.cancel_order(o.get("OrderRef", ""))
                            time.sleep(0.3)
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

                pending_close_vol = self._get_pending_close_volume(contract, pos_dir)
                available = actual_pos - pending_close_vol
                if available <= 0:
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                diff = min(eo["volume"], available)
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

                is_shfe = exchange_id in ("SHFE", "INE")
                today = detail.get("TodayPosition", 0)

                # 平今
                if is_shfe and today > 0 and diff > 0:
                    close_today = min(today, diff)
                    self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=contract,
                        direction=close_direction,
                        volume=close_today,
                        limit_price=limit_price,
                        offset_flag=tdapi.THOST_FTDC_OF_CloseToday,
                    )
                    diff -= close_today
                    time.sleep(0.2)  # 与 PositionManagerUI.py 一致

                # 平昨
                if diff > 0:
                    offset = tdapi.THOST_FTDC_OF_CloseYesterday if is_shfe else tdapi.THOST_FTDC_OF_Close
                    self._place_order(
                        exchange_id=exchange_id,
                        instrument_id=contract,
                        direction=close_direction,
                        volume=diff,
                        limit_price=limit_price,
                        offset_flag=offset,
                    )
                    time.sleep(0.2)  # 与 PositionManagerUI.py 一致

                self.print(f"[平] {contract} 提交成功 @{limit_price}")
                submitted_close[0] += 1
                close_orders.append({
                    "contract": contract,
                    "direction": "sell" if pos_dir == 2 else "buy",
                    "volume": diff,
                    "price": limit_price,
                })

        if excess_orders:
            self.print(f"[快速] 第三阶段：串行提交 {len(excess_orders)} 个平仓委托...")
            _submit_close_serial()
            self.print(f"[快速] 平仓完成: 提交 {submitted_close[0]} / 跳过 {skip_close[0]}")

        # 发送详细通知
        total_submit = submitted_open[0] + submitted_close[0]
        if total_submit > 0:
            lines = [f"⚡ 快速同步完成（共 {total_submit} 个委托）"]
            if open_orders:
                lines.append("📈 开仓委托:")
                for o in open_orders:
                    d = "买" if o["direction"] == "buy" else "卖"
                    lines.append(f"  {o['contract']} {d} {o['volume']}手 @{o['price']}")
            if close_orders:
                lines.append("📉 平仓委托:")
                for o in close_orders:
                    d = "卖" if o["direction"] == "sell" else "买"
                    lines.append(f"  {o['contract']} {d} {o['volume']}手 @{o['price']}")
            self._notify_async("\n".join(lines))
        else:
            self._notify_async("✅ 快速同步完成，无新委托提交")

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