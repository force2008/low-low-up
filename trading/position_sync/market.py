# -*- coding: utf-8 -*-
"""
行情持仓查询模块

包含：
- query_market_data, OnRspQryDepthMarketData
- query_positions, OnRspQryInvestorPosition
- query_orders (已经在 base.py 中有 OnRspQryOrder)
- _update_hold_json_file, _update_hold_json_from_ctp
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


class PositionSyncManagerMarket:
    """持仓同步管理器 - 行情持仓查询部分"""

    def query_market_data(self, instrument_id: str, timeout: int = 5, max_retries: int = 2) -> Optional[dict]:
        """获取合约行情快照，统一通过行情 API（MdApi）订阅获取。

        如果 MdApi 提供者未启动，才回退到交易 API 查询（保持兼容性）。
        """
        exact_id = self._standardize_contract(instrument_id)

        # 优先使用行情 API 订阅提供者（统一 simu/online 机制）
        md_provider = getattr(self, "_md_provider", None)
        if md_provider:
            md = md_provider.get_quote(exact_id, timeout=timeout)
            if md:
                return md
            self.print(f"[警告] {exact_id} 行情API订阅获取失败")
            return None

        # 兼容无行情前置的环境：回退到交易 API 查询
        self.print(f"[警告] 无行情API提供者，回退到交易API查询 {exact_id}")
        last_error = None
        for attempt in range(max_retries + 1):
            with self._md_lock:
                self._md_request_id += 1
                req_id = self._md_request_id
                pending = {"event": threading.Event(), "data": None}
                self._md_pending[req_id] = pending

            req = tdapi.CThostFtdcQryDepthMarketDataField()
            req.InstrumentID = exact_id
            self._api.ReqQryDepthMarketData(req, req_id)
            ok = pending["event"].wait(timeout=timeout)

            with self._md_lock:
                data = pending.get("data")
                self._md_pending.pop(req_id, None)

            if ok and data:
                return data

            last_error = f"行情查询超时 (attempt {attempt + 1}/{max_retries + 1})"
            if attempt < max_retries:
                time.sleep(0.5)

        self.print(f"[警告] {exact_id} 交易API行情查询连续失败: {last_error}")
        return None

    def query_market_data_batch(self, instrument_ids: List[str], timeout: int = 5) -> Dict[str, dict]:
        """批量查询多个合约的行情

        优先通过行情 API 批量订阅，再统一读取缓存，减少多次单合约订阅的延迟。
        """
        result = {}
        if not instrument_ids:
            return result

        md_provider = getattr(self, "_md_provider", None)
        if md_provider:
            md_provider.subscribe_many(instrument_ids)
            for inst in instrument_ids:
                md = md_provider.get_quote(inst, timeout=timeout, auto_subscribe=False)
                if md:
                    result[inst] = md
            return result

        # 兼容无行情前置的环境
        for inst in instrument_ids:
            md = self.query_market_data(inst, timeout=timeout)
            if md:
                result[inst] = md
        return result

    def query_positions(self, timeout: int = 10, retries: int = 2, blocking: bool = True) -> Optional[List[dict]]:
        """查询持仓，超时返回 None（调用者需区分"超时"和"确实无持仓"）
        blocking=False 时若已有查询在进行则返回 None，避免竞态覆盖数据
        retries: 最大重试次数（默认2次）
        """
        self.print("[持仓查询] 开始查询...")
        if not blocking and self._pos_query_lock.locked():
            self.print("[持仓查询] 已有查询在进行中，跳过本次")
            return None

        with self._pos_query_lock:
            for attempt in range(retries + 1):
                self._pos_query_event.clear()
                self._actual_positions = []
                req = tdapi.CThostFtdcQryInvestorPositionField()
                req.BrokerID = self._broker_id
                req.InvestorID = self._user_id
                self.print(f"[持仓查询] 发送请求，attempt={attempt + 1}/{retries + 1}")
                self._api.ReqQryInvestorPosition(req, 0)
                ok = self._pos_query_event.wait(timeout=timeout)
                if ok:
                    self.print(f"[持仓查询] 成功，返回 {len(self._actual_positions)} 条记录")
                    self._reset_query_health()
                    return list(self._actual_positions)
                if attempt < retries:
                    self.print(f"[警告] 持仓查询超时，第 {attempt + 1} 次重试...")
                    time.sleep(1)
            self.print(f"[错误] 持仓查询连续 {retries + 1} 次超时，返回 None")
            self._on_query_timeout(source="持仓查询")
            return None

    def query_orders(
        self, timeout: int = 10, only_pending: bool = False, today_only: bool = True
    ) -> Optional[List[dict]]:
        """查询当日委托。支持过滤：only_pending 只保留未成交/部分成交；today_only 只保留当天
        超时返回 None，以便调用方区分"查询失败"与"确实无委托"。"""
        event = threading.Event()
        self._orders_query_event = event
        self._orders_raw_query = []
        req = tdapi.CThostFtdcQryOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        self._api.ReqQryOrder(req, 0)
        ok = event.wait(timeout=timeout)
        if not ok:
            self.print("[警告] 委托查询超时")
            self._on_query_timeout(source="委托查询")
            return None
        self._reset_query_health()
        result = list(self._orders_raw_query)
        if today_only:
            today_str = time.strftime("%Y%m%d")
            result = [
                o for o in result
                if not o.get("InsertDate") or o.get("InsertDate", "") == today_str
            ]
        if only_pending:
            result = [o for o in result if o.get("OrderStatus", "") in ("1", "3")]
        return result

    def _update_hold_json_file(self):
        """
        使用已查询的持仓数据更新 hold.json 文件
        """
        try:
            if not self._actual_positions:
                self.print("[更新hold] 无持仓数据，跳过")
                return

            # 聚合持仓
            aggregated = {}
            for pos in self._actual_positions:
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
                    "来源": "CTP持仓查询"
                })

            # 写入 hold.json
            hold_json_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '..', 'order-check', 'hold.json'
            )
            os.makedirs(os.path.dirname(hold_json_path), exist_ok=True)
            with open(hold_json_path, 'w', encoding='utf-8') as f:
                json.dump(hold_rows, f, ensure_ascii=False, indent=2)
            self.print(f"[更新hold] hold.json 已更新（共 {len(hold_rows)} 条）")

        except Exception as e:
            self.print(f"[更新hold] 异常: {e}")

    def _update_hold_json_from_ctp(self):
        """
        从 CTP 查询持仓并更新 hold.json
        使用防抖机制避免频繁查询
        """
        # 防抖：同一进程内限制更新频率
        current_time = time.time()
        last_update = getattr(self, '_last_hold_json_update', 0)
        if current_time - last_update < 3.0:  # 3秒内不重复更新
            return

        self._last_hold_json_update = current_time

        try:
            # 查询当前持仓
            positions = self.query_positions(timeout=10)
            if positions is None:
                self.print("[更新hold] 持仓查询失败")
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
            self.print(f"[更新hold] hold.json 已从 CTP 成交回报更新（共 {len(hold_rows)} 条）")

        except Exception as e:
            self.print(f"[更新hold] 异常: {e}")