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

    # ==============================
    # 后台异步行情回填订阅（MdApi）
    # ==============================
    # 前 4 层兜（首阶段map / prefer_cached / 原始缓存 / 永久已知价）全空时，
    # 不阻塞本轮同步主流程，直接起 daemon 线程后台订阅 MdApi 长等 60s，
    # 只要收到首 tick 就回填 md_provider._quotes 和 _last_known_prices，
    # 下一轮 sync/巡检直接命中 prefer_cached / 永久已知价。
    # 用集合 + 双检去重，避免每轮每个合约都起新线程（15s 巡检×38 合约会炸线程数）。
    _async_fetch_lock = threading.RLock()
    _async_fetch_running = None  # Dict[str, float]，key=contract_upper，value=启动时间戳

    def submit_async_market_fetch(self, instrument_id: str) -> bool:
        """给指定合约提交一个「后台异步 MdApi 行情回填订阅任务」。
        去重：同一合约如果已经在跑（<15 分钟）就不重复启动，返回 True=提交或已在跑。
        """
        try:
            if not instrument_id:
                return False
            inst_key = str(self._standardize_contract(instrument_id) or instrument_id).strip().upper()
            if not inst_key:
                return False
            # 懒初始化
            if PositionSyncManagerMarket._async_fetch_running is None:
                with PositionSyncManagerMarket._async_fetch_lock:
                    if PositionSyncManagerMarket._async_fetch_running is None:
                        PositionSyncManagerMarket._async_fetch_running = {}
            now = time.time()
            # 双检去重：15 分钟内已经起过就不再起
            last_ts = 0.0
            with PositionSyncManagerMarket._async_fetch_lock:
                last_ts = float(PositionSyncManagerMarket._async_fetch_running.get(inst_key, 0.0) or 0.0)
            if 0 < now - last_ts < 900.0:
                return True
            with PositionSyncManagerMarket._async_fetch_lock:
                _cur = float(PositionSyncManagerMarket._async_fetch_running.get(inst_key, 0.0) or 0.0)
                if 0 < now - _cur < 900.0:
                    return True
                PositionSyncManagerMarket._async_fetch_running[inst_key] = now
            try:
                t = threading.Thread(
                    target=self._async_market_fetch_worker,
                    args=(inst_key, instrument_id,),
                    name=f"async-md-{inst_key}",
                    daemon=True,
                )
                t.start()
                return True
            except Exception as e:
                self.print(f"[警告] 启动后台异步行情回填线程失败: {inst_key}, err={e}")
                with PositionSyncManagerMarket._async_fetch_lock:
                    PositionSyncManagerMarket._async_fetch_running.pop(inst_key, None)
                return False
        except Exception:
            return False

    def _async_market_fetch_worker(self, inst_key: str, original_contract: str):
        """后台线程：MdApi 订阅单合约，长等 60 秒拿首 tick，回填两个缓存；任何异常静默吞掉，不影响主流程。"""
        try:
            self.print(f"[信息][后台行情回填] 启动后台 MdApi 订阅回填: {inst_key}")
            md_provider = getattr(self, "_md_provider", None)
            md = None
            if md_provider is not None:
                try:
                    # 第 1 轮：subscribe_many + 60s 长等（prefer_cached=False 强制等新 tick，不用历史缓存，
                    # 因为历史缓存如果有值前面 prefer_cached=True 那层早命中了，不会走到这里）
                    md_provider.subscribe_many([original_contract, inst_key])
                    md = md_provider.get_quote(inst_key, timeout=60.0, prefer_cached=False, auto_subscribe=True)
                    if not md:
                        md = md_provider.get_quote(original_contract, timeout=1.0, prefer_cached=False, auto_subscribe=True)
                    if not md:
                        try:
                            _kk = str(inst_key).strip().upper()
                            with md_provider._quotes_lock:
                                _dd = md_provider._quotes.get(_kk)
                            if _dd:
                                md = dict(_dd)
                        except Exception:
                            md = None
                except Exception as e:
                    self.print(f"[警告][后台行情回填] {inst_key} get_quote 异常: {e}")
                    md = None
            else:
                # 老环境没起 md_provider 时，fallback 到 self.query_market_data(TD)
                try:
                    md = self.query_market_data(original_contract, timeout=60, max_retries=1, prefer_cached=False)
                except Exception:
                    md = None
            if not md:
                self.print(f"[警告][后台行情回填] {inst_key} 60 秒仍未收到首 tick，本次回填放弃（下次同步会重新尝试，若合约正确请确认行情前置/订阅权限）")
                return
            # 回填 1：md_provider._quotes 原始缓存（下一轮 sync 的第 2/3 层兜直接命中）
            _final_key = inst_key
            try:
                _final_key = str(md.get("InstrumentID") or original_contract or inst_key).strip().upper() or inst_key
            except Exception:
                _final_key = inst_key
            if md_provider is not None:
                try:
                    with md_provider._quotes_lock:
                        md_provider._quotes[_final_key] = dict(md)
                except Exception:
                    pass
            # 回填 2：_last_known_prices 永久已知价（第 4 层兜命中 + shutdown 刷盘跨时段）
            try:
                def _f(x):
                    try:
                        fv = float(x); return fv if (fv > 0 and __import__("math").isfinite(fv) and fv < 1e9) else 0.0
                    except Exception:
                        return 0.0
                _lp = _f(md.get("LastPrice"))
                _bp = _f(md.get("BidPrice1"))
                _ap = _f(md.get("AskPrice1"))
                if hasattr(self, "_update_last_known_prices"):
                    _ok = self._update_last_known_prices(
                        _final_key,
                        last_price=_lp,
                        bid_price1=_bp,
                        ask_price1=_ap,
                        ts=time.time(),
                    )
                    if _ok:
                        self.print(f"[信息][后台行情回填] {_final_key} 成功回填：Last={_lp or 'N/A'} Bid={_bp or 'N/A'} Ask={_ap or 'N/A'}，下一轮同步直接命中缓存")
            except Exception as e:
                self.print(f"[警告][后台行情回填] {_final_key} 回填永久已知价失败: {e}")
        except Exception as e:
            self.print(f"[警告][后台行情回填] {inst_key} 线程异常退出: {e}")
        finally:
            # 跑过就清一下「正在跑」标记，但保留「15 分钟内不重跑」逻辑（用 now - ts < 900 自然过期，避免 pop 导致马上又重起）
            pass

    def query_market_data(self, instrument_id: str, timeout: int = 5, max_retries: int = 2, prefer_cached: bool = True) -> Optional[dict]:
        """获取合约行情快照，统一通过行情 API（MdApi）订阅获取。

        如果 MdApi 提供者未启动，才回退到交易 API 查询（保持兼容性）。

        Args:
            instrument_id: 合约代码
            timeout: 等待 tick 秒数（仅在缓存空时生效）
            max_retries: 交易 API 回退时的重试次数
            prefer_cached: True=如果已有历史缓存直接返回，不等最新 tick（次主力稀疏场景默认 True）
        """
        exact_id = self._standardize_contract(instrument_id)

        # 优先使用行情 API 订阅提供者（统一 simu/online 机制）
        md_provider = getattr(self, "_md_provider", None)
        if md_provider:
            md = md_provider.get_quote(exact_id, timeout=timeout, prefer_cached=prefer_cached)
            if md:
                return md
            # 融航/多数柜台只开放 MdApi 订阅，不开放 TD ReqQryDepthMarketData，
            # 次主力合约（如 SM701）tick 稀疏 3s 内没首 tick 是常态。
            # 这里不直接用 TD API 回退（无回调入口会永久超时挂住 _md_pending），
            # 而是返回 None 让调用方（sync.py 平仓分支）再用 prefer_cached=True 做二次兜底或使用缓存历史值。
            self.print(f"[警告] {exact_id} 行情API订阅获取失败（次主力tick稀疏，prefer_cached={prefer_cached}仍无缓存）")
            return None

        # 兼容无行情前置的环境：回退到交易 API 查询（仅在 md_provider 不存在时走此路径）
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
        _env = str(getattr(self, '_env_name', '') or '').strip() or None
        _uid = str(getattr(self, '_user_id', '') or '').strip() or None
        _bid = str(getattr(self, '_broker_id', '') or '').strip() or None
        _prefix_parts = ["[持仓查询]"]
        if _env:
            _prefix_parts.append(f"[{_env}]")
        if _uid:
            _prefix_parts.append(f"[{_uid}]")
        _prefix = "".join(_prefix_parts)
        _acc_tag = (
            f"(BrokerID={_bid}, InvestorID={_uid})" if (_bid or _uid) else ""
        )
        self.print(f"{_prefix} 开始查询... {_acc_tag}".rstrip())
        if not blocking and self._pos_query_lock.locked():
            self.print(f"{_prefix} 已有查询在进行中，跳过本次 {_acc_tag}".rstrip())
            return None

        with self._pos_query_lock:
            for attempt in range(retries + 1):
                self._pos_query_event.clear()
                self._actual_positions = []
                req = tdapi.CThostFtdcQryInvestorPositionField()
                req.BrokerID = self._broker_id
                req.InvestorID = self._user_id
                self.print(f"{_prefix} 发送请求，attempt={attempt + 1}/{retries + 1} {_acc_tag}".rstrip())
                self._api.ReqQryInvestorPosition(req, 0)
                ok = self._pos_query_event.wait(timeout=timeout)
                if ok:
                    self.print(f"{_prefix} 成功，返回 {len(self._actual_positions)} 条记录 {_acc_tag}".rstrip())
                    self._reset_query_health()
                    return list(self._actual_positions)
                if attempt < retries:
                    self.print(f"[警告] {_prefix} 持仓查询超时，第 {attempt + 1} 次重试... {_acc_tag}".rstrip())
                    time.sleep(1)
            self.print(f"[错误] {_prefix} 连续 {retries + 1} 次超时，返回 None {_acc_tag}".rstrip())
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