# -*- coding: utf-8 -*-
"""
同步逻辑模块 - 快速同步版本

核心设计：2-3分钟内完成所有委托提交
- 第一阶段：并行查询所有行情（无等待）
- 第二阶段：串行提交开仓委托（0.2秒间隔，与 PositionManagerUI.py 一致）
- 第三阶段：串行提交平仓委托（0.2秒间隔，与 PositionManagerUI.py 一致）
"""

import json
import math
import os
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__('sys').path:
    __import__('sys').path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import tdapi
from config.trading_time_config import get_contracts_trading_status


# CTP / TTS 仿真接口在"合约未订阅 / 无行情"时，会把 BidPrice1/AskPrice1/LastPrice 等字段
# 填充为 IEEE754 双精度最大值 = sys.float_info.max = 1.7976931348623157e+308。
# 若不拦截，会直接以"双精度最大值"下限价单 → 服务器报单被拒绝，
# 因此设定一个明显合理的上界（目前商品期货单价最高 ~ 1e6 量级足够），
# 超过该上界一律视为脏数据，等同 <= 0 处理。
_INVALID_PRICE_MAX_CAP = 1e9


def _is_valid_positive_price(v) -> bool:
    """判断一个行情价格字段是否为有效正数。

    同时拦截：None / 非数值 / NaN / ±Inf / 负数 / 零 / sys.float_info.max 等 CTP 脏数据。
    """
    if v is None:
        return False
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(fv):
        return False
    if fv <= 0:
        return False
    if fv >= _INVALID_PRICE_MAX_CAP:
        return False
    return True


def _safe_price(v, fallback=0.0) -> float:
    return float(v) if _is_valid_positive_price(v) else float(fallback)


def _clamp_price_within_limits(price: float, md: Optional[dict]) -> float:
    """若 md 中带涨跌停价，将价格钳位在 (停板价±2tick) 之内，避免越界报单被拒。"""
    if not _is_valid_positive_price(price):
        return price
    if not md:
        return price
    upper = _safe_price(md.get("UpperLimitPrice", 0))
    lower = _safe_price(md.get("LowerLimitPrice", 0))
    if upper > 0 and price > upper:
        return upper
    if lower > 0 and price < lower:
        return lower
    return price


def _ctp_order_elapsed_seconds(o: dict, now: Optional[float] = None) -> float:
    """用订单 InsertDate+InsertTime 估算已挂单秒数；缺失时按 0 处理（保守允许撤单）。"""
    if now is None:
        now = time.time()
    d = str(o.get("InsertDate", "") or "").strip()
    t = str(o.get("InsertTime", "") or "").strip()
    if len(d) < 8:
        return 0.0
    # InsertTime 常见格式：HH:MM:SS / HHMMSS / HH:MM:SS.mmm
    tt = t.replace(":", "")
    if len(tt) >= 6:
        tt = tt[:6]
    else:
        return 0.0
    try:
        struct = time.strptime(f"{d} {tt}", "%Y%m%d %H%M%S")
    except ValueError:
        return 0.0
    return max(0.0, now - time.mktime(struct))


def _passive_next_tick_price(
    direction: str,
    current_order_price: float,
    md: dict,
    price_tick: float,
) -> Tuple[float, str]:
    """passive 模式下逐档咬盘口：向对手价方向"前进一步"重挂，但不超过对手价。
    - buy 方向：当前挂 bid(买一) → 若 ask <= bid+tick 则直接吃 ask；否则挂 bid+tick。
    - sell 方向：当前挂 ask(卖一) → 若 bid >= ask-tick 则直接吃 bid；否则挂 ask-tick。
    返回 (new_price, 说明tag)
    """
    bid = _safe_price(md.get("BidPrice1", 0))
    ask = _safe_price(md.get("AskPrice1", 0))
    last = _safe_price(md.get("LastPrice", 0))
    tick = float(price_tick) if price_tick > 0 else 1.0
    if direction == "buy":
        anchor = bid or current_order_price or last or 0.0
        if not _is_valid_positive_price(anchor):
            return float(current_order_price), "invalid-anchor(keep)"
        proposed = anchor + tick
        # 若 proposed 已触及卖一(ask)或以上：直接用 ask，一次吃到位
        if ask and proposed >= ask:
            return ask, "passive-step-reach-ask"
        return proposed, "passive-step-bid+1tick"
    else:  # sell
        anchor = ask or current_order_price or last or 0.0
        if not _is_valid_positive_price(anchor):
            return float(current_order_price), "invalid-anchor(keep)"
        proposed = anchor - tick
        if bid and proposed <= bid:
            return bid, "passive-step-reach-bid"
        return proposed, "passive-step-ask-1tick"


def _should_keep_pending_order(
    self,
    o: dict,
    SYNC_COOLDOWN: int,
) -> Tuple[bool, str]:
    """判断一条 CTP 在途委托在 sync 入口阶段是否跳过撤销。

    返回 (keep=True 不撤, 原因字符串)。
    仅在"passive_mode 开仓挂单 且 挂单时长 < passive_wait 且 非 exclude/清仓/非被动"情况下保留；其他一律可撤。
    """
    use_passive = bool(getattr(self, '_passive_mode', False))
    wait_s = int(getattr(self, '_passive_wait_seconds', 300) or 300)
    offset_flag_raw = str(o.get("CombOffsetFlag", "") or "").strip()
    is_open = offset_flag_raw in (
        str(tdapi.THOST_FTDC_OF_Open),
        chr(tdapi.THOST_FTDC_OF_Open) if isinstance(tdapi.THOST_FTDC_OF_Open, int) else "",
        "0",
    )
    # 只有 passive 模式的开仓挂单才享受"挂单保留"；平仓/清仓/exclude 场景按旧逻辑优先撤
    if not (use_passive and is_open):
        return False, "not-passive-open"

    elapsed = _ctp_order_elapsed_seconds(o, time.time())
    if elapsed < wait_s:
        return True, f"passive-open keep ({elapsed:.0f}s < {wait_s}s, cooldown={SYNC_COOLDOWN}s)"
    return False, f"passive-open expired ({elapsed:.0f}s >= {wait_s}s)"


def _calc_limit_price(
    direction: str,
    md: dict,
    mode: str,
    contract: str = "",
    price_tick: float = 1.0,
    logger=None,
) -> Tuple[float, str]:
    """统一定价入口，返回 (limit_price, pricing_note)。

    Args:
        direction: "buy" 或 "sell"
        md:        行情 dict（含 LastPrice/BidPrice1/AskPrice1/UpperLimitPrice/LowerLimitPrice）
        mode:      "aggressive" 主动吃单（优先对手价）｜ "passive" 排队挂单（优先本方价）
        contract:  仅用于日志
        price_tick:合约最小变动价位，用于日志
        logger:    回调 logger（调用 .print(msg) 即可，传 PositionSyncManager self）
    """
    last = _safe_price(md.get("LastPrice", 0))
    bid = _safe_price(md.get("BidPrice1", 0))
    ask = _safe_price(md.get("AskPrice1", 0))

    note_parts: List[str] = [f"L={last:.4f}" if last else "L=None",
                             f"B={bid:.4f}" if bid else "B=None",
                             f"A={ask:.4f}" if ask else "A=None"]

    def _pick_buy():
        # buy 方向：对手价=ask(卖一)，本方价=bid(买一)
        if mode == "aggressive":
            pri = ask or bid or last
            tag = "A" if ask else ("B" if bid else "L")
        else:
            pri = bid or ask or last
            tag = "B" if bid else ("A" if ask else "L")
        tag_mode = "agg" if mode == "aggressive" else "pas"
        return pri, f"[{contract}] buy {tag_mode} -> {tag}"

    def _pick_sell():
        # sell 方向：对手价=bid(买一)，本方价=ask(卖一)
        if mode == "aggressive":
            pri = bid or ask or last
            tag = "B" if bid else ("A" if ask else "L")
        else:
            pri = ask or bid or last
            tag = "A" if ask else ("B" if bid else "L")
        tag_mode = "agg" if mode == "aggressive" else "pas"
        return pri, f"[{contract}] sell {tag_mode} -> {tag}"

    if direction == "buy":
        price, note = _pick_buy()
    else:  # sell
        price, note = _pick_sell()

    if not _is_valid_positive_price(price):
        price = 0.0
        note = f"{note} **PRICE_INVALID** ({','.join(note_parts)})"
    else:
        price = _clamp_price_within_limits(price, md)
        note = f"{note} => {price:.4f} (tick={price_tick})"

    if logger is not None:
        try:
            logger.print(f"[定价] {note}")
        except Exception:
            pass
    return float(price), note


class PositionSyncManagerSync:
    """持仓同步管理器 - 同步逻辑部分"""

    # ------------------------------------------------------------------
    # 风控公共闸口（B1+B2 + 数据一致性双端校验）
    #   凡是"持仓查询结果已经拿到、actual_agg 也聚合好了、target 也解析好了"
    #   的任何入口，都必须先调用本方法，任何一个条件命中都直接 return False abort。
    # ------------------------------------------------------------------
    def _guard_post_query_positions(
        self,
        positions: list,
        actual_agg: dict,
        target: dict,
        hold_mtime: float,
        sync_mode_tag: str,
    ) -> bool:
        """B1+B2 硬闸 + 双端一致性校验。返回 True=安全可继续、False=命中拦截需 abort。
        B1：positions 查询返回 0 条 但 target 有合约 → 脏空拦截
        B2：actual 总手=0 但 target 总手>0（非清仓模式）→ 脏空/脏0拦截
        B3：positions 返回条数 与 self._actual_positions 当前缓存长度 差>1 且两边非 0 →
            说明查询返回对象和聚合用的不是同一份数据（重试乱序/竞态污染），同样判脏
        """
        n_pos_rows = len(positions) if positions is not None else 0
        n_cache_rows = len(getattr(self, '_actual_positions', []) or [])
        n_target_contracts = len(target or {})
        total_target_hands = sum((target or {}).values())
        total_actual_hands = sum((actual_agg or {}).values())
        _raw_ratio = float(getattr(self, '_position_ratio', 1.0))
        is_liquidate_mode = abs(_raw_ratio) < 1e-9

        b1_hit = (n_pos_rows == 0 and n_target_contracts > 0)
        b2_hit = (total_actual_hands == 0 and total_target_hands > 0 and not is_liquidate_mode)
        _big_gap = abs(n_pos_rows - n_cache_rows) > 1 and n_pos_rows > 0 and n_cache_rows > 0
        b3_hit = _big_gap and n_target_contracts > 0

        if not (b1_hit or b2_hit or b3_hit):
            return True

        hold_tag = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hold_mtime)) if hold_mtime > 0 else "N/A"
        detail_lines = []
        if b1_hit:
            detail_lines.append("  B1(返回0条且标准仓非空)=✅命中")
        if b2_hit:
            detail_lines.append("  B2(实际总手=0且标准总手>0)=✅命中")
        if b3_hit:
            detail_lines.append(
                f"  B3(查询返回条数={n_pos_rows} 与 缓存条数={n_cache_rows} 严重不一致)=✅命中"
            )
        msg_lines = [
            "🛡️ 持仓查询脏空拦截（硬闸已触发，本轮对齐已终止）：",
            f"  模式：{sync_mode_tag}",
            *detail_lines,
            f"  持仓查询返回条数={n_pos_rows}，缓存聚合源条数={n_cache_rows}",
            f"  标准仓合约数={n_target_contracts}，标准总仓={total_target_hands}手",
            f"  实际聚合合约数={len(actual_agg or {})}，实际总仓={total_actual_hands}手",
            f"  hold-std写入时刻={hold_tag}",
            f"  -> 拒绝本轮任何开/平仓，避免用脏数据对齐导致翻倍超仓或平过头。",
        ]
        msg = "\n".join(msg_lines)
        self.print(msg)
        try:
            self._notify_async(msg)
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # F 总闸：最终提交前的绝对数学不可能成立拦截
    #   就算 B1~B3 + C1/C2 + D1/D2 + Fix3 全部漏过（极不可能），
    #   只要「账户已经有实际持仓 + 本次缺额覆盖目标合约≥50% + 缺额手数≈目标总手」
    #   就直接 abort：数学上已持仓的账户不可能突然又缺全仓，只能是 actual 被脏空/脏低。
    #   独立于所有上游计算逻辑，纯靠外部事实判断。
    # ------------------------------------------------------------------
    def _guard_final_total_missing(
        self,
        missing_orders: list,
        excess_orders: list,
        target: dict,
        actual_agg: dict,
        pending_map: dict,
    ) -> bool:
        """F 总闸：True=安全、False=命中 abort。"""
        target = target or {}
        actual_agg = actual_agg or {}
        pending_map = pending_map or {}
        missing_orders = missing_orders or []
        excess_orders = excess_orders or []

        n_target_contracts = len(target)
        total_target_hands = sum(target.values())
        total_actual_hands = sum(actual_agg.values())

        total_missing_vol = sum(int(mo.get("volume", 0) or 0) for mo in missing_orders)
        n_missing_contracts = len(missing_orders)

        hit_f = False
        reason = ""
        # 语义：跟单账户已经有实际仓位了（不是冷启动的真空状态）
        if total_actual_hands > 0 and n_target_contracts > 0 and total_missing_vol > 0:
            # 缺额合约数 / 目标合约数 占比（缺一半以上合约）
            coverage_ratio = n_missing_contracts / max(1, n_target_contracts)
            # 缺额手数 / 目标总手数（缺手数已达目标 ~= 全仓）
            missing_frac = total_missing_vol / max(1, total_target_hands)
            # 已经有持仓 + 缺额覆盖合约≥50% + 缺额手数已达目标80%以上
            # => 数学上不可能：真的缺 50% 合约且缺 80% 手意味着账户应该是空仓才对，
            #    但 actual>0 证明账户非空 → actual 被脏低估 100%
            if coverage_ratio >= 0.5 and missing_frac >= 0.8:
                hit_f = True
                reason = (
                    f"已持仓账户({total_actual_hands}手)突然缺 {n_missing_contracts}/{n_target_contracts}="
                    f"{coverage_ratio*100:.0f}% 合约，缺手 {total_missing_vol}/{total_target_hands}="
                    f"{missing_frac*100:.0f}%，数学上不可能成立"
                )

        # 第二道纯数学校验：缺额手数 > 目标总手 × 1.1（无论 actual 空不空都拦）
        if not hit_f and total_missing_vol > 0 and total_target_hands > 0:
            if total_missing_vol > total_target_hands * 1.1 + 2:
                hit_f = True
                reason = (
                    f"缺额总手({total_missing_vol}) > 目标总手({total_target_hands}) × 1.1 + 2，"
                    f"数学上不可能成立（maximum missing = target - actual ≤ target）"
                )

        # 第三道：非首次启动 + 缺额覆盖合约=100% + 缺手数≥目标95% → 直接拦
        #   非首次启动说明上一轮同步时 actual 还是正常的（或至少有过持仓），
        #   本轮不可能突然所有合约全缺（missing=target 恒等于 actual=0 才会发生），
        #   这就是 CTP 查仓脏空的精准签名（13:33 / 14:08 两次事故都命中本条件）。
        if not hit_f and n_target_contracts >= 5 and n_missing_contracts == n_target_contracts:
            missing_frac = total_missing_vol / max(1, total_target_hands)
            is_first_run = bool(getattr(self, '_is_first_run', True))
            if missing_frac >= 0.95 and not is_first_run:
                hit_f = True
                reason = (
                    f"非首次启动(已跑过多轮)却出现「全合约缺额」：缺{n_missing_contracts}/{n_target_contracts}="
                    f"100%合约，缺手{total_missing_vol}/{total_target_hands}="
                    f"{missing_frac*100:.0f}%。非冷启动场景 mathematical impossible，"
                    f"必为 CTP query_positions 脏空（actual 被读成 0）"
                )

        if not hit_f:
            return True

        # 统计 pending_map 开仓在途，用于诊断
        pending_open_hands = sum(
            v for k, v in pending_map.items()
            if len(k) >= 3 and k[2] and v > 0
        )
        msg_lines = [
            "🛡️ 最终提交总闸拦截（F 总闸已触发，本次缺额全部弃单）：",
            f"  触发原因：{reason}",
            f"  标准合约={n_target_contracts}个，标准总手={total_target_hands}手",
            f"  实际聚合={len(actual_agg)}个合约/{total_actual_hands}手",
            f"  在途开仓={pending_open_hands}手，在途映射条数={len(pending_map)}",
            f"  缺额计划={n_missing_contracts}个合约/共{total_missing_vol}手：",
        ]
        for i, mo in enumerate(missing_orders[:20]):
            d = "买" if mo.get("direction") == "buy" else "卖"
            msg_lines.append(f"    #{i+1:02d} {mo.get('contract','')} {d} {mo.get('volume',0)}手")
        if len(missing_orders) > 20:
            msg_lines.append(f"    ... 剩余 {len(missing_orders)-20} 个合约省略")
        if excess_orders:
            ex_vol = sum(int(eo.get("volume",0) or 0) for eo in excess_orders)
            msg_lines.append(f"  超额计划={len(excess_orders)}个合约/共{ex_vol}手（同样弃单）")
        msg_lines.append("  -> 本轮完全 ABORT，不提交任何委托，等待下一轮用干净数据重算。")
        msg = "\n".join(msg_lines)
        self.print(msg)
        try:
            self._notify_async(msg)
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # 核心流程：持仓对比 + 快速同步
    # ------------------------------------------------------------------
    def sync_and_trade(
        self,
        trade_volume: int = 1,
        timeout: int = 30,
        position_ratio: float = None,
    ) -> bool:
        # 获取线程锁，防止并发调用
        if not self._sync_lock.acquire(blocking=True, timeout=5):
            self.print("[跳过] 同步被锁定，跳过本次同步")
            return False

        self._is_syncing = True
        try:
            # 在拿到锁之后、进入 _do_sync 之前，先检查 hold-std 有没有更新。
            #
            # 因为 sync_and_trade 有两个来源：
            #   A) run_position_sync_loop 明确感知 hold 文件 mtime 触发 → 这时 hold 文件肯定有变更；
            #   B) _check_position_diff 定时巡检 / 关键时点 force_sync → 这时 hold 文件不一定变不变。
            # 只有 B 的情况下"同一份 hold 配置如果 15s 内反复触发，才需要 SYNC_COOLDOWN 拦住（CTP 持仓查询本身有刷新延迟）；
            # A 的情况下，hold 文件一旦有变更，必须立刻放行，否则就会出现“源账户下单 → 等 15s 才同步”这种 37s 级滑点。
            hold_std_path = getattr(self, "hold_std_path", None)
            hold_mtime_now = 0.0
            if hold_std_path and os.path.exists(str(hold_std_path)):
                try:
                    hold_mtime_now = os.path.getmtime(str(hold_std_path))
                except OSError:
                    hold_mtime_now = 0.0
            hold_changed = False
            last_hold_mtime = getattr(self, "_last_sync_hold_mtime", 0.0)
            if hold_mtime_now > 0 and last_hold_mtime > 0 and hold_mtime_now > last_hold_mtime:
                hold_changed = True
            # 记住本次看到的 mtime，作为下次比较基线（哪怕这次被 cooldown 跳过，也不更新，保证下一轮 hold 变了还能判断“又变过”）
            object.__setattr__(self, "_last_sync_hold_mtime_seen", hold_mtime_now)
            return self._do_sync(
                trade_volume, timeout, position_ratio=position_ratio, lock_held=True,
                _hold_mtime=hold_mtime_now, _hold_changed=hold_changed,
            )
        finally:
            self._is_syncing = False
            self._sync_lock.release()

    def _do_sync(self, trade_volume: int = 1, timeout: int = 30, position_ratio: float = None, lock_held: bool = False,
                 _hold_mtime: float = 0.0, _hold_changed: bool = False) -> bool:
        """执行同步：加载数据 -> 对比 -> 快速同步

        position_ratio 参数语义（2026-09-11 升级后）：
          - None（默认）：保持实例已有的 self._position_ratio，不覆盖。
            热加载 ration/ratio 后（可能 0 清仓、正数跟单、负数对冲），这里不动它。
          - 非 None：
              · 浮点数非法（NaN / inf） → 忽略，保持现有 ratio 不变
              · 合法实数：覆盖 self._position_ratio，可能 >0 跟单 =0 清仓 <0 对冲

        _hold_mtime/_hold_changed：
          仅在 sync_and_trade 调用时注入；用于区分“真的有新 hold 文件”和“定时巡检/关键时点的重复触发”。
        """
        if position_ratio is not None:
            try:
                r = float(position_ratio)
            except (TypeError, ValueError):
                self.print(f"[ratio] 传入的 position_ratio={position_ratio!r} 不是有效数字，忽略并保持 {self._position_ratio}")
            else:
                if r == r and r not in (float('inf'), float('-inf')):
                    self._position_ratio = r
                else:
                    self.print(f"[ratio] 传入的 position_ratio={position_ratio!r} 是非有限浮点数，忽略并保持 {self._position_ratio}")

        self.print("=" * 60)
        self.print("【持仓同步开始】")
        self.print(f"【持仓比例】{self._position_ratio}")
        self.print("=" * 60)

        # 冷却门：
        #   · hold 文件有变更 → 立即放行（哪怕上次同步才几秒钟前；
        #   · 否则用同一份 hold 配置在 15 秒内被重复调用 → 保留 15 秒冷却（防CTP昨/今仓刷新延迟导致的重复下单）。
        current_time = time.time()
        last_sync = getattr(self, '_last_sync_time', 0)
        SYNC_COOLDOWN = 15  # 同一份配置连续触发时的冷却（秒数；hold有变则完全绕过。
        bypass_reason = ""
        if _hold_changed and _hold_mtime > 0:
            # 明确 hold 文件有新变更 → 跳过 cooldown，让同步立刻执行。
            cooldown_skip = True
            bypass_reason = f"hold-std changed (hold_mtime=%.3f vs last=%.3f)" % (_hold_mtime, getattr(self, "_last_sync_hold_mtime", 0.0))
        elif last_sync <= 0:
            cooldown_skip = True
        else:
            cooldown_skip = False
        if not cooldown_skip and current_time - last_sync < SYNC_COOLDOWN:
            self.print(f"[跳过] 距离上次同步仅 {current_time - last_sync:.0f} 秒，冷却中（{SYNC_COOLDOWN}秒）")
            return False
        sync_entry_ts = time.time()
        self._last_sync_time = sync_entry_ts
        if _hold_mtime > 0:
            object.__setattr__(self, "_last_sync_hold_mtime", _hold_mtime)
        if bypass_reason:
            self.print(f"[同步加速] 绕过冷却（{bypass_reason}），立即同步")
        # 埋点：hold-std 写入时刻 → sync 真正开工的延迟，用于拆分“导出阶段耗时”和“同步阶段耗时”
        if _hold_mtime > 0:
            import datetime as _dt
            write_dt_str = _dt.datetime.fromtimestamp(_hold_mtime).strftime("%H:%M:%S")
            lag_hold_to_sync = sync_entry_ts - _hold_mtime
            self.print(
                f"[链路埋点] hold-std 写入时刻={write_dt_str}，写入→sync启动延迟={lag_hold_to_sync:.1f}s"
            )

        t_phase = time.time()
        try:
            # 1. 加载合约信息
            if not self._load_contract_info():
                self.print("[错误] 加载合约信息失败")
                return False
            t_1_done = time.time()
            self.print(f"[同步耗时] 步骤1(加载合约信息): {(t_1_done-t_phase)*1000:.0f}ms"); t_phase = t_1_done

            # 2. 查询在途委托：仅撤销需要重挂/要立刻执行对齐的委托；
            #    对 passive_mode 的开仓挂单，若挂单时长 < passive_wait_seconds，则跳过撤单。
            #    当 _hold_changed=True（真的有新导出/新订单）时，缩短查询与等待时间。
            if _hold_changed:
                qry_order_timeout = 6
                qry_pos_timeout = 8
                qry_order2_timeout = 5
            else:
                qry_order_timeout = 10
                qry_pos_timeout = 15
                qry_order2_timeout = 10
            ctp_orders = self.query_orders(timeout=qry_order_timeout, only_pending=True, today_only=True) or []
            kept_orders = []
            cancelled_ctp = []
            if ctp_orders:
                self.print(f"[撤销前] 共有 {len(ctp_orders)} 条在途委托，按 passive 规则决定是否保留...")
                cancel_success = 0
                for o in ctp_orders:
                    keep, reason = _should_keep_pending_order(self, o, SYNC_COOLDOWN)
                    if keep:
                        kept_orders.append(o)
                        inst = o.get("InstrumentID", "")
                        elapsed = _ctp_order_elapsed_seconds(o)
                        self.print(f"[保留在途] {inst} Ref={o.get('OrderRef','')} elapsed={elapsed:.0f}s -> {reason}")
                        continue
                    order_sys_id = str(o.get("OrderSysID", "")).strip()
                    exchange_id = str(o.get("ExchangeID", "")).strip()
                    instrument_id = str(o.get("InstrumentID", "")).strip()
                    cancelled_ctp.append(o)
                    if order_sys_id:
                        if self._cancel_order_by_sysid(order_sys_id, exchange_id, instrument_id):
                            cancel_success += 1
                    else:
                        order_ref = str(o.get("OrderRef", "")).strip()
                        if self.cancel_order(order_ref):
                            cancel_success += 1
                    time.sleep(0.15)
                self.print(f"[撤销] 已撤销 {cancel_success}/{len(cancelled_ctp)} 条委托（保留 {len(kept_orders)} 条 passive 开仓挂单）")
                if cancelled_ctp:
                    raw_wait = min(max(0.3, 0.3 * len(cancelled_ctp)), 1.6)
                    wait_s = raw_wait * (0.7 if _hold_changed else 1.0)
                    self.print(f"[撤销] 等待 {wait_s:.2f}s 让撤单回报落库")
                    time.sleep(wait_s)
            else:
                self.print("[撤销] 无在途委托需要撤销")
            t_2_done = time.time()
            self.print(f"[同步耗时] 步骤2(查单+撤单): {(t_2_done - t_phase):.2f}s"); t_phase = t_2_done

            # 3. 查询持仓（同步前再次确认，基于最新数据）
            if cancelled_ctp:
                time.sleep(0.5 if _hold_changed else 0.8)
            else:
                time.sleep(0.1 if _hold_changed else 0.2)
            positions = self.query_positions(timeout=qry_pos_timeout)
            if positions is None:
                self.print("[错误] 持仓查询失败")
                return False
            t_3_done = time.time()
            self.print(f"[同步耗时] 步骤3(查询持仓): {(t_3_done - t_phase):.2f}s"); t_phase = t_3_done

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
            t_4_done = time.time()
            self.print(f"[同步耗时] 步骤4(加载标准持仓): {(t_4_done - t_phase)*1000:.0f}ms"); t_phase = t_4_done

            # 5. 聚合持仓
            actual_agg = self._aggregate_actual_positions()
            target = self._parse_hold_std()
            _raw_ratio = float(getattr(self, '_position_ratio', 1.0))
            self._is_liquidate_mode = abs(_raw_ratio) < 1e-9  # 浮点数 0 兼容 0.0/-0.0/非标准极小值
            _sync_mode_tag = (
                "ratio=0清仓模式" if self._is_liquidate_mode
                else f"ratio={_raw_ratio:g}对冲模式(方向反转)" if _raw_ratio < 0
                else f"ratio={_raw_ratio:g}正跟单模式"
            )
            t_5_done = time.time()
            self.print(f"[同步耗时] 步骤5(聚合+解析): {(t_5_done - t_phase)*1000:.0f}ms [{_sync_mode_tag}]"); t_phase = t_5_done

            # ========== 🛡️ 13:33/14:08 两次超仓事故硬闸 B1+B2+B3：持仓查询脏空拦截 ==========
            # 调用公共闸口方法，保证所有入口（sync_and_trade / 巡检 / 冷启动 initial 分支）
            # 只要进入了"聚合完成、对比之前"的同一阶段，都走同一套拦截逻辑。
            if not self._guard_post_query_positions(
                positions=positions,
                actual_agg=actual_agg,
                target=target,
                hold_mtime=_hold_mtime,
                sync_mode_tag=_sync_mode_tag,
            ):
                return False
            # ========== 硬闸结束 ==========

            # 6. 再次查询在途委托（撤销后的状态）
            ctp_orders = self.query_orders(timeout=qry_order2_timeout, only_pending=True, today_only=True) or []
            if ctp_orders:
                self.print(f"[委托] 撤销后剩余 CTP 在途 {len(ctp_orders)} 条")
                self._sync_ctp_orders_to_memory(ctp_orders)
            else:
                self.print("[委托] 撤销后无在途委托")
            t_6_done = time.time()
            self.print(f"[同步耗时] 步骤6(二次查单): {(t_6_done - t_phase):.2f}s"); t_phase = t_6_done

            # 7. 构建在途映射
            pending_map = self._build_pending_map(ctp_orders)
            t_7_done = time.time()
            self.print(f"[同步耗时] 步骤7(构建在途映射): {(t_7_done - t_phase)*1000:.0f}ms"); t_phase = t_7_done

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

            # 9. 过滤当前非交易时段的合约
            # 7x24 / simu / --force / --skip-time-check 模式下（_skip_trading_time_check=True）整段跳过，
            # 不调用 get_contracts_trading_status，不丢合约，非开盘时间也能对齐+提交。
            all_contracts = set(contract for contract, _ in set(target.keys()) | set(effective_actual.keys()))
            non_trading_contracts = set()
            if not bool(getattr(self, '_skip_trading_time_check', False)):
                trading_status = get_contracts_trading_status(list(all_contracts))
                for contract in all_contracts:
                    if not trading_status.get(contract, False):
                        non_trading_contracts.add(contract.upper())
                        self.print(f"[非交易时段] {contract} 当前不可交易，跳过对齐")
            else:
                if all_contracts:
                    self.print(f"[信息] _skip_trading_time_check=True：跳过交易时段过滤（7x24/simu/--force），共 {len(all_contracts)} 个合约全部参与对齐")

            # 10. 计算缺额/超额
            missing_orders = []
            excess_orders = []

            # 检查是否有合约在 1009 冷却期（与入口 SYNC_COOLDOWN 保持一致：避免"昨仓/今仓"划分未更新时重复报持仓不足）
            current_time = time.time()
            cooling_contracts = []
            if hasattr(self, '_last_1009_reject'):
                for contract_upper, reject_time in list(self._last_1009_reject.items()):
                    if current_time - reject_time < SYNC_COOLDOWN:
                        cooling_contracts.append(contract_upper)

            # 计算缺额（使用 effective_actual = actual_agg + pending_open）
            # 避免在途开仓委托被错误判断为缺额
            for key, t_vol in target.items():
                contract, direction = key
                if contract.upper() in non_trading_contracts:
                    continue
                effective_vol = effective_actual.get(key, 0)
                if t_vol > effective_vol:
                    # ========== 🛡️ 开软闸 C1：生成阶段再核对 actual_agg + pending_open 是否确实 < target ==========
                    # 兜住「query_orders 超时 → pending_map 丢失 → effective_vol 被算低 → 重复补开」（如13:33:03 br2611前兆）
                    a_vol_check = actual_agg.get(key, 0)
                    p_open_check = pending_map.get((contract.upper(), direction, True), 0)
                    if a_vol_check + p_open_check >= t_vol:
                        self.print(
                            f"[开软闸] {contract} {'多' if direction == 2 else '空'} "
                            f"actual({a_vol_check}) + pending_open({p_open_check}) = {a_vol_check + p_open_check} "
                            f">= target({t_vol})，跳过重复补开（effective_vol={effective_vol} 疑似因查询超时被低估）"
                        )
                        continue
                    planned_vol = t_vol - effective_vol
                    self.print(f"[缺额计算] {contract} {'多' if direction == 2 else '空'}: 标准{t_vol} vs 有效{effective_vol}, 缺额{planned_vol}")
                    missing_orders.append({
                        "contract": contract,
                        "direction": "buy" if direction == 2 else "sell",
                        "volume": planned_vol,
                    })

            # 计算超额（使用 effective_actual = actual_agg + pending_open）
            # 避免在途开仓委托被错误判断为超额（平掉还没成交的持仓）
            for key, effective_vol in effective_actual.items():
                contract, direction = key
                if contract.upper() in non_trading_contracts:
                    continue
                t_vol = target.get(key, 0)
                vol_to_close = effective_vol - t_vol
                if vol_to_close > 0:
                    # ========== 🛡️ 平软闸 D1：生成阶段用 actual_agg 兜底（不能计划平比实际持有的还多） ==========
                    real_avail = actual_agg.get(key, 0)
                    if real_avail <= 0:
                        continue
                    if vol_to_close > real_avail:
                        self.print(
                            f"[平软闸] {contract} {'多' if direction == 2 else '空'} "
                            f"计划平{vol_to_close}截短到实际持有{real_avail}（effective_vol={effective_vol} 被高估）"
                        )
                        vol_to_close = real_avail
                    if vol_to_close <= 0:
                        continue
                    # 跳过1009冷却期内的合约
                    if contract.upper() in cooling_contracts:
                        self.print(f"[平] {contract} 在1009冷却期内（30秒），跳过本次平仓")
                        continue
                    is_exclude_exit = self._is_contract_excluded(contract)
                    excess_orders.append({
                        "contract": contract,
                        "direction": direction,
                        "volume": vol_to_close,
                        "is_liquidate_mode": getattr(self, '_is_liquidate_mode', False),
                        # exclude 品种老仓退出：与 ratio==0 清仓模式同样走 passive 排队挂单，多赚滑点
                        "is_exclude_exit": is_exclude_exit,
                    })
                    if is_exclude_exit:
                        self.print(
                            f"[exclude-退出] {contract} {'多' if direction == 2 else '空'} "
                            f"计划平 {vol_to_close} 手（命中 exclude，后续永不复开）"
                        )

            # 10. 更新 hold.json
            self._update_hold_json_file()
            t_10_done = time.time()
            self.print(f"[同步耗时] 步骤8~10(计算差异+更新hold): {(t_10_done - t_phase):.2f}s"); t_phase = t_10_done

            # 11. 输出对比摘要
            skipped_contracts = sorted(non_trading_contracts)
            self.print(f"[对比] 标准:{len(target)} 有效:{len(effective_actual)} 缺额:{len(missing_orders)} 超额:{len(excess_orders)} 非交易时段跳过:{len(skipped_contracts)}")
            if missing_orders:
                self.print(f"[缺额] {[mo['contract'] for mo in missing_orders]}")
            if excess_orders:
                self.print(f"[超额] {[eo['contract'] for eo in excess_orders]}")
            if skipped_contracts:
                self.print(f"[跳过] {skipped_contracts}")

            total_target = sum(t for t in target.values())
            total_actual = sum(a for a in actual_agg.values())
            has_diff = bool(missing_orders) or bool(excess_orders)

            # 12. 发送持仓差异通知
            if has_diff:
                diff_lines = [f"🔄 持仓差异检测到（比例={self._position_ratio}），准备同步："]

                if missing_orders:
                    total_missing = sum(mo["volume"] for mo in missing_orders)
                    diff_lines.append(f"📈 缺额开仓 ({len(missing_orders)} 个合约，共 {total_missing} 手):")
                    for mo in missing_orders:
                        d = "买" if mo["direction"] == "buy" else "卖"
                        diff_lines.append(f"  {mo['contract']} {d} {mo['volume']}手")
                if excess_orders:
                    total_excess = sum(eo["volume"] for eo in excess_orders)
                    exit_cnt = sum(1 for eo in excess_orders if eo.get("is_exclude_exit"))
                    common_cnt = len(excess_orders) - exit_cnt
                    exit_vol = sum(eo["volume"] for eo in excess_orders if eo.get("is_exclude_exit"))
                    common_vol = total_excess - exit_vol
                    if common_cnt > 0:
                        diff_lines.append(f"📉 超额平仓 ({common_cnt} 个合约，共 {common_vol} 手):")
                        for eo in excess_orders:
                            if eo.get("is_exclude_exit"):
                                continue
                            d = "多" if eo["direction"] == 2 else "空"
                            diff_lines.append(f"  {eo['contract']} {d} {eo['volume']}手")
                    if exit_cnt > 0:
                        diff_lines.append(f"🚪 exclude 品种退出平仓 ({exit_cnt} 个合约，共 {exit_vol} 手，退出后永不复开):")
                        for eo in excess_orders:
                            if not eo.get("is_exclude_exit"):
                                continue
                            d = "多" if eo["direction"] == 2 else "空"
                            diff_lines.append(f"  {eo['contract']} {d} {eo['volume']}手（退出）")

                if skipped_contracts:
                    diff_lines.append(f"⏸️ 以下 {len(skipped_contracts)} 个合约当前非交易时段，已跳过对齐：")
                    diff_lines.append(f"  {', '.join(skipped_contracts)}")

                self._notify_async("🔄 持仓差异检测到，准备同步：\n" + "\n".join(diff_lines))

                success = self._fast_sync(missing_orders, excess_orders, ctp_orders, target, actual_agg, pending_map, cancelled_ctp=cancelled_ctp, hold_mtime=_hold_mtime)
                t_end = time.time()
                total = t_end - sync_entry_ts
                pre_lag = (sync_entry_ts - _hold_mtime) if _hold_mtime > 0 else 0.0
                self.print(
                    f"[链路总耗时] hold-std写入→CTP报单完成 = 写→同步启动(前导)={pre_lag:.1f}s + 同步内部流程={total:.1f}s "
                    f"= {pre_lag + total:.1f}s"
                )
                self.print("[结论] 同步完成（委托已提交）")
                self._is_first_run = False
                return success
            else:
                self.print("[结论] 当前交易时段内持仓一致，无需操作")
                t_end = time.time()
                self.print(f"[同步耗时] 无差异路径总耗时: {t_end - sync_entry_ts:.1f}s")
                # 这条消息与 base.py 中"15秒持仓巡检"心跳不同：
                # 它表示"某一次实际进入 _do_sync 的完整同步流程（可能是文件变更触发、也可能是巡检触发）
                # 跑完后发现仓位一致"，因此不写固定秒数，避免与巡检心跳秒数形成误导
                aligned_lines = [
                    f"✅ 对齐流程校验通过（{SYNC_COOLDOWN}秒冷却门内）",
                    f"标准持仓: {len(target)} 个合约, {total_target} 手",
                    f"实际持仓: {len(actual_agg)} 个合约, {total_actual} 手",
                    f"状态: 当前交易时段内仓位一致 ✓",
                ]
                if skipped_contracts:
                    aligned_lines.append(f"⏸️ 以下 {len(skipped_contracts)} 个合约当前非交易时段，已跳过对齐：")
                    aligned_lines.append(f"  {', '.join(skipped_contracts)}")
                self._notify_async("\n".join(aligned_lines))
                self._is_first_run = False
                return True
        except Exception as e:
            import traceback
            self.print(f"[异常] _do_sync 出错: {e}")
            traceback.print_exc()
            return False

    def _fast_sync(self, missing_orders: list, excess_orders: list, ctp_orders: list, target: dict = None, actual_agg: dict = None, pending_map: dict = None, cancelled_ctp: list = None, hold_mtime: float = 0) -> bool:
        """快速同步：并行查询 + 批量提交"""
        if target is None:
            target = {}
        if actual_agg is None:
            actual_agg = {}
        if pending_map is None:
            pending_map = {}
        if cancelled_ctp is None:
            cancelled_ctp = []

        self.print("=" * 50)
        self.print("【快速同步模式】")
        self.print("=" * 50)
        _t_fast_start = time.time()

        # ============================================================
        # 🛡️ F 总闸：最终提交前的绝对数学不可能成立拦截
        #   独立于 B1~B3 / C1~C2 / D1~D2 / Fix3 全部上游逻辑，
        #   只要外部事实(actual>0但缺全仓; 或缺手>目标×1.1)成立就直接 ABORT 全弃。
        #   任何未来新增的同步入口（哪怕绕过 sync_and_trade 直接调 _fast_sync）
        #   都被这道闸口兜住。
        # ============================================================
        if not self._guard_final_total_missing(
            missing_orders=missing_orders,
            excess_orders=excess_orders,
            target=target,
            actual_agg=actual_agg,
            pending_map=pending_map,
        ):
            self.print("[F总闸] 🛡️ 本轮同步已被数学绝对拦截，完全 ABORT，不提交任何委托。")
            return False
        # ============================================================
        # 🛡️ 冷启动额外保护（首仓防御）：
        #   若是 _is_first_run 且 missing_orders 合约数超过一半目标或>10个，
        #   - 等 3 秒让 CTP 持仓刷新延迟稳定下来（避免首秒脏空）
        #   - 独立再查一次 positions，重新聚合同 actual_agg2，
        #     如果 actual_agg2 的总手数 > 传入 actual_agg 的 1.5 倍或合约数多>5，
        #     说明传入的 actual_agg 明显被低估了，本轮 ABORT 等待下一轮。
        # ============================================================
        if bool(getattr(self, '_is_first_run', True)) and len(missing_orders) >= 10:
            self.print(
                f"[🔒首仓防御] 首次运行且批量缺额合约={len(missing_orders)} 个，"
                f"等待3s让CTP持仓刷新稳定后再重查一次..."
            )
            time.sleep(3.0)
            try:
                re_positions = self.query_positions(timeout=10, retries=2) or []
            except Exception as _re:
                self.print(f"[🔒首仓防御] 重查持仓异常：{_re}，本轮保守弃单")
                return False
            actual_agg2 = {}
            try:
                _saved = getattr(self, '_actual_positions', [])
                # 临时替换缓存以聚合最新查仓结果
                object.__setattr__(self, '_actual_positions', re_positions)
                actual_agg2 = self._aggregate_actual_positions() or {}
                object.__setattr__(self, '_actual_positions', _saved)
            except Exception:
                try:
                    object.__setattr__(self, '_actual_positions', _saved)
                except Exception:
                    pass
            _total2 = sum(actual_agg2.values())
            _total_orig = sum((actual_agg or {}).values())
            _n2 = len(actual_agg2)
            _n_orig = len(actual_agg or {})
            self.print(
                f"[🔒首仓防御] 重查结果：原聚合={_n_orig}合约/{_total_orig}手 "
                f"重查聚合={_n2}合约/{_total2}手"
            )
            # 严重不一致：重查比原来多 5 合约以上 OR 多 50% 手数以上
            _bad_n = (_n2 - _n_orig) >= 5
            _bad_v = (_total_orig > 0 and _total2 >= _total_orig * 1.5 + 2) or (_total_orig == 0 and _total2 > 10)
            if _bad_n or _bad_v:
                lines = [
                    "🛡️ 首仓防御：重查持仓发现原聚合严重低估，本轮弃单：",
                    f"  原聚合={_n_orig}合约/{_total_orig}手",
                    f"  重查聚合={_n2}合约/{_total2}手",
                    "  -> 拒绝首轮大批量补开，等待下一轮用新持仓重新计算。",
                ]
                m = "\n".join(lines)
                self.print(m)
                try:
                    self._notify_async(m)
                except Exception:
                    pass
                return False
            # 首仓分批：如果 missing_orders > 目标的 60% 合约数（即接近全仓首开），
            # 先只开一半，另一半留给下一轮，避免一次性压 38 合约触发 CTP 风控或报单流控。
            n_target = len(target or {})
            if n_target > 10 and len(missing_orders) >= int(n_target * 0.6):
                half = max(1, len(missing_orders) // 2)
                dropped = missing_orders[half:]
                missing_orders[:] = missing_orders[:half]
                dropped_names = [(m.get("contract",""), m.get("direction",""), m.get("volume",0)) for m in dropped]
                tag_msg = (
                    f"[🔒首仓分批] 首运行缺{len(missing_orders)+len(dropped)}个合约已达"
                    f"目标{n_target}的60%以上，先开一半({len(missing_orders)})，"
                    f"留{len(dropped)}个到下一轮：{dropped_names[:10]}"
                    + ("..." if len(dropped_names) > 10 else "")
                )
                self.print(tag_msg)

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
                    # ---------- 关键：拿到行情后立刻写入永久已知价缓存（磁盘持久化 + 异步刷盘）----------
                    # 次主力合约（SM701 等）30+ 分钟才一笔 tick，
                    # 只要曾经拿到过一次，以后就算 md_provider 超时也能在 4 层兜底第 4 层命中。
                    # 封装到 base.py 的 _update_last_known_prices：
                    #   - 内置 _last_known_prices_lock（RLock 重入锁，并发查询多线程写入安全）
                    #   - 变更后立刻 daemon 线程写 tmp + replace 原子刷盘
                    #   - 任何异常全吞，不影响行情查询主流程。
                    try:
                        self._update_last_known_prices(
                            contract=contract,
                            last_price=md.get("LastPrice"),
                            bid_price1=md.get("BidPrice1"),
                            ask_price1=md.get("AskPrice1"),
                            ts=time.time(),
                        )
                    except Exception:
                        pass

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
                # 【开仓双保险拦截】：exclude 品种永不复新开仓
                # 正常情况下：_parse_hold_std 阶段 exclude 已剔除，到不了 missing_orders；
                # 这道保险是为了防止未来改动 _parse_hold_std 逻辑时意外放行 exclude 开仓。
                if self._is_contract_excluded(contract):
                    self.print(
                        f"[开] {contract} 命中 exclude 品种，开仓指令被拦截（永不复开）。"
                        f"若实际仍有该合约老仓，会在超额段走 exclude 退出平仓。"
                    )
                    skip_open[0] += 1
                    continue
                md = market_data_map.get(contract)
                _open_cache_fallback = False
                _open_lk_fallback = False
                _open_lk_ts = ""
                if not md:
                    # 与平仓阶段相同的双兜底：首阶段没收到首 tick 时，再走一次 prefer_cached=True 查历史缓存
                    # （开仓用的是标准仓里的新合约，首次订阅后首 tick 可能晚几秒/几十秒到）
                    _md_provider = getattr(self, '_md_provider', None)
                    if _md_provider is not None:
                        try:
                            md = _md_provider.get_quote(contract, timeout=3.0, auto_subscribe=True, prefer_cached=True)
                        except Exception:
                            md = None
                    if not md and _md_provider is not None:
                        try:
                            _k = str(contract).strip().upper()
                            with _md_provider._quotes_lock:
                                _d = _md_provider._quotes.get(_k)
                            if _d:
                                md = dict(_d)
                                _open_cache_fallback = True
                        except Exception:
                            md = None
                    # ---------- 开仓阶段第 4 层兜底：永久已知价缓存 ----------
                    # 注意：开仓如果完全没有历史行情数据，宁可跳过（避免开错价）；
                    # 但如果历史上曾经拿到过一次行情（_last_known_prices 有），哪怕是老的也用，避免永久开不出来。
                    if not md:
                        try:
                            _k2 = str(contract).strip().upper()
                            _lkt = getattr(self, '_last_known_prices', None) or {}
                            _lk = _lkt.get(_k2)
                            if _lk and isinstance(_lk, dict):
                                def _ok2(x):
                                    try:
                                        fv = float(x); return math.isfinite(fv) and 0 < fv < 1e9
                                    except Exception:
                                        return False
                                lp_ok = _ok2(_lk.get("LastPrice"))
                                b1_ok = _ok2(_lk.get("BidPrice1"))
                                a1_ok = _ok2(_lk.get("AskPrice1"))
                                if lp_ok or b1_ok or a1_ok:
                                    base = float(_lk.get("LastPrice")) if lp_ok else (float(_lk.get("BidPrice1")) if b1_ok else float(_lk.get("AskPrice1")))
                                    md = {
                                        "InstrumentID": contract,
                                        "LastPrice": base,
                                        "BidPrice1": float(_lk.get("BidPrice1")) if b1_ok else base,
                                        "AskPrice1": float(_lk.get("AskPrice1")) if a1_ok else base,
                                    }
                                    _open_lk_fallback = True
                                    try:
                                        import datetime as _dt_lk2
                                        _t = float(_lk.get("_ts", 0) or 0)
                                        if _t > 0:
                                            _open_lk_ts = ", 缓存时间=" + _dt_lk2.datetime.fromtimestamp(_t).strftime("%H:%M:%S")
                                    except Exception:
                                        pass
                        except Exception:
                            md = None
                            _open_lk_fallback = False
                if not md:
                    exact = self._standardize_contract(contract)
                    _dir = "买" if mo.get("direction") == "buy" else "卖"
                    _vol = mo.get("volume", 0)
                    # 当前轮拿不到 md 就立刻启动一个后台异步 MdApi 订阅线程（不阻塞本轮同步主流程）
                    # 后台线程长等 60s 拿首 tick，回填缓存后下一轮 sync/巡检直接命中 prefer_cached + 永久已知价
                    _async_submitted = False
                    try:
                        if hasattr(self, 'submit_async_market_fetch'):
                            _async_submitted = bool(self.submit_async_market_fetch(contract))
                    except Exception:
                        _async_submitted = False
                    _async_tag = "；已启动后台异步行情回填订阅，下一轮同步时命中缓存" if _async_submitted else ""
                    self.print(f"[开跳过] {contract}({exact}) {_dir}{_vol}手: 第一阶段行情查询失败，已尝试：1) prefer_cached=True 再查 2) md_provider._quotes 历史缓存回退 3) _last_known_prices 永久已知价缓存；仍空 => 启动后从未收到过此合约 tick，建议确认合约是否正确/行情前置是否订阅到该合约{_async_tag}")
                    skip_open[0] += 1
                    continue
                elif _open_cache_fallback:
                    self.print(f"[信息] {contract} 使用 md_provider._quotes 历史缓存行情定价（次主力/新合约首tick未达，历史价可用）")
                elif _open_lk_fallback:
                    self.print(f"[信息] {contract} 使用 _last_known_prices 永久已知价定价（md_provider未返回新 tick%s）" % _open_lk_ts)

                # 检查在途委托
                mo_upper = contract.upper()
                need_new_order = True
                wait_s = int(getattr(self, '_passive_wait_seconds', 300) or 300)
                use_passive = bool(getattr(self, '_passive_mode', False))
                info = self._get_contract_info(contract)
                price_tick = info.get("PriceTick", 1.0) if info else 1.0

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
                        elapsed = _ctp_order_elapsed_seconds(o, time.time())

                        # passive 开仓：挂单 < wait_s 秒 一律保留，不做"价格变了就撤"
                        if use_passive and elapsed < wait_s:
                            self.print(f"[开] {contract} passive挂单保留：{elapsed:.0f}s < {wait_s}s，价格{last_price}，等待排队")
                            need_new_order = False
                            skip_open[0] += 1
                            break

                        # 计算"当前应考虑的重挂价"：aggressive 直接对手价；passive 则逐档 +1 tick
                        if mo["direction"] == "buy":
                            agg_price = _safe_price(md.get("AskPrice1", 0)) or _safe_price(md.get("LastPrice", 0))
                            if use_passive:
                                cur_price, step_tag = _passive_next_tick_price(
                                    "buy", float(last_price or 0), md, price_tick
                                )
                                current_price = cur_price
                            else:
                                current_price = agg_price
                                step_tag = "aggressive-ask"
                        else:
                            agg_price = _safe_price(md.get("BidPrice1", 0)) or _safe_price(md.get("LastPrice", 0))
                            if use_passive:
                                cur_price, step_tag = _passive_next_tick_price(
                                    "sell", float(last_price or 0), md, price_tick
                                )
                                current_price = cur_price
                            else:
                                current_price = agg_price
                                step_tag = "aggressive-bid"

                        # 是否需要撤单重挂：
                        #  - passive 模式：超过 wait_s 且 价格变化 >= 1 tick 才重挂（逐档咬盘口）
                        #  - aggressive：只要价格变化 >= 1 tick 就重挂（原来的语义）
                        price_changed = (
                            _is_valid_positive_price(last_price)
                            and _is_valid_positive_price(current_price)
                            and abs(current_price - last_price) >= price_tick
                        )

                        if price_changed:
                            self.print(f"[开] {contract} 撤单重挂: {last_price}->{current_price} ({step_tag})"
                                       f" elapsed={elapsed:.0f}s passive={int(use_passive)}")
                            order_sys_id = o.get("OrderSysID", "")
                            exchange_id = o.get("ExchangeID", "")
                            if order_sys_id:
                                self._cancel_order_by_sysid(order_sys_id, exchange_id, contract)
                            else:
                                self.cancel_order(o.get("OrderRef", ""))
                            time.sleep(0.5)  # 等待撤单完成
                        else:
                            # 价格没变化，保持等待
                            self.print(f"[开] {contract} 在途足够且价格未变({step_tag})，保持等待 elapsed={elapsed:.0f}s")
                            need_new_order = False
                            skip_open[0] += 1
                        break

                if not need_new_order:
                    time.sleep(0.2)
                    continue

                # ========== 🛡️ Fix3 同合约短时间重开保护（防 double submit，如 br2611 前兆） ==========
                # 同一合约同方向 <3s 内发生过撤单/拒绝 → 本轮不再立即补开，等下一轮同步（0.5s 扫描粒度会很快追上来）
                # 【方向编码对齐 target/actual_agg/pending_map】：2=多/买入方向，3=空/卖出方向（CTP PosiDirection 枚举）
                dir2 = 2 if mo["direction"] == "buy" else 3
                now_ts = time.time()
                recent_cancel_hit = False
                recent_reason = ""
                # 命中 a)：本轮回撤单列表 cancelled_ctp 中刚撤过相同方向的开仓
                for co in cancelled_ctp:
                    co_instr = str(co.get("InstrumentID", "")).upper()
                    co_dir = str(co.get("Direction", "")).strip()
                    co_offset = str(co.get("CombOffsetFlag", "")).strip()
                    if co_instr != mo_upper:
                        continue
                    want_dir = (tdapi.THOST_FTDC_D_Buy if mo["direction"] == "buy" else tdapi.THOST_FTDC_D_Sell).strip()
                    if co_dir == want_dir and co_offset == str(tdapi.THOST_FTDC_OF_Open).strip():
                        recent_cancel_hit = True
                        recent_reason = "本轮刚撤过该合约同方向开仓(cancelled_ctp)"
                        break
                # 命中 b)：本地 _orders 内存中近 3s 内的撤单 / 拒绝记录
                if not recent_cancel_hit:
                    with self._order_lock:
                        for _ref, _info in list(self._orders.items()):
                            if str(_info.get("instr", "")).upper() != mo_upper:
                                continue
                            _info_dir = _info.get("direction", "")
                            _info_offset = _info.get("offset_flag", "")
                            if _info_dir != mo["direction"]:
                                continue
                            if _info_offset != tdapi.THOST_FTDC_OF_Open:
                                continue
                            _status = str(_info.get("OrderStatus", "")).strip()
                            _status_time = float(_info.get("status_ts", 0) or 0)
                            # 5: 已撤单 / a: 拒单 / 4: 撤单中（兼容状态码）
                            if _status in ("5", "a", "6", "4") and (now_ts - _status_time) < 3.0:
                                recent_cancel_hit = True
                                recent_reason = f"本地_orders 3s内状态={_status} elapsed={now_ts - _status_time:.1f}s"
                                break
                # 命中 c)：_last_1009_reject 中该合约仍在冷却（虽然生成段已有，但提交端兜底）
                if not recent_cancel_hit and hasattr(self, '_last_1009_reject'):
                    rej_ts = self._last_1009_reject.get(mo_upper, 0)
                    if 0 < (now_ts - rej_ts) < SYNC_COOLDOWN:
                        recent_cancel_hit = True
                        recent_reason = f"1009拒绝冷却期内 {now_ts - rej_ts:.1f}s<{SYNC_COOLDOWN}s"
                if recent_cancel_hit:
                    self.print(
                        f"[重开保护🛡️] {contract} {mo['direction']} vol={mo['volume']} → 本轮跳过不补开 "
                        f"（原因: {recent_reason}）→ 等下一轮同步再核对"
                    )
                    skip_open[0] += 1
                    time.sleep(0.2)
                    continue
                # ========== Fix3 结束 ==========

                # 下单
                # 开仓定价策略：
                #  - passive_mode=True（套利跟单被动模式）：用被动排队价，挂买一/卖一排队，赚滑点
                #       买开 = BidPrice1（买一排队价），卖开 = AskPrice1（卖一排队价）
                #  - passive_mode=False（默认）：用 aggressive 主动吃单价，尽快成交
                #       买开 = AskPrice1（卖一主动吃），卖开 = BidPrice1（买一主动吃）
                info = self._get_contract_info(contract)
                if not info:
                    self.print(f"[开] {contract} 获取合约信息失败，跳过")
                    skip_open[0] += 1
                    time.sleep(0.2)
                    continue
                price_tick = info.get("PriceTick", 1.0)

                mode = "passive" if getattr(self, '_passive_mode', False) else "aggressive"
                limit_price, pricing_note = _calc_limit_price(
                    direction=mo["direction"],
                    md=md,
                    mode=mode,
                    contract=contract,
                    price_tick=price_tick,
                    logger=self,
                )

                if not _is_valid_positive_price(limit_price):
                    self.print(f"[开] {contract} 无有效价格 ({pricing_note})")
                    skip_open[0] += 1
                    time.sleep(0.2)
                    continue

                # ========== 🛡️ 开软闸 C2：提交端再兜底（actual_agg + pending_open >= target 就不提交） ==========
                key_check = (contract, dir2)
                a_check2 = actual_agg.get(key_check, 0)
                p_check2 = pending_map.get((mo_upper, dir2, True), 0)
                t_check2 = target.get(key_check, 0)
                if t_check2 > 0 and a_check2 + p_check2 >= t_check2:
                    self.print(
                        f"[开软闸] {contract} {mo['direction']} actual={a_check2} + pending_open={p_check2} "
                        f"= {a_check2 + p_check2} >= target={t_check2}，跳过重复补开（提交端兜底）"
                    )
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
        clipped_cases = []  # 记录所有🛡️软闸截短的案例（开+平），末尾发汇总

        def _submit_close_serial():
            """串行提交平仓委托（与 PositionManagerUI.py 的 _do_close_all_batch 保持一致）"""
            for eo in excess_orders:
                contract = eo["contract"]
                pos_dir = eo["direction"]
                eo_volume = eo["volume"]  # 保存原始计划数量
                _sk_reason = ""

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
                    tag = "[exclude-退出]" if eo.get("is_exclude_exit") else "[平]"
                    _sk_reason = "已有平仓委托在途"
                    self.print(f"{tag} {contract} 已有平仓委托在途，等待30秒检查循环处理")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    if _sk_reason:
                        self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    continue

                md = market_data_map.get(contract)
                _cache_hard_fallback = False
                _last_known_fallback = False
                _lk_ts_str = ""
                if not md:
                    # 次主力合约（SM701 等）tick 非常稀疏，首阶段 query_market_data 时没收到最新 tick，但
                    # 可能在前面批量订阅后已经入缓存、或上一轮/上一次同步时缓存过历史 tick。
                    # 优先再试一次 prefer_cached=True 的查询，再不行去 md_provider._quotes 原始缓存里拿。
                    _md_provider = getattr(self, '_md_provider', None)
                    if _md_provider is not None:
                        try:
                            md = _md_provider.get_quote(contract, timeout=3.0, auto_subscribe=True, prefer_cached=True)
                        except Exception:
                            md = None
                    if not md and _md_provider is not None:
                        try:
                            _k = str(contract).strip().upper()
                            with _md_provider._quotes_lock:
                                _d = _md_provider._quotes.get(_k)
                            if _d:
                                md = dict(_d)
                                _cache_hard_fallback = True
                        except Exception:
                            md = None
                    # ---------- 第 4 层兜底：永久已知价缓存 ----------
                    # 上三层（market_data_map / prefer_cached / 原始缓存）都空，
                    # 但「历史上曾经拿到过一次行情」的话，_last_known_prices 里会存着一份。
                    # 对次主力平仓场景，哪怕是 30 分钟前的价，也比永久平不掉强。
                    if not md:
                        try:
                            _k2 = str(contract).strip().upper()
                            _lkt = getattr(self, '_last_known_prices', None) or {}
                            _lk = _lkt.get(_k2)
                            if _lk and isinstance(_lk, dict):
                                def _ok(x):
                                    try:
                                        fv = float(x); return math.isfinite(fv) and 0 < fv < 1e9
                                    except Exception:
                                        return False
                                lp_ok = _ok(_lk.get("LastPrice"))
                                b1_ok = _ok(_lk.get("BidPrice1"))
                                a1_ok = _ok(_lk.get("AskPrice1"))
                                if lp_ok or b1_ok or a1_ok:
                                    base = float(_lk.get("LastPrice")) if lp_ok else (float(_lk.get("BidPrice1")) if b1_ok else float(_lk.get("AskPrice1")))
                                    md = {
                                        "InstrumentID": contract,
                                        "LastPrice": base,
                                        "BidPrice1": float(_lk.get("BidPrice1")) if b1_ok else base,
                                        "AskPrice1": float(_lk.get("AskPrice1")) if a1_ok else base,
                                    }
                                    _last_known_fallback = True
                                    try:
                                        import datetime as _dt_lk
                                        _t = float(_lk.get("_ts", 0) or 0)
                                        if _t > 0:
                                            _lk_ts_str = ", 缓存时间=" + _dt_lk.datetime.fromtimestamp(_t).strftime("%H:%M:%S")
                                    except Exception:
                                        pass
                        except Exception:
                            md = None
                            _last_known_fallback = False
                if not md:
                    if eo.get("is_exclude_exit"):
                        self.print(f"[exclude-退出] {contract} 无行情，跳过退出平仓（下次同步重试）")
                    # 当前轮拿不到 md 就立刻启动一个后台异步 MdApi 订阅线程（不阻塞本轮同步主流程）
                    # 后台线程长等 60s 拿首 tick，回填缓存后下一轮 sync/巡检直接命中 prefer_cached + 永久已知价
                    _async_sub_close = False
                    try:
                        if hasattr(self, 'submit_async_market_fetch'):
                            _async_sub_close = bool(self.submit_async_market_fetch(contract))
                    except Exception:
                        _async_sub_close = False
                    _async_tg = "；已启动后台异步行情回填订阅，下一轮同步时命中缓存" if _async_sub_close else ""
                    _sk_reason = ("第一阶段行情查询未返回，已尝试：1) prefer_cached=True 再查一次 2) md_provider._quotes 历史缓存回退 3) _last_known_prices 永久已知价缓存；"
                                  "仍空 => 启动后从未收到过此合约 tick，请确认合约是否正确/行情前置是否有此合约（可能真的非主力或映射错）%s" % _async_tg)
                    _tag_dir = "多" if pos_dir == 2 else "空"
                    self.print("[平跳过] %s %s %s手: %s" % (contract, _tag_dir, eo_volume, _sk_reason))
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                elif _cache_hard_fallback:
                    self.print("[信息] %s 使用 md_provider._quotes 历史缓存行情定价（次主力无最新 tick，历史价仍可用）。" % contract)
                elif _last_known_fallback:
                    self.print("[信息] %s 使用 _last_known_prices 永久已知价定价（md_provider未返回新tick%s）。" % (contract, _lk_ts_str))

                detail = self._get_position_detail(contract, pos_dir)
                _det_pos = detail.get("Position", 0) if detail else 0
                if _det_pos <= 0:
                    if eo.get("is_exclude_exit"):
                        self.print(f"[exclude-退出] {contract} 实际持仓已为 0，退出完成 ✅")
                    _sk_reason = "查询实际持仓=0（detail.Position=%s，账户实际已无仓，可能上一回合刚平掉或CTP刷新延迟）" % _det_pos
                    _tag_dir = "多" if pos_dir == 2 else "空"
                    self.print("[平跳过] %s %s %s手: %s" % (contract, _tag_dir, eo_volume, _sk_reason))
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                info = self._get_contract_info(contract)
                if not info:
                    tag = "[exclude-退出]" if eo.get("is_exclude_exit") else "[平]"
                    _sk_reason = "获取合约信息失败（不在_contract_info_map映射内，主力main_contracts是否未更新？）"
                    self.print(f"{tag} {contract} 获取合约信息失败")
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                exchange_id = detail.get("ExchangeID", "") or info["ExchangeID"]
                actual_pos = detail.get("Position", 0)
                price_tick = info.get("PriceTick", 1.0)

                # ========== 关键修复：平仓前先撤销所有相反方向的委托 ==========
                # 如果有多头超额（需要平多），先撤销所有空头委托
                # 如果有空头超额（需要平空），先撤销所有多头委托
                opposite_dir = "sell" if pos_dir == 2 else "buy"

                # 检查该合约是否之前被 1009 拒绝过（冷却机制）
                current_time = time.time()
                last_rejected = getattr(self, '_last_1009_reject', {}).get(contract.upper(), 0)
                if current_time - last_rejected < 30:  # 30秒内不重复尝试同一合约
                    tag = "[exclude-退出]" if eo.get("is_exclude_exit") else "[平]"
                    _sk_reason = f"30秒内1009拒单冷却（last_reject={time.strftime('%H:%M:%S', time.localtime(last_rejected))}已过{int(current_time-last_rejected)}s<30s)"
                    self.print(f"{tag} {contract} 30秒内被1009拒绝过，跳过，等待下次同步")
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                # ========== 跳过有在途开仓委托的合约 ==========
                # 如果该合约+方向有在途开仓委托，说明持仓正在变化中
                # 不应该在这个时间点平仓，避免"开仓未成交但持仓已平"的错误
                pending_open_vol = pending_map.get((contract.upper(), pos_dir, True), 0)
                pending_open_vol = pending_open_vol[0] if isinstance(pending_open_vol, tuple) else pending_open_vol
                if pending_open_vol > 0:
                    tag = "[exclude-退出]" if eo.get("is_exclude_exit") else "[平]"
                    _sk_reason = f"pending_map里有同方向在途开仓{pending_open_vol}手，等成交确认后下一轮再平"
                    self.print(f"{tag} {contract} 有在途开仓委托 {pending_open_vol} 手，跳过平仓（等待成交确认）")
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
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
                        time.sleep(0.2)  # 原来 0.5s，压缩到 0.2s
                    # 重要：撤销后重新查询持仓，确保平仓量基于最新数据
                    self.print(f"[平] {contract} 撤销完成，重新查询持仓...")
                    time.sleep(0.5)  # 原来 1s，压缩到 0.5s
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
                    _sk_reason = f"撤销相反方向委托后重查持仓，available={available}≤0（可能刚被相反方向委托成交消耗掉实际可用持仓）"
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                # ========== 🛡️ 平软闸 D2a：按可用持仓截短（不允许计划平出比实际还多的量） ==========
                if eo["volume"] > available:
                    clipped_cases.append(f"[平软闸D2a] {contract} 计划{eo['volume']}→截短到{available}（实际持仓上限）")
                    self.print(
                        f"[平软闸🛡️] {contract} {'多' if pos_dir == 2 else '空'} 计划平仓{eo['volume']}手 "
                        f"→ 截短到可用持仓{available}手（effective_vol被高估）"
                    )
                diff = min(eo["volume"], available)
                self.print(f"[平调试] {contract} diff初始值={diff}")
                if diff <= 0:
                    _sk_reason = f"D2a截短后diff={diff}≤0（excess.eo_vol截到available后无剩余，无需平仓）"
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue

                if pos_dir == 2:  # 多头 → 卖出平仓
                    close_direction = "sell"
                    # 定价策略（优先级从高到低）：
                    #  ① exclude 品种老仓退出 (is_exclude_exit=True) → 永远 aggressive 主动吃盘（时间优先，尽快退干净）
                    #  ② ratio==0 全仓清仓模式 (is_liquidate_mode=True) → 永远 passive 排队挂单（不急成交多赚滑点）
                    #  ③ passive_mode=True（套利跟单账户）→ 普通对齐调仓也走 passive 排队价（赚滑点优先）
                    #  ④ 默认（普通对齐调仓 + passive_mode=False）→ aggressive 主动吃盘（尽快对齐）
                    is_exclude_exit = bool(eo.get("is_exclude_exit", False))
                    is_liquidate_mode = bool(eo.get("is_liquidate_mode", False))
                    use_passive = (not is_exclude_exit) and (is_liquidate_mode or bool(getattr(self, '_passive_mode', False)))
                    mode = "passive" if use_passive else "aggressive"
                    limit_price, pricing_note = _calc_limit_price(
                        direction=close_direction,
                        md=md,
                        mode=mode,
                        contract=contract,
                        price_tick=price_tick,
                        logger=self,
                    )
                else:  # 空头 → 买入平仓
                    close_direction = "buy"
                    is_exclude_exit = bool(eo.get("is_exclude_exit", False))
                    is_liquidate_mode = bool(eo.get("is_liquidate_mode", False))
                    use_passive = (not is_exclude_exit) and (is_liquidate_mode or bool(getattr(self, '_passive_mode', False)))
                    mode = "passive" if use_passive else "aggressive"
                    limit_price, pricing_note = _calc_limit_price(
                        direction=close_direction,
                        md=md,
                        mode=mode,
                        contract=contract,
                        price_tick=price_tick,
                        logger=self,
                    )

                if not _is_valid_positive_price(limit_price):
                    tag = "[exclude-退出]" if eo.get("is_exclude_exit") else "[平]"
                    _sk_reason = f"定价返回无效价格 limit_price={limit_price} ({pricing_note})，可能float_max脏价格/无最新行情LastPrice=0或负数"
                    self.print(f"{tag} {contract} 无有效价格 ({pricing_note})")
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
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
                    _sk_reason = f"提交前二次查询持仓latest_pos={latest_pos}≤0（可能前几秒CTP实际已被其它线程平掉或刷新延迟）"
                    self.print(f"[平] {contract} 最新查询持仓为 0，无需平仓，跳过")
                    self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
                    skip_close[0] += 1
                    time.sleep(0.2)
                    continue
                # 如果最新持仓小于计划平仓量，以最新持仓为准
                if latest_pos < diff:
                    clipped_cases.append(f"[平软闸D2b] {contract} diff{diff}→截短到最新持仓{latest_pos}（提交前二次查询）")
                    self.print(f"[平软闸🛡️] {contract} 二次查询截短: 计划{diff}手 → 最新持仓{latest_pos}手，按{latest_pos}手提交")
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
                        is_liquidate_mode=eo.get("is_liquidate_mode", False),
                    )
                    if not ok:
                        # 报单被拒绝（如1009持仓不足），跳过该合约继续下一个
                        _sk_reason = f"平今报单被_place_order拒绝（极可能CTP 1009-持仓不足，已记入30s冷却）"
                        self.print(f"[平] {contract} 平今报单被拒绝，跳过")
                        self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
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
                        is_liquidate_mode=eo.get("is_liquidate_mode", False),
                    )
                    if not ok:
                        # 报单被拒绝（如1009持仓不足），跳过该合约继续下一个
                        _sk_reason = f"平昨/平报单被_place_order拒绝（极可能CTP 1009-持仓不足/昨今划分错误，已记入30s冷却）"
                        self.print(f"[平] {contract} 平昨报单被拒绝，跳过")
                        self.print(f"[平跳过] {contract} {'多' if pos_dir==2 else '空'} {eo_volume}手: {_sk_reason}")
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
                    if eo.get("is_exclude_exit"):
                        self.print(
                            f"[exclude-退出] {contract} 提交成功 @{limit_price} ({close_vol_submitted}手，"
                            f"排队挂单模式，退出后永不复开)"
                        )
                    else:
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

        # ========== 🛡️ 软闸截短汇总（命中则单独发飞书） ==========
        if clipped_cases:
            hold_tag = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hold_mtime)) if hold_mtime > 0 else "N/A"
            clip_lines = [
                f"🛡️ 本轮同步风控软闸共命中 {len(clipped_cases)} 次（已全部自动截短，未超量提交）：",
                f"  hold-std写入时刻={hold_tag}",
                f"  标准仓={total_target}手 实际CTP={total_actual}手 提交={total_submit} 跳过开={skip_open[0]} 跳过平={skip_close[0]}",
            ]
            for idx, case in enumerate(clipped_cases, 1):
                clip_lines.append(f"  [{idx}] {case}")
            self._notify_async("\n".join(clip_lines))
            self.print("\n".join(clip_lines))

        self.print(f"[快速] 完成: 开仓 {submitted_open[0]}/{len(missing_orders)} 平仓 {submitted_close[0]}/{len(excess_orders)}")
        self.print(f"[快速同步耗时] 总耗时={time.time() - _t_fast_start:.2f}s")
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