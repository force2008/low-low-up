# -*- coding: utf-8 -*-
"""行情监控的触发条件实现。

所有条件函数签名统一为::

    func(closed_bars, params, forming_bar) -> Optional[dict]

- ``closed_bars``：已收盘 K 线列表，元素为 ``(datetime, open, high, low, close, volume)``
- ``params``：条件参数（来自配置文件的 params 字段）
- ``forming_bar``：当前正在形成的 K 线，可能为 ``None``

命中时返回字典，未命中返回 ``None``。命中字典包含：

============  ====================================================
字段          说明
============  ====================================================
bar_time      用于去重的行情时间（同一根 K 线只触发一次）
price         参考价格
direction     方向建议 short / long / None
detail        中文说明，用于日志与飞书通知
============  ====================================================
"""

from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# K 线元组：(datetime, open, high, low, close, volume)
Bar = Tuple[str, float, float, float, float, float]

# 布林带中文名
_BAND_CN = {"upper": "上轨", "middle": "中轨", "lower": "下轨"}

# 价格比较运算符
_PRICE_OPS = {
    ">=": lambda value, level: value >= level,
    "<=": lambda value, level: value <= level,
    ">": lambda value, level: value > level,
    "<": lambda value, level: value < level,
}

# 价格取值口径中文名
_PRICE_SOURCE_CN = {
    "price": "最新价",
    "high": "最高价",
    "low": "最低价",
    "close": "已收盘收盘价",
}

# 布林带触碰判定口径中文名
_BOLL_SOURCE_CN = {
    "range": "本根区间",
    "price": "最新价",
    "high": "本根最高",
    "low": "本根最低",
}


def _nums(bar) -> Tuple[Any, float, float, float, float, float]:
    """把原始 K 线统一转成 (datetime, o, h, l, c, v)"""
    return bar[0], float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4]), float(bar[5])


def _candle_size(bar, measure: str, prev_close: Optional[float] = None) -> Optional[float]:
    """计算 K 线「大小」。

    - ``measure="change"``：涨跌幅 |收盘 - 前收|（需要前一根收盘价，缺失时返回 None）
    - ``measure="body"``：实体 |收盘 - 开盘|
    - ``measure="range"``：全幅 最高 - 最低
    """
    if measure == "range":
        return float(bar[2]) - float(bar[3])
    if measure == "change":
        if prev_close is None:
            return None
        return abs(float(bar[4]) - float(prev_close))
    return abs(float(bar[4]) - float(bar[1]))


def _candle_direction(bar, color_basis: str, prev_close: Optional[float]) -> int:
    """K 线方向：1=阳(涨) / -1=阴(跌) / 0=平或无法判定。

    - ``color_basis="prev_close"``：收盘 > 前收 为阳，收盘 < 前收 为阴
    - ``color_basis="open"``：收盘 > 开盘 为阳，收盘 < 开盘 为阴
    """
    bar_close = float(bar[4])
    if color_basis == "open":
        ref = float(bar[1])
    else:
        if prev_close is None:
            return 0
        ref = float(prev_close)
    if bar_close > ref:
        return 1
    if bar_close < ref:
        return -1
    return 0


def _eval_max_extreme_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar],
    is_bearish: bool,
) -> Optional[Dict[str, Any]]:
    """通用实现：当前 K 线是否为「近期最大阴/阳线」。

    参数：
        - lookback：回看根数，默认 20，最小 2
        - color_basis：阴阳依据，prev_close=较前一根收盘价（默认）/ open=较开盘价
        - measure：大小口径，change=涨跌幅|收-前收|（默认）/ body=实体 / range=全幅
        - realtime：是否盘中实时，默认 True（用未收盘 K 线判定）；False 则仅收盘后判定
    """
    lookback = max(2, int(params.get("lookback", 20)))
    measure = str(params.get("measure") or "change").strip().lower()
    if measure not in ("change", "body", "range"):
        measure = "change"
    color_basis = str(params.get("color_basis") or "prev_close").strip().lower()
    if color_basis not in ("prev_close", "open"):
        color_basis = "prev_close"
    realtime = bool(params.get("realtime", True))

    # 选定「当前 K 线」：实时优先用未收盘 K 线，否则退回最后一根已收盘 K 线
    use_forming = realtime and forming_bar is not None
    if use_forming:
        current = forming_bar
        history = closed_bars
    elif closed_bars:
        current = closed_bars[-1]
        history = closed_bars[:-1]
    else:
        return None
    if not history:
        return None  # 至少需要一根参照 K 线

    want = -1 if is_bearish else 1

    # 当前 K 线的方向与大小
    cur_prev_close = float(history[-1][4])
    if _candle_direction(current, color_basis, cur_prev_close) != want:
        return None
    cur_size = _candle_size(current, measure, cur_prev_close)
    if cur_size is None or cur_size <= 0:
        return None

    # 历史对照窗口：当前 K 线之前的最近 lookback-1 根
    take_n = min(lookback - 1, len(history))
    start = len(history) - take_n
    others: List[float] = []
    for offset in range(take_n):
        seq_idx = start + offset
        bar = history[seq_idx]
        prev_close = float(history[seq_idx - 1][4]) if seq_idx - 1 >= 0 else None
        if _candle_direction(bar, color_basis, prev_close) != want:
            continue
        size = _candle_size(bar, measure, prev_close)
        if size is not None and size > 0:
            others.append(size)

    max_other = max(others) if others else 0.0
    if cur_size < max_other:
        return None

    kind = "阴" if is_bearish else "阳"
    measure_cn = {"change": "涨跌幅", "body": "实体", "range": "振幅"}[measure]
    basis_cn = "较前收" if color_basis == "prev_close" else "较开盘"
    timing_cn = "盘中实时" if use_forming else "收盘确认"
    return {
        "bar_time": str(current[0]),
        "price": float(current[4]),
        "direction": "short" if is_bearish else "long",
        "detail": (
            f"近 {lookback} 根内最大{kind}线触发（{basis_cn}，{measure_cn}，{timing_cn}）："
            f"{cur_size:.2f}（此前最大={max_other:.2f}）"
        ),
    }


def eval_max_bearish_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """近期最大阴线：当前 K 线是近 lookback 根内最大的阴线（默认较前收、涨跌幅、盘中实时）"""
    return _eval_max_extreme_candle(closed_bars, params, forming_bar, is_bearish=True)


def eval_max_bullish_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """近期最大阳线：当前 K 线是近 lookback 根内最大的阳线（默认较前收、涨跌幅、盘中实时）"""
    return _eval_max_extreme_candle(closed_bars, params, forming_bar, is_bearish=False)


def eval_boll_touch(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """触碰布林带：价格触碰到上轨 / 中轨 / 下轨。

    轨道由**已收盘** K 线计算，触碰判定使用当前正在形成的 K 线（若存在），
    因此盘中即可告警。

    参数：
        - period：布林带周期，默认 20
        - multiplier：标准差倍数，默认 2.0
        - band：upper / middle / lower，默认 upper
        - source：触碰判定口径，默认 range
            range = 本根最高/最低区间覆盖轨道（盘中曾触及即告警，不漏插针）
            price = 当前最新价（未完成 K 线的收盘价）
            high / low = 本根最高 / 最低
        - operator：单值口径（source 非 range）的比较方向，>= / <= / > / <
            缺省按轨道推导：upper/middle 用 >=，lower 用 <=
            判定式：>= 与 > 用 值 vs 轨道-容差，<= 与 < 用 值 vs 轨道+容差
        - tolerance_pct：触碰容差（相对轨道的比例），默认 0
    """
    if len(closed_bars) < 2:
        return None

    period = max(2, int(params.get("period", 20)))
    multiplier = float(params.get("multiplier", 2.0))
    band = str(params.get("band") or "upper").strip().lower()
    source = str(params.get("source") or "range").strip().lower()
    tolerance_pct = float(params.get("tolerance_pct", 0.0))
    if source not in ("range", "price", "high", "low"):
        return None
    operator = str(params.get("operator") or "").strip() or ("<=" if band == "lower" else ">=")
    if operator not in _PRICE_OPS or (source == "range" and params.get("operator")):
        return None

    closes = np.array([float(bar[4]) for bar in closed_bars[-period:]], dtype=float)
    ma = float(closes.mean())
    std = float(closes.std(ddof=0))
    upper = ma + multiplier * std
    middle = ma
    lower = ma - multiplier * std
    ref = {"upper": upper, "middle": middle, "lower": lower}[band]

    test_bar = forming_bar if forming_bar is not None else closed_bars[-1]
    _, _, test_high, test_low, test_close, _ = _nums(test_bar)
    tolerance = abs(ref) * tolerance_pct

    if source == "range":
        # 区间口径：用本根最高/最低与轨道比较，中轨看区间是否覆盖
        if band == "upper":
            hit = test_high >= ref - tolerance
        elif band == "lower":
            hit = test_low <= ref + tolerance
        else:
            hit = (test_low <= ref + tolerance) and (test_high >= ref - tolerance)
        value = ref
        value_desc = f"本根区间=最高{test_high:.4f}/最低{test_low:.4f}"
    else:
        # 单值口径：用指定价格与轨道按 operator 比较
        value = {"price": test_close, "high": test_high, "low": test_low}[source]
        compare = _PRICE_OPS[operator]
        if operator in (">=", ">"):
            hit = compare(value, ref - tolerance)
        else:
            hit = compare(value, ref + tolerance)
        value_desc = f"{_BOLL_SOURCE_CN[source]}={value:.4f} {operator} {ref:.4f}"

    if not hit:
        return None

    return {
        "bar_time": str(test_bar[0]),
        "price": value,
        "direction": None,
        "detail": (
            f"触碰 BOLL {_BAND_CN[band]}（{band}={ref:.4f}, {value_desc}, "
            f"周期={period}, 倍数={multiplier}, 容差={tolerance:.4f}）"
        ),
    }


def eval_price_breakout(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """价格突破固定价位：价格满足 >= / <= / > / < 某个指定价位。

    被比较的价格默认取**当前正在形成**的 K 线收盘价（即最新价），因此盘中即可告警；
    若没有 forming_bar 则退回最后一根已收盘 K 线。

    参数：
        - price：目标价位（必填，数字）
        - operator：比较符，>=（默认）/ <= / > / <
        - source：取值口径，price=最新价（默认）/ high=本根最高 /
                  low=本根最低 / close=最后一根已收盘 K 线的收盘价
        - tolerance：容差（价格绝对量），默认 0；判定时朝触发方向放宽该值
    """
    level_raw = params.get("price")
    if level_raw is None:
        return None
    try:
        level = float(level_raw)
    except (TypeError, ValueError):
        return None

    operator = str(params.get("operator") or ">=").strip()
    compare = _PRICE_OPS.get(operator)
    if compare is None:
        return None

    source = str(params.get("source") or "price").strip().lower()
    if source not in _PRICE_SOURCE_CN:
        return None

    try:
        tolerance = abs(float(params.get("tolerance", 0.0) or 0.0))
    except (TypeError, ValueError):
        tolerance = 0.0

    # close 口径强制用已收盘 K 线（收盘后才判定），其余口径优先用未完成 K 线
    if source == "close":
        if not closed_bars:
            return None
        test_bar = closed_bars[-1]
    else:
        if forming_bar is not None:
            test_bar = forming_bar
        elif closed_bars:
            test_bar = closed_bars[-1]
        else:
            return None

    _, _, test_high, test_low, test_close, _ = _nums(test_bar)
    if source == "high":
        value = test_high
    elif source == "low":
        value = test_low
    else:  # price / close：forming 时即为最新价
        value = test_close

    # 容差朝触发方向放宽：>=、> 放宽下限，<=、< 放宽上限
    if operator in (">=", ">"):
        hit = compare(value, level - tolerance)
    else:
        hit = compare(value, level + tolerance)

    if not hit:
        return None

    return {
        "bar_time": str(test_bar[0]),
        "price": value,
        "direction": None,
        "detail": (
            f"价格突破 {level:g}：{_PRICE_SOURCE_CN[source]}={value:g} "
            f"{operator} {level:g}（容差={tolerance:g}）"
        ),
    }


# 条件类型注册表
CONDITION_HANDLERS = {
    "max_bearish_candle": eval_max_bearish_candle,
    "max_bullish_candle": eval_max_bullish_candle,
    "boll_touch": eval_boll_touch,
    "price_breakout": eval_price_breakout,
}

SUPPORTED_TYPES = set(CONDITION_HANDLERS)


def evaluate(
    rule: Dict[str, Any],
    closed_bars: List[Bar],
    forming_bar: Optional[Bar],
) -> Optional[Dict[str, Any]]:
    """按规则计算条件是否命中。

    Args:
        rule: 归一化后的规则字典（需包含 type / params）
        closed_bars: 已收盘 K 线列表
        forming_bar: 当前正在形成的 K 线

    Returns:
        命中返回结果字典，否则返回 None
    """
    handler = CONDITION_HANDLERS.get(rule.get("type"))
    if handler is None:
        raise ValueError(f"未知条件类型：{rule.get('type')}")
    return handler(closed_bars, rule.get("params") or {}, forming_bar)
