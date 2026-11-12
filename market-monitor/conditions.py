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


def _candle_size(bar, measure: str) -> float:
    """计算 K 线幅度。

    - ``measure="body"``：实体大小（阴线为 open-close，阳线为 close-open）
    - ``measure="range"``：全幅（high-low）
    """
    if measure == "range":
        return float(bar[2]) - float(bar[3])
    bar_open = float(bar[1])
    bar_close = float(bar[4])
    return abs(bar_close - bar_open)


def _eval_max_extreme_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    is_bearish: bool,
) -> Optional[Dict[str, Any]]:
    """通用实现：最后一根已收盘 K 线是否为「近期最大阴/阳线」"""
    if len(closed_bars) < 2:
        return None

    lookback = max(2, int(params.get("lookback", 20)))
    measure = str(params.get("measure") or "body").strip().lower()
    if measure not in ("body", "range"):
        measure = "body"

    last = closed_bars[-1]
    _, last_open, _, _, last_close, _ = _nums(last)

    # 阴线要求收 < 开；阳线要求收 > 开
    if is_bearish and not last_close < last_open:
        return None
    if not is_bearish and not last_close > last_open:
        return None

    last_size = _candle_size(last, measure)
    if last_size <= 0:
        return None

    window = closed_bars[-min(lookback, len(closed_bars)):]
    others: List[float] = []
    for bar in window[:-1]:
        _, bar_open, _, _, bar_close, _ = _nums(bar)
        if is_bearish and bar_close >= bar_open:
            continue
        if not is_bearish and bar_close <= bar_open:
            continue
        size = _candle_size(bar, measure)
        if size > 0:
            others.append(size)

    max_other = max(others) if others else 0.0
    if last_size < max_other:
        return None

    kind = "阴" if is_bearish else "阳"
    measure_cn = "实体" if measure == "body" else "振幅"
    return {
        "bar_time": str(last[0]),
        "price": last_close,
        "direction": "short" if is_bearish else "long",
        "detail": (
            f"近 {lookback} 根内最大{kind}线触发：{measure_cn}={last_size:.2f}"
            f"（此前最大={max_other:.2f}）"
        ),
    }


def eval_max_bearish_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """近期最大阴线：最后一根已收盘 K 线是近 lookback 根内最大的阴线（默认按实体）"""
    return _eval_max_extreme_candle(closed_bars, params, is_bearish=True)


def eval_max_bullish_candle(
    closed_bars: List[Bar],
    params: Dict[str, Any],
    forming_bar: Optional[Bar] = None,
) -> Optional[Dict[str, Any]]:
    """近期最大阳线：最后一根已收盘 K 线是近 lookback 根内最大的阳线（默认按实体）"""
    return _eval_max_extreme_candle(closed_bars, params, is_bearish=False)


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
