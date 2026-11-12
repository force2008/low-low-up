# -*- coding: utf-8 -*-
"""行情监控配置文件的读取与校验。

配置文件为 JSON，允许以 ``#`` 或 ``//`` 开头的整行注释，便于人工维护。
"""

import json
import os
import re
from typing import Any, Dict, List, Optional

from conditions import SUPPORTED_TYPES

# ==================== 默认值 ====================
DEFAULT_ENV = "7x24"
DEFAULT_POLL_INTERVAL_SEC = 3
DEFAULT_HISTORY_BARS = 240
DEFAULT_COOLDOWN_SEC = 1800
DEFAULT_SIGNAL_FILE = "signals/monitor_signals.json"
DEFAULT_KLINE_DB = "data/monitor_kline.db"

# 周期别名 -> 分钟数
_PERIOD_ALIASES = {
    "d": 1440,
    "d1": 1440,
    "1d": 1440,
    "day": 1440,
}

# 动作别名（中英文皆可）-> 归一化动作
_ACTION_SIGNAL = {"signal", "开仓", "下单", "开空单", "开多单", "开空", "开多"}
_ACTION_FEISHU = {"feishu", "飞书", "通知", "告警", "预警"}

# 方向别名
_DIRECTION_SHORT = {"short", "sell", "空", "做空", "空头"}
_DIRECTION_LONG = {"long", "buy", "多", "做多", "多头"}


class ConfigError(Exception):
    """配置文件错误"""


def parse_period(period: str) -> int:
    """把周期字符串解析为分钟数。

    支持写法：``m5`` / ``m15`` / ``m60`` / ``m120`` / ``m240`` / ``d1`` / ``day``。

    Args:
        period: 周期字符串

    Returns:
        分钟数

    Raises:
        ConfigError: 无法识别时抛出
    """
    if period is None or str(period).strip() == "":
        raise ConfigError("周期不能为空")
    key = str(period).strip().lower()
    if key in _PERIOD_ALIASES:
        return _PERIOD_ALIASES[key]
    match = re.match(r"^m(\d+)$", key) or re.match(r"^(\d+)m$", key) or re.match(r"^(\d+)$", key)
    if match:
        minutes = int(match.group(1))
        if minutes <= 0:
            raise ConfigError(f"非法周期：{period}")
        return minutes
    raise ConfigError(f"无法识别的周期：{period}（示例：m5、m15、m60、d1）")


def period_name(minutes: int) -> str:
    """分钟数 -> 周期名（1440 记为 d1，其余记为 mN）"""
    return "d1" if int(minutes) >= 1440 else f"m{int(minutes)}"


def _normalize_direction(value: Optional[str]) -> Optional[str]:
    """方向字符串归一化为 short / long"""
    if value is None:
        return None
    key = str(value).strip().lower()
    if not key:
        return None
    if key in _DIRECTION_SHORT:
        return "short"
    if key in _DIRECTION_LONG:
        return "long"
    raise ConfigError(f"无法识别的方向：{value}（示例：short/long）")


def _strip_line_comments(text: str) -> str:
    """去掉以 # 或 // 开头的整行注释"""
    kept: List[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#") or stripped.startswith("//"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _resolve_path(path: str, base_dir: str) -> str:
    """相对路径按配置文件所在目录解析为绝对路径"""
    if not path:
        return path
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(base_dir, path))


def load_config(path: str) -> Dict[str, Any]:
    """读取并校验监控配置。

    Args:
        path: 配置文件路径

    Returns:
        归一化后的配置字典

    Raises:
        ConfigError: 文件不存在 / JSON 非法 / 字段校验失败
    """
    if not os.path.exists(path):
        raise ConfigError(f"配置文件不存在：{path}")

    with open(path, "r", encoding="utf-8") as fp:
        raw_text = fp.read()

    try:
        raw = json.loads(_strip_line_comments(raw_text))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"配置文件 JSON 解析失败：{exc}")

    if not isinstance(raw, dict):
        raise ConfigError("配置文件根节点必须是 JSON 对象")

    return _normalize(raw, os.path.dirname(os.path.abspath(path)))


def _normalize(raw: Dict[str, Any], base_dir: str) -> Dict[str, Any]:
    """把原始配置归一化为运行期结构"""
    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, list) or not rules_raw:
        raise ConfigError("配置项 rules 必须是非空数组")

    rules: List[Dict[str, Any]] = []
    for index, item in enumerate(rules_raw):
        if not isinstance(item, dict):
            raise ConfigError(f"rules[{index}] 必须是 JSON 对象")
        rules.append(_normalize_rule(item, index))

    # 订阅清单 = 所有「启用」规则用到的合约并集（去重且保持出现顺序）
    symbols: List[str] = []
    for rule in rules:
        if not rule["enabled"]:
            continue
        for symbol in rule["symbols"]:
            if symbol not in symbols:
                symbols.append(symbol)
    if not symbols:
        raise ConfigError("没有可监控的合约：请至少启用一条规则，并为其填写 symbols")

    # 收集所有规则用到的周期
    periods: Dict[str, int] = {}
    for rule in rules:
        periods[rule["period"]] = rule["period_minutes"]

    signal_file = _resolve_path(
        str(raw.get("signal_file") or DEFAULT_SIGNAL_FILE), base_dir
    )

    kline_db_raw = raw.get("kline_db") or {}
    kline_db = {
        "enabled": bool(kline_db_raw.get("enabled", True)),
        "path": _resolve_path(
            str(kline_db_raw.get("path") or DEFAULT_KLINE_DB), base_dir
        ),
    }

    feishu_raw = raw.get("feishu") or {}
    feishu = {
        "enabled": bool(feishu_raw.get("enabled", True)),
        "webhook": str(feishu_raw.get("webhook") or "").strip() or None,
    }

    history_bars = int(raw.get("history_bars") or DEFAULT_HISTORY_BARS)
    history_bars = max(30, history_bars)

    poll_interval = int(raw.get("poll_interval_sec") or DEFAULT_POLL_INTERVAL_SEC)

    return {
        "env": str(raw.get("env") or DEFAULT_ENV),
        "poll_interval_sec": max(1, poll_interval),
        "history_bars": history_bars,
        "signal_file": signal_file,
        "kline_db": kline_db,
        "feishu": feishu,
        "symbols": symbols,
        "periods": periods,
        "rules": rules,
        "base_dir": base_dir,
    }


def _normalize_rule(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """归一化单条规则"""
    name = str(item.get("name") or f"rule{index + 1}").strip()

    rule_type = str(item.get("type") or "").strip()
    if not rule_type:
        raise ConfigError(f"规则「{name}」缺少 type 字段")
    if rule_type not in SUPPORTED_TYPES:
        supported = "、".join(sorted(SUPPORTED_TYPES))
        raise ConfigError(f"规则「{name}」的条件类型不支持：{rule_type}（支持：{supported}）")

    minutes = parse_period(item.get("period") or "m5")
    pname = period_name(minutes)

    symbols_raw = item.get("symbols")
    if not isinstance(symbols_raw, list) or not symbols_raw:
        raise ConfigError(
            f"规则「{name}」必须指定 symbols（本规则监控的合约列表），例如 [\"m2605\"]"
        )
    symbols = [str(s).strip() for s in symbols_raw if str(s).strip()]
    if not symbols:
        raise ConfigError(f"规则「{name}」的 symbols 为空")

    params = item.get("params") or {}
    if not isinstance(params, dict):
        raise ConfigError(f"规则「{name}」的 params 必须是 JSON 对象")

    if rule_type == "boll_touch":
        band = str(params.get("band") or "upper").strip().lower()
        if band not in ("upper", "middle", "lower"):
            raise ConfigError(f"规则「{name}」的 boll_touch.band 只能是 upper/middle/lower")
        params["band"] = band
        source = str(params.get("source") or "range").strip().lower()
        if source not in ("range", "price", "high", "low"):
            raise ConfigError(
                f"规则「{name}」的 boll_touch.source 只能是 range/price/high/low"
            )
        params["source"] = source
        if params.get("operator"):
            operator = str(params["operator"]).strip()
            if operator not in (">=", "<=", ">", "<"):
                raise ConfigError(
                    f"规则「{name}」的 boll_touch.operator 只能是 >= / <= / > / <"
                )
            if source == "range":
                raise ConfigError(
                    f"规则「{name}」的 boll_touch.operator 只在单值口径"
                    f"（source=price/high/low）下生效，source=range 时请去掉"
                )
            params["operator"] = operator
    elif rule_type == "price_breakout":
        operator = str(params.get("operator") or ">=").strip()
        if operator not in (">=", "<=", ">", "<"):
            raise ConfigError(f"规则「{name}」的 price_breakout.operator 只能是 >= / <= / > / <")
        params["operator"] = operator
        try:
            params["price"] = float(params.get("price"))
        except (TypeError, ValueError):
            raise ConfigError(f"规则「{name}」的 price_breakout.price 必须是数字（目标价位）")
        source = str(params.get("source") or "price").strip().lower()
        if source not in ("price", "high", "low", "close"):
            raise ConfigError(f"规则「{name}」的 price_breakout.source 只能是 price/high/low/close")
        params["source"] = source

    direction = _normalize_direction(item.get("direction"))

    actions_raw = item.get("actions") or ["signal", "feishu"]
    if not isinstance(actions_raw, list) or not actions_raw:
        raise ConfigError(f"规则「{name}」的 actions 必须是非空数组")
    actions: List[str] = []
    for action in actions_raw:
        key = str(action).strip().lower()
        if key in _ACTION_SIGNAL:
            if key in ("开空单", "开空"):
                direction = direction or "short"
            elif key in ("开多单", "开多"):
                direction = direction or "long"
            if "signal" not in actions:
                actions.append("signal")
        elif key in _ACTION_FEISHU:
            if "feishu" not in actions:
                actions.append("feishu")
        else:
            raise ConfigError(f"规则「{name}」存在未知动作：{action}（示例：signal / feishu）")

    cooldown = int(item.get("cooldown_sec", DEFAULT_COOLDOWN_SEC))

    return {
        "name": name,
        "enabled": bool(item.get("enabled", True)),
        "symbols": symbols,
        "period": pname,
        "period_minutes": minutes,
        "type": rule_type,
        "params": params,
        "direction": direction,
        "actions": actions,
        "cooldown_sec": max(0, cooldown),
    }
