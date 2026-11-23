#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""持仓数据 Web 展示

数据来源：account_monitor.py 从融航风控端导出的 CSV（默认 E:\\ronghang\\data）

    操作账户 YYYY-MM-DD HH-MM-SS.csv        —— 全部账号的权益汇总
   操作账户总持仓合约 *.csv                 —— 各账号的持仓保证金（账号级子行）
   {账号}当前持仓 YYYY-MM-DD HH-MM-SS.csv  —— 单个账号的持仓明细
   {账号} 成交明细 YYYY-MM-DD HH-MM-SS.csv  —— 单个账号的成交明细

注意：融航导出的『当前持仓』里『占用保证金』列恒为 0，真实值只在
『操作账户总持仓合约』的账号级子行里，所以持仓页按（账号, 合约, 方向）
从该文件回填保证金。

缓存机制：

    cache/accounts.json            账户列表整理结果
    cache/positions/{账号}.json    单账号持仓整理结果
    cache/trades/{账号}.json       单账号成交整理结果

每次请求先扫描数据目录拿到最新的源文件；若与缓存里记录的 source 文件名不同，
说明产生了新的一批导出，就重新解析并覆盖缓存；否则直接返回缓存内容，
不再去读原始 CSV。

运行：python app.py（默认监听 0.0.0.0:8000）
可用环境变量覆盖：POSITIONS_DATA_DIR / POSITIONS_HOST / POSITIONS_PORT
"""

import csv
import datetime
import glob
import json
import logging
import os
import re
import threading

from flask import Flask, jsonify, render_template

# ==================== 配置 ====================

ACCOUNT_FILE_GLOB = "操作账户 *.csv"          # 账户汇总文件（注意「操作账户」后有空格，
                                               # 这样不会误匹配「操作账户总持仓品种 xxx.csv」）
POSITION_FILE_SUFFIX = "当前持仓"              # 持仓文件：{账号}当前持仓 YYYY-MM-DD HH-MM-SS.csv
TRADE_FILE_SUFFIX = "成交明细"                # 成交文件：{账号} 成交明细 YYYY-MM-DD HH-MM-SS.csv
TOTAL_POSITION_FILE_GLOB = "*操作账户总持仓合约*.csv"   # 账号级持仓保证金来源

# 详情页展示的持仓列：(CSV 列名, 是否按数值处理)
POSITION_COLUMNS = (
    ("合约ID", False),
    ("买卖", False),
    ("逐日浮盈", True),
    ("逐笔浮盈", True),
    ("今仓", True),
    ("持仓量", True),
    ("逐笔开仓均价", True),
    ("占用保证金", True),
)

# 成交明细页展示列：(CSV 列名, 是否按数值处理)
TRADE_COLUMNS = (
    ("成交时间", False),
    ("合约", False),
    ("买卖", False),
    ("开平", False),
    ("成交数量", True),
    ("成交价格", True),
    ("投保", False),
    ("成交编号", False),
)


def _default_data_dir():
    """按平台习惯探测数据目录（测试机是 C 盘，本地是 E 盘）"""
    for candidate in (r"E:\ronghang\data", r"C:\ronghang\data", r"D:\ronghang\data"):
        if os.path.isdir(candidate):
            return candidate
    return r"E:\ronghang\data"


DATA_DIR = os.environ.get("POSITIONS_DATA_DIR") or _default_data_dir()
HOST = os.environ.get("POSITIONS_HOST", "0.0.0.0")
PORT = int(os.environ.get("POSITIONS_PORT", "8000"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
POSITION_CACHE_DIR = os.path.join(CACHE_DIR, "positions")
TRADE_CACHE_DIR = os.path.join(CACHE_DIR, "trades")

app = Flask(__name__)
logger = logging.getLogger("positions-web")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_cache_lock = threading.Lock()

# 文件名里的时间戳，例如 2026-11-18 07-36-10（兼容小时不补零的 7-36-10）
_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2} \d{1,2}-\d{2}-\d{2})\.csv$", re.IGNORECASE)


# ==================== 通用工具 ====================

def _file_ts(filename):
    """从文件名里解析时间戳；解析不到返回 None"""
    matched = _TS_RE.search(filename)
    if not matched:
        return None
    try:
        return datetime.datetime.strptime(matched.group(1), "%Y-%m-%d %H-%M-%S")
    except ValueError:
        return None


def _pick_latest(paths):
    """按文件名里的时间戳取最新的一份，取不到时间戳就退回文件修改时间"""
    if not paths:
        return None

    def sort_key(path):
        stamp = _file_ts(os.path.basename(path))
        return stamp or datetime.datetime.fromtimestamp(os.path.getmtime(path))

    return max(paths, key=sort_key)


def _to_number(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in ("", "-", "--", "nan", "NaN"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _is_total_account(account):
    """识别 CSV 末尾自带的那行『合 计』（兼容半角/全角空格与尾部冒号）"""
    if not account:
        return False
    text = account.strip().rstrip(":").rstrip("\uff1a")
    return text.replace(" ", "").replace("\u3000", "").replace("\t", "") == "合计"


def _read_csv_rows(path):
    """按常见中文编码读取 CSV，返回 (行列表, 实际使用的编码)"""
    last_error = None
    for encoding in ("gbk", "gb18030", "utf-8-sig", "utf-8"):
        try:
            with open(path, "r", encoding=encoding, newline="") as handle:
                return list(csv.DictReader(handle)), encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    raise RuntimeError("无法解码文件 %s: %s" % (path, last_error))


def _load_cache(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def _save_cache(path, payload):
    """先写 .tmp 再原子替换，避免读到写了一半的缓存"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=1)
    os.replace(tmp_path, path)


def _clean_for_cache(payload):
    """缓存文件里只保存原始整理结果，不含每次请求才会变的展示字段"""
    return {key: value for key, value in payload.items() if key not in ("cached", "has_source")}


# ==================== 解析 ====================

def _read_margin_lookup(path):
    """读取『操作账户总持仓合约』，返回 {账号: {(合约, 方向): 持仓保证金}}

    该文件是两级结构：『展开』为「+」的是合约分组行（合约列是合约代码），
    其余是账号级子行（合约列是账号、合约名列是所属合约）。两种键都记下，
    取值时按『当前持仓』里的合约ID 或 合约名依次尝试。
    """
    if not path:
        return {}
    rows, _ = _read_csv_rows(path)

    lookup = {}
    group_code = ""
    for row in rows:
        normalized = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
        if normalized.get("展开", "").startswith("+"):
            group_code = normalized.get("合约", "")
            continue
        account = normalized.get("合约", "")
        direction = normalized.get("方向", "")
        contract = normalized.get("合约名", "")
        margin = _to_number(normalized.get("持仓保证金"))
        if not account or not direction or margin is None:
            continue
        table = lookup.setdefault(account, {})
        if contract:
            table[(contract, direction)] = margin
        if group_code:
            table[(group_code, direction)] = margin
    return lookup


def _lookup_margin(margin_lookup, account, row):
    """按（账号, 合约, 方向）取真实占用保证金，取不到返回 None"""
    table = (margin_lookup or {}).get(account)
    if not table:
        return None
    direction = row.get("买卖", "")
    for key in (row.get("合约ID", ""), row.get("合约名", "")):
        if key and (key, direction) in table:
            return table[(key, direction)]
    return None


def _parse_accounts(csv_path, source_name):
    rows, encoding = _read_csv_rows(csv_path)
    accounts = []
    total = None

    for row in rows:
        normalized = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
        account = normalized.get("账号") or normalized.get("帐号") or ""
        if not account:
            continue
        item = {
            "account": account,
            "name": normalized.get("姓名", ""),
            "equity": _to_number(normalized.get("动态权益")),
            "margin": _to_number(normalized.get("保证金")),
            "hold_profit": _to_number(normalized.get("持盈")),
            "close_profit": _to_number(normalized.get("平盈")),
        }
        if _is_total_account(account):
            total = item
        else:
            accounts.append(item)

    return {
        "source": source_name,
        "encoding": encoding,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total": total,
        "accounts": accounts,
    }


def _parse_positions(csv_path, account, source_name, margin_lookup=None):
    rows, encoding = _read_csv_rows(csv_path)
    positions = []

    for row in rows:
        normalized = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
        contract = normalized.get("合约ID") or normalized.get("合约") or ""
        if not contract:
            continue
        item = {}
        for column, is_number in POSITION_COLUMNS:
            item[column] = _to_number(normalized.get(column)) if is_number else normalized.get(column, "")
        # 『当前持仓』的占用保证金恒为 0，用『操作账户总持仓合约』里的真实值覆盖
        margin = _lookup_margin(margin_lookup, account, normalized)
        if margin is not None:
            item["占用保证金"] = margin
        positions.append(item)

    return {
        "account": account,
        "source": source_name,
        "encoding": encoding,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "positions": positions,
    }


def _trade_sort_key(trade):
    """成交明细排序键：按『同一交易日内的时间先后』，用于倒序展示（最新在最上）。

    融航成交明细只有 HH:MM:SS、没有日期，而一个交易日的先后是：
        前夜夜盘(20:00-23:59) → 跨零点夜盘(00:00-03:59) → 日盘(其余时段)
    所以不能拿裸时钟直接比大小（21:00 其实是该交易日最早的一批成交），
    要把夜盘放到日盘之前，才能得到正确的时序。
    """
    raw = str(trade.get("成交时间") or "").strip()
    matched = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", raw)
    if not matched:
        return (-1, 0.0)                      # 时间缺失/异常 → 排到最后
    hour = int(matched.group(1))
    minute = int(matched.group(2))
    second = int(matched.group(3) or 0)
    minutes = hour * 60 + minute + second / 60.0
    if hour >= 20:
        bucket = 0                            # 前夜夜盘：该交易日最早
    elif hour < 4:
        bucket = 1                            # 跨零点夜盘：次之
    else:
        bucket = 2                            # 日盘：该交易日最晚
    return (bucket, minutes)


def _parse_trades(csv_path, account, source_name):
    rows, encoding = _read_csv_rows(csv_path)
    trades = []

    for row in rows:
        normalized = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
        contract = normalized.get("合约") or normalized.get("合约名") or ""
        if not contract:
            continue
        item = {}
        for column, is_number in TRADE_COLUMNS:
            item[column] = _to_number(normalized.get(column)) if is_number else normalized.get(column, "")
        trades.append(item)

    # 按交易日内时间倒序：最新成交排最上面（同一时间的多笔保持原相对顺序）
    trades.sort(key=_trade_sort_key, reverse=True)

    return {
        "account": account,
        "source": source_name,
        "encoding": encoding,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trades": trades,
    }


def _list_position_files():
    """扫描数据目录，返回 {账号: 最新持仓文件路径}"""
    if not os.path.isdir(DATA_DIR):
        return {}
    grouped = {}
    for name in os.listdir(DATA_DIR):
        if not name.lower().endswith(".csv") or POSITION_FILE_SUFFIX not in name:
            continue
        account = name.split(POSITION_FILE_SUFFIX)[0].strip()
        if not account:
            continue
        grouped.setdefault(account, []).append(os.path.join(DATA_DIR, name))
    return {account: _pick_latest(paths) for account, paths in grouped.items()}


def _find_position_file(account, mapping=None):
    mapping = mapping if mapping is not None else _list_position_files()
    if account in mapping:
        return mapping[account]
    lowered = {key.lower(): value for key, value in mapping.items()}
    return lowered.get(account.lower())


def _list_trade_files():
    """扫描数据目录，返回 {账号: 最新成交文件路径}"""
    if not os.path.isdir(DATA_DIR):
        return {}
    grouped = {}
    for name in os.listdir(DATA_DIR):
        if not name.lower().endswith(".csv") or TRADE_FILE_SUFFIX not in name:
            continue
        account = name.split(TRADE_FILE_SUFFIX)[0].strip()
        if not account:
            continue
        grouped.setdefault(account, []).append(os.path.join(DATA_DIR, name))
    return {account: _pick_latest(paths) for account, paths in grouped.items()}


def _find_trade_file(account, mapping=None):
    mapping = mapping if mapping is not None else _list_trade_files()
    if account in mapping:
        return mapping[account]
    lowered = {key.lower(): value for key, value in mapping.items()}
    return lowered.get(account.lower())


def _position_cache_path(account):
    safe = re.sub(r"[^\w\-]", "_", account)
    return os.path.join(POSITION_CACHE_DIR, "%s.json" % safe)


def _trade_cache_path(account):
    safe = re.sub(r"[^\w\-]", "_", account)
    return os.path.join(TRADE_CACHE_DIR, "%s.json" % safe)


# ==================== 对外数据 ====================

def get_accounts():
    """账户列表：有新的『操作账户』文件就重建缓存，否则直接返回缓存"""
    source_path = _pick_latest(glob.glob(os.path.join(DATA_DIR, ACCOUNT_FILE_GLOB)))
    source_name = os.path.basename(source_path) if source_path else None
    cache_path = os.path.join(CACHE_DIR, "accounts.json")

    with _cache_lock:
        cached = _load_cache(cache_path)

        if cached and cached.get("source") == source_name:
            cached["cached"] = True
            cached["has_source"] = True
            return cached

        if source_path is None:
            # 数据目录里一份账户文件都没有：能用的缓存就先用着
            if cached:
                cached["cached"] = True
                cached["has_source"] = False
                return cached
            return {
                "source": None,
                "generated_at": None,
                "cached": False,
                "has_source": False,
                "total": None,
                "accounts": [],
            }

        payload = _parse_accounts(source_path, source_name)
        _save_cache(cache_path, _clean_for_cache(payload))
        logger.info("[缓存] 账户列表已重建：%s（%d 个账号）", source_name, len(payload["accounts"]))
        payload["cached"] = False
        payload["has_source"] = True
        return payload


def get_positions(account):
    """单账号持仓：有新的持仓文件就重建缓存，否则直接返回缓存"""
    source_path = _find_position_file(account)
    source_name = os.path.basename(source_path) if source_path else None
    margin_path = _pick_latest(glob.glob(os.path.join(DATA_DIR, TOTAL_POSITION_FILE_GLOB)))
    margin_source = os.path.basename(margin_path) if margin_path else None
    cache_path = _position_cache_path(account)

    with _cache_lock:
        cached = _load_cache(cache_path)

        if (cached and cached.get("source") == source_name
                and cached.get("margin_source") == margin_source):
            cached["cached"] = True
            cached["has_source"] = True
            return cached

        if source_path is None:
            if cached:
                cached["cached"] = True
                cached["has_source"] = False
                return cached
            return {
                "account": account,
                "source": None,
                "margin_source": margin_source,
                "generated_at": None,
                "cached": False,
                "has_source": False,
                "positions": [],
            }

        payload = _parse_positions(source_path, account, source_name,
                                   _read_margin_lookup(margin_path))
        payload["margin_source"] = margin_source
        _save_cache(cache_path, _clean_for_cache(payload))
        logger.info("[缓存] %s 持仓已重建：%s（%d 条）", account, source_name, len(payload["positions"]))
        payload["cached"] = False
        payload["has_source"] = True
        return payload


def get_trades(account):
    """单账号成交明细：有新的成交文件就重建缓存，否则直接返回缓存"""
    source_path = _find_trade_file(account)
    source_name = os.path.basename(source_path) if source_path else None
    cache_path = _trade_cache_path(account)

    with _cache_lock:
        cached = _load_cache(cache_path)

        if cached and cached.get("source") == source_name:
            cached["cached"] = True
            cached["has_source"] = True
            return cached

        if source_path is None:
            if cached:
                cached["cached"] = True
                cached["has_source"] = False
                return cached
            return {
                "account": account,
                "source": None,
                "generated_at": None,
                "cached": False,
                "has_source": False,
                "trades": [],
            }

        payload = _parse_trades(source_path, account, source_name)
        _save_cache(cache_path, _clean_for_cache(payload))
        logger.info("[缓存] %s 成交明细已重建：%s（%d 条）", account, source_name, len(payload["trades"]))
        payload["cached"] = False
        payload["has_source"] = True
        return payload


# ==================== 路由 ====================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/position/<path:account>")
def position_page(account):
    return render_template("position.html", account=account)


@app.route("/trades/<path:account>")
def trades_page(account):
    return render_template("trades.html", account=account)


@app.route("/api/accounts")
def api_accounts():
    data = get_accounts()
    known = {name.lower() for name in _list_position_files()}
    for item in data.get("accounts", []):
        item["has_position"] = item.get("account", "").lower() in known
    return jsonify(data)


@app.route("/api/positions/<account>")
def api_positions(account):
    return jsonify(get_positions(account))


@app.route("/api/trades/<account>")
def api_trades(account):
    return jsonify(get_trades(account))


if __name__ == "__main__":
    logger.info("数据目录: %s", DATA_DIR)
    logger.info("缓存目录: %s", CACHE_DIR)
    logger.info("监听地址: http://%s:%d", HOST, PORT)
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
