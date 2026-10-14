# -*- coding: utf-8 -*-
"""
交易时段配置、CTP 常量、项目路径
"""

import os
import sys
from datetime import time as dt_time
from typing import Dict, List, Tuple

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# =====================================================================
# 交易时段配置（基于中国期货市场实际规则）
# =====================================================================

def _sess(start_h: int, start_m: int, end_h: int, end_m: int) -> Tuple[dt_time, dt_time]:
    """快速构造时段元组，支持跨午夜"""
    return (dt_time(start_h, start_m), dt_time(end_h, end_m))


# 日盘统一三段（中金所股指/国债除外）
DAY_3SEG = [_sess(9, 0, 10, 15), _sess(10, 30, 11, 30), _sess(13, 30, 15, 0)]

# 中金所股指日盘
CFFEX_INDEX_DAY = [_sess(9, 30, 11, 30), _sess(13, 0, 15, 0)]
# 中金所国债日盘
CFFEX_BOND_DAY = [_sess(9, 30, 11, 30), _sess(13, 0, 15, 15)]

# 夜盘（跨午夜的在判断逻辑中自动处理）
NIGHT_2300 = _sess(21, 0, 23, 0)
NIGHT_0100 = _sess(21, 0, 1, 0)
NIGHT_0230 = _sess(21, 0, 2, 30)

# 按 ProductID 聚合的完整时段映射
PRODUCT_TRADING_SESSIONS: Dict[str, List] = {
    # === 中金所 - 股指（无夜盘） ===
    "IF": CFFEX_INDEX_DAY,
    "IC": CFFEX_INDEX_DAY,
    "IM": CFFEX_INDEX_DAY,
    "IH": CFFEX_INDEX_DAY,
    # === 中金所 - 国债（无夜盘） ===
    "T": CFFEX_BOND_DAY,
    "TF": CFFEX_BOND_DAY,
    "TS": CFFEX_BOND_DAY,
    "TL": CFFEX_BOND_DAY,

    # === 上期所 - 夜盘到 23:00 ===
    "bu": DAY_3SEG + [NIGHT_2300],
    "ru": DAY_3SEG + [NIGHT_2300],
    "rb": DAY_3SEG + [NIGHT_2300],
    "hc": DAY_3SEG + [NIGHT_2300],
    "fu": DAY_3SEG + [NIGHT_2300],
    "sp": DAY_3SEG + [NIGHT_2300],
    "br": DAY_3SEG + [NIGHT_2300],
    # === 上期所 - 夜盘到 01:00 ===
    "ao": DAY_3SEG + [NIGHT_0100],
    "cu": DAY_3SEG + [NIGHT_0100],
    "al": DAY_3SEG + [NIGHT_0100],
    "zn": DAY_3SEG + [NIGHT_0100],
    "pb": DAY_3SEG + [NIGHT_0100],
    "ni": DAY_3SEG + [NIGHT_0100],
    "sn": DAY_3SEG + [NIGHT_0100],
    "ss": DAY_3SEG + [NIGHT_0100],
    "ad": DAY_3SEG + [NIGHT_0100],
    # === 上期所 - 夜盘到 02:30 ===
    "au": DAY_3SEG + [NIGHT_0230],
    "ag": DAY_3SEG + [NIGHT_0230],
    # === 上期所 - 无夜盘 ===
    "wr": DAY_3SEG,

    # === 能源中心 - 夜盘到 23:00 ===
    "lu": DAY_3SEG + [NIGHT_2300],
    "nr": DAY_3SEG + [NIGHT_2300],
    # === 能源中心 - 夜盘到 01:00 ===
    "bc": DAY_3SEG + [NIGHT_0100],
    # === 能源中心 - 夜盘到 02:30 ===
    "sc": DAY_3SEG + [NIGHT_0230],
    # === 能源中心 - 无夜盘（指数类） ===
    "ec": DAY_3SEG,

    # === 大商所 - 夜盘到 23:00 ===
    "m": DAY_3SEG + [NIGHT_2300],
    "a": DAY_3SEG + [NIGHT_2300],
    "b": DAY_3SEG + [NIGHT_2300],
    "p": DAY_3SEG + [NIGHT_2300],
    "y": DAY_3SEG + [NIGHT_2300],
    "l": DAY_3SEG + [NIGHT_2300],
    "pp": DAY_3SEG + [NIGHT_2300],
    "v": DAY_3SEG + [NIGHT_2300],
    "eg": DAY_3SEG + [NIGHT_2300],
    "eb": DAY_3SEG + [NIGHT_2300],
    "pg": DAY_3SEG + [NIGHT_2300],
    "rr": DAY_3SEG + [NIGHT_2300],
    "i": DAY_3SEG + [NIGHT_2300],
    "j": DAY_3SEG + [NIGHT_2300],
    "jm": DAY_3SEG + [NIGHT_2300],
    "bz": DAY_3SEG + [NIGHT_2300],
    "c": DAY_3SEG + [NIGHT_2300],
    "cs": DAY_3SEG + [NIGHT_2300],
    # === 大商所 - 无夜盘 ===
    "jd": DAY_3SEG,
    "fb": DAY_3SEG,
    "bb": DAY_3SEG,
    "lg": DAY_3SEG,
    "lh": DAY_3SEG,

    # === 郑商所 - 夜盘到 23:00 ===
    "CF": DAY_3SEG + [NIGHT_2300],
    "RM": DAY_3SEG + [NIGHT_2300],
    "MA": DAY_3SEG + [NIGHT_2300],
    "SR": DAY_3SEG + [NIGHT_2300],
    "TA": DAY_3SEG + [NIGHT_2300],
    "OI": DAY_3SEG + [NIGHT_2300],
    "FG": DAY_3SEG + [NIGHT_2300],
    "SA": DAY_3SEG + [NIGHT_2300],
    "PX": DAY_3SEG + [NIGHT_2300],
    "PR": DAY_3SEG + [NIGHT_2300],
    "PF": DAY_3SEG + [NIGHT_2300],
    "PL": DAY_3SEG + [NIGHT_2300],
    "SH": DAY_3SEG + [NIGHT_2300],
    "CY": DAY_3SEG + [NIGHT_2300],
    # === 郑商所 - 无夜盘 ===
    "UR": DAY_3SEG,
    "SM": DAY_3SEG,
    "SF": DAY_3SEG,
    "PK": DAY_3SEG,
    "AP": DAY_3SEG,
    "CJ": DAY_3SEG,
    "JR": DAY_3SEG,
    "PM": DAY_3SEG,
    "RS": DAY_3SEG,
    "WH": DAY_3SEG,
    "ZC": DAY_3SEG,

    # === 广期所 - 无夜盘 ===
    "lc": DAY_3SEG,
    "si": DAY_3SEG,
    "ps": DAY_3SEG,
    "pt": DAY_3SEG,
    "pd": DAY_3SEG,
}