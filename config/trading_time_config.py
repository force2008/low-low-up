# @Project: https://github.com/Jedore/ctp.examples
# @File:    trading_time_config.py
# @Time:    2026/03/11
# @Author:  Assistant
# @Description: 各期货品种交易时间配置

import re
from datetime import datetime, time
from typing import Dict, List, Optional

# ==================== 交易时间配置 ====================
# 各品种每日交易分钟数（用于计算年化因子）
# 格式：{产品代码：每日交易分钟数}
# 日盘统一时间：9:00-10:15 (75 分钟) + 10:30-11:30 (60 分钟) + 13:30-15:00 (90 分钟) = 225 分钟
# 夜盘分三档：
#   - 21:00-23:00 (120 分钟): 大部分商品
#   - 21:00-次日 1:00 (240 分钟): 部分活跃品种
#   - 21:00-次日 2:30 (330 分钟): 贵金属、铜、铝等
# 中金所无夜盘：9:30-11:30 (120 分钟) + 13:00-15:00 (120 分钟) = 240 分钟

PRODUCT_TRADING_MINUTES = {
    # ===== 中金所 (无夜盘) =====
    # 股指期货：09:30-11:30, 13:00-15:00 = 240 分钟
    "IF": 240,
    "IC": 240,
    "IM": 240,
    "IH": 240,
    # 国债期货：09:15-11:30, 13:00-15:15 = 255 分钟
    "T": 255,
    "TF": 255,
    "TS": 255,
    "TL": 255,

    # ===== 上期所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "bu": 345,  # 沥青
    "ru": 345,  # 橡胶
    "rb": 345,  # 螺纹钢
    "hc": 345,  # 热卷
    "fu": 345,  # 燃油
    "sp": 345,  # 纸浆
    "br": 345,  # 丁二烯橡胶

    # ===== 上期所 - 夜盘到 次日 1:00 (225+240=465 分钟) =====
    "cu": 465,  # 铜
    "al": 465,  # 铝
    "zn": 465,  # 锌
    "pb": 465,  # 铅
    "ni": 465,  # 镍
    "sn": 465,  # 锡
    "ss": 465,  # 不锈钢
    "ao": 465,  # 氧化铝

    # ===== 上期所 - 夜盘到 次日 2:30 (225+330=555 分钟) =====
    "au": 555,  # 黄金
    "ag": 555,  # 白银

    # ===== 上期所 - 无夜盘 (225 分钟) =====
    "wr": 225,  # 线材

    # ===== 能源中心 - 夜盘到 23:00 (225+120=345 分钟) =====
    "lu": 345,  # 低硫燃油
    "nr": 345,  # 20 号胶

    # ===== 能源中心 - 夜盘到 次日 1:00 (225+240=465 分钟) =====
    "bc": 465,  # 国际铜

    # ===== 能源中心 - 夜盘到 次日 2:30 (225+330=555 分钟) =====
    "sc": 555,  # 原油

    # ===== 能源中心 - 无夜盘 (225 分钟) =====
    "ec": 225,  # 集运欧线

    # ===== 大商所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "m": 345,   # 豆粕
    "a": 345,   # 豆一
    "b": 345,   # 豆二
    "p": 345,   # 棕榈油
    "y": 345,   # 豆油
    "l": 345,   # 塑料
    "pp": 345,  # 聚丙烯
    "v": 345,   # PVC
    "eg": 345,  # 乙二醇
    "eb": 345,  # 苯乙烯
    "c": 345,   # 玉米
    "cs": 345,  # 玉米淀粉
    "pg": 345,  # 液化气
    "rr": 345,  # 粳米
    "i": 345,   # 铁矿石
    "j": 345,   # 焦炭
    "jm": 345,  # 焦煤
    "bz": 345,  # 纯苯

    # ===== 大商所 - 无夜盘 (225 分钟) =====
    "jd": 225,  # 鸡蛋
    "lh": 225,  # 生猪
    "fb": 225,  # 纤维板
    "bb": 225,  # 胶合板
    "lg": 225,  # 原木
    "pk": 225,  # 花生

    # ===== 郑商所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "CF": 345,  # 棉花
    "RM": 345,  # 菜粕
    "MA": 345,  # 甲醇
    "SR": 345,  # 白糖
    "TA": 345,  # PTA
    "OI": 345,  # 菜油
    "FG": 345,  # 玻璃
    "SA": 345,  # 纯碱
    "PX": 345,  # 对二甲苯
    "PF": 345,  # 短纤
    "PR": 345,  # 瓶片
    "PL": 345,  # 丙烯
    "SH": 345,  # 烧碱

    # ===== 郑商所 - 无夜盘 (225 分钟) =====
    "AP": 225,  # 苹果
    "UR": 225,  # 尿素
    "CJ": 225,  # 红枣
    "CY": 225,  # 棉纱
    "JR": 225,  # 粳稻
    "PM": 225,  # 普麦
    "RS": 225,  # 菜籽
    "WH": 225,  # 强麦
    "ZC": 225,  # 动力煤
    "SM": 225,  # 硅锰
    "SF": 225,  # 硅铁

    # ===== 广期所 - 无夜盘 (225 分钟) =====
    "lc": 225,  # 碳酸锂
    "si": 225,  # 工业硅
    "ps": 225,  # 多晶硅
    "pt": 225,  # 铂
    "pd": 225,  # 钯

    # ===== 上期所新品种 =====
    "ad": 465,  # 铸造铝合金，夜盘到次日 01:00

    # 默认值（无夜盘品种）
    "DEFAULT": 240,
}

# 每年交易日数（扣除周末和节假日）
TRADING_DAYS_PER_YEAR = 242


def get_annual_factor(product_id: str = None) -> float:
    """
    获取年化因子（将 5 分钟波动率年化）
    
    计算逻辑：
    1. 获取品种每日交易分钟数
    2. 计算每年交易分钟数 = 每日分钟数 × 交易日数
    3. 年化因子 = sqrt(每年交易分钟数 / 5)
    
    Args:
        product_id: 产品代码（如 "IC", "au", "m" 等）
        
    Returns:
        float: 年化因子
    """
    if product_id:
        daily_minutes = PRODUCT_TRADING_MINUTES.get(product_id, PRODUCT_TRADING_MINUTES["DEFAULT"])
    else:
        daily_minutes = PRODUCT_TRADING_MINUTES["DEFAULT"]
    
    annual_minutes = daily_minutes * TRADING_DAYS_PER_YEAR
    annual_factor = np.sqrt(annual_minutes / 5)
    return annual_factor


def get_trading_minutes_for_product(product_id: str) -> int:
    """
    获取某品种的每日交易分钟数
    
    Args:
        product_id: 产品代码
        
    Returns:
        int: 每日交易分钟数
    """
    return PRODUCT_TRADING_MINUTES.get(product_id, PRODUCT_TRADING_MINUTES["DEFAULT"])


def get_trading_hours_info(product_id: str) -> dict:
    """
    获取某品种的交易时间详细信息
    
    Args:
        product_id: 产品代码
        
    Returns:
        dict: 交易时间信息 {daily_minutes, night_session, night_end_time}
    """
    daily_minutes = get_trading_minutes_for_product(product_id)
    
    # 根据分钟数推断夜盘信息
    if daily_minutes <= 240:
        return {
            "daily_minutes": daily_minutes,
            "has_night": False,
            "night_end_time": None,
            "description": "无夜盘"
        }
    elif daily_minutes == 255:
        return {
            "daily_minutes": daily_minutes,
            "has_night": False,
            "night_end_time": None,
            "description": "中金所国债期货（无夜盘）"
        }
    elif daily_minutes == 345:
        return {
            "daily_minutes": daily_minutes,
            "has_night": True,
            "night_end_time": "23:00",
            "description": "夜盘到 23:00"
        }
    elif daily_minutes == 465:
        return {
            "daily_minutes": daily_minutes,
            "has_night": True,
            "night_end_time": "01:00",
            "description": "夜盘到次日 01:00"
        }
    elif daily_minutes == 495:
        return {
            "daily_minutes": daily_minutes,
            "has_night": True,
            "night_end_time": "01:30",
            "description": "夜盘到次日 01:30"
        }
    elif daily_minutes == 555:
        return {
            "daily_minutes": daily_minutes,
            "has_night": True,
            "night_end_time": "02:30",
            "description": "夜盘到次日 02:30"
        }
    else:
        return {
            "daily_minutes": daily_minutes,
            "has_night": daily_minutes > 240,
            "night_end_time": "未知",
            "description": "自定义"
        }


# 如果需要导入 numpy
try:
    import numpy as np
except ImportError:
    print("警告：numpy 未安装，请运行 pip install numpy")


# ==================== 实时交易时段判断 ====================
# 中金所股指期货日盘时段
_CFFEX_STOCK_INDEX_DAY_SESSIONS = [
    (time(9, 30), time(11, 30)),
    (time(13, 0), time(15, 0)),
]

# 中金所国债期货日盘时段
_CFFEX_TREASURY_DAY_SESSIONS = [
    (time(9, 15), time(11, 30)),
    (time(13, 0), time(15, 15)),
]

# 商品期货（除中金所外）默认日盘时段
_COMMODITY_DAY_SESSIONS = [
    (time(9, 0), time(10, 15)),
    (time(10, 30), time(11, 30)),
    (time(13, 30), time(15, 0)),
]

# 夜盘统一开始时间
_NIGHT_START = time(21, 0)

# 中金所产品代码
_CFFEX_STOCK_INDEX_PRODUCTS = {"IF", "IC", "IM", "IH"}
_CFFEX_TREASURY_PRODUCTS = {"T", "TF", "TS", "TL"}


def _extract_product_id(instrument_id: str) -> str:
    """从合约代码中提取产品代码。

    例如：m2609 -> m, zn2610 -> zn, CF509 -> CF, IF2609 -> IF
    """
    if not instrument_id:
        return ""
    m = re.match(r"^([A-Za-z]+)", instrument_id.strip())
    return m.group(1) if m else ""


def _is_in_sessions(t: time, sessions: List[tuple]) -> bool:
    """判断当前时间是否落在任一交易时段内（闭区间）"""
    for start, end in sessions:
        if start <= t <= end:
            return True
    return False


def is_contract_trading_now(instrument_id: str, now: Optional[datetime] = None) -> bool:
    """判断指定合约在当前时刻是否处于可交易时段。

    Args:
        instrument_id: 合约代码，如 "m2609", "zn2610", "IF2609"
        now: 可选，指定判断的时间点；默认使用 datetime.now()

    Returns:
        bool: 可交易返回 True，否则返回 False
    """
    product = _extract_product_id(instrument_id)
    if not product:
        return False

    info = get_trading_hours_info(product)
    if now is None:
        now = datetime.now()
    t = now.time()

    # 日盘判断
    if product in _CFFEX_STOCK_INDEX_PRODUCTS:
        day_sessions = _CFFEX_STOCK_INDEX_DAY_SESSIONS
    elif product in _CFFEX_TREASURY_PRODUCTS:
        day_sessions = _CFFEX_TREASURY_DAY_SESSIONS
    else:
        day_sessions = _COMMODITY_DAY_SESSIONS
    if _is_in_sessions(t, day_sessions):
        return True

    # 夜盘判断（跨午夜处理）
    if info.get("has_night") and info.get("night_end_time"):
        try:
            end_h, end_m = map(int, info["night_end_time"].split(":"))
            night_end = time(end_h, end_m)
        except Exception:
            return False

        if _NIGHT_START <= night_end:
            # 不跨午夜（实际所有夜盘都跨午夜，保留兼容）
            return _NIGHT_START <= t <= night_end
        else:
            # 跨午夜：21:00 之后 或 结束时间之前都算
            return t >= _NIGHT_START or t <= night_end

    return False


def get_contracts_trading_status(instrument_ids: List[str], now: Optional[datetime] = None) -> Dict[str, bool]:
    """批量判断多个合约当前是否可交易。

    Returns:
        {instrument_id: True/False}
    """
    return {inst: is_contract_trading_now(inst, now) for inst in instrument_ids}
