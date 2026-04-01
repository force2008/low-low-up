# @Project: https://github.com/Jedore/ctp.examples
# @File:    trading_time_config.py
# @Time:    2026/03/11
# @Author:  Assistant
# @Description: 各期货品种交易时间配置

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
    "IF": 240,  # 股指期货
    "IC": 240,  # 中证 500
    "IM": 240,  # 中证 1000
    "IH": 240,  # 上证 50
    "T": 240,   # 10 年期国债
    "TF": 240,  # 5 年期国债
    "TS": 240,  # 2 年期国债
    "TL": 240,  # 30 年期国债
    
    # ===== 上期所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "bu": 345,  # 沥青
    "ru": 345,  # 橡胶
    "zn": 345,  # 锌
    "pb": 345,  # 铅
    "al": 345,  # 铝
    "cu": 345,  # 铜
    
    # ===== 上期所 - 夜盘到 次日 1:00 (225+240=465 分钟) =====
    "ni": 465,  # 镍
    "sn": 465,  # 锡
    
    # ===== 上期所 - 夜盘到 次日 2:30 (225+330=555 分钟) =====
    "au": 555,  # 黄金
    "ag": 555,  # 白银
    "ss": 555,  # 不锈钢
    
    # 其他上期所品种 (默认 345 分钟)
    "rb": 345,  # 螺纹钢
    "hc": 345,  # 热卷
    "fu": 345,  # 燃油
    "sp": 345,  # 纸浆
    "br": 345,  # 氧化铝
    "ao": 345,  # 原油 (上期所)
    "op": 345,  # 期权相关
    
    # ===== 能源中心 - 同上期所 =====
    "sc": 345,  # 原油
    "lu": 345,  # 低硫燃油
    "bc": 345,  # 国际铜
    "ec": 345,  # 集运欧线
    "nr": 345,  # 20 号胶
    
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
    
    # 大商所 - 无夜盘
    "c": 225,   # 玉米
    "cs": 225,  # 玉米淀粉
    "jd": 225,  # 鸡蛋
    
    # 大商所 - 夜盘到 次日 1:30 (225+270=495 分钟)
    "i": 495,   # 铁矿石
    "j": 495,   # 焦炭
    "jm": 495,  # 焦煤
    "lh": 495,  # 生猪
    
    # 其他大商所品种
    "pg": 345,  # 液化气
    "rr": 345,  # 粳米
    "fb": 345,  # 纤维板
    "bb": 345,  # 胶合板
    "lg": 345,  # 原木
    
    # ===== 郑商所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "CF": 345,  # 棉花
    "RM": 345,  # 菜粕
    "MA": 345,  # 甲醇
    "SR": 345,  # 白糖
    "TA": 345,  # PTA
    "OI": 345,  # 菜油
    "FG": 345,  # 玻璃
    "SA": 345,  # 纯碱
    "AP": 345,  # 苹果
    
    # 郑商所 - 无夜盘
    "CJ": 225,  # 红枣
    "CY": 225,  # 棉纱
    "JR": 225,  # 粳稻
    "PM": 225,  # 普麦
    "RS": 225,  # 菜籽
    "WH": 225,  # 强麦
    "ZC": 225,  # 动力煤
    
    # 郑商所 - 夜盘到 次日 1:30 (225+270=495 分钟)
    "SM": 495,  # 硅锰
    "SF": 495,  # 硅铁
    "PX": 495,  # 对二甲苯
    "PR": 495,  # 早籼稻
    "PF": 495,  # 短纤
    "PK": 495,  # 花生
    "PL": 495,  # 烧碱
    "SH": 495,  # 尿素
    "UR": 495,  # 尿素
    
    # ===== 广期所 - 夜盘到 23:00 (225+120=345 分钟) =====
    "lc": 345,  # 碳酸锂
    "si": 345,  # 工业硅
    
    # 广期所 - 新品种
    "ps": 345,  # 多晶硅
    "pt": 345,  # 铂
    "pd": 345,  # 钯
    
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