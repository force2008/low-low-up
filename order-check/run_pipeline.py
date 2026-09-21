#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键流水线：自动导出 -> 多账户持仓同步

使用方式:
    python run_pipeline.py [online|simu|7x24] [--skip-time-check] [--force] [--ratio RATIO]

说明:
    - 默认只在配置的交易时段内执行导出与同步
    - --skip-time-check: 开发/测试时显式跳过交易时段检查，允许非交易时段运行
    - --force: 非交易日也强制运行
    - --ratio: 持仓同步比例，默认 1.0
    - 多账户同步：在 order-check/account_targets.py 中配置源账户到目标CTP账户的映射
"""

import sys
import os
import time
import datetime
import json
import logging
import threading
import faulthandler
import atexit
import importlib
from logging.handlers import RotatingFileHandler
from typing import Optional, Tuple, List, Dict, Set, Union, Any

# 启用 C 级崩溃转储：在 SIGSEGV / access violation 时把 Python 栈写到 stderr / 日志
faulthandler.enable()
# 每 30 秒把各线程栈覆写到 crash dump（覆盖写，防止文件无限膨胀）
_crash_timer_running = [False]
_crash_dump_timer_ref = [None]

def _stop_crash_dump_timer():
    """停止 crash dump 递归调度，确保进程能真正退出。"""
    _crash_timer_running[0] = False
    try:
        t = _crash_dump_timer_ref[0]
        if t is not None:
            t.cancel()
            _crash_dump_timer_ref[0] = None
    except Exception:
        pass

try:
    _crash_dump_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "logs", "pipeline_crash.dump"
    )
    os.makedirs(os.path.dirname(_crash_dump_path), exist_ok=True)
    _crash_timer_running = [True]

    def _write_crash_dump():
        if not _crash_timer_running[0]:
            return
        try:
            with open(_crash_dump_path, "w", encoding="utf-8") as f:
                faulthandler.dump_traceback_all(f)
        except Exception:
            pass
        # 最多写 5MB，超出则清空
        try:
            if os.path.getsize(_crash_dump_path) > 5 * 1024 * 1024:
                with open(_crash_dump_path, "w", encoding="utf-8") as f:
                    f.write("")
        except Exception:
            pass

    def _schedule_next_dump():
        if not _crash_timer_running[0]:
            return
        _write_crash_dump()
        if not _crash_timer_running[0]:
            return
        t = threading.Timer(30, _schedule_next_dump)
        t.daemon = True
        _crash_dump_timer_ref[0] = t
        t.start()

    _initial_timer = threading.Timer(30, _schedule_next_dump)
    _initial_timer.daemon = True
    _crash_dump_timer_ref[0] = _initial_timer
    _initial_timer.start()

    # 进程正常退出前（非交易日、sys.exit、atexit），主动取消调度链，避免 Python 因非 daemon Timer 存活而不退出
    atexit.register(_stop_crash_dump_timer)
except Exception:
    pass

# ==================== 交易日检查 ====================
# 交易日列表文件路径
_TRADE_DATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "trade_date.json"
)
# 解析强制运行参数（用于非交易日测试）
_FORCE_RUN = "--force" in sys.argv
if _FORCE_RUN:
    sys.argv.remove("--force")  # 移除，避免影响后续参数解析

# 解析跳过交易时段检查参数（开发/测试环境可在非交易时段运行）
_SKIP_TIME_CHECK = "--skip-time-check" in sys.argv
if _SKIP_TIME_CHECK:
    sys.argv.remove("--skip-time-check")  # 移除，避免影响后续参数解析

# 解析持仓比例参数
_POSITION_RATIO = 1.0
if "--ratio" in sys.argv:
    idx = sys.argv.index("--ratio")
    if idx + 1 < len(sys.argv):
        try:
            _POSITION_RATIO = float(sys.argv[idx + 1])
            sys.argv.pop(idx)      # 移除 --ratio
            sys.argv.pop(idx)      # 移除值
        except ValueError:
            print("[警告] --ratio 参数值无效，使用默认值 1.0")
    else:
        print("[警告] --ratio 参数缺少值，使用默认值 1.0")


def _is_trading_day() -> bool:
    """检查今天是否为交易日

    Returns:
        True: 今天可以运行
        False: 今天不能运行，程序应该退出
    """
    today = datetime.date.today().isoformat()

    # 强制运行模式
    if _FORCE_RUN:
        logger.info(f"[交易日] 强制运行模式，今日({today})作为交易日处理")
        return True

    # 检查交易日文件是否存在
    if not os.path.exists(_TRADE_DATE_FILE):
        logger.warning(f"[交易日] 交易日文件不存在: {_TRADE_DATE_FILE}，跳过检查")
        return True

    try:
        with open(_TRADE_DATE_FILE, 'r', encoding='utf-8') as f:
            trade_dates = json.load(f)
        trade_dates_set = set(trade_dates)

        if today in trade_dates_set:
            logger.info(f"[交易日] 今日({today})是交易日，可以运行")
            return True
        else:
            logger.warning(f"[交易日] 今日({today})不是交易日，程序退出")
            print(f"[交易日] 今日({today})不是交易日，程序退出")
            print(f"如需强制运行，请使用: python run_pipeline.py {' '.join(sys.argv[1:])} --force")
            return False
    except Exception as e:
        logger.error(f"[交易日] 读取交易日文件失败: {e}")
        return True  # 出错时允许运行

# 确保当前目录在模块搜索路径中
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _CURR_DIR)

# 项目根目录
PROJECT_ROOT = os.path.dirname(_CURR_DIR)
sys.path.insert(0, PROJECT_ROOT)

# 导入本地配置（源账户名称、坐标等）
try:
    from local_config import ACCOUNT
except ImportError:
    ACCOUNT = ""

# 导入源账户到目标账户的映射配置（多账户仓位同步）
try:
    from account_targets import ACCOUNT_TARGETS
except ImportError:
    ACCOUNT_TARGETS = {}

# ==================== 日志配置 ====================
LOG_DIR = os.path.join(_CURR_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

for h in logger.handlers:
    h.flush()
# =================================================

# ==================== CTP 环境检测 ====================
try:
    from config import config as _ctp_config
    _CTP_ENV_NAME = "7x24"
    if len(sys.argv) > 1 and sys.argv[1].lower() in _ctp_config.envs:
        _CTP_ENV_NAME = sys.argv[1].lower()
    elif os.getenv("CTP_ENV") in _ctp_config.envs:
        _CTP_ENV_NAME = os.getenv("CTP_ENV")
    # 默认所有环境都检查交易时段，只有显式传入 --skip-time-check 才允许非交易时段运行
    SKIP_TRADING_TIME_CHECK = _SKIP_TIME_CHECK
except ImportError:
    _CTP_ENV_NAME = "7x24"
    # 默认所有环境都检查交易时段，只有显式传入 --skip-time-check 才允许非交易时段运行
    SKIP_TRADING_TIME_CHECK = _SKIP_TIME_CHECK
# =================================================

# ==================== 交易时间段配置 ====================
# 注意：这里控制的是流水线是否允许执行导出/同步。
# 不同品种的具体交易时段由 config/trading_time_config.py 控制，
# 非当前交易时段的合约会在同步时被跳过。
TRADING_SESSIONS = [
    ("09:00:15", "11:30:00"),    # 日盘上午（商品 09:00 起，中金所 09:15/09:30 起，由品种过滤处理）
    ("13:00:15", "15:15:00"),    # 日盘下午（中金所 13:00 起，商品 13:30 起，由品种过滤处理）
    ("21:00:15", "23:59:00"),    # 夜盘（跨午夜）
    ("00:00:15", "02:30:00"),    # 夜盘（跨午夜续）
]
CHECK_INTERVAL = 10  # 秒（导出线程轮询间隔）
# 注意：真正的同步冷却在 trading/position_sync/sync.py sync_and_trade() 里 SYNC_COOLDOWN=15 秒；
# 此处常量仅作注释参考，同步循环实际按 hold-std.json 文件变更事件触发（约 2 秒扫描一次）。
SYNC_INTERVAL = 15    # 持仓同步冷却参考值（秒）

# 持仓同步比例：1.0=全仓，0.5=半仓；可通过 --ratio 覆盖
POSITION_RATIO = _POSITION_RATIO


# 关键时间点强制对齐（格式：HH:MM）
KEY_ALIGN_TIMES = ["14:58", "14:59", "15:00"]

# 心跳文件：用于外部判断进程是存活还是僵死
_HEARTBEAT_FILE = os.path.join(_CURR_DIR, "logs", ".pipeline_heartbeat")


def _write_heartbeat(status: str = "running"):
    """写入心跳文件，包含 PID、时间戳和当前状态。"""
    try:
        with open(_HEARTBEAT_FILE, "w", encoding="utf-8") as f:
            f.write(
                f"pid={os.getpid()}\n"
                f"timestamp={datetime.datetime.now().isoformat()}\n"
                f"status={status}\n"
            )
    except Exception as e:
        logger.warning("[心跳] 写入心跳文件失败: %s", e)


def _mark_exiting(reason: str = "unknown"):
    """标记进程正在退出。"""
    try:
        with open(_HEARTBEAT_FILE, "w", encoding="utf-8") as f:
            f.write(
                f"pid={os.getpid()}\n"
                f"timestamp={datetime.datetime.now().isoformat()}\n"
                f"status=exiting\n"
                f"reason={reason}\n"
            )
    except Exception:
        pass


def _heartbeat_loop():
    """后台心跳线程：每 10 秒更新一次心跳文件。"""
    while not shutdown_event.is_set():
        _write_heartbeat()
        shutdown_event.wait(timeout=10)


# ==================== 单实例检测 ====================
_INSTANCE_LOCK_FILE = os.path.join(_CURR_DIR, '.pipeline_instance.lock')

def check_single_instance():
    """检查是否已有实例在运行，防止多实例同时启动"""
    if sys.platform == 'win32':
        import msvcrt
        try:
            fd = os.open(_INSTANCE_LOCK_FILE, os.O_CREAT | os.O_RDWR)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            # 成功获取锁，写入 PID
            os.write(fd, str(os.getpid()).encode())
            # 保持文件句柄打开，维持锁
            return fd
        except (IOError, OSError):
            # 已有实例在运行
            return None
    else:
        import fcntl
        try:
            fd = os.open(_INSTANCE_LOCK_FILE, os.O_CREAT | os.O_RDWR)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.write(fd, str(os.getpid()).encode())
            return fd
        except (IOError, OSError):
            return None

def release_single_instance(fd):
    """释放单实例锁"""
    if fd is not None:
        try:
            if sys.platform == 'win32':
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            os.close(fd)
        except Exception:
            pass
# =================================================

# 尝试导入飞书 webhook
try:
    from compare_orders import FEISHU_WEBHOOK_URL
except ImportError:
    FEISHU_WEBHOOK_URL = ""


def send_feishu_text(text):
    """发送纯文本飞书通知"""
    if not FEISHU_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {"msg_type": "text", "content": {"text": text}}
        resp = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=10)
        logger.info("飞书通知发送状态: %s", resp.status_code)
    except Exception as e:
        logger.error("飞书通知发送失败: %s", e)


def _dismiss_startup_popup():
    """20:59 启动时，点击 local_config 配置窗口上的启动弹框。

    仅在启动时间接近 20:59:00（前后 90 秒内）执行一次，在早盘8:59启动时也做一下这个窗口的点击操作
    避免其他时段启动时误点。
    """
    try:
        now = datetime.datetime.now()
        target = now.replace(hour=20, minute=59, second=0, microsecond=0)
        morningStart = now.replace(hour=8, minute=59, second=0, microsecond=0)
        diff = abs((now - target).total_seconds())
        morningDiff = abs((now-morningStart).total_seconds())
        if diff > 90 or morningDiff>90:
            return

        from local_config import APP_TITLE
        import pyautogui

        logger.info("[启动弹框] 20:59 启动，准备点击弹框")
        windows = pyautogui.getWindowsWithTitle(APP_TITLE)
        if not windows:
            logger.warning("[启动弹框] 未找到窗口: %s", APP_TITLE)
            return
        window = windows[0]
        if window.isMinimized:
            window.restore()
            time.sleep(0.3)
        if not window.isActive:
            window.activate()
            time.sleep(0.5)
        pyautogui.click(1091, 744)
        logger.info("[启动弹框] 已点击")
        time.sleep(0.5)
    except Exception as e:
        logger.warning("[启动弹框] 点击失败或无需点击: %s", e)


def _cleanup_export_files():
    """清理 DEFAULT_SAVE_PATH 目录下的旧导出文件，防止文件夹无限增长。

    保留今天的文件（今天日期开头的），删除昨天及更早的文件。
    仅在夜盘结束（02:30）退出时调用一次。
    """
    import re
    try:
        from local_config import DEFAULT_SAVE_PATH
        if not DEFAULT_SAVE_PATH or not os.path.isdir(DEFAULT_SAVE_PATH):
            logger.warning("[清理] DEFAULT_SAVE_PATH 不存在或不是目录: %s", DEFAULT_SAVE_PATH)
            return

        # 获取今天和昨天的日期字符串（格式：YYYY-MM-DD）
        today = datetime.date.today().isoformat()  # e.g. "2026-09-02"
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()  # e.g. "2026-09-01"

        removed_count = 0
        kept_count = 0
        failed_paths = []

        for entry in os.scandir(DEFAULT_SAVE_PATH):
            if entry.is_file():
                # 文件名格式："资金账户申报费监控 2026-09-02 09-35-59"
                # 提取文件名中的日期部分
                match = re.search(r'(\d{4}-\d{2}-\d{2})', entry.name)
                if not match:
                    # 没有日期格式，保留
                    kept_count += 1
                    continue
                file_date = match.group(1)
                # 只删除昨天及更早的文件，保留今天的
                if file_date < today:
                    try:
                        os.remove(entry.path)
                        removed_count += 1
                    except Exception as e:
                        failed_paths.append((entry.path, str(e)))
                else:
                    kept_count += 1

        logger.info("[清理] 已删除 %d 个旧导出文件，保留 %d 个: %s", removed_count, kept_count, DEFAULT_SAVE_PATH)
        if failed_paths:
            for path, err in failed_paths[:5]:
                logger.warning("[清理] 删除失败: %s - %s", path, err)
    except Exception as e:
        logger.warning("[清理] 清理导出文件失败: %s", e)


def is_in_trading_time():
    """判断当前是否在配置的交易时间段内"""
    now = datetime.datetime.now().time()
    for start_str, end_str in TRADING_SESSIONS:
        start = datetime.datetime.strptime(start_str, "%H:%M:%S").time()
        end = datetime.datetime.strptime(end_str, "%H:%M:%S").time()
        if start <= end:
            if start <= now <= end:
                return True
        else:
            if now >= start or now <= end:
                return True
    return False


def seconds_until_next_session():
    """计算距离下一个交易时段开始还有多少秒"""
    now = datetime.datetime.now()
    now_time = now.time()
    candidates = []
    for start_str, _ in TRADING_SESSIONS:
        start_time = datetime.datetime.strptime(start_str, "%H:%M:%S").time()
        start_dt = datetime.datetime.combine(now.date(), start_time)
        if start_time > now_time:
            candidates.append(start_dt)
        else:
            candidates.append(start_dt + datetime.timedelta(days=1))
    if candidates:
        next_dt = min(candidates)
        return int((next_dt - now).total_seconds())
    return 3600


def get_target_session_end(now_time):
    """根据当前时间，返回本次任务应负责到的时段结束时间。

    多时段定时任务模式：08:59/12:59/20:59 各启动一次，
    每个任务实例只跑一个"任务窗口"，到该窗口结束即退出：
      - 02:30 ~ 11:30  → 上午盘窗口，退出 11:30
      - 11:30 ~ 15:15  → 下午盘窗口（含午间等待），退出 15:15
      - 15:15 ~ 02:30  → 夜盘窗口（跨午夜），退出 02:30
    """
    morning_end = datetime.time(11, 30, 0)
    afternoon_end = datetime.time(15, 15, 0)
    night_end = datetime.time(2, 30, 0)

    # 02:30 ~ 11:30：上午盘任务窗口（含早间等待）
    if night_end <= now_time < morning_end:
        return morning_end
    # 11:30 ~ 15:15：下午盘任务窗口（含午间等待）
    if morning_end <= now_time < afternoon_end:
        return afternoon_end
    # 15:15 ~ 02:30：夜盘任务窗口（跨午夜）
    if afternoon_end <= now_time or now_time < night_end:
        return night_end

    return morning_end


# 根据本实例启动时间确定负责的交易时段结束时间，避免运行中跨越时段边界后改变目标
_RESPONSIBLE_SESSION_END = get_target_session_end(datetime.datetime.now().time())


def is_after_session_end(now_time, session_end):
    """判断当前时间是否已超过目标时段结束时间。

    处理夜盘跨午夜的情况：结束时间 02:30 表示 02:30 之后到 09:00 之前都算结束后。
    """
    if session_end == datetime.time(2, 30, 0):
        return datetime.time(2, 30, 0) <= now_time < datetime.time(9, 0, 0)
    return now_time >= session_end


def get_source_accounts() -> list:
    """获取所有配置的源账户名称及其目标账户列表。

    返回 [(source_account, targets_list), ...] 元组列表。
    如果 account_targets.py 未配置任何源账户，返回空列表。
    """
    if not ACCOUNT_TARGETS:
        return []
    return [(src, list(tgts)) for src, tgts in ACCOUNT_TARGETS.items()]


def build_target_conf(target: dict) -> dict:
    """根据目标账户配置构造 CTP 登录配置。

    以 config.envs[env_name] 为基座，覆盖 user_id/password 等字段。
    """
    from config import config as _ctp_config

    env_name = target.get("env_name") or _CTP_ENV_NAME
    if env_name not in _ctp_config.envs:
        raise ValueError(f"未知环境: {env_name}")

    conf = dict(_ctp_config.envs[env_name])
    # 覆盖目标账户特定字段
    for key in ("user_id", "password", "broker_id", "authcode", "appid", "user_product_info"):
        if key in target and target[key]:
            conf[key] = target[key]
    return conf


# ==================== 反跟单侦察防护：全局/账号级配置 ====================
# ① 全局冷门品种黑名单（任何账号都不跟，强剔除，不参与开/平仓）
#    这些品种全市场常年持仓量/成交量极低，是"下探单查跟单"的天然陷阱。
GLOBAL_DENY_PRODUCTS = {"WR", "FB", "BB", "RR", "RS", "WH", "PM", "RI","ZC","JR"}

# ② 全局允许的合约级别（取 main_contracts_by_product.json 中对应 key）
#    默认只跟主力 main + 次主力 main2；如需宽松可加 "main3"。
#    任何非 main/main2 的合约（远月/冷门月）都视为非主流通合约，
#    不满足大单豁免时直接剔除，避免在远月小流通池里下探单直接暴露跟单身份。
GLOBAL_ALLOW_CONTRACT_LEVEL = {"main", "main2","main3"}

# ③ 大单豁免阈值（元）：即使是非主流通合约，单合约目标成交额 >= 该值也放行。
#    用于防误杀：换月前大资金提前移仓到次次主、或者策略专门做跨期套利时的真实大额交易。
#    计算公式：notional = qty_hand × Price(昨收/最新价) × VolumeMultiple（合约乘数）
BIG_NOTIONAL_EXEMPTION = 1_000_000  # 100 万

# ④ 下单前随机延迟（摧毁"时序指纹"，防止源方用 hold-std 更新后 X 秒必下单的统计相关性锁定跟单）
#    默认关闭（避免今日第一次上线引入新变量），生产确认没问题可打开 True。
#    启用后：每笔开仓/平仓委托 submit 前，随机 sleep 0 ~ RANDOM_ORDER_DELAY_MAX_MS 毫秒。
RANDOM_ORDER_DELAY_ENABLED = False
RANDOM_ORDER_DELAY_MAX_MS = 3000  # 3 秒内均匀随机

# ⑤ 被动挂单模式（专为套利跟单账户设计，不主动吃单，用排队价挂单赚滑点）
#    全局默认：False（不启用，保持原 aggressive 主动吃单 + 30 秒撤单重挂逻辑）
#    账号级可通过 account_targets.py 的 passive/passive_mode/enable_passive 字段覆盖为 True。
#    passive_wait_seconds：被动模式下的「排队等待窗口」秒数；
#        挂单后 < 此窗口：即使盘口偏离也不撤单，让排队价自然成交（默认 300 = 5 分钟）；
#        挂单后 >= 此窗口仍未成交：撤单 → 向对手盘方向进 1 tick 重新排队，逐档咬盘口。
PASSIVE_MODE_DEFAULT = False
PASSIVE_WAIT_SECONDS_DEFAULT = 300

# ================================================================


def _get_target_ratio(target: dict) -> float:
    """获取目标账户的持仓同步比例/模式（ration / ratio 都行）。

    支持字段名：ratio / ration / position_ratio，优先使用 ratio。
    值的语义：
      - 值 >  0：正常跟单，按比例缩放（ration=2 → 两倍；0.5 → 半仓）
      - 值 == 0：【清仓模式】目标持仓强制置 0，将按卖一/买一限价把实际持仓全部平掉
      - 值 == -N（如 -1、-2、-3.5…）：【对冲模式】取 source_account 的持仓方向反转，
        再乘 N 倍。例：source wangk0402 持 FG 多 1 手，ration=-1 → target 持 FG 空 1 手；
        ration=-2 → target 持 FG 空 2 手。
    如果都没有配置，则回退到全局 POSITION_RATIO（命令行 --ratio，默认 1.0）。
    """
    for key in ("ratio", "ration", "position_ratio"):
        if key in target:
            try:
                value = float(target[key])
                # 现在允许所有实数：正数=跟单、0=清仓、负数=对冲
                # 只有 NaN / inf 这种非法浮点数才回退到 1.0
                if not (value == value) or value in (float("inf"), float("-inf")):
                    logger.warning(
                        "[ratio] 目标账户 %s 的 %s=%s 是非有限浮点数，使用默认值 1.0",
                        target.get("user_id", "unknown"),
                        key,
                        target[key],
                    )
                    return 1.0
                return value
            except (ValueError, TypeError):
                logger.warning(
                    "[ratio] 目标账户 %s 的 %s=%s 不是有效数字，使用默认值 1.0",
                    target.get("user_id", "unknown"),
                    key,
                    target[key],
                )
                return 1.0
    return float(POSITION_RATIO)


def _get_target_exclude(target: dict) -> list:
    """获取目标账户的排除品种列表（已规范化，全大写）。

    支持：exclude / exclude_products / exclude_symbols，任意一种均可；
    值可以是逗号分隔字符串（如 "sc, FG"）或列表（如 ["sc","FG"]）；
    空值或无法识别时返回空 list，表示不排除任何品种。
    """
    raw = None
    for key in ("exclude", "exclude_products", "exclude_symbols"):
        if key in target and target[key] is not None:
            raw = target[key]
            break
    if raw is None:
        return []

    # 支持 str："sc, FG" / "SC FG" / "sc;FG"
    if isinstance(raw, str):
        items = []
        for chunk in raw.replace(',', ' ').replace(';', ' ').split():
            chunk = chunk.strip()
            if chunk:
                items.append(chunk.upper())
        return items

    # 支持 list / tuple / set
    try:
        items = []
        for x in list(raw):
            s = str(x).strip()
            if s:
                items.append(s.upper())
        return items
    except Exception as e:
        logger.warning(
            "[exclude] 目标账户 %s 的 exclude 配置解析失败: %s （原始值=%s），忽略排除",
            target.get("user_id", "unknown"), e, raw,
        )
        return []


def _get_target_allow_contract_level(target: dict) -> set:
    """获取目标账户的「允许跟单的合约级别」(set of str)。

    支持字段：allow_contract_level / allow_level / contract_level。
    值可以是 list（如 ["main","main2"]）或逗号分隔字符串（如 "main,main2"）。
    未配置时回退到全局 GLOBAL_ALLOW_CONTRACT_LEVEL = {"main","main2"}。
    合法取值：main（主力）/ main2（次主力）/ main3（次次主）。
    """
    raw = None
    for key in ("allow_contract_level", "allow_level", "contract_level"):
        if key in target and target[key] is not None:
            raw = target[key]
            break
    if raw is None:
        return set(GLOBAL_ALLOW_CONTRACT_LEVEL)
    valid_keys = {"main", "main2", "main3", "main4"}
    try:
        if isinstance(raw, str):
            pieces = [p.strip() for p in raw.replace(',', ' ').replace(';', ' ').split() if p.strip()]
        else:
            pieces = [str(p).strip() for p in list(raw) if str(p).strip()]
        result = {p for p in pieces if p in valid_keys}
        if not result:
            logger.warning(
                "[allow_level] 目标账户 %s 的 allow_contract_level=%s 无合法取值（合法:%s），"
                "回退默认全局 %s",
                target.get("user_id", "unknown"), raw, sorted(valid_keys),
                sorted(GLOBAL_ALLOW_CONTRACT_LEVEL),
            )
            return set(GLOBAL_ALLOW_CONTRACT_LEVEL)
        return result
    except Exception as e:
        logger.warning(
            "[allow_level] 目标账户 %s allow_contract_level 解析失败: %s（原始=%s），回退全局 %s",
            target.get("user_id", "unknown"), e, raw, sorted(GLOBAL_ALLOW_CONTRACT_LEVEL),
        )
        return set(GLOBAL_ALLOW_CONTRACT_LEVEL)


def _get_target_deny_products(target: dict) -> set:
    """获取目标账户级的追加冷门品种黑名单（叠加到全局 GLOBAL_DENY_PRODUCTS 上，返回合并 set，全大写）。

    支持字段：deny_products / deny。
    """
    raw = None
    for key in ("deny_products", "deny"):
        if key in target and target[key] is not None:
            raw = target[key]
            break
    result = set(GLOBAL_DENY_PRODUCTS)
    if raw is None:
        return result
    try:
        if isinstance(raw, str):
            pieces = [p.strip().upper() for p in raw.replace(',', ' ').replace(';', ' ').split() if p.strip()]
        else:
            pieces = [str(p).strip().upper() for p in list(raw) if str(p).strip()]
        result.update(pieces)
    except Exception as e:
        logger.warning(
            "[deny] 目标账户 %s deny_products 解析失败: %s（原始=%s），仅保留全局黑名单 %s",
            target.get("user_id", "unknown"), e, raw, sorted(GLOBAL_DENY_PRODUCTS),
        )
    return result


def _get_target_min_qty_hand(target: dict) -> Optional[int]:
    """单合约最低手数阈值（低于该手数的小单视为探单，直接跳过）。未配置返回 None 表示不启用。

    支持字段：min_qty_hand / min_qty_hand / min_hand / min_qty。
    """
    for key in ("min_qty_hand", "min_hand", "min_qty"):
        if key in target and target[key] is not None:
            try:
                v = int(target[key])
                if v <= 0:
                    return None
                return v
            except (ValueError, TypeError):
                logger.warning(
                    "[min_qty] 目标账户 %s 的 %s=%s 不是正整数，忽略",
                    target.get("user_id", "unknown"), key, target[key],
                )
    return None


def _get_target_min_notional(target: dict) -> Optional[float]:
    """单合约最低成交额阈值（元，低于该值的小单视为探单跳过）。未配置返回 None 表示不启用。

    支持字段：min_notional / min_amount / min_value。
    """
    for key in ("min_notional", "min_amount", "min_value"):
        if key in target and target[key] is not None:
            try:
                v = float(target[key])
                if v <= 0:
                    return None
                return v
            except (ValueError, TypeError):
                logger.warning(
                    "[min_notional] 目标账户 %s 的 %s=%s 不是正数，忽略",
                    target.get("user_id", "unknown"), key, target[key],
                )
    return None


def _get_random_delay_config(target: dict) -> Tuple[bool, int]:
    """下单前随机延迟配置 -> (enabled:bool, max_ms:int)。

    目标账户配置优先（字段：random_delay / enable_random_delay / random_delay_ms）；
    目标账户未配置时回退到全局 RANDOM_ORDER_DELAY_ENABLED / RANDOM_ORDER_DELAY_MAX_MS。
    """
    enabled = RANDOM_ORDER_DELAY_ENABLED
    max_ms = RANDOM_ORDER_DELAY_MAX_MS
    # 字段1：显式启用/禁用
    for key in ("random_delay", "enable_random_delay"):
        if key in target and target[key] is not None:
            v = target[key]
            if isinstance(v, bool):
                enabled = v
            elif isinstance(v, (int, float)):
                enabled = bool(v)
            elif isinstance(v, str):
                s = v.strip().lower()
                if s in ("1", "true", "yes", "on", "开启", "启用"):
                    enabled = True
                elif s in ("0", "false", "no", "off", "关闭", "禁用"):
                    enabled = False
            break
    # 字段2：最大毫秒数
    for key in ("random_delay_ms", "random_delay_max_ms", "delay_max_ms"):
        if key in target and target[key] is not None:
            try:
                v = int(target[key])
                if v >= 0:
                    max_ms = v
            except (ValueError, TypeError):
                logger.warning(
                    "[delay] 目标账户 %s 的 %s=%s 不是非负整数，使用默认 %s ms",
                    target.get("user_id", "unknown"), key, target[key], max_ms,
                )
            break
    return enabled, max_ms


def _get_target_passive_config(target: dict) -> Tuple[bool, int]:
    """被动挂单模式配置 -> (passive_mode:bool, wait_seconds:int)。

    目标账户配置优先（字段：passive / passive_mode / enable_passive 与
    passive_wait_seconds / passive_wait_sec / passive_timeout）；
    未配置时回退到全局 PASSIVE_MODE_DEFAULT / PASSIVE_WAIT_SECONDS_DEFAULT。
    """
    # ① 是否启用 passive 模式
    passive_mode = PASSIVE_MODE_DEFAULT
    for key in ("passive", "passive_mode", "enable_passive"):
        if key in target and target[key] is not None:
            v = target[key]
            if isinstance(v, bool):
                passive_mode = v
            elif isinstance(v, (int, float)):
                passive_mode = bool(v)
            elif isinstance(v, str):
                s = v.strip().lower()
                if s in ("1", "true", "yes", "on", "开启", "启用"):
                    passive_mode = True
                elif s in ("0", "false", "no", "off", "关闭", "禁用"):
                    passive_mode = False
            break

    # ② 等待窗口秒数（>= 10 才合法，否则兜底 300）
    wait_seconds = PASSIVE_WAIT_SECONDS_DEFAULT
    for key in ("passive_wait_seconds", "passive_wait_sec", "passive_timeout"):
        if key in target and target[key] is not None:
            try:
                v = int(target[key])
                if v >= 10:
                    wait_seconds = v
                else:
                    logger.warning(
                        "[passive] 目标账户 %s 的 %s=%s < 10 秒，兜底使用默认 %s 秒",
                        target.get("user_id", "unknown"), key, target[key],
                        PASSIVE_WAIT_SECONDS_DEFAULT,
                    )
                    wait_seconds = PASSIVE_WAIT_SECONDS_DEFAULT
            except (ValueError, TypeError):
                logger.warning(
                    "[passive] 目标账户 %s 的 %s=%s 不是合法整数，使用默认 %s 秒",
                    target.get("user_id", "unknown"), key, target[key],
                    PASSIVE_WAIT_SECONDS_DEFAULT,
                )
            break
    return passive_mode, wait_seconds


# 热加载 account_targets 的模块引用（不要用 import 多次，始终 reload 这个模块对象）
_at_module = None  # 懒加载：在首次 _reload_account_targets 时绑定


def _reload_account_targets_module():
    """reload account_targets.py，返回 ACCOUNT_TARGETS dict。

    加载失败（import error / reload error / 语法错误）时：回退到全局内存中的 ACCOUNT_TARGETS，
    不影响现有正在运行的同步线程。
    """
    global ACCOUNT_TARGETS, _at_module
    # 首次调用时通过 sys.modules 或者直接 import 找到模块对象
    try:
        if _at_module is None:
            if 'account_targets' in sys.modules:
                _at_module_ref = sys.modules['account_targets']
            else:
                import account_targets as _tmp
                _at_module_ref = _tmp
            _at_module_2 = importlib.reload(_at_module_ref)
        else:
            _at_module_2 = importlib.reload(_at_module)
    except Exception as e:
        # reload 失败（例如 account_targets.py 语法错、缺字段）：
        # 打印一条 warning，但返回全局内存中上次成功加载的 ACCOUNT_TARGETS，
        # 保证运行时不崩。
        logger.warning(
            "[hot-reload] reload account_targets.py 失败: %s，沿用上次成功加载的配置",
            e,
        )
        return dict(ACCOUNT_TARGETS or {})
    # 成功 reload -> 覆盖全局 ACCOUNT_TARGETS 供其它链路（导出循环、主列表循环等）继续用
    new_cfg = getattr(_at_module_2, 'ACCOUNT_TARGETS', None) or {}
    _at_module = _at_module_2
    if isinstance(new_cfg, dict):
        ACCOUNT_TARGETS = new_cfg
    return dict(ACCOUNT_TARGETS or {})


def _resolve_latest_target_config(source_account: str, user_id: str):
    """根据 (source_account, user_id) 从 account_targets.py 实时 reload 后取到最新配置。

    返回 11-tuple:
      (ratio, exclude, allow_level, deny_products, min_qty_hand, min_notional,
       random_enabled, random_max_ms, passive_mode, passive_wait_seconds, matched_target)
    找不到匹配条目时 -> 返回全 None，调用方沿用旧值。
    """
    targets_cfg = _reload_account_targets_module()
    if not targets_cfg or not source_account or not user_id:
        return None, None, None, None, None, None, None, None, None, None, None
    target_list = targets_cfg.get(source_account) or []
    if not isinstance(target_list, (list, tuple)):
        return None, None, None, None, None, None, None, None, None, None, None
    matched = None
    for t in target_list:
        if not isinstance(t, dict):
            continue
        if str(t.get("user_id") or "").strip() == str(user_id).strip():
            matched = t
            break
    if matched is None:
        return None, None, None, None, None, None, None, None, None, None, None
    ratio = _get_target_ratio(matched)
    exclude = _get_target_exclude(matched)
    allow_level = _get_target_allow_contract_level(matched)
    deny = _get_target_deny_products(matched)
    min_qty = _get_target_min_qty_hand(matched)
    min_not = _get_target_min_notional(matched)
    rnd_enabled, rnd_max_ms = _get_random_delay_config(matched)
    passive_mode, passive_wait_sec = _get_target_passive_config(matched)
    return (ratio, exclude, allow_level, deny, min_qty, min_not,
            rnd_enabled, rnd_max_ms, passive_mode, passive_wait_sec, matched)


# ==================== 线程间通信 ====================
shutdown_event = threading.Event()
_last_sync_time = [0]
_last_key_alert_time = [None]  # 记录上次发送关键时间点提醒的日期
# =================================================

# 导入跨进程交易锁
from trading_lock import get_trading_lock, with_trading_lock


def check_key_time_and_alert():
    """检查是否接近关键时间点，发送飞书提醒并执行强制同步"""
    now = datetime.datetime.now()
    today = now.date()

    for key_time_str in KEY_ALIGN_TIMES:
        key_time = datetime.datetime.strptime(key_time_str, "%H:%M").time()
        # 计算距离关键时间点的秒数
        key_dt = datetime.datetime.combine(today, key_time)
        diff_seconds = (key_dt - now).total_seconds()

        # 如果距离关键时间点小于60秒，执行强制对齐
        if -5 <= diff_seconds <= 30:
            # 避免同一天重复执行
            alert_key = (today, key_time_str)
            if _last_key_alert_time[0] != alert_key:
                _last_key_alert_time[0] = alert_key
                logger.info(f"[关键时间] 到达 {key_time_str}，执行强制对齐")
                send_feishu_text(
                    f"🔔 已到达 {key_time_str}，强制执行持仓对齐"
                )
                # 执行强制同步（跳过冷却）
                force_sync()
                return True

        # 如果距离关键时间点在 60-120 秒之间，发送提醒
        if 60 <= diff_seconds <= 120:
            # 避免同一天重复发送
            alert_key = (today, f"{key_time_str}_alert")
            if _last_key_alert_time[0] != alert_key:
                _last_key_alert_time[0] = alert_key
                send_feishu_text(
                    f"⚠️ 距 {key_time_str} 仅剩 {int(diff_seconds)} 秒，"
                    f"请确认有无仓差需要处理"
                )
                logger.info("[关键时间提醒] %s 后将执行强制对齐", key_time_str)
    return False


def run_sync(source_account=None):
    """执行持仓同步

    Args:
        source_account: 源账户名称（多账户模式传入，生成 hold-std-{name}.json）。
                        为 None 时使用旧单账户逻辑（全局 ACCOUNT + hold-std.json）。
    """
    # 分阶段等待锁，每阶段检查 shutdown_event
    lock = None
    for attempt in range(10):  # 最多尝试10次，每次等1秒
        if shutdown_event.is_set():
            logger.info("[run_sync] 检测到关闭信号，跳过本次同步")
            return False

        logger.info(f"等待交易锁: 持仓同步 (尝试 {attempt + 1}/10)")
        lock = get_trading_lock(timeout=1)
        if lock.acquire():
            break
        logger.info("交易锁被占用，等待中...")
    else:
        logger.warning("获取交易锁超时（10秒），跳过本次同步")
        return False

    logger.info("=" * 50)
    logger.info("已获取交易锁: 开始持仓同步")

    try:
        # 重新生成标准持仓文件（从导出的持仓明细 CSV）
        import compare_orders

        if source_account is None:
            # 旧单账户模式：使用全局 ACCOUNT
            current_account = ACCOUNT
            hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')
            gen_ok = compare_orders.generate_hold_std(account=current_account)
            path_basename = 'hold-std.json'
        else:
            # 多账户模式：使用传入的 source_account
            current_account = source_account
            hold_std_path = os.path.join(_CURR_DIR, f'hold-std-{source_account}.json')
            gen_ok = compare_orders.generate_hold_std(
                account=current_account, output_path=hold_std_path
            )
            path_basename = os.path.basename(hold_std_path)

        if not gen_ok:
            logger.warning("生成 %s 失败", path_basename)

        # 读取当前标准仓（供日志使用）
        if os.path.exists(hold_std_path):
            with open(hold_std_path, 'r', encoding='utf-8') as f:
                hold_rows = json.load(f)
            # 支持多种字段名：持仓量、手数、数量
            total_vol = 0
            for item in hold_rows:
                vol = item.get("持仓量") or item.get("手数") or item.get("数量") or "0"
                try:
                    total_vol += int(float(str(vol).strip()))
                except (ValueError, TypeError):
                    pass
            logger.info("标准持仓: %d 条记录，共 %d 手", len(hold_rows), total_vol)

        # 执行持仓同步（PositionSyncManager 会详细打印补单/平仓日志）
        from trading.position_sync.position_sync_manager import run_position_sync
        MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'contracts', 'main_contracts.json')

        logger.info(">>> 开始持仓对比与同步...")
        _skip_time = bool(SKIP_TRADING_TIME_CHECK) or bool(_FORCE_RUN) or (str(_CTP_ENV_NAME).lower() in ("simu", "7x24"))
        sync_ok = run_position_sync(
            hold_std_path=hold_std_path,
            main_contracts_path=MAIN_CONTRACTS_PATH,
            trade_volume=1,
            logger=logger,
            timeout=60,
            conf=None,
            env_name=_CTP_ENV_NAME,
            position_ratio=POSITION_RATIO,
            skip_trading_time_check=_skip_time,
        )
        logger.info(">>> 持仓同步返回: sync_ok=%s", sync_ok)
        _last_sync_time[0] = time.time()

        if sync_ok:
            logger.info("=" * 50)
            logger.info("【持仓同步完成】")
            logger.info("  PositionSyncManager 已发送详细飞书通知")
            logger.info("=" * 50)
            # 注意：详细通知由 PositionSyncManager._send_sync_notification 发送
        else:
            logger.warning("持仓同步未完成（可能处于冷却期或有错误）")

        return sync_ok
    except Exception as e:
        logger.error("持仓同步异常: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if lock:
            lock.release()
            logger.info("已释放交易锁: 持仓同步结束")
            logger.info("=" * 50)


def force_sync():
    """强制同步（忽略冷却，直接执行）。

    自动识别多账户/单账户模式：
    - 多账户：为 ACCOUNT_TARGETS 中的每个 source_account 分别生成标准持仓文件，
              并按每个 target 的配置（user_id / env / ratio / conf）独立同步。
    - 单账户：回退到全局 ACCOUNT + hold-std.json + POSITION_RATIO。
    """
    # 分阶段等待锁，每阶段检查 shutdown_event
    lock = None
    for attempt in range(10):
        if shutdown_event.is_set():
            logger.info("[force_sync] 检测到关闭信号，跳过本次同步")
            return False

        logger.info(f"等待交易锁: 强制同步 (尝试 {attempt + 1}/10)")
        lock = get_trading_lock(timeout=1)
        if lock.acquire():
            break
        logger.info("交易锁被占用，等待中...")
    else:
        logger.warning("获取交易锁超时，跳过本次强制同步")
        return False

    logger.info("已获取交易锁: 执行强制持仓同步")

    logger.info("=" * 60)
    logger.info("执行强制持仓同步...")
    logger.info("=" * 60)

    # 发送飞书通知
    send_feishu_text("🔔 强制执行持仓对齐，修复仓差/超额")

    _CURR_DIR_SYNC = _CURR_DIR
    _PROJECT_ROOT_SYNC = PROJECT_ROOT
    MAIN_CONTRACTS_PATH = os.path.join(_PROJECT_ROOT_SYNC, 'data', 'contracts', 'main_contracts.json')

    try:
        import compare_orders
        from trading.position_sync.position_sync_manager import run_position_sync

        # 构造任务列表：[(source_account, hold_std_path, ratio, env_label, conf, user_id)]
        sync_tasks = []

        if ACCOUNT_TARGETS:
            # ========= 多账户模式 =========
            for source_account, targets in ACCOUNT_TARGETS.items():
                hold_std_path = os.path.join(_CURR_DIR_SYNC, f'hold-std-{source_account}.json')
                # 为该源账户生成标准持仓文件
                gen_ok = compare_orders.generate_hold_std(
                    account=source_account, output_path=hold_std_path
                )
                if not gen_ok or not os.path.exists(hold_std_path):
                    logger.warning("[强制同步] %s 生成失败，跳过该源账户",
                                   os.path.basename(hold_std_path))
                    continue
                for target in targets:
                    user_id = target.get("user_id")
                    if not user_id:
                        continue
                    env_label = f"{target.get('env_name', _CTP_ENV_NAME)}_{user_id}"
                    conf = build_target_conf(target)
                    ratio = _get_target_ratio(target)
                    sync_tasks.append((
                        source_account, hold_std_path, ratio, env_label, conf, user_id,
                    ))
        else:
            # ========= 回退：旧单账户模式 =========
            hold_std_path = os.path.join(_CURR_DIR_SYNC, 'hold-std.json')
            gen_ok = compare_orders.generate_hold_std(account=ACCOUNT)
            if gen_ok and os.path.exists(hold_std_path):
                sync_tasks.append((
                    "default", hold_std_path, POSITION_RATIO, _CTP_ENV_NAME, None, "default",
                ))
            else:
                logger.warning("[强制同步] 生成 hold-std.json 失败")

        if not sync_tasks:
            logger.warning("[强制同步] 没有任何可执行的同步任务")
            send_feishu_text("⚠️ 强制同步：未找到可执行的同步任务")
            return False

        all_ok = True
        total = len(sync_tasks)
        for idx, (src_account, hold_std_path, ratio, env_label, conf, user_id) in enumerate(sync_tasks):
            logger.info("[强制同步] (%d/%d) 开始: %s -> %s", idx + 1, total, src_account, user_id)
            _skip_time_f = bool(SKIP_TRADING_TIME_CHECK) or bool(_FORCE_RUN) or (str(env_label).lower() in ("simu", "7x24"))
            sync_ok = run_position_sync(
                hold_std_path=hold_std_path,
                main_contracts_path=MAIN_CONTRACTS_PATH,
                trade_volume=1,
                timeout=120,  # 首次建仓可能需要挂出50+合约的委托
                conf=conf,
                env_name=env_label,
                logger=logger,
                position_ratio=ratio,
                skip_trading_time_check=_skip_time_f,
            )
            if not sync_ok:
                all_ok = False
                logger.warning("[强制同步] %s -> %s 未完成", src_account, user_id)

        if all_ok:
            logger.info("[强制同步] 全部 %d 个任务完成", total)
            # 详细通知由 PositionSyncManager._send_sync_notification 发送
        else:
            logger.warning("[强制同步] 部分任务未完成")
            send_feishu_text("⚠️ 强制同步：部分任务未完成")
        return all_ok
    except Exception as e:
        logger.error("强制同步异常: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        send_feishu_text(f"❌ 强制同步失败: {e}")
        return False
    finally:
        if lock:
            lock.release()
            logger.info("已释放交易锁: 强制同步结束")


def run_once() -> bool:
    """单次执行：导出 -> 为所有配置源账户生成持仓文件。

    返回 True 表示本次导出+文件生成成功；False 表示中途失败。
    调用方会按“返回时刻 + 耗时”打日志，便于用户核对“从点击OK 按钮算起到下一次OK”的节奏。
    """
    t0 = time.time()
    step_t = t0
    logger.info("=" * 50)
    logger.info("开始执行: 导出 -> 持仓差异对比")
    logger.info(">>> 步骤 1/3: 执行自动导出...")
    try:
        import automate_export
        success = automate_export.main()
    except Exception as e:
        logger.error("导出步骤异常: %s", e)
        success = False
    t1 = time.time()
    logger.info("步骤1(自动导出) 耗时: %.2fs 结果=%s", t1 - step_t, "OK" if success else "FAIL")
    step_t = t1

    if not success:
        logger.warning("导出失败，中断后续流程。总体耗时: %.2fs", time.time() - t0)
        return False
    logger.info("导出成功。")

    if t1 - t0 > 5:
        # 导出本身已经花了 >5s（GUI 自动化 UI 窗口点击 OK + 导出），不再额外等 1s，
        # 避免“总节奏=8s导出+1s额外+其他>10s”继续往上叠加。
        extra_sleep = 0.15
    else:
        extra_sleep = 0.2
    time.sleep(extra_sleep)
    step_t = time.time()

    logger.info(">>> 步骤 2/3: 生成持仓文件...")
    t_gen_start = time.time()
    try:
        import compare_orders
        hold_files_written = 0
        if ACCOUNT_TARGETS:
            # 多源账户模式：为每个源账户生成独立的 hold-std 文件
            for source_account in ACCOUNT_TARGETS.keys():
                output_path = os.path.join(_CURR_DIR, f"hold-std-{source_account}.json")
                gen_ok = compare_orders.generate_hold_std(
                    account=source_account, output_path=output_path
                )
                if gen_ok:
                    hold_files_written += 1
                    logger.info("标准持仓 %s 生成完成", os.path.basename(output_path))
                else:
                    logger.warning("标准持仓 %s 生成失败", os.path.basename(output_path))
        else:
            # 兼容旧模式：生成单个 hold-std.json
            gen_ok = compare_orders.generate_hold_std(account=ACCOUNT)
            if gen_ok:
                hold_files_written += 1
                logger.info("标准持仓 hold-std.json 生成完成")
            else:
                logger.warning("标准持仓 hold-std.json 生成失败")

        # 初始化 hold.json 占位文件
        # 实际数据由 PositionSyncManager 从 CTP 持仓查询后更新
        compare_orders.generate_hold()

    except Exception as e:
        logger.error("生成持仓文件异常: %s", e)

    hold_gen_cost = time.time() - t_gen_start
    hold_paths_for_log = []
    try:
        if ACCOUNT_TARGETS:
            for sa in ACCOUNT_TARGETS.keys():
                p = os.path.join(_CURR_DIR, f"hold-std-{sa}.json")
                hold_paths_for_log.append(p)
        else:
            hold_paths_for_log.append(os.path.join(_CURR_DIR, "hold-std.json"))
    except Exception:
        hold_paths_for_log = []
    hold_file_infos = []
    for p in hold_paths_for_log:
        try:
            if os.path.exists(p):
                import datetime as _dt
                m = os.path.getmtime(p)
                hold_file_infos.append(
                    f"{os.path.basename(p)} mtime={_dt.datetime.fromtimestamp(m).strftime('%H:%M:%S')}"
                )
        except Exception:
            pass
    logger.info(
        "步骤2(生成持仓文件) 耗时: %.2fs (文件数=%d)。hold-std 写入时刻: %s",
        hold_gen_cost, hold_files_written, "；".join(hold_file_infos) if hold_file_infos else "<none>",
    )
    step_t = time.time()

    logger.info(">>> 步骤 3/3: 持仓差异将在同步时对比（由 PositionSyncManager 处理）")

    total_elapsed = time.time() - t0
    logger.info("单次 run_once 总体耗时: %.2fs (导出=%.2fs + 中间sleep=%.2fs + 生成hold=%.2fs)",
                total_elapsed,
                t1 - t0,
                extra_sleep,
                hold_gen_cost,
                )
    logger.info("=" * 50)
    # 不再在此处对比，返回 False 让 run_sync 处理对比逻辑
    return False


def export_loop():
    """导出线程：严格按 CHECK_INTERVAL 节奏启动 run_once（不随执行耗时漂移）。

    之前的节奏 = run_once 实际耗时(8~12s, 主要是点OK + UI 导出) + 固定 sleep(CHECK_INTERVAL)
               ≈ 18~22s，所以你计时“从OK按钮到下一次OK”接近 20s。
    现在的节奏 = 下一次 run_once 启动时刻 = max(上一次启动时刻 + CHECK_INTERVAL, 当前时刻)
               → 即使 run_once 自己吃掉了 8-12s，间隔也只会补到 10s，不会叠加到 20s。
    """
    logger.info("[导出线程] 启动 (目标节奏: 每 %ds 启动 1 次 run_once)", CHECK_INTERVAL)
    last_heartbeat = time.time()
    # 记录上一次 run_once 的“启动时刻”，用于严格按间隔调度
    next_run_at = 0.0  # 0 表示下一轮立即执行（不额外等）

    while not shutdown_event.is_set():
        try:
            # ------ 计算本轮需要等多久才能启动 run_once ------
            now = time.time()
            if next_run_at <= 0:
                sleep_until_next = 0.0  # 第一轮 / 重置后：立即跑
            else:
                sleep_until_next = max(0.0, next_run_at - now)

            # 为了保证 shutdown_event 能被及时响应，用 1 秒粒度切分等待。
            waited = 0.0
            while waited < sleep_until_next and not shutdown_event.is_set():
                chunk = min(1.0, sleep_until_next - waited)
                if shutdown_event.wait(timeout=chunk):
                    break
                waited += chunk
                # 等待过程中，仍按 10s 节奏打心跳日志
                if time.time() - last_heartbeat >= 10:
                    logger.info(
                        "[导出线程] 心跳 - 仍在运行 (距下次 run_once=%.1fs)",
                        max(0.0, next_run_at - time.time()),
                    )
                    last_heartbeat = time.time()

            if shutdown_event.is_set():
                break

            # ------ 交易时段判断：只在交易时段/跳过时段时真正执行导出 ------
            # 7x24 / simu 环境、--force、--skip-time-check 任一成立就当做在交易时段，
            # 允许非开盘时间（周末/节假日/夜间）正常导出 hold-std + 对比 + 生成报表，
            # 不再进入 else 分支打印「距离下次开盘 X 分 Y 秒」，也不再触发 L1366 时段结束强制退出。
            _env_low = str(_CTP_ENV_NAME or "").lower().strip()
            in_session = (
                bool(is_in_trading_time())
                or bool(SKIP_TRADING_TIME_CHECK)
                or bool(_FORCE_RUN)
                or (_env_low in ("simu", "7x24"))
            )
            run_start_ts = time.time()
            next_run_at = run_start_ts + CHECK_INTERVAL  # 严格按“启动时刻 + 间隔”推进下一次
            # 本轮执行完立刻预写下一次启动时间：哪怕 run_once 异常退出，节奏也不漂移。

            if in_session:
                # 关键时间点提醒（该函数自身幂等，重复调只会提醒一次）
                check_key_time_and_alert()

                t0_one = time.time()
                run_once()  # 返回值 true/false 都不影响下一次节奏
                one_cost = time.time() - t0_one

                # 若本次 run_once 自己就用了超过 CHECK_INTERVAL（极端 10s+），
                # 则下一轮不再额外 sleep，立刻跑（让节奏追赶预期，最多允许落后 1 轮）。
                if one_cost >= CHECK_INTERVAL:
                    next_run_at = time.time()
                last_heartbeat = time.time()

                # 打印一次“从本次OK到下一次OK理论间隔”，用户对照自己计时即可验证是不是 10s 节奏
                logger.info(
                    "[导出线程] 本轮 run_once 耗时=%.2fs，下一次启动预计 %.1fs 后 "
                    "(总节奏≈%ds，不再叠加执行耗时与固定 sleep)",
                    one_cost, max(0.0, next_run_at - time.time()), CHECK_INTERVAL,
                )
            else:
                now_time = datetime.datetime.now().time()
                wait_sec = seconds_until_next_session()
                logger.info(
                    "[导出线程] 非交易时间，距离下次开盘还有 %d 分 %d 秒",
                    wait_sec // 60, wait_sec % 60,
                )

                # 多时段定时任务模式：每个任务只跑启动时确定的负责时段，
                # 到该时段结束时间后即退出，等待下一个定时任务启动。
                if is_after_session_end(now_time, _RESPONSIBLE_SESSION_END):
                    if _RESPONSIBLE_SESSION_END == datetime.time(15, 15, 0):
                        logger.info("[导出线程] 日盘已结束，准备退出")
                        send_feishu_text("日盘结束，流水线退出")
                    elif _RESPONSIBLE_SESSION_END == datetime.time(2, 30, 0):
                        logger.info("[导出线程] 夜盘已结束，准备退出")
                        send_feishu_text("夜盘结束，流水线退出")
                        _cleanup_export_files()
                    else:
                        logger.info("[导出线程] 上午盘已结束，准备退出")
                        send_feishu_text("上午盘结束，流水线退出")
                    shutdown_event.set()
                    break

                last_heartbeat = time.time()

                # 非交易时段不推进 next_run_at（进入交易时段立即执行一轮），
                # 把下一次启动时间置 0，保证切时段后立即触发。
                next_run_at = 0.0

            # 每轮结束都补一次心跳兜底（避免 10s 心跳在“切时段/异常”时漏掉）
            if time.time() - last_heartbeat >= 10:
                logger.info("[导出线程] 心跳 - 仍在运行")
                last_heartbeat = time.time()

        except Exception as e:
            logger.error("[导出线程] 异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            # 异常后也不能让节奏漂移：仍然按“本次启动 + CHECK_INTERVAL”推进下次
            try:
                now = time.time()
                if next_run_at <= now:
                    next_run_at = now + CHECK_INTERVAL
            except Exception:
                pass
            # 兜底短 sleep 1s，避免异常死循环打爆 CPU
            if shutdown_event.wait(timeout=1):
                break

    logger.info("[导出线程] 退出")


def main():

    # 交易日检查
    if not _is_trading_day():
        # 非交易日立即退出：显式停掉 crash dump Timer 链 + 刷日志，保证 cmd 窗口能关闭
        try:
            _stop_crash_dump_timer()
        except Exception:
            pass
        try:
            for h in logger.handlers:
                h.flush()
        except Exception:
            pass
        return

    # 检查单实例，防止多任务同时运行导致持仓加倍
    instance_fd = check_single_instance()
    if instance_fd is None:
        logger.error("[错误] 已有实例在运行，本次启动被阻止")
        print("[错误] 已有实例在运行，请先停止当前任务")
        sys.exit(1)
    logger.info("[检查] 单实例检查通过，PID=%d", os.getpid())

    # 启动时立即发送飞书通知并写入心跳，方便判断进程是否成功启动
    _write_heartbeat("starting")
    send_feishu_text(
        f"🚀 流水线已启动\n环境: {_CTP_ENV_NAME}\nPID: {os.getpid()}\n"
        f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    import signal as _signal_module
    def _signal_handler(sig, frame):
        logger.info("收到中断信号，准备退出...")
        shutdown_event.set()
    _signal_module.signal(_signal_module.SIGINT, _signal_handler)
    if hasattr(_signal_module, 'SIGTERM'):
        _signal_module.signal(_signal_module.SIGTERM, _signal_handler)

    logger.info("=" * 60)
    logger.info("流水线已启动（双线程模式）")
    logger.info("  - 导出线程: 每%d秒检查并导出，生成 hold-std.json", CHECK_INTERVAL)
    logger.info("  - 同步线程: 持续监控，发现差异立即处理")
    logger.info("  - 关键时间: %s 强制对齐", KEY_ALIGN_TIMES)
    logger.info("  - 当前CTP环境: %s", _CTP_ENV_NAME)
    logger.info("  - 持仓同步比例: %s", POSITION_RATIO)
    if SKIP_TRADING_TIME_CHECK:
        logger.info("  - 跳过交易时段检查")
    logger.info("交易时间段: %s", TRADING_SESSIONS)
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)

    # 20:59 启动时，先点击 local_config 窗口上的启动弹框
    _dismiss_startup_popup()

    # 启动导出线程（持仓对齐完全由 PositionSyncManager 处理，无报单线程）
    export_thread = threading.Thread(target=export_loop, name="ExportThread", daemon=True)
    export_thread.start()

    # 启动后台心跳线程
    heartbeat_thread = threading.Thread(target=_heartbeat_loop, name="HeartbeatThread", daemon=True)
    heartbeat_thread.start()

    # 启动时先执行导出，获取最新持仓数据
    logger.info(">>> 启动时先执行导出...")
    try:
        import automate_export
        export_ok = automate_export.main()
        if export_ok:
            logger.info("导出成功")
        else:
            logger.warning("导出失败或条件不满足")
    except Exception as e:
        logger.error("启动导出异常: %s", e)
        export_ok = False

    # 根据配置确定要处理的源账户及其目标账户
    source_account_jobs = get_source_accounts()
    if not source_account_jobs:
        # 没有配置任何映射，回退到旧模式
        logger.info("未配置多账户映射，将使用默认单账户模式 (ACCOUNT=%s)", ACCOUNT)
        source_account_jobs = [(ACCOUNT, [{}])]

    # 生成每个源账户的 hold-std 文件
    if export_ok:
        try:
            import compare_orders
            for source_account, targets in source_account_jobs:
                if not targets or (len(targets) == 1 and not targets[0].get("user_id")):
                    # 默认单账户模式：使用原来的 hold-std.json
                    output_path = os.path.join(_CURR_DIR, "hold-std.json")
                else:
                    output_path = os.path.join(_CURR_DIR, f"hold-std-{source_account}.json")
                gen_ok = compare_orders.generate_hold_std(
                    account=source_account, output_path=output_path
                )
                if gen_ok and os.path.exists(output_path):
                    with open(output_path, 'r', encoding='utf-8') as f:
                        hold_rows = json.load(f)
                    compare_orders.send_feishu_hold_notification(hold_rows)
                    logger.info("%s 共 %d 条记录", os.path.basename(output_path), len(hold_rows))
        except Exception as e:
            logger.error("生成持仓文件失败: %s", e)

    # 执行持仓同步（持续运行模式，保持 CTP 连接，持续接收成交回报）
    logger.info("=" * 60)
    logger.info("启动持仓同步线程（持续运行模式）...")
    logger.info("=" * 60)

    MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")

    def _run_single_target_sync(source_account, target):
        """单个目标账户的同步循环"""
        user_id = target.get("user_id")
        try:
            if user_id:
                env_label = f"{target.get('env_name', _CTP_ENV_NAME)}_{user_id}"
                conf = build_target_conf(target)
                ratio = _get_target_ratio(target)
                exclude = _get_target_exclude(target)
                allow_level = _get_target_allow_contract_level(target)
                deny_products = _get_target_deny_products(target)
                min_qty_hand = _get_target_min_qty_hand(target)
                min_notional = _get_target_min_notional(target)
                rnd_enabled, rnd_max_ms = _get_random_delay_config(target)
                passive_mode, passive_wait_sec = _get_target_passive_config(target)
                hold_std_path = os.path.join(_CURR_DIR, f"hold-std-{source_account}.json")
                logger.info(
                    "[同步][%s -> %s] 启动目标账户同步 (ratio=%s, exclude=%s, "
                    "allow_level=%s, deny=%s, min_qty=%s, min_notional=%s, "
                    "random_delay=%s@%sms, passive=%s@%ss)",
                    source_account, user_id, ratio, exclude or '[]',
                    sorted(allow_level), sorted(deny_products), min_qty_hand, min_notional,
                    rnd_enabled, rnd_max_ms, passive_mode, passive_wait_sec,
                )
            else:
                # 回退到默认单账户模式
                user_id = "default"
                env_label = _CTP_ENV_NAME
                conf = None
                ratio = POSITION_RATIO
                exclude = []
                allow_level = set(GLOBAL_ALLOW_CONTRACT_LEVEL)
                deny_products = set(GLOBAL_DENY_PRODUCTS)
                min_qty_hand = None
                min_notional = None
                rnd_enabled, rnd_max_ms = RANDOM_ORDER_DELAY_ENABLED, RANDOM_ORDER_DELAY_MAX_MS
                passive_mode, passive_wait_sec = PASSIVE_MODE_DEFAULT, PASSIVE_WAIT_SECONDS_DEFAULT
                hold_std_path = os.path.join(_CURR_DIR, "hold-std.json")
                source_account = None
                logger.info("[同步-default] 启动默认账户同步")

            from trading.position_sync.position_sync_manager import run_position_sync_loop

            _skip_time_loop = bool(SKIP_TRADING_TIME_CHECK) or bool(_FORCE_RUN) or (str(env_label).lower() in ("simu", "7x24"))
            run_position_sync_loop(
                hold_std_path=hold_std_path,
                main_contracts_path=MAIN_CONTRACTS_PATH,
                trade_volume=1,
                conf=conf,
                env_name=env_label,
                logger=logger,
                stop_event=shutdown_event,
                position_ratio=ratio,
                exclude_products=exclude,
                allow_contract_level=allow_level,
                deny_products=deny_products,
                big_notional_exemption=BIG_NOTIONAL_EXEMPTION,
                min_qty_hand=min_qty_hand,
                min_notional=min_notional,
                random_delay_enabled=rnd_enabled,
                random_delay_max_ms=rnd_max_ms,
                passive_mode=passive_mode,
                passive_wait_seconds=passive_wait_sec,
                main_by_product_path=os.path.join(
                    PROJECT_ROOT, "data", "contracts", "main_contracts_by_product.json",
                ),
                source_account=source_account,
                target_user_id=user_id,
                # 热加载解析器：每 10 秒 reload account_targets.py，并返回 (ratio, exclude, ...)
                runtime_config_resolver=_resolve_latest_target_config,
                skip_trading_time_check=_skip_time_loop,
            )
        except Exception as e:
            logger.error("[同步][%s -> %s] 异常: %s", source_account, user_id, e)
            import traceback
            logger.error(traceback.format_exc())

    # 构建所有同步任务：每个源账户的每个目标账户作为一个任务
    sync_jobs = []
    for source_account, targets in source_account_jobs:
        for target in targets:
            sync_jobs.append((source_account, target))

    # 预创建所有同步线程对象
    sync_threads = []
    for idx, (source_account, target) in enumerate(sync_jobs):
        user_id = target.get("user_id", "default")
        thread_name = f"SyncThread-{source_account}-{user_id}"
        t = threading.Thread(
            target=_run_single_target_sync,
            args=(source_account, target),
            daemon=True,
            name=thread_name,
        )
        sync_threads.append(t)

    def sync_loop():
        """同步线程：为每个目标账户启动一个持仓同步循环并等待全部结束"""
        for t in sync_threads:
            t.start()
        for t in sync_threads:
            t.join()

    # 启动同步总控线程
    sync_controller = threading.Thread(target=sync_loop, name="SyncController", daemon=True)
    sync_controller.start()

    # 主线程监控，等待退出信号
    target_desc = ", ".join([
        f"{src}->{t.get('user_id', 'default')}" for src, t in sync_jobs
    ])
    logger.info("工作线程已启动：导出线程 + %d个同步线程", len(sync_threads))
    logger.info("同步任务: %s", target_desc)
    logger.info("按 Ctrl+C 停止")

    try:
        while not shutdown_event.is_set():
            time.sleep(1)
            alive_sync = sum(1 for t in sync_threads if t.is_alive())
            if export_thread.is_alive():
                logger.info(
                    "[主线程] 心跳 - 导出线程: 运行 | 同步线程: %d/%d 运行",
                    alive_sync,
                    len(sync_threads),
                )
            else:
                logger.warning("[主线程] 导出线程已停止")

        # 等待同步线程退出
        logger.info("[主线程] 收到退出信号，等待同步线程退出...")
        _mark_exiting("shutdown_event")
        sync_controller.join(timeout=10)
        if sync_controller.is_alive():
            logger.warning("[主线程] 同步总控线程未能正常退出")
        # 再等待各个目标账户同步线程（外层总控结束后，子线程可能仍在收尾）
        for t in sync_threads:
            t.join(timeout=5)

    except Exception as e:
        logger.exception("[主线程] 运行异常: %s", e)
        _mark_exiting(f"exception: {e}")
        send_feishu_text(f"❌ 流水线异常退出: {e}")
        raise

    _mark_exiting("normal")
    logger.info("流水线已退出")
    # 退出前刷新所有日志 handler，确保日志写入磁盘
    for h in logger.handlers:
        h.flush()
    # 等待子线程最多 3 秒优雅退出，然后强制终止
    def _force_exit():
        time.sleep(3)
        logger.warning("[主线程] 优雅退出超时，强制终止")
        os._exit(0)
    force_thread = threading.Thread(target=_force_exit, daemon=True)
    force_thread.start()
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("[顶层] 未捕获异常，程序退出: %s", e)
        _mark_exiting(f"uncaught: {e}")
        send_feishu_text(f"❌ 流水线未捕获异常退出: {e}")
        sys.exit(1)
