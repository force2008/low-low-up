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
from logging.handlers import RotatingFileHandler

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
TRADING_SESSIONS = [
    ("09:00:15", "11:30:00"),
    ("13:30:15", "15:15:00"),
    ("21:00:15", "23:59:00"),
    ("00:00:15", "02:30:00"),
]
CHECK_INTERVAL = 20  # 秒
SYNC_INTERVAL = 30    # 持仓同步间隔（秒）

# 持仓同步比例：1.0=全仓，0.5=半仓；可通过 --ratio 覆盖
POSITION_RATIO = _POSITION_RATIO


# 关键时间点强制对齐（格式：HH:MM）
KEY_ALIGN_TIMES = ["14:58", "14:59", "15:00"]

# ==================== 单实例检测 ====================
# 防止 Windows 任务计划程序多实例同时运行
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


def _get_target_ratio(target: dict) -> float:
    """获取目标账户的持仓同步比例。

    支持字段名：ratio / ration / position_ratio，优先使用 ratio。
    如果都没有配置，则回退到全局 POSITION_RATIO（命令行 --ratio，默认 1.0）。
    """
    for key in ("ratio", "ration", "position_ratio"):
        if key in target:
            try:
                value = float(target[key])
                if value <= 0:
                    logger.warning(
                        "[ratio] 目标账户 %s 的 %s=%s 必须大于 0，使用默认值 1.0",
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


def run_sync():
    """执行持仓同步"""
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
        # 重新生成 hold-std.json（从导出的持仓明细 CSV）
        import compare_orders
        gen_ok = compare_orders.generate_hold_std(account=_CURRENT_ACCOUNT)
        if not gen_ok:
            logger.warning("生成 hold-std.json 失败")
        hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')

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
        sync_ok = run_position_sync(
            hold_std_path=hold_std_path,
            main_contracts_path=MAIN_CONTRACTS_PATH,
            trade_volume=1,
            logger=logger,
            timeout=60,
            conf=None,
            env_name=_CTP_ENV_NAME,
            position_ratio=POSITION_RATIO,
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
    """强制同步（忽略冷却，直接执行）"""
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

    try:
        import compare_orders
        compare_orders.generate_hold_std(account=_CURRENT_ACCOUNT)
        hold_std_path = os.path.join(_CURR_DIR_SYNC, 'hold-std.json')

        from trading.position_sync.position_sync_manager import run_position_sync
        MAIN_CONTRACTS_PATH = os.path.join(_PROJECT_ROOT_SYNC, 'data', 'contracts', 'main_contracts.json')

        sync_ok = run_position_sync(
            hold_std_path=hold_std_path,
            main_contracts_path=MAIN_CONTRACTS_PATH,
            trade_volume=1,
            timeout=120,  # 首次建仓可能需要挂出50+合约的委托
            conf=None,
            env_name=_CTP_ENV_NAME,
            logger=logger,
            position_ratio=POSITION_RATIO,
        )
        if sync_ok:
            logger.info("强制同步完成")
            # 详细通知由 PositionSyncManager._send_sync_notification 发送
        else:
            logger.warning("强制同步未完成")
            send_feishu_text("⚠️ 强制同步未完成")
        return sync_ok
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


def run_once():
    """单次执行：导出 -> 为所有配置源账户生成持仓文件"""
    logger.info("=" * 50)
    logger.info("开始执行: 导出 -> 持仓差异对比")
    logger.info(">>> 步骤 1/3: 执行自动导出...")
    try:
        import automate_export
        success = automate_export.main()
    except Exception as e:
        logger.error("导出步骤异常: %s", e)
        success = False

    if not success:
        logger.warning("导出失败，中断后续流程。")
        return False
    logger.info("导出成功。")

    time.sleep(1)

    logger.info(">>> 步骤 2/3: 生成持仓文件...")
    try:
        import compare_orders

        if ACCOUNT_TARGETS:
            # 多源账户模式：为每个源账户生成独立的 hold-std 文件
            for source_account in ACCOUNT_TARGETS.keys():
                output_path = os.path.join(_CURR_DIR, f"hold-std-{source_account}.json")
                gen_ok = compare_orders.generate_hold_std(
                    account=source_account, output_path=output_path
                )
                if gen_ok:
                    logger.info("标准持仓 %s 生成完成", os.path.basename(output_path))
                else:
                    logger.warning("标准持仓 %s 生成失败", os.path.basename(output_path))
        else:
            # 兼容旧模式：生成单个 hold-std.json
            gen_ok = compare_orders.generate_hold_std(account=ACCOUNT)
            if gen_ok:
                logger.info("标准持仓 hold-std.json 生成完成")
            else:
                logger.warning("标准持仓 hold-std.json 生成失败")

        # 初始化 hold.json 占位文件
        # 实际数据由 PositionSyncManager 从 CTP 持仓查询后更新
        compare_orders.generate_hold()

    except Exception as e:
        logger.error("生成持仓文件异常: %s", e)

    logger.info(">>> 步骤 3/3: 持仓差异将在同步时对比（由 PositionSyncManager 处理）")

    logger.info("=" * 50)
    # 不再在此处对比，返回 False 让 run_sync 处理对比逻辑
    return False


def export_loop():
    """导出线程：每CHECK_INTERVAL秒执行导出+同步

    持仓对齐完全由 PositionSyncManager.sync_and_trade() 处理：
    - 直接计算 target vs actual_agg → missing_orders / excess_orders
    - 批量并行下单，不受时间限制
    - 支持首次建仓一次性挂出所有合约的委托
    """
    logger.info("[导出线程] 启动")
    last_heartbeat = time.time()

    while not shutdown_event.is_set():
        try:
            if is_in_trading_time() or SKIP_TRADING_TIME_CHECK:
                # 检查关键时间点提醒
                check_key_time_and_alert()

                has_diff = run_once()
                last_heartbeat = time.time()

                # 注意：同步由 sync_loop() 持续监控，不需要定时调用
            else:
                now_time = datetime.datetime.now().time()
                wait_sec = seconds_until_next_session()
                logger.info("[导出线程] 非交易时间，距离下次开盘还有 %d 分 %d 秒", wait_sec // 60, wait_sec % 60)

                # 多时段定时任务模式：每个任务只跑启动时确定的负责时段，
                # 到该时段结束时间后即退出，等待下一个定时任务启动。
                if is_after_session_end(now_time, _RESPONSIBLE_SESSION_END):
                    if _RESPONSIBLE_SESSION_END == datetime.time(15, 15, 0):
                        logger.info("[导出线程] 日盘已结束，准备退出")
                        send_feishu_text("日盘结束，流水线退出")
                    elif _RESPONSIBLE_SESSION_END == datetime.time(2, 30, 0):
                        logger.info("[导出线程] 夜盘已结束，准备退出")
                        send_feishu_text("夜盘结束，流水线退出")
                    else:
                        logger.info("[导出线程] 上午盘已结束，准备退出")
                        send_feishu_text("上午盘结束，流水线退出")
                    shutdown_event.set()
                    break

                last_heartbeat = time.time()

            # 等待下一个周期，定期输出心跳
            for _ in range(CHECK_INTERVAL):
                if shutdown_event.wait(timeout=1):
                    break
                if time.time() - last_heartbeat >= 10:
                    logger.info("[导出线程] 心跳 - 仍在运行")
                    last_heartbeat = time.time()
        except Exception as e:
            logger.error("[导出线程] 异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(CHECK_INTERVAL)

    logger.info("[导出线程] 退出")


def main():

    # 交易日检查
    if not _is_trading_day():
        return  # 非交易日，直接退出

    # 检查单实例，防止多任务同时运行导致持仓加倍
    instance_fd = check_single_instance()
    if instance_fd is None:
        logger.error("[错误] 已有实例在运行，本次启动被阻止")
        print("[错误] 已有实例在运行，请先停止当前任务")
        sys.exit(1)
    logger.info("[检查] 单实例检查通过，PID=%d", os.getpid())

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

    # 启动导出线程（持仓对齐完全由 PositionSyncManager 处理，无报单线程）
    export_thread = threading.Thread(target=export_loop, name="ExportThread", daemon=True)
    export_thread.start()

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
                hold_std_path = os.path.join(_CURR_DIR, f"hold-std-{source_account}.json")
                logger.info("[同步][%s -> %s] 启动目标账户同步", source_account, user_id)
            else:
                # 回退到默认单账户模式
                user_id = "default"
                env_label = _CTP_ENV_NAME
                conf = None
                ratio = POSITION_RATIO
                hold_std_path = os.path.join(_CURR_DIR, "hold-std.json")
                logger.info("[同步-default] 启动默认账户同步")

            from trading.position_sync.position_sync_manager import run_position_sync_loop

            run_position_sync_loop(
                hold_std_path=hold_std_path,
                main_contracts_path=MAIN_CONTRACTS_PATH,
                trade_volume=1,
                conf=conf,
                env_name=env_label,
                logger=logger,
                stop_event=shutdown_event,
                position_ratio=ratio,
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
    sync_controller.join(timeout=10)
    if sync_controller.is_alive():
        logger.warning("[主线程] 同步总控线程未能正常退出")
    # 再等待各个目标账户同步线程（外层总控结束后，子线程可能仍在收尾）
    for t in sync_threads:
        t.join(timeout=5)

    logger.info("流水线已退出")


if __name__ == "__main__":
    main()
