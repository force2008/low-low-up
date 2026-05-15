#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键流水线：自动导出 -> 委托对比 -> 飞书通知
使用多线程架构：
  - 导出线程：每20秒执行导出+对比
  - 报单线程：检测到diff时执行报单（独立线程）
两个线程独立运行，互不阻塞

使用方式:
    python run_pipeline.py [online|simu|7x24]
"""

import sys
import os
import time
import datetime
import json
import logging
import threading
import queue
from logging.handlers import RotatingFileHandler

# 确保当前目录在模块搜索路径中
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _CURR_DIR)

# 项目根目录
PROJECT_ROOT = os.path.dirname(_CURR_DIR)
sys.path.insert(0, PROJECT_ROOT)

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
    SKIP_TRADING_TIME_CHECK = (_CTP_ENV_NAME != "online")
except ImportError:
    _CTP_ENV_NAME = "7x24"
    SKIP_TRADING_TIME_CHECK = True
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
AUTO_EXIT_AFTER_DAILY_CLOSE = True
DAILY_CLOSE_TIME = datetime.time(15, 15, 0)

# 关键时间点强制对齐（格式：HH:MM）
KEY_ALIGN_TIMES = ["14:58", "14:59", "15:00"]

# 尝试导入飞书 webhook
try:
    from compare_orders import FEISHU_WEBHOOK_URL
except ImportError:
    FEISHU_WEBHOOK_URL = ""
# =================================================


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


# ==================== 线程间通信 ====================
order_queue = queue.Queue()
shutdown_event = threading.Event()
_active_order_thread = [None]
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
        gen_ok = compare_orders.generate_hold_std()
        if not gen_ok:
            logger.warning("生成 hold-std.json 失败")
        hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')

        # 读取当前标准仓（供日志使用）
        if os.path.exists(hold_std_path):
            with open(hold_std_path, 'r', encoding='utf-8') as f:
                hold_rows = json.load(f)
            total_vol = sum(int(str(item.get("手数", "0") or "0")) for item in hold_rows)
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
        compare_orders.generate_hold_std()
        hold_std_path = os.path.join(_CURR_DIR_SYNC, 'hold-std.json')

        from trading.position_sync.position_sync_manager import run_position_sync
        MAIN_CONTRACTS_PATH = os.path.join(_PROJECT_ROOT_SYNC, 'data', 'contracts', 'main_contracts.json')

        sync_ok = run_position_sync(
            hold_std_path=hold_std_path,
            main_contracts_path=MAIN_CONTRACTS_PATH,
            trade_volume=1,
            timeout=60,
            conf=None,
            env_name=_CTP_ENV_NAME,
            logger=logger,
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
    """单次执行：导出 -> 持仓差异对比"""
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
        # 生成标准持仓 hold-std.json（从 CSV 持仓明细）
        gen_std_ok = compare_orders.generate_hold_std()

        # 初始化 hold.json 占位文件
        # 实际数据由 PositionSyncManager 从 CTP 持仓查询后更新
        compare_orders.generate_hold()

        if gen_std_ok:
            logger.info("标准持仓 hold-std.json 生成完成")
        else:
            logger.warning("标准持仓 hold-std.json 生成失败")

    except Exception as e:
        logger.error("生成持仓文件异常: %s", e)

    logger.info(">>> 步骤 3/3: 持仓差异将在同步时对比（由 PositionSyncManager 处理）")

    logger.info("=" * 50)
    # 不再在此处对比，返回 False 让 run_sync 处理对比逻辑
    return False


def export_loop():
    """导出线程：每CHECK_INTERVAL秒执行导出+对比"""
    logger.info("[导出线程] 启动")
    last_heartbeat = time.time()

    while not shutdown_event.is_set():
        try:
            if is_in_trading_time() or SKIP_TRADING_TIME_CHECK:
                # 检查关键时间点提醒
                check_key_time_and_alert()

                has_diff = run_once()
                if has_diff:
                    order_queue.put('new_diff')
                    logger.info("[导出线程] 已发送报单信号")
                last_heartbeat = time.time()

                # 定时持仓同步（每SYNC_INTERVAL秒）
                if time.time() - _last_sync_time[0] >= SYNC_INTERVAL:
                    logger.info("[导出线程] 触发定时持仓同步")
                    if run_sync():
                        order_queue.put('new_diff')  # 同步后可能产生新委托
            else:
                now_time = datetime.datetime.now().time()
                wait_sec = seconds_until_next_session()
                logger.info("[导出线程] 非交易时间，距离下次开盘还有 %d 分 %d 秒", wait_sec // 60, wait_sec % 60)
                if now_time >= DAILY_CLOSE_TIME:
                    send_feishu_text("当日交易结束")
                    if AUTO_EXIT_AFTER_DAILY_CLOSE and _CTP_ENV_NAME == "online":
                        logger.info("[导出线程] 日盘已结束，准备退出")
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


def order_loop():
    """报单线程：监听diff信号，启动独立线程执行报单"""
    logger.info("[报单线程] 启动")
    last_execution_time = 0

    while not shutdown_event.is_set():
        try:
            try:
                signal = order_queue.get(timeout=1)
            except Exception as q_err:
                logger.debug("[报单线程] 队列获取异常: %s", q_err)
                time.sleep(0.5)
                continue

            if signal == 'new_diff':
                current_time = time.time()
                if current_time - last_execution_time < 3:
                    logger.info("[报单线程] 跳过重复信号（距上次%.1f秒）", current_time - last_execution_time)
                    continue

                if _active_order_thread[0] and _active_order_thread[0].is_alive():
                    logger.info("[报单线程] 上一个报单还在执行，跳过本次")
                    continue

                last_execution_time = current_time
                logger.info("[报单线程] 收到报单信号，启动报单执行线程...")

                def _run_order():
                    try:
                        import submit_order
                        sys.argv = ['submit_order', _CTP_ENV_NAME]
                        submit_order.main()
                        logger.info("[报单执行线程] 报单完成")
                    except Exception as e:
                        logger.error("[报单执行线程] 报单异常: %s", e)
                        import traceback
                        logger.error(traceback.format_exc())

                t = threading.Thread(target=_run_order, daemon=True, name="OrderExecution")
                _active_order_thread[0] = t
                t.start()

        except Exception as e:
            logger.error("[报单线程] 循环异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(1)

    logger.info("[报单线程] 退出")


def main():
    import signal as _signal_module
    def _signal_handler(sig, frame):
        logger.info("收到中断信号，准备退出...")
        shutdown_event.set()
    _signal_module.signal(_signal_module.SIGINT, _signal_handler)
    if hasattr(_signal_module, 'SIGTERM'):
        _signal_module.signal(_signal_module.SIGTERM, _signal_handler)

    logger.info("=" * 60)
    logger.info("流水线已启动（多线程模式）")
    logger.info("  - 导出线程: 每%d秒检查并导出", CHECK_INTERVAL)
    logger.info("  - 报单线程: 检测到diff时执行报单")
    logger.info("  - 定时同步: 每%d秒执行持仓对齐", SYNC_INTERVAL)
    logger.info("  - 关键时间: %s 强制对齐", KEY_ALIGN_TIMES)
    logger.info("  - 启动时: 执行持仓同步")
    logger.info("  - 当前CTP环境: %s", _CTP_ENV_NAME)
    if SKIP_TRADING_TIME_CHECK:
        logger.info("  - 跳过交易时段检查")
    logger.info("交易时间段: %s", TRADING_SESSIONS)
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)

    # 启动导出线程和报单线程（先启动，让导出线程可以并行运行）
    export_thread = threading.Thread(target=export_loop, name="ExportThread", daemon=True)
    order_thread = threading.Thread(target=order_loop, name="OrderThread", daemon=True)
    export_thread.start()
    order_thread.start()

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

    # 生成持仓文件
    hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')
    if export_ok:
        try:
            import compare_orders
            compare_orders.generate_hold_std()
            if os.path.exists(hold_std_path):
                with open(hold_std_path, 'r', encoding='utf-8') as f:
                    hold_rows = json.load(f)
                compare_orders.send_feishu_hold_notification(hold_rows)
                logger.info("hold-std.json 共 %d 条记录", len(hold_rows))
        except Exception as e:
            logger.error("生成持仓文件失败: %s", e)

    # 执行持仓同步（同步到 hold-std.json）
    logger.info("=" * 60)
    logger.info("启动持仓同步...")
    logger.info("=" * 60)
    try:
        from trading.position_sync.position_sync_manager import run_position_sync
        MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'contracts', 'main_contracts.json')
        sync_ok = run_position_sync(
            hold_std_path=hold_std_path,
            main_contracts_path=MAIN_CONTRACTS_PATH,
            trade_volume=1,
            timeout=60,
            conf=None,
            env_name=_CTP_ENV_NAME,
            logger=logger,
        )
        if sync_ok:
            logger.info("持仓同步完成")
        else:
            logger.warning("持仓同步未完成")
    except Exception as e:
        logger.error("持仓同步异常: %s", e)
        import traceback
        logger.error(traceback.format_exc())

    # 主线程监控，定期输出心跳
    check_count = 0
    while export_thread.is_alive() or order_thread.is_alive():
        time.sleep(3)
        check_count += 1
        order_exec_status = "无" if _active_order_thread[0] is None else ("运行" if _active_order_thread[0].is_alive() else "停止")
        if check_count % 3 == 0:
            logger.info("[主线程] 心跳 - 导出线程: %s, 报单线程: %s, 报单执行: %s",
                        "运行" if export_thread.is_alive() else "停止",
                        "运行" if order_thread.is_alive() else "停止",
                        order_exec_status)
        if shutdown_event.is_set():
            logger.info("[主线程] 收到退出信号，等待子线程退出...")
    export_thread.join(timeout=10)
    order_thread.join(timeout=10)
    if export_thread.is_alive():
        logger.warning("[主线程] 导出线程未能正常退出")
    if order_thread.is_alive():
        logger.warning("[主线程] 报单线程未能正常退出")
    logger.info("流水线已退出")


if __name__ == "__main__":
    main()
