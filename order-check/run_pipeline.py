#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键流水线：自动导出 -> 委托对比 -> 飞书通知
每30秒在开盘时间内执行一次

使用方式:
    python run_pipeline.py
"""

import sys
import os
import time
import datetime
import json
import re
import logging
import threading
from logging.handlers import RotatingFileHandler

# 确保当前目录及项目根目录在模块搜索路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 检测当前 CTP 环境，7x24 模式下跳过交易时段检查
from config import config as _ctp_config
_CTP_ENV_NAME = "7x24"
if len(sys.argv) > 1 and sys.argv[1].lower() in _ctp_config.envs:
    _CTP_ENV_NAME = sys.argv[1].lower()
elif os.getenv("CTP_ENV") in _ctp_config.envs:
    _CTP_ENV_NAME = os.getenv("CTP_ENV")
SKIP_TRADING_TIME_CHECK = (_CTP_ENV_NAME != "online")

# ==================== 日志配置 ====================
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

# 避免重复添加 handler
if not logger.handlers:
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 输出到文件（按大小轮转，单个 5MB，保留 3 个备份）
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    # 同时输出到控制台
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)
# =================================================

# ==================== 交易时间段配置 ====================
# 格式: (开始时间, 结束时间)  "HH:MM:SS"
# 夜盘跨天时段请拆分为两段，如 21:00-02:30 -> 21:00-23:59 和 00:00-02:30
TRADING_SESSIONS = [
    ("09:00:15", "11:30:00"),
    ("13:30:15", "15:15:00"),
    ("21:00:15", "23:59:00"),
    ("00:00:15", "02:30:00"),
]
CHECK_INTERVAL = 30  # 秒

# 日盘结束后是否自动退出程序（True=日盘收盘后退出，False=继续等待夜盘）
AUTO_EXIT_AFTER_DAILY_CLOSE = True

# ------------------------------------------------------
# 持仓同步交易配置
# ------------------------------------------------------
# 启动时是否自动对比 hold-std 并限价加仓
ENABLE_POSITION_SYNC_AT_STARTUP = True
# 每个合约加仓手数（默认 1 手；设为 0 则使用 hold_std 中的原手数）
POSITION_SYNC_VOLUME = 1
# 单订单超时秒数
POSITION_SYNC_TIMEOUT = 30
# CTP 环境配置（None=自动从 config.py 读取）
POSITION_SYNC_CTP_CONF = None
# ======================================================

# 日盘结束时间（收盘通知用）
DAILY_CLOSE_TIME = datetime.time(15, 15, 0)

# 持仓同步需要的文件路径
HOLD_STD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold-std.json')
MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'contracts', 'main_contracts.json')

# 尝试导入飞书 webhook（可选）
try:
    from compare_orders import FEISHU_WEBHOOK_URL
except ImportError:
    FEISHU_WEBHOOK_URL = ""

# 导入 CSV 保存路径及账户（用于旧文件清理）
try:
    from local_config import DEFAULT_SAVE_PATH as DATA_DIR, ACCOUNT
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    ACCOUNT = "wangk0402"


def send_feishu_text(text):
    """发送纯文本飞书通知"""
    if not FEISHU_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {
            "msg_type": "text",
            "content": {"text": text}
        }
        resp = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=10)
        logger.info("飞书通知发送状态: %s", resp.status_code)
    except Exception as e:
        logger.error("飞书通知发送失败: %s", e)


def cleanup_old_csv_files(directory: str, account: str = ACCOUNT):
    """清理 directory 下的旧 CSV：
    - 所有委托：保留最新 2 份（compare_orders 对比需要）
    - 持仓明细：保留最新 1 份
    - 其他 CSV：保留最新 1 份
    """
    if not os.path.isdir(directory):
        return
    files = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
    if not files:
        return

    # 按文件名前缀分组（去掉末尾日期时间戳）
    groups = {}
    date_pattern = re.compile(r"\s+\d{4}-\d{2}-\d{2}\s+\d{2}-\d{2}-\d{2}")
    for f in files:
        m = date_pattern.search(f)
        prefix = f[:m.start()] if m else os.path.splitext(f)[0]
        groups.setdefault(prefix, []).append(f)

    for prefix, file_list in groups.items():
        file_list.sort(
            key=lambda x: os.path.getmtime(os.path.join(directory, x)),
            reverse=True,
        )
        # 所有委托保留 2 份，其余保留 1 份
        keep = 2 if "所有委托" in prefix else 1
        for old_file in file_list[keep:]:
            try:
                os.remove(os.path.join(directory, old_file))
                logger.info("已删除旧 CSV: %s", old_file)
            except Exception as e:
                logger.warning("删除旧 CSV 失败 %s: %s", old_file, e)


# 保护 _mgr_holder 实例创建/销毁的锁
_mgr_lock = threading.Lock()


def _ensure_mgr(mgr_holder, hold_std_path, main_contracts_path, conf, env_name):
    """确保 PositionSyncManager 实例存在且已登录（线程安全）"""
    with _mgr_lock:
        if mgr_holder[0] is None or not getattr(mgr_holder[0], "is_login", False):
            if mgr_holder[0] is not None:
                try:
                    del mgr_holder[0]
                except Exception:
                    pass
            from trading.PositionSyncManager import PositionSyncManager

            mgr_holder[0] = PositionSyncManager(
                hold_std_path=hold_std_path,
                main_contracts_path=main_contracts_path,
                conf=conf,
                env_name=env_name,
            )
            mgr_holder[0].wait_login(timeout=30)


def _do_sync(
    mgr_holder,
    hold_std_path,
    main_contracts_path,
    conf,
    env_name,
):
    """后台线程：持仓对齐（sync_and_trade），内部有 30 分钟冷却"""
    try:
        _ensure_mgr(mgr_holder, hold_std_path, main_contracts_path, conf, env_name)
        sync_ok = mgr_holder[0].sync_and_trade()
        if sync_ok:
            logger.info("持仓对比: 已对齐（缺额已补/超额已平）")
        else:
            logger.warning("持仓对比: 处理失败")
    except Exception as e:
        logger.error("持仓对齐异常: %s", e)
        with _mgr_lock:
            if mgr_holder and mgr_holder[0] is not None:
                try:
                    del mgr_holder[0]
                except Exception:
                    pass
            if not mgr_holder:
                mgr_holder.append(None)
            else:
                mgr_holder[0] = None


def _do_execute(
    mgr_holder,
    signal_path,
):
    """后台线程：委托执行（execute_orders），只处理 signal.json，快速返回"""
    # 快速检查：如果实例不存在或未登录，跳过本次（不阻塞等待登录）
    with _mgr_lock:
        if mgr_holder[0] is None or not getattr(mgr_holder[0], "is_login", False):
            logger.info("CTP 未登录，跳过委托执行")
            return

    try:
        # 委托执行
        if os.path.exists(signal_path):
            try:
                with open(signal_path, "r", encoding="utf-8") as f:
                    sig = json.load(f)
                logger.info("signal.json 内容: %s", json.dumps(sig, ensure_ascii=False))
            except Exception as e:
                logger.warning("读取 signal.json 失败: %s", e)
        else:
            logger.info("signal.json 不存在，跳过委托执行")
            return

        exec_ok = mgr_holder[0].execute_orders(signal_path)
        if exec_ok:
            logger.info("委托执行: 完成")
            # 处理成功后清空 signal.json，防止下次重复读取同一批委托
            try:
                with open(signal_path, "w", encoding="utf-8") as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                logger.info("signal.json 已清空")
            except Exception as e:
                logger.warning("清空 signal.json 失败: %s", e)
        else:
            logger.warning("委托执行: 有失败或无需执行")

        # 把已提交的委托也记到日志，方便排查
        orders_file = os.path.join(os.path.dirname(signal_path), "orders_submitted.json")
        if os.path.exists(orders_file):
            try:
                with open(orders_file, "r", encoding="utf-8") as f:
                    submitted = json.load(f)
                logger.info(
                    "orders_submitted.json 当前共 %d 条: %s",
                    len(submitted),
                    json.dumps(submitted, ensure_ascii=False),
                )
            except Exception as e:
                logger.warning("读取 orders_submitted.json 失败: %s", e)

    except Exception as e:
        logger.error("委托执行异常: %s", e)


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
            # 明天的同一时段
            candidates.append(start_dt + datetime.timedelta(days=1))

    if candidates:
        next_dt = min(candidates)
        return int((next_dt - now).total_seconds())
    return 3600


def run_once():
    logger.info("=" * 60)
    logger.info("开始执行: 导出 -> 对比 -> 通知")
    logger.info("=" * 60)

    # 步骤 1: 导出数据
    logger.info(">>> 步骤 1/2: 执行自动导出...")
    try:
        import automate_export
        success = automate_export.main()
    except Exception as e:
        logger.error("导出步骤异常: %s", e)
        success = False

    if not success:
        logger.warning("导出失败，中断后续流程。")
        return

    logger.info("导出成功。")

    # 清理旧 CSV 文件（只保留最新几份）
    try:
        cleanup_old_csv_files(DATA_DIR)
    except Exception as e:
        logger.warning("清理旧 CSV 失败: %s", e)

    # 稍等片刻确保文件系统刷新
    time.sleep(2)

    # 步骤 2: 对比并通知
    logger.info(">>> 步骤 2/2: 执行委托对比...")
    try:
        import compare_orders
        has_change = compare_orders.main()
    except Exception as e:
        logger.error("对比步骤异常: %s", e)
        has_change = False

    if has_change:
        logger.info("检测到委托数据变化，已触发飞书通知（如已配置 webhook）。")
    else:
        logger.info("委托数据无变化。")

    logger.info("=" * 60)
    logger.info("全流程执行完毕!")
    logger.info("=" * 60)


def main():
    logger.info("流水线已启动，每20秒检查一次...")
    logger.info("交易时间段: %s", TRADING_SESSIONS)
    logger.info("当前 CTP 环境: %s", _CTP_ENV_NAME)
    if SKIP_TRADING_TIME_CHECK:
        logger.info("7x24 模式: 跳过交易时段检查，任意时间均可执行")
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)

    # ==================================================================
    # 启动时：导出 CSV -> 生成 hold-std -> 持仓同步
    # ==================================================================
    # 步骤 1: 先执行导出（确保有最新 CSV，首次运行也需要）
    logger.info(">>> 启动时先执行导出...")
    try:
        import automate_export
        export_ok = automate_export.main()
        # export_ok = True
        if export_ok:
            logger.info("导出成功")
        else:
            logger.warning("导出失败或条件不满足")
    except Exception as e:
        logger.error("启动导出异常: %s", e)
        export_ok = False

    # 步骤 2: 从 CSV 生成标准持仓
    logger.info(">>> 生成标准持仓文件...")
    try:
        import compare_orders
        ok = compare_orders.generate_hold_std()
        if not ok:
            logger.warning(
                "未找到 '%s 持仓明细' CSV，hold-std.json 未从 CSV 生成",
                compare_orders.ACCOUNT,
            )
        else:
            logger.info("hold-std.json 已生成")
            if os.path.exists(HOLD_STD_PATH):
                with open(HOLD_STD_PATH, 'r', encoding='utf-8') as f:
                    hold_rows = json.load(f)
                compare_orders.send_feishu_hold_notification(hold_rows)
    except Exception as e:
        logger.error("生成标准持仓失败: %s", e)

    # 步骤 3: 持仓同步
    # 首次运行时若 hold-std.json 不存在，会从 CTP 自动查询持仓生成标准持仓
    if ENABLE_POSITION_SYNC_AT_STARTUP:
        logger.info("=" * 60)
        logger.info("启动持仓同步检查...")
        logger.info("=" * 60)
        try:
            from trading.PositionSyncManager import run_position_sync
            sync_ok = run_position_sync(
                hold_std_path=HOLD_STD_PATH,
                main_contracts_path=MAIN_CONTRACTS_PATH,
                trade_volume=POSITION_SYNC_VOLUME,
                timeout=POSITION_SYNC_TIMEOUT,
                conf=POSITION_SYNC_CTP_CONF,
                env_name=_CTP_ENV_NAME,
            )
            if sync_ok:
                logger.info("持仓同步完成")
            else:
                logger.warning("持仓同步未完成或部分失败")
        except Exception as e:
            logger.error("持仓同步过程中异常: %s", e)
        logger.info("=" * 60)
    else:
        logger.info("ENABLE_POSITION_SYNC_AT_STARTUP 为 False，跳过持仓同步")

    last_notification_date = None
    end_notified_today = False

    # 后台线程共享的 PositionSyncManager 引用（列表用于可变闭包）
    _mgr_holder = [None]
    _sync_thread = None
    _execute_thread = None
    _signal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal.json")

    while True:
        try:
            today = datetime.datetime.now().date()
            if today != last_notification_date:
                last_notification_date = today
                end_notified_today = False

            if is_in_trading_time() or SKIP_TRADING_TIME_CHECK:
                # 步骤 1: 导出 + 委托对比通知（主线程，确保每 30 秒都能执行）
                run_once()

                # 步骤 2: 重新生成 hold-std（从最新持仓明细）
                logger.info(">>> 更新标准持仓文件...")
                try:
                    import compare_orders
                    compare_orders.generate_hold_std()
                except Exception as e:
                    logger.error("更新 hold-std 失败: %s", e)

                # 步骤 3a: 委托执行（独立线程，每 30 秒快速检查 signal.json）
                if _execute_thread is None or not _execute_thread.is_alive():
                    _execute_thread = threading.Thread(
                        target=_do_execute,
                        args=(_mgr_holder, _signal_path),
                        daemon=True,
                    )
                    _execute_thread.start()
                else:
                    logger.info("上一个委托执行线程仍在运行，跳过本次")

                # 步骤 3b: 持仓对齐（独立线程，sync_and_trade 内部有 30 分钟冷却）
                if _sync_thread is None or not _sync_thread.is_alive():
                    _sync_thread = threading.Thread(
                        target=_do_sync,
                        args=(
                            _mgr_holder,
                            HOLD_STD_PATH,
                            MAIN_CONTRACTS_PATH,
                            POSITION_SYNC_CTP_CONF,
                            _CTP_ENV_NAME,
                        ),
                        daemon=True,
                    )
                    _sync_thread.start()
                else:
                    logger.info("上一个持仓对齐线程仍在运行，跳过本次")
            else:
                # 非交易时间：也尝试运行一次委托执行（处理在途委托等），持仓对齐也照常
                if _execute_thread is None or not _execute_thread.is_alive():
                    _execute_thread = threading.Thread(
                        target=_do_execute,
                        args=(_mgr_holder, _signal_path),
                        daemon=True,
                    )
                    _execute_thread.start()
                else:
                    logger.info("上一个委托执行线程仍在运行，跳过本次")

                if _sync_thread is None or not _sync_thread.is_alive():
                    _sync_thread = threading.Thread(
                        target=_do_sync,
                        args=(
                            _mgr_holder,
                            HOLD_STD_PATH,
                            MAIN_CONTRACTS_PATH,
                            POSITION_SYNC_CTP_CONF,
                            _CTP_ENV_NAME,
                        ),
                        daemon=True,
                    )
                    _sync_thread.start()
                else:
                    logger.info("上一个持仓对齐线程仍在运行，跳过本次")

                now_time = datetime.datetime.now().time()
                wait_sec = seconds_until_next_session()
                logger.info(
                    "当前非交易时间，距离下次开盘还有 %d 分 %d 秒",
                    wait_sec // 60,
                    wait_sec % 60,
                )

                # 日盘结束后发送一次当日交易结束通知
                if not end_notified_today and now_time >= DAILY_CLOSE_TIME:
                    send_feishu_text("当日交易结束")
                    end_notified_today = True
                    # 仅 online（实盘）环境才自动退出，测试/仿真环境持续运行
                    if AUTO_EXIT_AFTER_DAILY_CLOSE and _CTP_ENV_NAME == "online":
                        logger.info("日盘已结束，程序自动退出。")
                        break

            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("用户中断，程序退出。")
            break
        except Exception as e:
            logger.error("主循环异常: %s", e)
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
