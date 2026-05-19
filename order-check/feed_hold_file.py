#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 feed 文件夹拷贝持仓文件到 data 文件夹，并持续同步持仓

功能：
- 拷贝线程：每 20 秒从 C:\ronghang\feed 按文件创建时间顺序拷贝到 C:\projects\data
- 同步线程：持续监控 hold-std.json 变化，执行持仓同步（与 run_pipeline.py 一致）

使用方式:
    python feed_hold_file.py [online|simu|7x24]
"""

import sys
import os
import time
import shutil
import datetime
import json
import logging
import threading
from logging.handlers import RotatingFileHandler

# 确保当前目录在模块搜索路径中
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _CURR_DIR)

# 项目根目录
PROJECT_ROOT = os.path.dirname(_CURR_DIR)
sys.path.insert(0, PROJECT_ROOT)

# ==================== 文件夹配置 ====================
FEED_DIR = r"C:\projects\feed"      # 源文件夹
DATA_DIR = r"C:\ronghang\data"      # 目标文件夹
COPY_INTERVAL = 20  # 拷贝间隔（秒）
COPIED_FILES_LOG = os.path.join(_CURR_DIR, "copied_files.json")  # 已拷贝文件记录
# =================================================

# ==================== 日志配置 ====================
LOG_DIR = os.path.join(_CURR_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "feed_pipeline.log")

logger = logging.getLogger("feed_pipeline")
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
except ImportError:
    _CTP_ENV_NAME = "7x24"
# =================================================

# 交易时间段配置
TRADING_SESSIONS = [
    ("09:00:15", "11:30:00"),
    ("13:30:15", "15:15:00"),
    ("21:00:15", "23:59:00"),
    ("00:00:15", "02:30:00"),
]

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


# ==================== 线程间通信 ====================
shutdown_event = threading.Event()
# =================================================

# 导入跨进程交易锁
from trading_lock import get_trading_lock


def _get_copied_files():
    """获取已拷贝文件列表"""
    if os.path.exists(COPIED_FILES_LOG):
        try:
            with open(COPIED_FILES_LOG, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def _save_copied_files(copied_set):
    """保存已拷贝文件列表"""
    try:
        with open(COPIED_FILES_LOG, 'w', encoding='utf-8') as f:
            json.dump(list(copied_set), f, ensure_ascii=False)
    except Exception as e:
        logger.error("保存已拷贝文件列表失败: %s", e)


def _get_file_creation_time(filepath):
    """获取文件创建时间（用于排序）"""
    try:
        # Windows 上使用 os.path.getctime 获取创建时间
        return os.path.getctime(filepath)
    except Exception:
        # 如果获取失败，使用修改时间
        return os.path.getmtime(filepath)


def _copy_oldest_file():
    """从 feed 文件夹拷贝最早创建的文件到 data 文件夹（跳过已拷贝的）"""
    try:
        os.makedirs(FEED_DIR, exist_ok=True)
        os.makedirs(DATA_DIR, exist_ok=True)

        # 获取已拷贝文件列表
        copied_files = _get_copied_files()

        # 获取 feed 文件夹中所有文件（排除已拷贝的）
        all_files = [f for f in os.listdir(FEED_DIR) if os.path.isfile(os.path.join(FEED_DIR, f))]
        files_to_copy = [f for f in all_files if f not in copied_files]

        if not files_to_copy:
            return False, "没有新文件需要拷贝"

        # 按创建时间排序
        files_with_time = []
        for f in files_to_copy:
            filepath = os.path.join(FEED_DIR, f)
            ctime = _get_file_creation_time(filepath)
            files_with_time.append((ctime, filepath, f))

        # 按创建时间升序（最早的在前面）
        files_with_time.sort(key=lambda x: x[0])

        # 拷贝最早的文件
        ctime, src_path, filename = files_with_time[0]
        dest_path = os.path.join(DATA_DIR, filename)

        # 如果目标文件已存在，先删除
        if os.path.exists(dest_path):
            os.remove(dest_path)
            logger.info("[拷贝] 目标文件已存在，删除: %s", dest_path)

        # 拷贝文件
        shutil.copy2(src_path, dest_path)
        create_time = datetime.datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("[拷贝] %s -> %s (创建时间: %s)", filename, DATA_DIR, create_time)

        # 记录已拷贝的文件
        copied_files.add(filename)
        _save_copied_files(copied_files)
        logger.info("[拷贝] 已记录: %s (共 %d 个文件已拷贝)", filename, len(copied_files))

        return True, filename
    except Exception as e:
        logger.error("[拷贝] 拷贝失败: %s", e)
        return False, str(e)


def _generate_hold_std():
    """从 data 文件夹生成 hold-std.json"""
    try:
        import compare_orders

        # 设置 compare_orders 的数据源为 DATA_DIR
        original_dir = compare_orders.DATA_DIR
        compare_orders.DATA_DIR = DATA_DIR

        try:
            gen_ok = compare_orders.generate_hold_std()
            return gen_ok
        finally:
            # 恢复原始目录
            compare_orders.DATA_DIR = original_dir
    except Exception as e:
        logger.error("[生成] 生成 hold-std.json 失败: %s", e)
        return False


def run_sync():
    """执行持仓同步"""
    lock = None
    for attempt in range(10):
        if shutdown_event.is_set():
            logger.info("[run_sync] 检测到关闭信号，跳过本次同步")
            return False

        logger.info("等待交易锁: 持仓同步 (尝试 %d/10)", attempt + 1)
        lock = get_trading_lock(timeout=1)
        if lock.acquire():
            break
        logger.info("交易锁被占用，等待中...")
    else:
        logger.warning("获取交易锁超时（10秒），跳过本次同步")
        return False

    logger.info("已获取交易锁: 开始持仓同步")

    try:
        # 生成 hold-std.json
        hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')
        gen_ok = _generate_hold_std()

        if not gen_ok:
            logger.warning("生成 hold-std.json 失败")
            return False

        # 读取并打印标准持仓
        if os.path.exists(hold_std_path):
            with open(hold_std_path, 'r', encoding='utf-8') as f:
                hold_rows = json.load(f)
            total_vol = 0
            for item in hold_rows:
                vol = item.get("持仓量") or item.get("手数") or item.get("数量") or "0"
                try:
                    total_vol += int(float(str(vol).strip()))
                except (ValueError, TypeError):
                    pass
            logger.info("标准持仓: %d 条记录，共 %d 手", len(hold_rows), total_vol)

        # 执行持仓同步
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


def copy_loop():
    """拷贝线程：每 COPY_INTERVAL 秒从 feed 文件夹拷贝文件到 data 文件夹"""
    logger.info("[拷贝线程] 启动，源目录: %s，目标目录: %s", FEED_DIR, DATA_DIR)
    last_copy_time = 0

    while not shutdown_event.is_set():
        try:
            now = time.time()
            if now - last_copy_time >= COPY_INTERVAL:
                success, msg = _copy_oldest_file()
                if success:
                    last_copy_time = now
                    logger.info("[拷贝线程] 拷贝成功: %s", msg)

                    # 拷贝成功后生成 hold-std.json（同步由 sync_loop 统一处理）
                    logger.info("[拷贝线程] 生成标准持仓 hold-std.json...")
                    gen_ok = _generate_hold_std()
                    if gen_ok:
                        logger.info("[拷贝线程] hold-std.json 生成成功")
                    else:
                        logger.warning("[拷贝线程] hold-std.json 生成失败")
                else:
                    if msg != "没有新文件需要拷贝":
                        logger.warning("[拷贝线程] 拷贝失败: %s", msg)
                    else:
                        logger.debug("[拷贝线程] 没有新文件")

            # 等待 1 秒检查一次
            if shutdown_event.wait(timeout=1):
                break

        except Exception as e:
            logger.error("[拷贝线程] 异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(COPY_INTERVAL)

    logger.info("[拷贝线程] 退出")


def sync_loop():
    """同步线程：持续同步持仓（使用长连接，自动重试）"""
    retry_count = 0
    max_retries = 5

    while not shutdown_event.is_set() and retry_count < max_retries:
        try:
            from trading.position_sync.position_sync_manager import run_position_sync_loop
            MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'contracts', 'main_contracts.json')
            hold_std_path = os.path.join(_CURR_DIR, 'hold-std.json')

            logger.info("[同步线程] 启动持仓同步循环（长连接模式）...")

            run_position_sync_loop(
                hold_std_path=hold_std_path,
                main_contracts_path=MAIN_CONTRACTS_PATH,
                trade_volume=1,
                conf=None,
                env_name=_CTP_ENV_NAME,
                logger=logger,
                stop_event=shutdown_event,
            )

            logger.info("[同步线程] 同步循环正常结束")
            break  # 正常退出

        except TimeoutError as e:
            if "登录超时" in str(e):
                retry_count += 1
                logger.warning("[同步线程] CTP 登录超时，重试 %d/%d (60秒后)...", retry_count, max_retries)
                if retry_count < max_retries:
                    # 等待后重试
                    for _ in range(60):
                        if shutdown_event.wait(timeout=1):
                            break
                    continue
                else:
                    logger.error("[同步线程] 达到最大重试次数，退出")
                    break
            else:
                raise

        except Exception as e:
            logger.error("[同步线程] 异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            # 等待后重试
            for _ in range(30):
                if shutdown_event.wait(timeout=1):
                    break
            continue

    if retry_count >= max_retries:
        logger.warning("[同步线程] 因多次登录失败而退出，请检查 CTP 连接")


def main():
    import signal as _signal_module

    def _signal_handler(sig, frame):
        logger.info("收到中断信号，准备退出...")
        shutdown_event.set()

    _signal_module.signal(_signal_module.SIGINT, _signal_handler)
    if hasattr(_signal_module, 'SIGTERM'):
        _signal_module.signal(_signal_module.SIGTERM, _signal_handler)

    logger.info("=" * 60)
    logger.info("Feed 流水线已启动（双线程模式）")
    logger.info("  - 拷贝线程: 每 %d 秒从 %s 拷贝文件到 %s", COPY_INTERVAL, FEED_DIR, DATA_DIR)
    logger.info("  - 同步线程: 持续监控并同步持仓")
    logger.info("  - 当前CTP环境: %s", _CTP_ENV_NAME)
    logger.info("  - 源文件夹: %s", FEED_DIR)
    logger.info("  - 目标文件夹: %s", DATA_DIR)
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)

    # 启动拷贝线程
    copy_thread = threading.Thread(target=copy_loop, name="CopyThread", daemon=True)
    copy_thread.start()

    # 启动同步线程
    sync_thread = threading.Thread(target=sync_loop, name="SyncThread", daemon=True)
    sync_thread.start()

    # 主线程监控
    logger.info("两个工作线程已启动：拷贝线程 + 同步线程")
    logger.info("按 Ctrl+C 停止")

    while not shutdown_event.is_set():
        time.sleep(1)
        if copy_thread.is_alive():
            logger.info("[主线程] 心跳 - 拷贝线程: 运行 | 同步线程: 运行")
        else:
            logger.warning("[主线程] 拷贝线程已停止")

    logger.info("[主线程] 收到退出信号，等待子线程退出...")
    copy_thread.join(timeout=10)
    sync_thread.join(timeout=10)
    logger.info("流水线已退出")


if __name__ == "__main__":
    main()
