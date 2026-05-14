"""
报单模块：根据 signal.json 提交委托
由 run_pipeline.py 的报单线程调用
"""

import os
import sys
import json
import logging
import threading
import time

logger = logging.getLogger("pipeline")

# 添加项目根目录到路径
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

# 检测 CTP 环境
_CTP_ENV_NAME = "7x24"
if len(sys.argv) > 1 and sys.argv[1].lower() in ("online", "simu", "7x24"):
    _CTP_ENV_NAME = sys.argv[1].lower()
elif os.getenv("CTP_ENV") in ("online", "simu", "7x24"):
    _CTP_ENV_NAME = os.getenv("CTP_ENV")

# 文件路径配置
_HOLD_STD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold-std.json')
_MAIN_CONTRACTS_PATH = os.path.join(_PROJECT_ROOT, 'data', 'contracts', 'main_contracts.json')
_SIGNAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'signal.json')

# 共享的 PositionSyncManager 实例
_mgr_holder = [None]
_mgr_lock = threading.Lock()

# 导入跨进程交易锁
from trading_lock import get_trading_lock


def _ensure_mgr():
    """确保 PositionSyncManager 实例存在且已登录"""
    with _mgr_lock:
        if _mgr_holder[0] is None:
            from trading.PositionSyncManager import PositionSyncManager
            _mgr_holder[0] = PositionSyncManager(
                hold_std_path=_HOLD_STD_PATH,
                main_contracts_path=_MAIN_CONTRACTS_PATH,
                conf=None,
                env_name=_CTP_ENV_NAME,
            )
            logger.info("[报单] CTP 实例已创建，等待登录...")
        # 检查是否已登录
        if not getattr(_mgr_holder[0], "is_login", False):
            for _ in range(10):
                time.sleep(1)
                if getattr(_mgr_holder[0], "is_login", False):
                    logger.info("[报单] CTP 登录成功")
                    break
            else:
                logger.warning("[报单] CTP 登录超时（10秒），继续尝试...")


def _cleanup_signal():
    """清空 signal.json"""
    try:
        with open(_SIGNAL_PATH, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        logger.info("[报单] signal.json 已清空")
    except Exception as e:
        logger.warning("[报单] 清空 signal.json 失败: %s", e)


def _do_execute_orders():
    """在独立线程中执行报单（需要获取交易锁）"""
    # 分阶段等待锁，每阶段检查 shutdown_event
    # 注意：submit_order.py 没有 shutdown_event，使用线程退出机制
    logger.info("[报单] 等待交易锁")
    lock = None
    for attempt in range(10):
        logger.info(f"[报单] 尝试获取交易锁 ({attempt + 1}/10)")
        lock = get_trading_lock(timeout=1)
        if lock.acquire():
            break
        logger.info("[报单] 交易锁被占用，等待中...")
    else:
        logger.warning("[报单] 获取交易锁超时，跳过本次报单")
        return False

    logger.info("[报单] 已获取交易锁: 开始执行委托")

    try:
        return _mgr_holder[0].execute_orders(_SIGNAL_PATH)
    finally:
        if lock:
            lock.release()
            logger.info("[报单] 已释放交易锁: 委托执行结束")


def main():
    """读取 signal.json 并执行报单"""
    import concurrent.futures

    logger.info("[报单] ========== 开始执行 ==========")

    if not os.path.exists(_SIGNAL_PATH):
        logger.info("[报单] signal.json 不存在，跳过")
        return False

    try:
        with open(_SIGNAL_PATH, "r", encoding="utf-8") as f:
            sig = json.load(f)
        logger.info("[报单] signal.json 内容: %s", json.dumps(sig, ensure_ascii=False)[:200])
    except Exception as e:
        logger.warning("[报单] 读取 signal.json 失败: %s", e)
        return False

    if not sig:
        logger.info("[报单] signal.json 为空，跳过")
        return True

    # 确保 CTP 已登录
    try:
        _ensure_mgr()
    except Exception as e:
        logger.error("[报单] CTP 准备失败: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return False

    # 使用独立线程执行报单，30秒超时
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_do_execute_orders)

    try:
        exec_ok = future.result(timeout=30)
        logger.info("[报单] execute_orders 返回: %s", exec_ok)
        if exec_ok:
            _cleanup_signal()
        return exec_ok
    except concurrent.futures.TimeoutError:
        logger.warning("[报单] 执行超时（30秒），强制结束")
        return False
    except Exception as e:
        logger.error("[报单] 异常: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        logger.info("[报单] ========== 执行完毕 ==========")


if __name__ == "__main__":
    main()