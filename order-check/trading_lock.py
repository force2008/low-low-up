# -*- coding: utf-8 -*-
"""
交易锁模块：防止持仓同步和委托执行同时进行，导致超仓
所有交易相关操作（持仓同步、委托执行、手工对齐）都需要获取此锁
支持跨进程文件锁，使用 fcntl.flock 实现
"""

import os
import sys
import time
import threading

# 锁文件路径
_LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.trading.lock')


class FileLock:
    """
    基于 fcntl.flock 的文件锁实现
    支持非阻塞模式，不阻塞信号处理
    """

    def __init__(self, lock_file, timeout=10):
        self.lock_file = lock_file
        self.timeout = timeout
        self.acquired = False
        self._fd = None

        # 确保锁文件目录存在
        lock_dir = os.path.dirname(lock_file)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir, exist_ok=True)

    def acquire(self):
        """获取锁，尝试 timeout 秒，超时返回 False"""
        # 确保锁文件存在
        try:
            with open(self.lock_file, 'a') as f:
                pass
        except Exception:
            pass

        start_time = time.time()
        while time.time() - start_time < self.timeout:
            try:
                # 以非阻塞模式尝试获取锁
                self._fd = os.open(self.lock_file, os.O_RDWR)

                # 导入 fcntl（Unix/Linux/macOS）或使用 Windows 方式
                if sys.platform == 'win32':
                    # Windows: 使用 msvcrt
                    import msvcrt
                    # Windows 文件锁实现（轮询方式）
                    try:
                        # 尝试排他锁
                        msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)
                        self.acquired = True
                        return True
                    except (IOError, OSError):
                        os.close(self._fd)
                        self._fd = None
                        time.sleep(0.1)
                        continue
                else:
                    # Unix: 使用 fcntl
                    import fcntl
                    try:
                        fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        self.acquired = True
                        return True
                    except (IOError, OSError):
                        os.close(self._fd)
                        self._fd = None
                        time.sleep(0.1)
                        continue

            except Exception:
                time.sleep(0.1)

        return False

    def release(self):
        """释放锁"""
        if self._fd is not None:
            try:
                if sys.platform == 'win32':
                    import msvcrt
                    msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
                os.close(self._fd)
            except Exception:
                pass
            finally:
                self._fd = None
                self.acquired = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()


# 进程内线程锁（同一进程内共享）
_process_lock = threading.Lock()

# 确保锁文件目录存在
_lock_dir = os.path.dirname(_LOCK_FILE)
if _lock_dir and not os.path.exists(_lock_dir):
    os.makedirs(_lock_dir, exist_ok=True)


def get_trading_lock(timeout=10):
    """获取交易锁（支持跨进程）"""
    return FileLock(_LOCK_FILE, timeout)


def acquire_trading_lock(timeout=10):
    """获取交易锁，返回锁对象（需要手动release）"""
    lock = get_trading_lock(timeout)
    if lock.acquire():
        return lock
    return None


def with_trading_lock(timeout=10):
    """上下文管理器：自动获取和释放锁"""
    return get_trading_lock(timeout)