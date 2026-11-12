# -*- coding: utf-8 -*-
"""行情 tick 聚合为分钟 K 线，并维护内存滚动窗口。

- :class:`KlineStore`：把已收盘 K 线持久化到本地 SQLite，供重启后预热
- :class:`KlineAggregator`：把 CTP tick 合成多周期 K 线，维护历史窗口
"""

import os
import sqlite3
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Tuple

# K 线元组：(datetime, open, high, low, close, volume)
Bar = Tuple[str, float, float, float, float, int]

_DEFAULT_HISTORY_BARS = 240


def bar_time(dt: datetime, minutes: int) -> datetime:
    """返回 dt 所在周期 K 线的起始时间"""
    step = max(1, int(minutes))
    minutes_since_midnight = dt.hour * 60 + dt.minute
    bucket = (minutes_since_midnight // step) * step
    return dt.replace(
        hour=(bucket // 60) % 24,
        minute=bucket % 60,
        second=0,
        microsecond=0,
    )


class KlineStore:
    """本地 K 线持久化（SQLite）。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kline (
                symbol   TEXT NOT NULL,
                period   TEXT NOT NULL,
                datetime TEXT NOT NULL,
                open     REAL NOT NULL,
                high     REAL NOT NULL,
                low      REAL NOT NULL,
                close    REAL NOT NULL,
                volume   INTEGER NOT NULL,
                PRIMARY KEY (symbol, period, datetime)
            )
            """
        )
        self._conn.commit()

    def save_bar(self, symbol: str, period: str, bar: Bar) -> None:
        """写入/更新一根已收盘 K 线"""
        try:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO kline
                (symbol, period, datetime, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (symbol, period, str(bar[0]), bar[1], bar[2], bar[3], bar[4], bar[5]),
            )
            self._conn.commit()
        except Exception as exc:
            print(f"K 线入库失败 {symbol} {period} {bar[0]}: {exc}")

    def load_recent(self, symbol: str, period: str, limit: int) -> List[Bar]:
        """读取最近 limit 根 K 线（按时间正序返回）"""
        try:
            rows = self._conn.execute(
                """
                SELECT datetime, open, high, low, close, volume FROM kline
                WHERE symbol = ? AND period = ?
                ORDER BY datetime DESC LIMIT ?
                """,
                (symbol, period, int(limit)),
            ).fetchall()
        except Exception as exc:
            print(f"读取历史 K 线失败 {symbol} {period}: {exc}")
            return []

        result: List[Bar] = []
        for row in reversed(rows):
            result.append(
                (str(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), int(row[5] or 0))
            )
        return result

    def close(self) -> None:
        """关闭数据库连接"""
        try:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        except Exception:
            pass


class KlineAggregator:
    """把 tick 合成多周期 K 线，并维护每根 K 线的历史窗口。"""

    def __init__(
        self,
        symbols: List[str],
        periods: Dict[str, int],
        history_bars: int = _DEFAULT_HISTORY_BARS,
        store: Optional[KlineStore] = None,
    ):
        """
        Args:
            symbols: 订阅的合约列表
            periods: 周期名字典，如 ``{"m5": 5, "m60": 60}``
            history_bars: 每个 (合约, 周期) 保留的历史 K 线根数
            store: 可选，K 线持久化对象
        """
        self._lock = threading.RLock()
        self._symbols: List[str] = [str(s) for s in symbols]
        self._symbol_set = set(self._symbols)
        self._periods: Dict[str, int] = {str(k): int(v) for k, v in periods.items()}
        self._history_bars = max(30, int(history_bars))
        self._store = store

        # (symbol, period) -> 当前正在形成的 K 线
        self._current: Dict[Tuple[str, str], dict] = {}
        # (symbol, period) -> 已收盘 K 线队列
        self._history: Dict[Tuple[str, str], Deque[Bar]] = {}
        # 合约上一次的累计成交量（用于计算增量）
        self._last_volume: Dict[str, int] = {}

        for symbol in self._symbols:
            for period in self._periods:
                self._history[(symbol, period)] = deque(maxlen=self._history_bars)

    # ==================== 属性 ====================
    @property
    def symbols(self) -> List[str]:
        """订阅的合约列表"""
        return list(self._symbols)

    @property
    def periods(self) -> Dict[str, int]:
        """周期名 -> 分钟数"""
        return dict(self._periods)

    # ==================== 预热 ====================
    def warmup(self) -> int:
        """从本地 K 线库预热历史窗口，返回加载的 K 线根数"""
        if self._store is None:
            return 0

        total = 0
        with self._lock:
            for symbol in self._symbols:
                for period in self._periods:
                    bars = self._store.load_recent(symbol, period, self._history_bars)
                    if not bars:
                        continue
                    history = self._history[(symbol, period)]
                    history.clear()
                    history.extend(bars)
                    total += len(bars)
        return total

    # ==================== 行情接入 ====================
    def add_tick(
        self,
        symbol: str,
        price: float,
        cum_volume: int,
        open_interest: float,
        dt: datetime,
    ) -> None:
        """接入一个 tick，更新各周期当前 K 线，跨周期时收盘上一根。

        Args:
            symbol: 合约代码
            price: 最新价
            cum_volume: 当日累计成交量（CTP 的 Volume 字段）
            open_interest: 持仓量
            dt: tick 时间
        """
        if symbol not in self._symbol_set or price is None or price <= 0:
            return

        with self._lock:
            cum_volume = int(cum_volume or 0)
            last_volume = self._last_volume.get(symbol)
            if last_volume is None or cum_volume < last_volume:
                # 新的一天或首次收到，累计量即为增量
                delta = cum_volume
            else:
                delta = cum_volume - last_volume
            if delta < 0:
                delta = 0
            self._last_volume[symbol] = cum_volume

            for period, minutes in self._periods.items():
                key = (symbol, period)
                kline_start = bar_time(dt, minutes)
                current = self._current.get(key)

                if current is None:
                    self._current[key] = self._new_bar(kline_start, price, delta, open_interest, dt)
                elif kline_start > current["time"]:
                    self._close_bar(key, current)
                    self._current[key] = self._new_bar(kline_start, price, delta, open_interest, dt)
                else:
                    current["close"] = float(price)
                    current["high"] = max(current["high"], float(price))
                    current["low"] = min(current["low"], float(price))
                    current["volume"] += delta
                    current["open_interest"] = float(open_interest or 0.0)
                    current["timestamp"] = dt

    def sweep_stale(self, now: Optional[datetime] = None) -> List[Tuple[str, str, str]]:
        """收盘已过期的当前 K 线（无新 tick 时也能正常收盘）。

        Returns:
            已收盘的记录列表 ``[(symbol, period, datetime), ...]``
        """
        now = now or datetime.now()
        closed: List[Tuple[str, str, str]] = []
        with self._lock:
            for key, current in list(self._current.items()):
                minutes = self._periods.get(key[1], 0)
                if minutes <= 0:
                    continue
                if now >= current["time"] + timedelta(minutes=minutes):
                    self._close_bar(key, current)
                    closed.append((key[0], key[1], current["time"].strftime("%Y-%m-%d %H:%M:%S")))
                    del self._current[key]
        return closed

    def flush_all(self) -> None:
        """收盘所有当前 K 线（退出时调用）"""
        with self._lock:
            for key, current in list(self._current.items()):
                self._close_bar(key, current)
            self._current.clear()

    # ==================== 快照 ====================
    def get_snapshot(self, symbol: str, period: str) -> Tuple[List[Bar], Optional[Bar]]:
        """获取某合约某周期的历史 K 线 + 当前正在形成的 K 线。

        Returns:
            ``(closed_bars, forming_bar)``，``forming_bar`` 可能为 None
        """
        key = (symbol, period)
        with self._lock:
            closed = list(self._history.get(key, []))
            current = self._current.get(key)
            forming: Optional[Bar] = None
            if current is not None:
                forming = (
                    current["time"].strftime("%Y-%m-%d %H:%M:%S"),
                    current["open"],
                    current["high"],
                    current["low"],
                    current["close"],
                    int(current["volume"]),
                )
            return closed, forming

    # ==================== 内部方法 ====================
    @staticmethod
    def _new_bar(kline_start: datetime, price: float, volume: int, open_interest: float, dt: datetime) -> dict:
        price = float(price)
        return {
            "time": kline_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": int(volume),
            "open_interest": float(open_interest or 0.0),
            "timestamp": dt,
        }

    def _close_bar(self, key: Tuple[str, str], current: dict) -> None:
        """把当前 K 线收盘，写入历史窗口与本地库"""
        bar: Bar = (
            current["time"].strftime("%Y-%m-%d %H:%M:%S"),
            current["open"],
            current["high"],
            current["low"],
            current["close"],
            int(current["volume"]),
        )
        history = self._history[key]
        if history and history[-1][0] == bar[0]:
            history[-1] = bar
        else:
            history.append(bar)

        if self._store is not None:
            self._store.save_bar(key[0], key[1], bar)
