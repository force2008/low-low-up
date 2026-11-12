# -*- coding: utf-8 -*-
"""行情监控程序（只出信号，不下单）。

功能：
    读取配置文件 -> 通过 CTP 行情接口订阅合约 -> tick 合成分钟 K 线 ->
    按配置的触发条件评估 -> 命中后写信号文件 + 飞书通知。

运行：
    python market_monitor.py                 # 使用同目录下的 config.json
    python market_monitor.py -c my.json      # 指定配置文件
    python market_monitor.py --offline       # 离线自检：仅用本地 K 线库评估一次条件
    python market_monitor.py --no-notify     # 不发送飞书，只写信号文件
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

# 把项目根目录加入 sys.path，以复用 ctp / config / utils 等公共模块
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from conditions import evaluate  # noqa: E402
from config_loader import ConfigError, load_config  # noqa: E402
from kline_aggregator import KlineAggregator, KlineStore  # noqa: E402
from signal_sender import RuleNotifier, SignalStore  # noqa: E402

logger = logging.getLogger("market_monitor")

# CTP 行情基类（离线模式下允许缺失）
try:
    from ctp.base_mdapi import CMdSpiBase  # noqa: E402
    _CTP_AVAILABLE = True
    _CTP_IMPORT_ERROR = None
except Exception as _exc:  # pragma: no cover - 取决于运行环境
    CMdSpiBase = object  # type: ignore
    _CTP_AVAILABLE = False
    _CTP_IMPORT_ERROR = _exc


# ==================== 日志 ====================
def setup_logging(log_dir: str, level: str = "INFO") -> None:
    """配置控制台 + 文件日志（文件按 10MB 滚动，保留 5 份）"""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "market_monitor.log")

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)


# ==================== 监控引擎 ====================
class MonitorEngine:
    """遍历规则，评估条件并派发动作。"""

    def __init__(self, config: dict, aggregator: KlineAggregator, store: SignalStore, notifier: RuleNotifier):
        self.config = config
        self.aggregator = aggregator
        self.store = store
        self.notifier = notifier
        self.rules = config["rules"]
        # (规则名, 合约) -> 上次触发的行情时间（同一根 K 线只触发一次）
        self._last_fire_bar: dict = {}
        # (规则名, 合约) -> 上次触发的墙钟时间（用于冷却）
        self._last_fire_time: dict = {}

    def evaluate(self, now: datetime = None) -> list:
        """评估所有规则，返回本轮命中的信号记录"""
        now = now or datetime.now()
        fired = []
        for rule in self.rules:
            if not rule["enabled"]:
                continue
            for symbol in rule["symbols"]:
                record = self._evaluate_one(rule, symbol, now)
                if record is not None:
                    fired.append(record)
        return fired

    def _evaluate_one(self, rule: dict, symbol: str, now: datetime):
        """评估单条规则 + 单合约，命中则派发动作"""
        closed_bars, forming_bar = self.aggregator.get_snapshot(symbol, rule["period"])
        try:
            payload = evaluate(rule, closed_bars, forming_bar)
        except Exception as exc:
            logger.error("[%s][%s] 条件计算失败：%s", rule["name"], symbol, exc)
            return None

        if not payload:
            return None

        key = (rule["name"], symbol)
        bar_time = str(payload.get("bar_time", ""))

        # 同一根 K 线不重复触发
        if self._last_fire_bar.get(key) == bar_time:
            return None

        # 冷却时间
        cooldown = int(rule.get("cooldown_sec", 0))
        last_time = self._last_fire_time.get(key)
        if cooldown > 0 and last_time is not None and (now - last_time).total_seconds() < cooldown:
            return None

        self._last_fire_bar[key] = bar_time
        self._last_fire_time[key] = now
        return self._dispatch(rule, symbol, payload, now)

    def _dispatch(self, rule: dict, symbol: str, payload: dict, now: datetime) -> dict:
        """执行规则动作（写信号文件 / 飞书通知）"""
        direction = payload.get("direction") or rule.get("direction") or ""
        record = {
            "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "period": rule["period"],
            "rule": rule["name"],
            "type": rule["type"],
            "direction": direction,
            "bar_time": payload.get("bar_time", ""),
            "price": payload.get("price", 0),
            "detail": payload.get("detail", ""),
            "actions": rule["actions"],
        }

        if "signal" in rule["actions"]:
            try:
                self.store.append(record)
                logger.info("📝 信号记录 [%s] %s %s", rule["name"], symbol, record["detail"])
            except Exception as exc:
                logger.error("[%s][%s] 信号落盘失败：%s", rule["name"], symbol, exc)

        if "feishu" in rule["actions"]:
            ok = self.notifier.send_signal(symbol, rule, payload)
            if ok:
                logger.info("🔔 飞书已通知 [%s] %s", rule["name"], symbol)
            else:
                logger.warning("[%s][%s] 飞书通知未发送（未启用或发送失败）", rule["name"], symbol)

        return record


# ==================== CTP 行情回调 ====================
class MonitorMdSpi(CMdSpiBase):
    """订阅行情并把 tick 交给 KlineAggregator。"""

    def __init__(self, aggregator: KlineAggregator, symbols: list, conf: dict):
        super().__init__(conf=conf)
        self._aggregator = aggregator
        self._symbols = list(symbols)
        self._symbol_set = set(symbols)
        self._tick_count = 0

    @staticmethod
    def print(*args, **kwargs):
        """覆盖基类的 print，统一走日志"""
        logger.info(" ".join(str(a) for a in args))

    def subscribe_market_data(self):
        """订阅配置中的合约行情"""
        if not self._symbols:
            return
        encoded = [symbol.encode("utf-8") for symbol in self._symbols]
        ret = self._api.SubscribeMarketData(encoded, len(encoded))
        logger.info("已发送订阅请求（%d 个合约），返回码=%s", len(encoded), ret)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        """行情推送回调"""
        try:
            if not pDepthMarketData:
                return
            instrument_id = getattr(pDepthMarketData, "InstrumentID", "")
            if instrument_id not in self._symbol_set:
                return

            last_price = getattr(pDepthMarketData, "LastPrice", 0.0) or 0.0
            if last_price <= 0:
                return

            cum_volume = int(getattr(pDepthMarketData, "Volume", 0) or 0)
            open_interest = float(getattr(pDepthMarketData, "OpenInterest", 0.0) or 0.0)
            action_day = getattr(pDepthMarketData, "ActionDay", "") or ""
            update_time = getattr(pDepthMarketData, "UpdateTime", "") or ""
            if not action_day or not update_time:
                return

            tick_time = datetime.strptime(f"{action_day} {update_time}", "%Y%m%d %H:%M:%S")
            self._aggregator.add_tick(instrument_id, float(last_price), cum_volume, open_interest, tick_time)

            self._tick_count += 1
            if self._tick_count % 500 == 0:
                logger.info("已接收 %d 个 tick", self._tick_count)
        except Exception as exc:
            logger.error("处理行情推送失败：%s", exc)

    def close(self):
        """释放行情 API"""
        try:
            self._api.Release()
        except Exception as exc:
            logger.warning("释放行情 API 失败：%s", exc)


# ==================== 运行逻辑 ====================
def run_live(config: dict, aggregator: KlineAggregator, engine: MonitorEngine) -> None:
    """连接 CTP 行情，进入常驻监控循环"""
    if not _CTP_AVAILABLE:
        logger.error("无法加载 CTP 行情模块，无法启动实时监控：%s", _CTP_IMPORT_ERROR)
        return

    from config import config as ctp_config

    env_name = config.get("env") or "7x24"
    conf = ctp_config.envs.get(env_name) or ctp_config.get_env_config()
    logger.info("使用 CTP 环境：%s（行情前置 %s）", env_name, conf.get("md"))

    spi = MonitorMdSpi(aggregator, config["symbols"], conf)
    spi.subscribe_market_data()

    interval = max(1, int(config["poll_interval_sec"]))
    logger.info("进入监控循环：轮询间隔 %ss，订阅合约 %s", interval, ", ".join(config["symbols"]))

    try:
        while True:
            closed = aggregator.sweep_stale()
            for symbol, period, dt_str in closed:
                logger.debug("K 线收盘 %s %s %s", symbol, period, dt_str)
            engine.evaluate()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("收到退出信号，正在关闭...")
    finally:
        try:
            aggregator.flush_all()
        except Exception as exc:
            logger.warning("退出前刷新 K 线失败：%s", exc)
        spi.close()
        logger.info("行情监控已停止")


def run_offline(config: dict, aggregator: KlineAggregator, engine: MonitorEngine) -> None:
    """离线自检：仅使用本地 K 线库数据评估一次条件，用于校验配置"""
    logger.info("离线自检模式：使用本地 K 线库评估一次条件（不连接行情）")
    fired = engine.evaluate()
    if not fired:
        logger.info("未命中任何条件")
        return
    for record in fired:
        logger.info(
            "命中：%s %s %s 参考价=%s",
            record["symbol"], record["rule"], record["detail"], record["price"],
        )


# ==================== 入口 ====================
def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    default_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    parser = argparse.ArgumentParser(description="期货行情监控程序（只出信号，不下单）")
    parser.add_argument("-c", "--config", default=default_config, help="配置文件路径，默认同目录 config.json")
    parser.add_argument("--env", default=None, help="覆盖配置中的 CTP 环境（7x24/simu/online）")
    parser.add_argument("--offline", action="store_true", help="离线自检：不连接行情，只用本地 K 线库评估一次")
    parser.add_argument("--no-notify", action="store_true", help="不发送飞书通知，只写信号文件")
    parser.add_argument("--log-level", default="INFO", help="日志级别，默认 INFO")
    return parser.parse_args()


def main() -> int:
    """程序入口"""
    args = parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    setup_logging(os.path.join(base_dir, "logs"), args.log_level)

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logger.error("配置加载失败：%s", exc)
        return 1

    if args.env:
        config["env"] = args.env

    logger.info("=" * 60)
    logger.info("行情监控程序启动（只出信号，不下单）")
    logger.info("配置文件：%s", os.path.abspath(args.config))
    logger.info("监控合约：%s", ", ".join(config["symbols"]))
    logger.info("监控周期：%s", ", ".join(sorted(config["periods"].keys())))
    logger.info("规则数量：%d", len(config["rules"]))

    # K 线本地库（持久化 + 重启预热）
    store = None
    if config["kline_db"]["enabled"]:
        store = KlineStore(config["kline_db"]["path"])
        logger.info("K 线本地库：%s", config["kline_db"]["path"])

    aggregator = KlineAggregator(
        symbols=config["symbols"],
        periods=config["periods"],
        history_bars=config["history_bars"],
        store=store,
    )
    warmed = aggregator.warmup()
    logger.info("历史 K 线预热：%d 根", warmed)

    signal_store = SignalStore(config["signal_file"])
    logger.info("信号文件：%s", config["signal_file"])

    notifier = RuleNotifier(
        enabled=config["feishu"]["enabled"] and not args.no_notify,
        webhook_url=config["feishu"]["webhook"],
    )
    logger.info("飞书通知：%s", "已启用" if notifier.enabled else "未启用")

    engine = MonitorEngine(config, aggregator, signal_store, notifier)

    try:
        if args.offline:
            run_offline(config, aggregator, engine)
        else:
            run_live(config, aggregator, engine)
    finally:
        if store is not None:
            store.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
