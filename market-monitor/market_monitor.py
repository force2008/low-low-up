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

运行时段（默认行为）：
    只在订阅合约的交易时段运行；启动时若距下次开盘还早则直接退出，
    盘中持续监控，所有合约收盘后（距下次开盘超过 PRE_OPEN_WAIT_SEC）自动退出。
    交易日由 config/trade_date.json 判断（文件缺失则跳过检查）。
    可用 --force / --skip-time-check 关闭上述判断，恢复常驻运行。
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, time as _time, timedelta
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

# 交易时段工具（离线模式下允许缺失）
try:
    from config.trading_time_config import is_contract_trading_now, get_next_session_start  # noqa: E402
    _TIME_CONFIG_ERROR = None
except Exception as _exc:  # pragma: no cover - 取决于运行环境
    is_contract_trading_now = None  # type: ignore
    get_next_session_start = None  # type: ignore
    _TIME_CONFIG_ERROR = _exc


# ==================== 运行时段控制 ====================
# 启动时若距下次开盘超过该秒数，则判定为「非交易时段」，直接退出，避免长时间空转。
# 取值需大于品种日内最短休市间隔（商品 10:15-10:30 共 15 分钟），
# 同时覆盖 12:59 启动到 13:30 商品下午开盘的 31 分钟等待。
PRE_OPEN_WAIT_SEC = 3600

# 交易日历文件（由 utils/trade_date.py 生成）
_TRADE_DATE_FILE = os.path.join(_PROJECT_ROOT, "config", "trade_date.json")

# 单实例锁文件（防止计划任务重复拉起多个进程）
_INSTANCE_LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".market_monitor.lock")


def is_trading_day(now: datetime = None) -> bool:
    """判断 now 所属交易日是否可交易。

    读取 config/trade_date.json；文件缺失或读取失败时放行（返回 True）并告警。
    夜盘跨午夜：凌晨 03:00 之前同时认「前一天」为交易日。
    """
    now = now or datetime.now()
    if not os.path.exists(_TRADE_DATE_FILE):
        logger.warning("交易日历不存在：%s，跳过交易日检查（可运行 utils/trade_date.py 生成）", _TRADE_DATE_FILE)
        return True
    try:
        with open(_TRADE_DATE_FILE, "r", encoding="utf-8") as fp:
            trade_dates = set(json.load(fp))
    except Exception as exc:
        logger.warning("读取交易日历失败：%s，跳过交易日检查", exc)
        return True

    if now.date().isoformat() in trade_dates:
        return True
    # 凌晨属于前一交易日的夜盘
    if now.time() < _time(3, 0):
        if (now.date() - timedelta(days=1)).isoformat() in trade_dates:
            return True
    return False


def any_contract_trading(symbols: list, now: datetime) -> bool:
    """判断给定合约中是否有任一处于可交易时段"""
    if is_contract_trading_now is None:
        return True
    for symbol in symbols:
        try:
            if is_contract_trading_now(symbol, now):
                return True
        except Exception:
            continue
    return False


def next_session_start(symbols: list, now: datetime):
    """返回给定合约集合下一次开盘时间；交易时段工具缺失时返回 None"""
    if get_next_session_start is None:
        return None
    try:
        return get_next_session_start(symbols, now)
    except Exception as exc:
        logger.warning("计算下次开盘时间失败：%s", exc)
        return None


def check_single_instance():
    """获取单实例锁，已有实例在跑时返回 None"""
    fd = None
    try:
        fd = os.open(_INSTANCE_LOCK_FILE, os.O_CREAT | os.O_RDWR)
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (IOError, OSError):
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        return None


def release_single_instance(fd) -> None:
    """释放单实例锁"""
    if fd is None:
        return
    try:
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    except Exception:
        pass
    try:
        os.close(fd)
    except Exception:
        pass


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
        # (规则名, 合约) -> 上次打印「未触发」日志的 K 线时间（每根 K 线只打印一次）
        self._last_eval_bar: dict = {}

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

        key = (rule["name"], symbol)

        if not payload:
            self._log_unmet(rule, symbol, closed_bars, forming_bar)
            return None

        bar_time = str(payload.get("bar_time", ""))

        # 同一根 K 线不重复触发
        if self._last_fire_bar.get(key) == bar_time:
            logger.debug("[%s][%s] 命中但同一根 K 线已触发过（%s），跳过", rule["name"], symbol, bar_time)
            return None

        # 冷却时间
        cooldown = int(rule.get("cooldown_sec", 0))
        last_time = self._last_fire_time.get(key)
        if cooldown > 0 and last_time is not None and (now - last_time).total_seconds() < cooldown:
            logger.debug("[%s][%s] 命中但仍在冷却期（%ss），跳过", rule["name"], symbol, cooldown)
            return None

        logger.info(
            "命中 [%s] %s %s(%s) 触发K线=%s 价格=%s 参数=%s 说明=%s",
            rule["name"], symbol, rule["period"], rule["type"], bar_time,
            payload.get("price"),
            json.dumps(rule.get("params") or {}, ensure_ascii=False),
            payload.get("detail", ""),
        )
        self._last_fire_bar[key] = bar_time
        self._last_fire_time[key] = now
        return self._dispatch(rule, symbol, payload, now)

    def _log_unmet(self, rule: dict, symbol: str, closed_bars: list, forming_bar) -> None:
        """输出「未触发」评估日志；同一个 (规则, 合约) 每根 K 线只输出一次，避免刷屏"""
        bar = forming_bar if forming_bar is not None else (closed_bars[-1] if closed_bars else None)
        bar_time = str(bar[0]) if bar is not None else ""
        key = (rule["name"], symbol)
        if self._last_eval_bar.get(key) == bar_time:
            return
        self._last_eval_bar[key] = bar_time

        params = json.dumps(rule.get("params") or {}, ensure_ascii=False)
        if bar is None:
            logger.info(
                "未触发 [%s] %s %s 暂无 K 线数据（等待行情） 参数=%s",
                rule["name"], symbol, rule["period"], params,
            )
            return
        logger.info(
            "未触发 [%s] %s %s(%s) 当前K线=%s O=%.4f H=%.4f L=%.4f C=%.4f 参数=%s",
            rule["name"], symbol, rule["period"], rule["type"], bar_time,
            float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4]), params,
        )

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
def _format_duration(seconds: float) -> str:
    """把秒数格式化为「X 小时 Y 分」"""
    hours, remainder = divmod(max(0, int(seconds)), 3600)
    minutes = remainder // 60
    return f"{hours} 小时 {minutes} 分" if hours else f"{minutes} 分"


def _notify_start(notifier: RuleNotifier, config: dict, started_at: datetime) -> None:
    """发送「程序启动」飞书通知"""
    enabled_rules = sum(1 for rule in config["rules"] if rule["enabled"])
    lines = [
        "🟢 行情监控已启动\n",
        f"时间：{started_at.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"环境：{config.get('env') or '7x24'}\n",
        f"合约：{', '.join(config['symbols'])}\n",
        f"周期：{', '.join(sorted(config['periods']))}\n",
        f"启用规则：{enabled_rules} 条\n",
        f"轮询间隔：{config['poll_interval_sec']}s",
    ]
    ok = notifier.send_text("".join(lines))
    logger.info("启动通知已发送" if ok else "启动通知未发送（未启用或发送失败）")


def _notify_stop(notifier: RuleNotifier, started_at: datetime, stopped_at: datetime) -> None:
    """发送「程序退出」飞书通知"""
    lines = [
        "🔴 行情监控已停止\n",
        f"时间：{stopped_at.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"运行时长：{_format_duration((stopped_at - started_at).total_seconds())}",
    ]
    ok = notifier.send_text("".join(lines))
    logger.info("退出通知已发送" if ok else "退出通知未发送（未启用或发送失败）")


def run_live(config: dict, aggregator: KlineAggregator, engine: MonitorEngine,
             check_time: bool = True) -> None:
    """连接 CTP 行情，进入常驻监控循环。

    Args:
        check_time: 为 True 时按交易时段运行——启动时距下次开盘过久则直接退出，
            盘中持续监控，所有合约收盘后自动退出；为 False 时常驻运行。
    """
    if not _CTP_AVAILABLE:
        logger.error("无法加载 CTP 行情模块，无法启动实时监控：%s", _CTP_IMPORT_ERROR)
        return

    from config import config as ctp_config

    env_name = config.get("env") or "7x24"
    conf = ctp_config.envs.get(env_name) or ctp_config.get_env_config()
    logger.info("使用 CTP 环境：%s（行情前置 %s）", env_name, conf.get("md"))

    symbols = config["symbols"]

    # 启动前的时段判断：离开盘还早就直接退出，不建立行情连接
    if check_time:
        now = datetime.now()
        if not any_contract_trading(symbols, now):
            nxt = next_session_start(symbols, now)
            remain = (nxt - now).total_seconds() if nxt else 0
            if remain > PRE_OPEN_WAIT_SEC:
                logger.info(
                    "当前非交易时段（距下次开盘约 %.0f 分钟），程序退出",
                    remain / 60.0,
                )
                return
            logger.info(
                "当前非交易时段，等待开盘：%s",
                nxt.strftime("%Y-%m-%d %H:%M:%S") if nxt else "未知",
            )

    spi = MonitorMdSpi(aggregator, symbols, conf)
    spi.subscribe_market_data()

    interval = max(1, int(config["poll_interval_sec"]))
    logger.info("进入监控循环：轮询间隔 %ss，订阅合约 %s", interval, ", ".join(symbols))

    notifier = engine.notifier
    started_at = datetime.now()
    _notify_start(notifier, config, started_at)

    waiting_logged = not any_contract_trading(symbols, datetime.now()) if check_time else False

    try:
        while True:
            if check_time:
                now = datetime.now()
                if not any_contract_trading(symbols, now):
                    nxt = next_session_start(symbols, now)
                    remain = (nxt - now).total_seconds() if nxt else 0
                    if remain > PRE_OPEN_WAIT_SEC:
                        logger.info("所有合约已收盘（距下次开盘约 %.0f 分钟），程序退出", remain / 60.0)
                        break
                    if not waiting_logged:
                        logger.info(
                            "所有合约已收盘，等待下次开盘：%s",
                            nxt.strftime("%Y-%m-%d %H:%M:%S") if nxt else "未知",
                        )
                        waiting_logged = True
                    time.sleep(min(interval, max(1, int(remain))))
                    continue
                if waiting_logged:
                    logger.info("行情已开盘，开始监控")
                    waiting_logged = False

            closed = aggregator.sweep_stale()
            if closed:
                logger.debug("本轮收盘 %d 根过期 K 线", len(closed))
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
        _notify_stop(notifier, started_at, datetime.now())
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
    parser.add_argument("--force", action="store_true", help="跳过交易日与交易时段检查，强制常驻运行（测试用）")
    parser.add_argument("--skip-time-check", action="store_true", help="只跳过交易时段检查（非交易日仍按日历判断）")
    parser.add_argument("--log-level", default="INFO", help="日志级别，默认 INFO")
    return parser.parse_args()


def main() -> int:
    """程序入口"""
    args = parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    setup_logging(os.path.join(base_dir, "logs"), args.log_level)

    check_time = not (args.force or args.skip_time_check)

    # 交易日检查（离线自检与 --force 不限制）
    if not args.offline and not args.force:
        if not is_trading_day(datetime.now()):
            logger.info("今日不是交易日，程序退出")
            return 0

    # 单实例锁（仅实时监控需要，离线自检可并发）
    lock_fd = None
    if not args.offline:
        lock_fd = check_single_instance()
        if lock_fd is None:
            logger.warning("已有监控实例在运行，本次启动退出")
            return 0

    try:
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
        for rule in config["rules"]:
            if not rule["enabled"]:
                logger.info("规则[已禁用]：%s", rule["name"])
                continue
            logger.info(
                "规则[启用]：%s | %s %s | 合约=%s | 动作=%s | 冷却=%ss | 参数=%s",
                rule["name"], rule["period"], rule["type"],
                ", ".join(rule["symbols"]), ", ".join(rule["actions"]),
                rule.get("cooldown_sec", 0),
                json.dumps(rule.get("params") or {}, ensure_ascii=False),
            )
        logger.info("交易时段控制：%s", "已启用（收盘自动退出）" if check_time else "已关闭（常驻运行）")
        if _TIME_CONFIG_ERROR is not None:
            logger.warning("交易时段工具不可用（%s），已退化为常驻运行", _TIME_CONFIG_ERROR)
            check_time = False

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
                run_live(config, aggregator, engine, check_time=check_time)
        finally:
            if store is not None:
                store.close()
    finally:
        release_single_instance(lock_fd)

    return 0


if __name__ == "__main__":
    sys.exit(main())
