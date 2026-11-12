# -*- coding: utf-8 -*-
"""按监控配置（rules 里的合约）导入历史 K 线到本地库，供启动预热。

为什么需要：
    market_monitor.py 启动时用本地 SQLite（config.json 的 kline_db.path）预热历史窗口。
    库中历史不足时（尤其 m60 周期）指标算不出来，规则无法正确评估。
    本程序用天勤（tqsdk）按 config.json 中「启用规则」用到的合约与周期拉取历史 K 线，
    写入与 KlineAggregator 完全一致的 kline 表（symbol, period, datetime, o/h/l/c/v）。

用法（在 market-monitor 目录下执行）：
    python data/import_kline.py                  # 按 config.json 导入全部合约/周期
    python data/import_kline.py --bars 500       # 每个(合约,周期)最多导入 500 根
    python data/import_kline.py -c other.json    # 指定其它配置文件

说明：
    - 合约、周期、库路径、根数全部取自 config.json，与监控程序保持一致，改配置后重跑即可。
    - 未收盘的当前 K 线不写入（避免被当成已收盘 K 线参与指标计算）。
    - 依赖：tqsdk（含 pandas），且需能联网登录天勤账号。
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

# 脚本位于 market-monitor/data/ 下，需要把 market-monitor 与项目根目录加入 sys.path
_HERE = os.path.dirname(os.path.abspath(__file__))
_MM_DIR = os.path.dirname(_HERE)
_PROJECT_ROOT = os.path.dirname(_MM_DIR)
for _p in (_MM_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from config_loader import ConfigError, load_config  # noqa: E402
from kline_aggregator import KlineStore  # noqa: E402

logger = logging.getLogger("import_kline")

# 天勤账号（与 utils/ImportKlineToSqlite.py 使用同一账号）
TQ_USER = "15558190923"
TQ_PASSWORD = "283200"

# tqsdk 单个序列最大可请求的 K 线根数
_TQSDK_MAX_BARS = 10000


def setup_logging(level: str = "INFO") -> None:
    """控制台日志（本程序为一次性任务，不写文件）"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    default_config = os.path.join(_MM_DIR, "config.json")
    parser = argparse.ArgumentParser(description="按监控配置导入历史 K 线到本地库")
    parser.add_argument("-c", "--config", default=default_config, help="监控配置文件，默认 market-monitor/config.json")
    parser.add_argument("--bars", type=int, default=0, help="每个(合约,周期)最多导入的 K 线根数，默认取配置的 history_bars")
    parser.add_argument("--log-level", default="INFO", help="日志级别，默认 INFO")
    return parser.parse_args()


def build_symbol_map(api) -> dict:
    """拉取天勤全部期货合约，构建 {合约代码: 天勤代码} 映射，如 {"c2701": "DCE.c2701"}"""
    mapping = {}
    for tq_symbol in api.query_quotes(ins_class="FUTURE"):
        text = str(tq_symbol)
        if "." not in text:
            continue
        code = text.split(".", 1)[1]
        mapping.setdefault(code, text)
    return mapping


def fetch_closed_bars(api, tq_symbol: str, duration_sec: int, data_length: int) -> list:
    """拉取已收盘 K 线。

    Returns:
        ``[(datetime_str, open, high, low, close, volume), ...]``，按时间升序
    """
    klines = api.get_kline_serial(tq_symbol, duration_sec, data_length)
    now = datetime.now()
    bars = []
    for row in klines.itertuples(index=False):
        # datetime 为 K 线起点，纳秒时间戳（北京时间）
        try:
            bar_dt = datetime.fromtimestamp(int(row.datetime) / 1e9)
        except (TypeError, ValueError):
            continue
        if bar_dt + timedelta(seconds=duration_sec) > now:
            continue  # 当前未收盘的 K 线，跳过
        try:
            bar_open, bar_high = float(row.open), float(row.high)
            bar_low, bar_close = float(row.low), float(row.close)
            volume = int(row.volume)
        except (TypeError, ValueError):
            continue
        if not (bar_open > 0 and bar_high > 0 and bar_low > 0 and bar_close > 0):
            continue  # 无成交的占位 K 线
        bars.append(
            (bar_dt.strftime("%Y-%m-%d %H:%M:%S"), bar_open, bar_high, bar_low, bar_close, volume)
        )
    return bars


def main() -> int:
    """程序入口"""
    args = parse_args()
    setup_logging(args.log_level)

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logger.error("配置加载失败：%s", exc)
        return 1

    symbols = config["symbols"]
    periods = config["periods"]  # {"m5": 5, "m60": 60}
    db_path = config["kline_db"]["path"]
    bars_wanted = int(args.bars) if args.bars > 0 else int(config["history_bars"])

    logger.info("配置文件：%s", os.path.abspath(args.config))
    logger.info("监控合约（%d 个）：%s", len(symbols), ", ".join(symbols))
    logger.info("监控周期：%s", ", ".join(sorted(periods)))
    logger.info("目标数据库：%s", db_path)
    logger.info("每个(合约,周期)最多导入：%d 根", bars_wanted)
    if not config["kline_db"]["enabled"]:
        logger.warning("配置中 kline_db.enabled=false，仍按上述路径写入")

    try:
        from tqsdk import TqApi, TqAuth
    except ImportError as exc:
        logger.error("未安装 tqsdk：%s（请用能 import tqsdk 的 Python 运行）", exc)
        return 1

    store = KlineStore(db_path)  # 建表结构与监控程序完全一致
    api = TqApi(auth=TqAuth(TQ_USER, TQ_PASSWORD))
    total = 0
    failed = []
    try:
        symbol_map = build_symbol_map(api)
        logger.info("天勤合约列表：%d 个", len(symbol_map))

        for symbol in symbols:
            tq_symbol = symbol_map.get(symbol)
            if not tq_symbol:
                logger.warning("天勤合约列表中未找到 %s，跳过", symbol)
                failed.append(symbol)
                continue
            for period, minutes in sorted(periods.items()):
                duration_sec = max(60, int(minutes) * 60)
                data_length = min(bars_wanted + 5, _TQSDK_MAX_BARS)
                try:
                    bars = fetch_closed_bars(api, tq_symbol, duration_sec, data_length)
                except Exception as exc:
                    logger.error("[%s] %s（%s）拉取失败：%s", symbol, period, tq_symbol, exc)
                    failed.append(f"{symbol}@{period}")
                    continue

                for bar in bars:
                    store.save_bar(symbol, period, bar)
                total += len(bars)
                if bars:
                    logger.info(
                        "[%s] %s（%s）导入 %d 根：%s ~ %s",
                        symbol, period, tq_symbol, len(bars), bars[0][0], bars[-1][0],
                    )
                else:
                    logger.warning("[%s] %s（%s）未取到已收盘 K 线", symbol, period, tq_symbol)
                    failed.append(f"{symbol}@{period}")
    finally:
        try:
            api.close()
        except Exception:
            pass
        store.close()

    logger.info("=" * 60)
    logger.info("导入完成：共写入 %d 根 K 线 -> %s", total, db_path)
    if failed:
        logger.warning("以下合约/周期未导入：%s", ", ".join(failed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
