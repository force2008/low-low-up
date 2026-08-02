"""期货日报运行入口。"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from .config import get_config
from .report_generator import DailyNewsReportGenerator, setup_logging

logger = logging.getLogger(__name__)


def infer_trade_date(session: str) -> str:
    """推断当前应生成的交易日。"""
    now = datetime.now()
    if session == "night" and now.hour < 12:
        # 凌晨的夜盘属于前一个交易日
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return now.strftime("%Y-%m-%d")


def is_trading_day(trade_date: str, trade_date_file: str) -> bool:
    """根据交易日历判断是否为交易日。"""
    path = Path(trade_date_file)
    if not path.exists():
        logger.warning(f"交易日历不存在: {trade_date_file}，默认按交易日处理")
        return True
    try:
        with open(path, "r", encoding="utf-8") as f:
            dates = json.load(f)
        return trade_date in dates
    except Exception as e:
        logger.error(f"读取交易日历失败: {e}")
        return True


def should_run_session(session: str, cfg) -> bool:
    """根据当前时间判断是否应运行该时段的日报。"""
    now = datetime.now()
    time_str = now.strftime("%H:%M")
    if session == "day":
        target = cfg.get("schedule", {}).get("day_session_time", "15:35")
        # 日盘收盘后 15:35 左右，允许 15:00-18:00
        return "15:00" <= time_str <= "18:00"
    if session == "night":
        target = cfg.get("schedule", {}).get("night_session_time", "02:35")
        # 夜盘收盘后 02:35 左右，允许 02:30-05:00
        return "02:30" <= time_str <= "05:00"
    return True


def main():
    parser = argparse.ArgumentParser(description="期货日报生成器")
    parser.add_argument("--session", choices=["day", "night"], help="生成日盘/夜盘日报")
    parser.add_argument("--date", help="指定交易日 YYYY-MM-DD")
    parser.add_argument("--config", help="配置文件路径")
    parser.add_argument("--force", action="store_true", help="强制生成，跳过交易日/时段检查")
    parser.add_argument("--test", action="store_true", help="测试模式：不写入文件，不发送通知")
    args = parser.parse_args()

    config = get_config(args.config)
    setup_logging(config.log_dir)

    session = args.session
    if not session:
        # 根据当前时间推断
        now = datetime.now()
        if "15:00" <= now.strftime("%H:%M") <= "18:00":
            session = "day"
        elif "02:30" <= now.strftime("%H:%M") <= "05:00":
            session = "night"
        else:
            logger.error("未指定 --session，且当前时间不在日盘/夜盘收盘窗口内")
            sys.exit(1)

    trade_date = args.date or infer_trade_date(session)
    logger.info(f"session={session}, trade_date={trade_date}, force={args.force}, test={args.test}")

    if not args.force:
        trade_date_file = config.get("schedule.trade_date_file", "")
        if trade_date_file and not is_trading_day(trade_date, trade_date_file):
            logger.info(f"{trade_date} 非交易日，跳过生成")
            sys.exit(0)

        if not should_run_session(session, config.to_dict()):
            logger.info(f"当前时间不在 {session} 收盘窗口内，跳过生成")
            sys.exit(0)

    generator = DailyNewsReportGenerator(config)
    try:
        report = generator.generate(trade_date, session, test_mode=args.test)
        if args.test:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            logger.info(f"日报生成完成: {report.get('overall_sentiment')}")
    except Exception as e:
        logger.exception(f"日报生成失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
