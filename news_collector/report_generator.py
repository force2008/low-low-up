"""日报报告生成器：编排采集、统计、摘要、输出与通知。"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .config import get_config
from .sector_analyzer import SectorAnalyzer
from .ths_collector import ThsCollector
from .llm_summarizer import LLMSummarizer

# 复用现有飞书通知
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))
from feishu_notifier import FeishuNotifier  # noqa: E402

logger = logging.getLogger(__name__)


class DailyNewsReportGenerator:
    """期货日报生成器。"""

    def __init__(self, config=None):
        self.config = config or get_config()
        self.analyzer = SectorAnalyzer(self.config)
        self.collector = ThsCollector(self.config)
        self.summarizer = LLMSummarizer(self.config)
        self.feishu = None
        webhook = self.config.feishu_webhook
        if webhook:
            self.feishu = FeishuNotifier(webhook_url=webhook)

    def generate(self, trade_date: str, session: str, test_mode: bool = False) -> Dict:
        """生成一份日报。"""
        logger.info(f"开始生成日报: {trade_date} {session}")

        # 1. 板块行情统计
        logger.info("统计板块行情...")
        sector_results = self.analyzer.analyze_all_sectors(trade_date, session)
        overall = self.analyzer.get_overall_summary(sector_results)

        # 2. 新闻/研报采集
        logger.info("采集同花顺新闻/研报...")
        try:
            collected = self.collector.fetch_all()
            news = collected.get("news", [])
            reports = collected.get("reports", [])
        except Exception as e:
            logger.error(f"新闻采集失败: {e}")
            news, reports = [], []

        # 3. LLM 摘要生成
        logger.info("生成 AI 摘要...")
        summary = self.summarizer.summarize(trade_date, session, sector_results, news, reports)

        # 4. 合并为最终报告
        report = {
            "trade_date": trade_date,
            "session": session,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "overall_sentiment": summary.get("overall_sentiment", overall.get("sentiment", "暂无数据")),
            "overall_summary": summary.get("overall_summary", overall.get("summary", "")),
            "overall": overall,
            "sector_summary": summary.get("sector_summary", []),
            "sectors_detail": sector_results,
            "news_highlights": summary.get("news_highlights", [{"title": n.get("title", ""), "url": n.get("url", "")} for n in news[:5]]),
            "news_raw": news,
            "report_highlights": summary.get("report_highlights", []),
            "reports_raw": reports,
            "risks": summary.get("risks", []),
            "ai_generated": summary.get("ai_generated", False),
        }

        if not test_mode:
            # 5. 写入文件
            self._write_report(report, trade_date, session)
            # 6. 飞书通知
            self._send_feishu(report)

        return report

    def _write_report(self, report: Dict, trade_date: str, session: str):
        """写入 Web 静态目录和本地归档。"""
        web_dir = self.config.web_static_dir
        archive_dir = self.config.archive_dir

        try:
            os.makedirs(web_dir, exist_ok=True)
            os.makedirs(archive_dir, exist_ok=True)
        except OSError as e:
            logger.error(f"创建目录失败: {e}")
            return

        filename = f"daily_news_{trade_date}_{session}.json"
        web_path = web_dir / filename
        archive_path = archive_dir / filename
        latest_path = web_dir / "daily_news_latest.json"

        content = json.dumps(report, ensure_ascii=False, indent=2)

        try:
            with open(web_path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"日报已写入 Web 目录: {web_path}")
        except OSError as e:
            logger.error(f"写入 Web 目录失败: {e}")

        try:
            with open(archive_path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"日报已归档: {archive_path}")
        except OSError as e:
            logger.error(f"归档失败: {e}")

        try:
            with open(latest_path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"已更新 latest: {latest_path}")
        except OSError as e:
            logger.error(f"写入 latest 失败: {e}")

    def _send_feishu(self, report: Dict):
        """发送飞书通知。"""
        if not self.feishu:
            logger.warning("未配置飞书 webhook，跳过通知")
            return

        session_label = "日盘" if report["session"] == "day" else "夜盘"
        title = f"📰 期货日报（{report['trade_date']} {session_label}收盘）"

        sector_lines = []
        sectors = sorted(
            report.get("sector_summary", []),
            key=lambda x: x.get("return_pct", 0),
            reverse=True,
        )
        for s in sectors[:5]:
            emoji = "🟥" if s.get("return_pct", 0) > 0 else "🟩" if s.get("return_pct", 0) < 0 else "⬜"
            sector_lines.append(
                f"{emoji} {s['sector']} {s['return_pct']*100:+.2f}% | {s.get('sentiment', '')}"
            )

        news_lines = report.get("news_highlights", [])[:3]

        text = (
            f"{title}\n\n"
            f"大盘情绪：{report.get('overall_sentiment', '')}\n"
            f"{report.get('overall_summary', '')}\n\n"
            f"板块排行：\n" + "\n".join(sector_lines) + "\n\n"
            f"新闻要点：\n" + "\n".join(f"• {n}" for n in news_lines) + "\n\n"
            f"完整日报：http://localhost:5000/daily_news"
        )

        try:
            success = self.feishu.send_text(text)
            logger.info(f"飞书通知发送{'成功' if success else '失败'}")
        except Exception as e:
            logger.error(f"飞书通知异常: {e}")


def setup_logging(log_dir: Path):
    """配置日志。"""
    os.makedirs(log_dir, exist_ok=True)
    log_file = log_dir / f"daily_news_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
