"""LLM 摘要生成器：基于新闻、研报、板块行情生成结构化日报。"""
import json
import logging
import re
from typing import Dict, List, Optional

from openai import OpenAI, APIError

from .config import get_config

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = "你是一位专业的期货日报分析师，擅长从行情数据和新闻中提炼市场情绪与交易逻辑。"

PROMPT_TEMPLATE = """你是一位期货日报分析师。请根据以下当日期市新闻、研报摘要和板块涨跌幅数据，撰写一份简短的期货日报。

要求：
1. 先给出大盘整体情绪（50字以内），使用标签：强势上涨/偏多/震荡/偏空/弱势下跌/暂无数据。
2. 分板块给出情绪（偏多/偏空/震荡/强势上涨/弱势下跌/暂无数据）及核心逻辑（每条30字以内）。
3. 提炼 3-5 条最重要的新闻要点。
4. 如果有研报，列出研报标题与一句话核心观点（只选最近、最相关的2-3条）。
5. 指出当前主要风险点（1-3条）。
6. 最终输出必须是合法的 JSON，不要包含 markdown 代码块，不要使用 ```json 包裹。

JSON 结构：
{
  "trade_date": "YYYY-MM-DD",
  "session": "day|night",
  "overall_sentiment": "",
  "overall_summary": "",
  "sector_summary": [
    {"sector": "", "sentiment": "", "return_pct": 0.0, "reason": "", "key_products": []}
  ],
  "news_highlights": ["..."],
  "report_highlights": [{"title": "", "core_view": ""}],
  "risks": ["..."]
}

注意：
- 如果某板块 return_pct 为空或暂无数据，sentiment 填"暂无数据"，reason 说明原因。
- 请基于数据客观分析，不要编造新闻中没有的信息。

数据如下：
{input_json}
"""


class LLMSummarizer:
    """基于大模型生成日报摘要。"""

    def __init__(self, config=None):
        self.config = config or get_config()
        self.llm_cfg = self.config.llm
        self.api_key = self.config.llm_api_key
        self.base_url = self.llm_cfg.get("base_url", "https://api.deepseek.com/v1")
        self.model = self.llm_cfg.get("model", "deepseek-chat")
        self.temperature = self.llm_cfg.get("temperature", 0.4)
        self.max_tokens = self.llm_cfg.get("max_tokens", 2048)
        self.timeout = self.llm_cfg.get("timeout", 60)

    def _build_input(self, trade_date: str, session: str, sector_results: List[Dict],
                     news: List[Dict], reports: List[Dict]) -> Dict:
        """构建传给 LLM 的输入数据，控制长度。"""
        sectors = []
        for s in sector_results:
            sectors.append({
                "sector": s["sector"],
                "sentiment": s["sentiment"],
                "return_pct": s["return_pct"],
                "up_ratio": s.get("up_ratio", 0),
                "top_gainers": [f"{r['product']}({r['change_pct']*100:+.2f}%)" for r in s.get("top_gainers", [])[:2]],
                "top_losers": [f"{r['product']}({r['change_pct']*100:+.2f}%)" for r in s.get("top_losers", [])[:2]],
            })

        news_list = []
        for n in news[:10]:
            news_list.append({
                "title": n.get("title", "")[:80],
                "summary": n.get("summary", "")[:200],
            })

        report_list = []
        for r in reports[:5]:
            report_list.append({
                "title": r.get("title", "")[:80],
                "summary": r.get("summary", "")[:200],
            })

        return {
            "trade_date": trade_date,
            "session": session,
            "sectors": sectors,
            "news": news_list,
            "reports": report_list,
        }

    def summarize(self, trade_date: str, session: str, sector_results: List[Dict],
                  news: List[Dict], reports: List[Dict]) -> Dict:
        """调用 LLM 生成日报摘要，失败则返回模板化结果。"""
        if not self.api_key:
            logger.warning("未配置 LLM_API_KEY，使用模板化日报")
            return self._fallback_summary(trade_date, session, sector_results, news, reports)

        input_data = self._build_input(trade_date, session, sector_results, news, reports)
        prompt = PROMPT_TEMPLATE.replace(
            "{input_json}",
            json.dumps(input_data, ensure_ascii=False, indent=2)
        )

        try:
            client = OpenAI(base_url=self.base_url, api_key=self.api_key, timeout=self.timeout)
            kwargs = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": self.temperature,
                "max_tokens": self.max_tokens,
            }
            # 部分模型支持 json_object
            if "gpt" in self.model or "qwen" in self.model.lower():
                kwargs["response_format"] = {"type": "json_object"}

            resp = client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content
            parsed = self._extract_json(content)
            if parsed:
                parsed["ai_generated"] = True
                parsed["trade_date"] = trade_date
                parsed["session"] = session
                return parsed
            else:
                logger.warning("LLM 返回无法解析为 JSON，使用降级模板")
                return self._fallback_summary(trade_date, session, sector_results, news, reports)
        except APIError as e:
            logger.error(f"LLM API 错误: {e}")
            return self._fallback_summary(trade_date, session, sector_results, news, reports)
        except Exception as e:
            logger.error(f"LLM 调用异常: {e}")
            return self._fallback_summary(trade_date, session, sector_results, news, reports)

    @staticmethod
    def _extract_json(text: str) -> Optional[Dict]:
        """从文本中提取 JSON。"""
        if not text:
            return None
        text = text.strip()
        # 尝试直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 尝试提取 ```json ... ``` 代码块
        code_block = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
        if code_block:
            try:
                return json.loads(code_block.group(1))
            except json.JSONDecodeError:
                pass

        # 尝试提取第一个 { ... } 对象
        brace_match = re.search(r"\{.*\}", text, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    def _fallback_summary(self, trade_date: str, session: str, sector_results: List[Dict],
                          news: List[Dict], reports: List[Dict]) -> Dict:
        """模板化降级日报。"""
        valid_sectors = [s for s in sector_results if s.get("sentiment") != "暂无数据"]
        if valid_sectors:
            avg_return = sum(s["return_pct"] for s in valid_sectors) / len(valid_sectors)
            overall_sentiment = self._sentiment_label(avg_return, sum(1 for s in valid_sectors if s["return_pct"] > 0) / len(valid_sectors))
        else:
            avg_return = 0.0
            overall_sentiment = "暂无数据"

        sector_summary = []
        for s in sector_results:
            gainers = [f"{r['product']} {r['change_pct']*100:+.2f}%" for r in s.get("top_gainers", [])[:2]]
            losers = [f"{r['product']} {r['change_pct']*100:+.2f}%" for r in s.get("top_losers", [])[:2]]
            reason = ""
            if s["sentiment"] != "暂无数据":
                parts = []
                if gainers:
                    parts.append("领涨: " + ", ".join(gainers))
                if losers:
                    parts.append("领跌: " + ", ".join(losers))
                reason = "；".join(parts)
            sector_summary.append({
                "sector": s["sector"],
                "sentiment": s["sentiment"],
                "return_pct": s["return_pct"],
                "reason": reason,
                "key_products": [r["product"] for r in s.get("top_gainers", [])[:3]],
            })

        return {
            "trade_date": trade_date,
            "session": session,
            "overall_sentiment": overall_sentiment,
            "overall_summary": f"平均涨跌幅 {avg_return*100:+.2f}%，{len(valid_sectors)} 个板块有数据。",
            "sector_summary": sector_summary,
            "news_highlights": [n.get("title", "") for n in news[:5]],
            "report_highlights": [{"title": r.get("title", ""), "core_view": r.get("summary", "")[:100]} for r in reports[:3]],
            "risks": ["数据不完整，请以交易所官方信息为准。"],
            "ai_generated": False,
        }

    @staticmethod
    def _sentiment_label(return_pct: float, up_ratio: float) -> str:
        """简单的情绪标签。"""
        if return_pct >= 0.01 and up_ratio >= 0.6:
            return "强势上涨"
        if return_pct <= -0.01 and up_ratio <= 0.4:
            return "弱势下跌"
        if return_pct >= 0.005:
            return "偏多"
        if return_pct <= -0.005:
            return "偏空"
        return "震荡"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="LLM 摘要测试")
    parser.add_argument("--date", required=True, help="交易日 YYYY-MM-DD")
    parser.add_argument("--session", choices=["day", "night"], default="day")
    parser.add_argument("--sectors", help="板块统计 JSON 文件")
    parser.add_argument("--news", help="新闻 JSON 文件")
    parser.add_argument("--config", help="配置文件路径")
    args = parser.parse_args()

    config = get_config(args.config)
    summarizer = LLMSummarizer(config)

    sector_results = []
    if args.sectors:
        with open(args.sectors, "r", encoding="utf-8") as f:
            sector_results = json.load(f)
    news = []
    if args.news:
        with open(args.news, "r", encoding="utf-8") as f:
            news = json.load(f)

    result = summarizer.summarize(args.date, args.session, sector_results, news, [])
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
