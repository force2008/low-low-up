"""同花顺期货新闻/研报采集器，支持 AKShare 降级。"""
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .config import get_config

logger = logging.getLogger(__name__)


class ThsCollector:
    """同花顺期货新闻采集器。"""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    def __init__(self, config=None):
        self.config = config or get_config()
        self.news_cfg = self.config.news_config.get("ths", {})
        self.timeout = self.news_cfg.get("timeout", 15)
        self.pages = self.news_cfg.get("pages", 2)
        self.user_agent = self.news_cfg.get("user_agent", self.HEADERS["User-Agent"])
        self.max_items = self.config.news_config.get("max_items", 20)
        self.hours_lookback = self.config.news_config.get("hours_lookback", 24)
        self.report_days_lookback = self.config.news_config.get("report_days_lookback", 7)

    def _fetch_page(self, url: str, retries: int = 2) -> Optional[str]:
        """抓取页面 HTML。"""
        headers = dict(self.HEADERS)
        headers["User-Agent"] = self.user_agent
        last_error = None
        for attempt in range(retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout)
                resp.raise_for_status()
                # 同花顺常见编码为 gbk / gb2312
                if resp.encoding in ("ISO-8859-1", "utf-8"):
                    for enc in ["gbk", "gb2312", "utf-8"]:
                        try:
                            resp.encoding = enc
                            resp.text.encode(enc)
                            break
                        except (UnicodeEncodeError, UnicodeDecodeError):
                            continue
                return resp.text
            except Exception as e:
                last_error = e
                logger.warning(f"请求 {url} 第 {attempt + 1} 次失败: {e}")
        logger.error(f"请求 {url} 失败: {last_error}")
        return None

    @staticmethod
    def _parse_time(text: str, base_year: int = None) -> Optional[datetime]:
        """从文本解析时间。"""
        if base_year is None:
            base_year = datetime.now().year
        # 匹配 "07月31日 08:57" 或 "07-31 08:57"
        patterns = [
            r"(\d{2})月(\d{2})日\s+(\d{2}):(\d{2})",
            r"(\d{2})-(\d{2})\s+(\d{2}):(\d{2})",
            r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if not m:
                continue
            groups = m.groups()
            try:
                if len(groups) == 4:
                    month, day, hour, minute = map(int, groups)
                    return datetime(base_year, month, day, hour, minute)
                else:
                    year, month, day, hour, minute = map(int, groups)
                    return datetime(year, month, day, hour, minute)
            except ValueError:
                continue
        return None

    def _extract_items(self, html: str, base_url: str) -> List[Dict]:
        """从 HTML 提取新闻/研报列表。"""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # 候选选择器列表，按优先级
        selectors = [
            "div.list-con li",
            "ul.news-list li",
            "div.newslist li",
            "div.item",
            "li.news-item",
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                break
        else:
            logger.warning("未找到新闻列表元素")
            return []

        for elem in elements:
            a = elem.find("a")
            if not a or not a.get("href"):
                continue
            title = a.get_text(strip=True)
            url = urljoin(base_url, a["href"])
            full_text = elem.get_text(" ", strip=True)
            # 去掉标题后的文本作为摘要
            summary = full_text.replace(title, "", 1).strip()
            pub_time = self._parse_time(full_text)
            if not pub_time and a.get("title"):
                pub_time = self._parse_time(a["title"])

            items.append({
                "title": title,
                "url": url,
                "publish_time": pub_time.isoformat() if pub_time else None,
                "summary": summary[:300],
                "source": "同花顺",
            })

        return items

    def fetch_news(self) -> List[Dict]:
        """抓取同花顺商品期货新闻。"""
        base_url = self.news_cfg.get("news_url", "http://news.10jqka.com.cn/spqh_list/")
        all_items = []
        for page in range(1, self.pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f"{base_url.rstrip('/')}/index_{page}.shtml"
            html = self._fetch_page(url)
            items = self._extract_items(html, base_url)
            all_items.extend(items)
        return self._filter_items(all_items)

    def fetch_reports(self) -> List[Dict]:
        """抓取同花顺期货研报。"""
        base_url = self.news_cfg.get("report_url", "http://goodsfu.10jqka.com.cn/qhyb_list/")
        all_items = []
        for page in range(1, self.pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f"{base_url.rstrip('/')}/index_{page}.shtml"
            html = self._fetch_page(url)
            items = self._extract_items(html, base_url)
            # 研报增加 institution 字段（从摘要中简单提取）
            for item in items:
                item["source"] = "同花顺研报"
                m = re.search(r"([一-龥]{2,}(?:证券|期货|研究|宏观))", item.get("summary", ""))
                item["institution"] = m.group(1) if m else ""
            all_items.extend(items)
        cutoff = datetime.now() - timedelta(days=self.report_days_lookback)
        return self._filter_items(all_items, cutoff=cutoff)

    def _filter_items(self, items: List[Dict], cutoff: datetime = None) -> List[Dict]:
        """过滤最近 N 小时/天的数据并去重。"""
        if cutoff is None:
            cutoff = datetime.now() - timedelta(hours=self.hours_lookback)
        filtered = []
        seen = set()
        for item in items:
            key = item.get("title", "") + item.get("url", "")
            if key in seen:
                continue
            seen.add(key)
            pub_str = item.get("publish_time")
            if pub_str:
                try:
                    pub = datetime.fromisoformat(pub_str)
                    if pub < cutoff:
                        continue
                except ValueError:
                    pass
            filtered.append(item)
        return filtered[:self.max_items]

    def fetch_all(self) -> Dict[str, List[Dict]]:
        """抓取新闻和研报，失败时尝试 AKShare 降级。"""
        all_items = self.fetch_news()
        reports = self.fetch_reports()

        # 若同花顺新闻为空，尝试 AKShare
        if not all_items and self.config.news_config.get("akshare", {}).get("enabled", True):
            logger.info("同花顺新闻为空，尝试 AKShare 降级")
            all_items = self._fetch_akshare_news()

        return {
            "news": all_items,
            "reports": reports,
        }

    def _fetch_akshare_news(self) -> List[Dict]:
        """AKShare 备用新闻源。"""
        try:
            import akshare as ak
            df = ak.stock_info_global_ths()
            items = []
            cutoff = datetime.now() - timedelta(hours=self.hours_lookback)
            for _, row in df.iterrows():
                title = str(row.get("title", "")).strip()
                if not title:
                    continue
                pub = row.get("pub_time", "")
                if pub:
                    try:
                        pub_dt = datetime.strptime(str(pub), "%Y-%m-%d %H:%M:%S")
                        if pub_dt < cutoff:
                            continue
                    except ValueError:
                        pub_dt = None
                items.append({
                    "title": title,
                    "url": "",
                    "publish_time": pub_dt.isoformat() if pub_dt else None,
                    "summary": str(row.get("content", ""))[:300],
                    "source": "AKShare",
                })
            return self._filter_items(items)
        except Exception as e:
            logger.error(f"AKShare 抓取失败: {e}")
            return []


def main():
    import argparse
    parser = argparse.ArgumentParser(description="同花顺期货新闻采集测试")
    parser.add_argument("--type", choices=["news", "reports", "all"], default="all")
    parser.add_argument("--config", help="配置文件路径")
    args = parser.parse_args()

    config = get_config(args.config)
    collector = ThsCollector(config)

    if args.type == "news":
        result = {"news": collector.fetch_news()}
    elif args.type == "reports":
        result = {"reports": collector.fetch_reports()}
    else:
        result = collector.fetch_all()

    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
