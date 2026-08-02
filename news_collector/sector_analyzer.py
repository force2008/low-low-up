"""板块行情统计：从 kline_data.db 计算板块指数与情绪。"""
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import get_config


class SectorAnalyzer:
    """期货板块行情分析器。"""

    def __init__(self, config=None):
        self.config = config or get_config()
        self.db_path = self.config.db_path
        self.main_contracts = self._load_main_contracts()
        self.category_info = self._load_category()

    def _load_main_contracts(self) -> List[Dict]:
        path = Path(self.config.main_contracts_path)
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_category(self) -> Dict[str, Dict]:
        path = Path(self.config.category_path)
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 以 code 小写为 key
        return {item["code"].lower(): item for item in data if "code" in item}

    def get_main_contract(self, product_code: str) -> Optional[Dict]:
        """按产品代码从 main_contracts.json 查找正在交易的主力合约。"""
        target = product_code.upper()
        candidates = [
            c for c in self.main_contracts
            if c.get("ProductID", "").upper() == target
            and c.get("IsTrading", 0) == 1
        ]
        if not candidates:
            return None
        # 过滤掉 OpenInterest 为 0 或空的，优先 OpenInterest 最大的
        valid = [c for c in candidates if c.get("OpenInterest")]
        if valid:
            return max(valid, key=lambda x: x.get("OpenInterest", 0))
        return max(candidates, key=lambda x: x.get("OpenInterest", 0) or 0)

    def _normalize_symbol(self, contract: Dict) -> str:
        """构造数据库查询用的 symbol。"""
        exchange = contract.get("ExchangeID", "")
        instrument = contract.get("MainContractID", "")
        return f"{exchange}.{instrument}"

    def _try_find_symbol(self, base_symbol: str) -> Optional[str]:
        """尝试查找数据库中真实存在的 symbol，兼容 CZCE 3/4 位年月。"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM kline_data WHERE symbol = ? AND duration = 3600 LIMIT 1",
                (base_symbol,),
            )
            if cursor.fetchone():
                return base_symbol

            # CZCE 兼容：CF609 -> CF2609
            if base_symbol.startswith("CZCE."):
                parts = base_symbol.split(".")
                code = parts[1]
                match = re.match(r"^([A-Za-z]+)(\d{3})$", code)
                if match:
                    prod, mm = match.groups()
                    current_year = datetime.now().year
                    # 尝试当前年份和次年补齐
                    for year in [current_year, current_year + 1]:
                        yy = str(year)[-2:]
                        candidate = f"CZCE.{prod}{yy}{mm}"
                        cursor.execute(
                            "SELECT 1 FROM kline_data WHERE symbol = ? AND duration = 3600 LIMIT 1",
                            (candidate,),
                        )
                        if cursor.fetchone():
                            return candidate
            return None
        finally:
            conn.close()

    def _query_kline(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """查询指定时间窗口的 60 分钟 K 线。"""
        conn = sqlite3.connect(self.db_path)
        try:
            query = """
                SELECT datetime, open, high, low, close, volume, close_oi
                FROM kline_data
                WHERE symbol = ? AND duration = 3600 AND datetime BETWEEN ? AND ?
                ORDER BY datetime ASC
            """
            return pd.read_sql_query(query, conn, params=(symbol, start, end))
        finally:
            conn.close()

    @staticmethod
    def _parse_trading_period(period_str: str) -> List[Tuple[int, int, int, int]]:
        """解析 trading_period 为 (start_h, start_m, end_h, end_m) 列表。"""
        if not period_str:
            return []
        result = []
        for seg in period_str.split(","):
            seg = seg.strip()
            if len(seg) != 17 or "-" not in seg:
                continue
            start, end = seg.split("-")
            try:
                sh, sm = int(start[:2]), int(start[2:4])
                eh, em = int(end[:2]), int(end[2:4])
                result.append((sh, sm, eh, em))
            except ValueError:
                continue
        return result

    def _has_night_session(self, product_code: str) -> bool:
        """判断品种是否有夜盘。"""
        info = self.category_info.get(product_code.lower())
        if not info:
            return False
        period = info.get("trading_period", "")
        segments = self._parse_trading_period(period)
        # 如果存在结束时间 >= 21:00 或开始时间 >= 21:00 的时段，认为有夜盘
        for sh, sm, eh, em in segments:
            start_min = sh * 60 + sm
            end_min = eh * 60 + em
            night_start = 21 * 60
            if start_min >= night_start or end_min >= night_start:
                return True
        return False

    def _get_night_window(self, product_code: str, trade_date: str) -> Tuple[str, str]:
        """返回品种夜盘时间窗口 (start, end)。"""
        info = self.category_info.get(product_code.lower())
        if not info:
            return None, None
        period = info.get("trading_period", "")
        segments = self._parse_trading_period(period)

        # 找出包含夜盘的结束时段
        night_end = None
        for sh, sm, eh, em in segments:
            end_min = eh * 60 + em
            night_start = 21 * 60
            if end_min >= night_start:
                night_end = (eh, em)
                break

        if night_end is None:
            return None, None

        # 夜盘从 21:00 开始
        prev_day = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        start = f"{prev_day} 21:00:00"
        eh, em = night_end
        # 如果结束时间是 00/01/02 点，则属于 trade_date 当天
        end = f"{trade_date} {eh:02d}:{em:02d}:00"
        return start, end

    def _get_session_window(self, product_code: str, trade_date: str, session: str) -> Tuple[Optional[str], Optional[str]]:
        """根据 session 返回时间窗口。"""
        if session == "day":
            return f"{trade_date} 09:00:00", f"{trade_date} 15:00:00"
        if session == "night":
            if not self._has_night_session(product_code):
                return None, None
            return self._get_night_window(product_code, trade_date)
        return None, None

    def _sentiment_label(self, return_pct: float, up_ratio: float) -> str:
        """根据收益率和上涨占比判定情绪。"""
        thresholds = self.config.sentiment_thresholds

        strong_bull = thresholds.get("strong_bull", {})
        if (return_pct >= strong_bull.get("return_pct", 0.01)
                and up_ratio >= strong_bull.get("up_ratio", 0.6)):
            return "强势上涨"

        strong_bear = thresholds.get("strong_bear", {})
        if (return_pct <= strong_bear.get("return_pct", -0.01)
                and up_ratio <= strong_bear.get("up_ratio", 0.4)):
            return "弱势下跌"

        bull_thr = thresholds.get("bull", {}).get("return_pct", 0.005)
        bear_thr = thresholds.get("bear", {}).get("return_pct", -0.005)
        neutral_range = thresholds.get("neutral", {}).get("return_pct", [-0.005, 0.005])

        if return_pct >= bull_thr:
            return "偏多"
        if return_pct <= bear_thr:
            return "偏空"
        low, high = neutral_range
        if low <= return_pct <= high:
            return "震荡"
        return "震荡" if return_pct > 0 else "震荡"

    def analyze_sector(self, sector_name: str, products: List[str], trade_date: str, session: str) -> Dict:
        """分析单个板块。"""
        records = []
        missing_products = []

        for prod in products:
            contract = self.get_main_contract(prod)
            if not contract:
                missing_products.append(prod)
                continue

            start, end = self._get_session_window(prod, trade_date, session)
            if start is None or end is None:
                continue

            base_symbol = self._normalize_symbol(contract)
            symbol = self._try_find_symbol(base_symbol)
            if not symbol:
                missing_products.append(prod)
                continue

            df = self._query_kline(symbol, start, end)
            if df.empty:
                missing_products.append(prod)
                continue

            first_close = float(df.iloc[0]["close"])
            last_close = float(df.iloc[-1]["close"])
            change_pct = (last_close - first_close) / first_close if first_close else 0
            volume = int(df["volume"].sum())
            weight = contract.get("OpenInterest", 0) or 1

            records.append({
                "product": prod.upper(),
                "symbol": symbol,
                "start_price": round(first_close, 4),
                "end_price": round(last_close, 4),
                "change_pct": round(change_pct, 6),
                "volume": volume,
                "weight": float(weight),
            })

        if not records:
            return {
                "sector": sector_name,
                "return_pct": 0.0,
                "up_ratio": 0.0,
                "sentiment": "暂无数据",
                "reason": "本时段无有效行情数据",
                "top_gainers": [],
                "top_losers": [],
                "missing_products": missing_products,
            }

        total_weight = sum(r["weight"] for r in records)
        sector_return = sum(r["change_pct"] * r["weight"] for r in records) / total_weight if total_weight else 0
        up_ratio = sum(1 for r in records if r["change_pct"] > 0) / len(records)
        down_ratio = sum(1 for r in records if r["change_pct"] < 0) / len(records)

        sorted_records = sorted(records, key=lambda x: x["change_pct"], reverse=True)

        return {
            "sector": sector_name,
            "return_pct": round(sector_return, 6),
            "up_ratio": round(up_ratio, 4),
            "down_ratio": round(down_ratio, 4),
            "sentiment": self._sentiment_label(sector_return, up_ratio),
            "reason": "",
            "top_gainers": sorted_records[:3],
            "top_losers": sorted_records[-3:][::-1],
            "missing_products": missing_products,
        }

    def analyze_all_sectors(self, trade_date: str, session: str) -> List[Dict]:
        """分析所有板块。"""
        sectors = self.config.sectors
        results = []
        for sector_name, cfg in sectors.items():
            products = cfg.get("products", [])
            result = self.analyze_sector(sector_name, products, trade_date, session)
            results.append(result)
        return results

    def get_overall_summary(self, sector_results: List[Dict]) -> Dict:
        """基于板块结果生成大盘摘要。"""
        valid = [s for s in sector_results if s.get("sentiment") != "暂无数据"]
        if not valid:
            return {"sentiment": "暂无数据", "summary": "无有效板块数据"}

        avg_return = sum(s["return_pct"] for s in valid) / len(valid)
        up_sectors = sum(1 for s in valid if s["return_pct"] > 0)
        down_sectors = sum(1 for s in valid if s["return_pct"] < 0)
        top = max(valid, key=lambda x: x["return_pct"])
        bottom = min(valid, key=lambda x: x["return_pct"])

        overall_sentiment = self._sentiment_label(avg_return, up_sectors / len(valid))
        summary = (
            f"{len(valid)}个板块中{up_sectors}个上涨、{down_sectors}个下跌，"
            f"平均涨跌幅{avg_return*100:+.2f}%。"
            f"最强板块为{top['sector']}({top['return_pct']*100:+.2f}%)，"
            f"最弱板块为{bottom['sector']}({bottom['return_pct']*100:+.2f}%)。"
        )

        return {
            "sentiment": overall_sentiment,
            "summary": summary,
            "avg_return_pct": round(avg_return, 6),
            "up_sectors": up_sectors,
            "down_sectors": down_sectors,
            "strongest_sector": top["sector"],
            "weakest_sector": bottom["sector"],
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="板块行情统计测试")
    parser.add_argument("--date", required=True, help="交易日 YYYY-MM-DD")
    parser.add_argument("--session", choices=["day", "night"], default="day", help="时段")
    parser.add_argument("--sector", help="指定板块")
    parser.add_argument("--config", help="配置文件路径")
    args = parser.parse_args()

    config = get_config(args.config)
    analyzer = SectorAnalyzer(config)

    if args.sector:
        sector_cfg = config.sectors.get(args.sector)
        if not sector_cfg:
            print(f"未知板块: {args.sector}")
            return
        result = analyzer.analyze_sector(args.sector, sector_cfg["products"], args.date, args.session)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        results = analyzer.analyze_all_sectors(args.date, args.session)
        overall = analyzer.get_overall_summary(results)
        print(json.dumps({"overall": overall, "sectors": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
