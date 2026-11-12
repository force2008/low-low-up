# -*- coding: utf-8 -*-
"""信号落盘与飞书通知。"""

import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

# 把项目根目录加入 sys.path，以复用低低上项目中的飞书通知模块
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _fmt_price(value: Any) -> str:
    """价格格式化：数值保留 4 位小数，其余原样输出"""
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return str(value)


class SignalStore:
    """把命中的信号追加写入 JSON 文件（原子写入，避免读取到半截文件）。"""

    def __init__(self, path: str, max_records: int = 1000):
        self.path = path
        self.max_records = max_records
        directory = os.path.dirname(os.path.abspath(path))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def _load(self) -> List[dict]:
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def append(self, record: Dict[str, Any]) -> None:
        """追加一条信号并原子落盘"""
        records = self._load()
        records.append(record)
        if len(records) > self.max_records:
            records = records[-self.max_records:]

        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fp:
                json.dump(records, fp, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise


class RuleNotifier:
    """规则信号 -> 飞书富文本通知。"""

    def __init__(self, enabled: bool = True, webhook_url: Optional[str] = None):
        self.enabled = bool(enabled)
        self._notifier = None
        if self.enabled:
            try:
                from utils.feishu_notifier import FeishuNotifier

                self._notifier = FeishuNotifier(webhook_url) if webhook_url else FeishuNotifier()
            except Exception as exc:
                print(f"飞书通知初始化失败，将只写信号文件：{exc}")
                self.enabled = False

    def send_signal(self, symbol: str, rule: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        """发送一条规则命中通知，返回是否成功"""
        if not self.enabled or self._notifier is None:
            return False

        try:
            direction = payload.get("direction") or rule.get("direction") or ""
            direction_cn = {"short": "做空", "long": "做多"}.get(direction, "")

            lines = [
                f"合约：{symbol}\n",
                f"周期：{rule.get('period', '')}\n",
                f"条件：{rule.get('type', '')}\n",
            ]
            if direction_cn:
                lines.append(f"方向：{direction_cn}\n")
            lines.append(f"参考价：{_fmt_price(payload.get('price', 0))}\n")
            lines.append(f"行情时间：{payload.get('bar_time', '')}\n")
            lines.append(f"\n{payload.get('detail', '')}\n")
            lines.append(f"\n触发时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            content = [{"tag": "text", "text": line} for line in lines]
            title = f"🔔 {rule.get('name') or '行情监控信号'}"
            return bool(self._notifier.send_post(title, content))
        except Exception as exc:
            print(f"发送飞书通知失败：{exc}")
            return False
