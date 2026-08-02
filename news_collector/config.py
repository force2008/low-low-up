"""加载日报模块配置。"""
import json
import os
from pathlib import Path
from typing import Any, Dict


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "sector_config.json"


class DailyNewsConfig:
    """日报配置封装，支持环境变量覆盖敏感字段。"""

    def __init__(self, config_path: str = None):
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self._raw = self._load_json(self.config_path)

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get(self, key: str, default: Any = None) -> Any:
        """点号分隔读取配置，如 'llm.model'。"""
        parts = key.split(".")
        value = self._raw
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        return value

    @property
    def llm(self) -> Dict[str, Any]:
        return self._raw.get("llm", {})

    @property
    def llm_api_key(self) -> str:
        """优先从环境变量 LLM_API_KEY 读取。"""
        return os.getenv("LLM_API_KEY", self.llm.get("api_key", ""))

    @property
    def feishu_webhook(self) -> str:
        """优先从环境变量 FEISHU_WEBHOOK 读取。"""
        return os.getenv("FEISHU_WEBHOOK", self._raw.get("feishu", {}).get("webhook", ""))

    @property
    def db_path(self) -> str:
        return self._raw.get("data", {}).get("db_path", "")

    @property
    def main_contracts_path(self) -> str:
        return self._raw.get("data", {}).get("main_contracts", "")

    @property
    def category_path(self) -> str:
        return self._raw.get("data", {}).get("category", "")

    @property
    def web_static_dir(self) -> Path:
        return Path(self._raw.get("output", {}).get("web_static_dir", ""))

    @property
    def archive_dir(self) -> Path:
        return Path(self._raw.get("output", {}).get("archive_dir", ""))

    @property
    def log_dir(self) -> Path:
        return Path(self._raw.get("output", {}).get("log_dir", ""))

    @property
    def sectors(self) -> Dict[str, Dict[str, Any]]:
        return self._raw.get("sectors", {})

    @property
    def sentiment_thresholds(self) -> Dict[str, Any]:
        return self._raw.get("sentiment_thresholds", {})

    @property
    def news_config(self) -> Dict[str, Any]:
        return self._raw.get("news", {})

    def to_dict(self) -> Dict[str, Any]:
        return self._raw


# 全局默认配置实例
_config: DailyNewsConfig = None


def get_config(config_path: str = None) -> DailyNewsConfig:
    """获取或创建配置实例。"""
    global _config
    if _config is None or config_path is not None:
        _config = DailyNewsConfig(config_path)
    return _config


def reload_config(config_path: str = None) -> DailyNewsConfig:
    """重新加载配置。"""
    global _config
    _config = DailyNewsConfig(config_path)
    return _config
