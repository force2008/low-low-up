# -*- coding: utf-8 -*-
"""行情监控 - 规则编辑器（本地 Web 服务）。

用途：
    在浏览器里以 UI 方式维护 market-monitor/config.json 的 rules 字段
    （新增 / 编辑 / 复制 / 删除 / 启用停用），保存后写回 config.json。
    正在运行的 market_monitor.py 会在下一次轮询（默认 3s）内自动热加载，
    无需重启。

只改 rules：配置文件的其余字段（含 _readme、env、feishu、kline_db 等）
原样保留。保存前会用 config_loader.load_config 做完整校验，校验不通过
则不落盘，错误信息会返回给页面。

运行：
    python rule_editor.py                  # http://127.0.0.1:8765
    python rule_editor.py --port 9000      # 换端口
    python rule_editor.py -c config.json   # 指定配置文件

依赖：仅 Python 标准库。
"""

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from config_loader import ConfigError, load_config  # noqa: E402
from conditions import SUPPORTED_TYPES  # noqa: E402

_HTML_FILE = os.path.join(_BASE_DIR, "rule_editor.html")


# ==================== 配置文件读写 ====================
def _load_json(path: str) -> dict:
    """读取配置文件（兼容以 # 或 // 开头的整行注释）"""
    with open(path, "r", encoding="utf-8") as fp:
        text = fp.read()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        cleaned = "\n".join(
            line for line in text.splitlines()
            if not line.lstrip().startswith(("#", "//"))
        )
        return json.loads(cleaned)


def _public_rule(rule: dict) -> dict:
    """去掉运行期字段，得到可写回文件的规则形态"""
    return {key: value for key, value in rule.items() if key != "period_minutes"}


def _save_rules(config_path: str, rules) -> dict:
    """把 rules 写回配置文件，返回校验通过的归一化配置。

    Raises:
        ConfigError: rules 非法或完整配置校验失败
    """
    if not isinstance(rules, list) or not rules:
        raise ConfigError("rules 必须是非空数组")

    raw = _load_json(config_path)
    raw["rules"] = rules

    # 先在临时文件上落盘并完整校验，通过后再原子替换，避免写坏配置
    tmp_path = config_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fp:
        json.dump(raw, fp, ensure_ascii=False, indent=2)
        fp.write("\n")

    try:
        config = load_config(tmp_path)
    except Exception as exc:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise ConfigError(str(exc)) from exc

    os.replace(tmp_path, config_path)
    return config


# ==================== HTTP 处理 ====================
class _Handler(BaseHTTPRequestHandler):
    server_version = "RuleEditor/1.0"
    protocol_version = "HTTP/1.1"

    config_path = ""  # 由 main() 注入

    # ---------- 基础响应 ----------
    def log_message(self, fmt, *args):
        sys.stdout.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))
        sys.stdout.flush()

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(code, body, "application/json; charset=utf-8")

    # ---------- 路由 ----------
    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            return self._serve_html()
        if path == "/api/config":
            return self._api_get_config()
        self._send_json(404, {"ok": False, "error": f"未知路径：{path}"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/rules":
            return self._api_save_rules()
        self._send_json(404, {"ok": False, "error": f"未知路径：{path}"})

    # ---------- 页面 ----------
    def _serve_html(self):
        try:
            with open(_HTML_FILE, "rb") as fp:
                body = fp.read()
        except OSError as exc:
            return self._send_json(500, {"ok": False, "error": f"读取页面文件失败：{exc}"})
        self._send(200, body, "text/html; charset=utf-8")

    # ---------- API ----------
    def _api_get_config(self):
        try:
            raw = _load_json(self.config_path)
        except Exception as exc:
            return self._send_json(500, {"ok": False, "error": f"读取配置文件失败：{exc}"})
        self._send_json(200, {
            "ok": True,
            "path": self.config_path,
            "rules": raw.get("rules") or [],
            "types": sorted(SUPPORTED_TYPES),
        })

    def _api_save_rules(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b""
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            return self._send_json(400, {"ok": False, "error": f"请求体不是合法 JSON：{exc}"})

        try:
            config = _save_rules(self.config_path, data.get("rules"))
        except ConfigError as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            return self._send_json(500, {"ok": False, "error": f"保存失败：{exc}"})

        self._send_json(200, {
            "ok": True,
            "message": "已保存；正在运行的监控程序会在下一次轮询内自动热加载",
            "rules": [_public_rule(rule) for rule in config["rules"]],
        })


def main() -> int:
    parser = argparse.ArgumentParser(description="market-monitor 规则编辑器")
    parser.add_argument("-c", "--config", default=os.path.join(_BASE_DIR, "config.json"),
                        help="配置文件路径，默认同目录 config.json")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="监听端口，默认 8765")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    if not os.path.exists(config_path):
        print(f"配置文件不存在：{config_path}")
        return 1

    _Handler.config_path = config_path

    try:
        server = ThreadingHTTPServer((args.host, args.port), _Handler)
    except OSError as exc:
        print(f"启动失败（端口 {args.port} 可能被占用）：{exc}")
        return 1

    print("规则编辑器已启动")
    print(f"  配置文件：{config_path}")
    print(f"  访问地址：http://{args.host}:{args.port}/")
    print("  Ctrl+C 退出")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已退出")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
