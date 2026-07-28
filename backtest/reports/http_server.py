#!/usr/bin/env python3
"""
简单HTTP服务 - 用于浏览reports目录下的图表

特点：
- 启动时自动扫描 charts 目录，生成图片列表
- 每次访问 index.html 都会加载最新的图片

用法:
    python http_server.py              # 默认端口 8080
    python http_server.py 9000      # 指定端口 9000
"""

import sys
import os
import glob
import json
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
CHARTS_DIR = 'charts'

# 切换到当前目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)


def scan_charts():
    """扫描 charts 目录，生成图片列表"""
    charts_path = os.path.join(BASE_DIR, CHARTS_DIR)

    if not os.path.exists(charts_path):
        return []

    # 扫描 png 和 jpg 文件
    patterns = ['*.png', '*.jpg', '*.jpeg']
    files = []

    for pattern in patterns:
        files.extend(glob.glob(os.path.join(charts_path, pattern)))

    # 只返回文件名，按名称排序
    filenames = [os.path.basename(f) for f in sorted(files)]
    return filenames


def generate_images_js(images):
    """生成 images.js 文件"""
    js_content = f"// 自动生成于 {os.popen('date').read().strip()}\n"
    js_content += "const IMAGES_PLACEHOLDER = " + json.dumps(images, ensure_ascii=False) + ";\n"

    with open('images.js', 'w', encoding='utf-8') as f:
        f.write(js_content)

    return len(images)


class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()


# 启动时扫描并生成
images = scan_charts()
count = generate_images_js(images)

print(f"""
╔═══════════════════════════════════════════════════════════╗
║           回测报告 HTTP 服务                          ║
╠═══════════════════════════════════════════════════════════╣
║  自动扫描: {CHARTS_DIR} 目录                              ║
║  发现图片: {count} 张                                      ║
║                                                       ║
║  访问地址: http://localhost:{PORT}                     ║
║  关闭服务: Ctrl+C                                      ║
╚═══════════════════════════════════════════════════════════╝
""")

server = HTTPServer(('0.0.0.0', PORT), CORSRequestHandler)

try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\n服务已停止")
    server.server_close()