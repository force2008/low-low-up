import os
import sys
import threading
from datetime import datetime

try:
    from pynput import mouse, keyboard
except ImportError:
    print("缺少依赖: pynput")
    print("请先安装: pip install pynput")
    sys.exit(1)

EXPORT_DIR = r"./export"
OUTPUT_FILE = os.path.join(EXPORT_DIR, "coordinates.txt")

os.makedirs(EXPORT_DIR, exist_ok=True)

print("=== 鼠标坐标记录器 ===")
print("操作说明:")
print("  左键点击  -> 记录当前坐标")
print("  右键点击  -> 退出程序")
print("  按 ESC键  -> 退出程序")
print(f"坐标将保存到: {OUTPUT_FILE}\n")

# 记录启动时间，方便区分不同次运行
with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
    f.write(f"\n--- 开始记录 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")

stop_event = threading.Event()

def on_click(x, y, button, pressed):
    if not pressed:
        return
    if button == mouse.Button.left:
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"{timestamp}  ({x}, {y})\n"
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        print(f"已记录: ({x}, {y})")
    elif button == mouse.Button.right:
        print("右键点击，退出程序...")
        stop_event.set()
        return False

def on_press(key):
    if key == keyboard.Key.esc:
        print("ESC 按下，退出程序...")
        stop_event.set()
        return False

m_listener = mouse.Listener(on_click=on_click)
k_listener = keyboard.Listener(on_press=on_press)

m_listener.start()
k_listener.start()

# 阻塞等待任意一个监听器触发退出
stop_event.wait()

# 统一停止两个监听器
m_listener.stop()
k_listener.stop()

m_listener.join()
k_listener.join()

print(f"\n坐标已追加保存至: {OUTPUT_FILE}")
