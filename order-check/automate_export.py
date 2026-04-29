# Windows应用程序数据导出自动化脚本
# 使用前需安装: pip install pyautogui pillow

import pyautogui
import time
import os
import sys
from datetime import datetime

# 从本地配置文件导入坐标（不同平台各自维护，避免冲突）
try:
    from local_config import (
        APP_TITLE,
        MENU_POSITION,
        EXPORT_MENU_POSITION,
        EXPORT_DIALOG_TITLE,
        SAVE_DIALOG_TITLE,
        FOLDER_CLICK_SEQUENCE,
        SAVE_BUTTON_POSITION,
        OK_BUTTON_POSITION,
        DEFAULT_SAVE_PATH,
    )
except ImportError:
    print("错误: 找不到 local_config.py")
    print("请复制 local_config.example.py 为 local_config.py，并根据当前平台配置坐标。")
    sys.exit(1)


def wait_for_window(title, timeout=10):
    """等待指定标题的窗口出现"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # 尝试获取窗口
            window = pyautogui.getWindowsWithTitle(title)[0]
            if window:
                print(f"找到窗口: {title}")
                return window
        except IndexError:
            pass
        time.sleep(0.5)
    raise Exception(f"超时: 未找到窗口 '{title}'")


def activate_window(window):
    """激活指定窗口

    Args:
        window: 要激活的窗口对象

    Returns:
        bool: 是否成功激活窗口
    """
    if window is None:
        print("错误: 无效的窗口对象")
        return False

    try:
        # 如果窗口最小化，先恢复
        if hasattr(window, 'isMinimized') and window.isMinimized:
            print("窗口处于最小化状态，尝试恢复...")
            window.restore()
            time.sleep(0.5)

        # 尝试激活窗口
        window.activate()
        time.sleep(0.5)

        # 验证窗口是否激活成功
        print(f"窗口位置: ({window.left}, {window.top}), 大小: ({window.width}, {window.height})")
        print(f"窗口是否激活: {window.isActive}")

        if window.isActive:
            print("窗口激活成功")
            return True
        else:
            # 尝试其他方法 - 模拟点击窗口标题栏
            left, top, width, height = window.box
            title_bar_center = (left + width // 2, top + 20)
            print(f"尝试点击标题栏: {title_bar_center}")
            pyautogui.moveTo(title_bar_center, duration=0.5)
            pyautogui.click()
            time.sleep(0.5)

            if window.isActive:
                print("通过点击标题栏激活窗口成功")
                return True
            else:
                print("警告: 窗口未能成功激活")
                return False
    except Exception as e:
        print(f"激活窗口时发生错误: {str(e)}")
        return False


def click_menu_and_export():
    """点击菜单并导出数据

    Returns:
        bool: 是否成功点击菜单并打开导出对话框
    """
    try:
        # 移动到菜单并点击
        print(f"当前鼠标位置: {pyautogui.position()}")
        print(f"目标菜单位置: {MENU_POSITION}")
        pyautogui.moveTo(MENU_POSITION[0], MENU_POSITION[1])
        time.sleep(0.5)
        print(f"移动后鼠标位置: {pyautogui.position()}")
        pyautogui.click()
        time.sleep(0.5)  # 等待菜单展开

        # 移动到导出菜单项并点击
        print(f"目标导出菜单位置: {EXPORT_MENU_POSITION}")
        pyautogui.moveTo(EXPORT_MENU_POSITION[0], EXPORT_MENU_POSITION[1])
        time.sleep(0.5)
        print(f"移动后鼠标位置: {pyautogui.position()}")
        pyautogui.click()
        time.sleep(0.5)  # 等待导出对话框出现（最长2秒）
        return True
    except Exception as e:
        print(f"点击菜单时发生错误: {str(e)}")
        return False
def navigate_to_folder():
    """按配置的坐标序列依次点击文件夹"""
    for name, coord in FOLDER_CLICK_SEQUENCE:
        print(f"点击文件夹: {name} {coord}")
        pyautogui.moveTo(coord[0], coord[1])
        pyautogui.click()
        time.sleep(0.5)
    return True


def save_exported_data():
    # 等待浏览文件夹对话框出现
    wait_for_window(SAVE_DIALOG_TITLE)
    time.sleep(1)

    # 导航到目标文件夹
    navigate_to_folder()
    time.sleep(1)  # 等待文件夹切换完成

    # 点击确定按钮
    print("点击'确定'按钮...")
    pyautogui.moveTo(SAVE_BUTTON_POSITION)
    pyautogui.click()
    time.sleep(2)  # 等待保存完成

    # 处理导出完成确认弹框（循环等待，导出完成后才会弹出）
    print("等待导出完成确认弹框...")
    try:
        wait_for_window("融航风控", timeout=15)
        print("检测到导出完成弹框，点击确认按钮...")
        pyautogui.moveTo(OK_BUTTON_POSITION)
        pyautogui.click()
        time.sleep(1)
        print("已确认导出完成弹框")
    except Exception:
        print("未检测到导出完成弹框，继续...")


def main():
    try:
        print("开始数据导出自动化...")

        # 等待并激活应用程序窗口
        print(f"等待应用程序窗口: {APP_TITLE}...")
        app_window = wait_for_window(APP_TITLE)
        if not app_window:
            print(f"错误: 未能找到窗口 '{APP_TITLE}'")
            return False

        print("尝试激活窗口...")
        if not activate_window(app_window):
            print("错误: 无法激活窗口，程序终止")
            return False

        # 点击菜单并导出数据
        print("点击菜单并导出数据...")
        if not click_menu_and_export():
            print("错误: 菜单操作失败")
            return False

        # 保存导出的数据
        save_exported_data()

        print("数据导出完成!")
        return True

    except Exception as e:
        print(f"发生错误: {str(e)}")
        return False


if __name__ == "__main__":
    main()

# ======================================
# 使用说明:
# 1. 安装依赖: pip install pyautogui pillow
# 2. 替换配置区域中的参数以匹配您的应用程序
# 3. 可以使用pyautogui.displayMousePosition()来获取坐标
# 4. 运行脚本: python automate_export.py
# ======================================