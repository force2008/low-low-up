"""
融航风控端自动登录程序（云主机 100% 缩放下使用，最简版本）
风格对齐 run_pipeline.py：用 pyautogui + pygetwindow，坐标直接填 record_coordinates 录的值。

流程：
  1. 启动融航风控端 exe（os.startfile）
  2. 按标题关键字把登录窗口拉到前台
  3. 点击密码输入框
  4. 输入密码 + 点击确定（或按 Enter）
"""
import os
import sys
import time

try:
    import pyautogui
except ImportError as e:
    print(f"缺少依赖: {e.name}")
    print("请先安装: pip install pyautogui")
    sys.exit(1)

pyautogui.FAILSAFE = True    # 鼠标撞到屏幕左上角可紧急中止
pyautogui.PAUSE = 0.05       # 每个 pyautogui 动作之间的默认等待

# ============================================================
# 配置区域（坐标直接填 record_coordinates.py 录出来的值即可，云主机无需 DPI 换算）
# ============================================================
PROGRAM_EXE_PATH = r"D:\Program Files\融航资管交易平台风控端\RohonServerRiskControl.exe"

# 登录窗口标题关键字（pygetwindow 会用 contains 匹配）
LOGIN_WINDOW_TITLE_KEYWORD = "风控"          # 例：综合交易平台风控终端 / 融航资管...

# 四个点击位置坐标（直接从 record_coordinates.py / export/coordinates.txt 复制）
PASSWORD_INPUT_COORDS = (739, 470)        # 密码输入框中心
CONFIRM_BUTTON_COORDS   = (807, 586)      # 确定按钮中心
CLOSE_BUTTON_COORDS     = (870, 381)            # 登录窗口右上角「×」关闭按钮（自行录入坐标；0,0 表示未配置，会走 .close()/taskkill 降级）
DESKTOP_ICON_COORDS     = (13, 351)       # 备用：桌面图标坐标（路径启动失败时用）

# 密码
PASSWORD = "yqj123456"

# ============================================================
# 时间配置（云主机较慢，等待时间给得稍长，稳定第一）
# ============================================================
WAIT_PROGRAM_LAUNCH   = 10   # 启动 exe 后，等登录窗口出现（秒）
WAIT_WINDOW_ACTIVATE  = 1.0  # 激活窗口后等待稳定
WAIT_CLICK_SETTLE     = 0.6  # 点击密码框后等待焦点
WAIT_TYPE_SETTLE      = 0.8  # 输入完密码后等待
WAIT_AFTER_CONFIRM    = 3    # 点确定后等待主界面（短时间观察）

# ---------- 登录失败自动关闭（非交易日等情况） ----------
AUTO_CLOSE_ON_LOGIN_FAIL = True     # True=登录窗口还没消失就自动关了它
WAIT_LOGIN_RESULT        = 10       # 点确定后多少秒内仍有登录窗口 → 判定失败
FORCE_KILL_ON_FAIL       = True     # 优雅关闭/坐标点击失败时，是否用 taskkill 强杀 RohonServerRiskControl.exe

# 密码输入方式：
#   "typewrite"  ->  pyautogui.typewrite（推荐，纯数字密码够用）
#   "clipboard"  ->  写入剪贴板后 Ctrl+V（密码含特殊字符时使用，若风控禁粘贴则不可用）
#   "press_each" ->  pyautogui.press 逐个按键（与 typewrite 差异不大）
PASSWORD_INPUT_METHOD = "typewrite"


def _is_login_window_still_visible(win):
    """
    判断「登录窗口是否还存在/可见」。
    传入 _activate_login_window 返回的窗口对象（可能是 None）。
    逻辑：尝试按关键字重新查一次，并且检查窗口是否可见、没有最小化、尺寸正常。
    """
    if win is None:
        # 没传窗口对象，就重新按关键字查是否还存在登录窗口
        for kw in [LOGIN_WINDOW_TITLE_KEYWORD, "融航", "登录", "Risk", "综合交易平台"]:
            if not kw:
                continue
            try:
                wins = pyautogui.getWindowsWithTitle(kw)
            except Exception:
                wins = []
            for w in wins:
                if (w and getattr(w, "title", "")
                        and not getattr(w, "isMinimized", True)
                        and getattr(w, "width", 0) > 100
                        and getattr(w, "height", 0) > 100):
                    return w  # 返回命中的登录窗口
        return None
    # 传入了登录窗口对象：判断它是否还活着且可见
    try:
        if getattr(win, "isMinimized", True):
            return None
        if not getattr(win, "visible", True):
            return None
        w = getattr(win, "width", 0); h = getattr(win, "height", 0)
        if w <= 50 or h <= 50:
            return None
        # 再确认一次标题还在（有些软件登录成功后会把同一窗口改名）
        title = getattr(win, "title", "")
        for kw in [LOGIN_WINDOW_TITLE_KEYWORD, "融航", "登录", "Risk", "综合交易平台"]:
            if kw and kw in title:
                return win
        return None
    except Exception:
        # 窗口对象已失效 → 登录窗口基本就是关了
        return None


def _close_login_window(login_win):
    """
    尝试关闭登录窗口，优先级：
    1) 点窗口对象的 .close()（优雅关闭）
    2) 用窗口的 left/top/width/height 算右上角关闭按钮坐标，pyautogui.click
    3) taskkill /F /IM 进程名 强杀
    """
    if login_win is None:
        # 没有记录登录窗口对象，重新按关键字搜索一次
        for kw in [LOGIN_WINDOW_TITLE_KEYWORD, "融航", "登录", "Risk", "综合交易平台"]:
            if not kw:
                continue
            try:
                wins = pyautogui.getWindowsWithTitle(kw)
            except Exception:
                wins = []
            for w in wins:
                if (w and getattr(w, "title", "")
                        and getattr(w, "width", 0) > 100
                        and getattr(w, "height", 0) > 100):
                    login_win = w
                    break
            if login_win is not None:
                break
    if login_win is None:
        print("  未找到要关闭的登录窗口，可能已经自行关闭了")
        return True

    # 步骤1: 优雅关闭
    closed = False
    title = getattr(login_win, "title", "<unknown>")
    print(f"[关闭] 准备关闭登录窗口: '{title}'")
    try:
        login_win.close()
        time.sleep(1.0)
        closed_after = _is_login_window_still_visible(login_win) is None
        if closed_after:
            closed = True
            print("  [关闭] 调用 .close() 成功")
    except Exception as e:
        print(f"  [关闭] .close() 异常: {e}")

    # 步骤2: 用你手动录入的固定坐标，点登录窗口右上角的「×」按钮
    if not closed and CLOSE_BUTTON_COORDS and CLOSE_BUTTON_COORDS != (0, 0):
        cx, cy = CLOSE_BUTTON_COORDS
        print(f"  [关闭] 尝试按固定坐标点击关闭按钮: ({cx},{cy})")
        try:
            # 先激活到前台，防止点到别的窗口上
            try:
                login_win.activate()
                time.sleep(0.3)
            except Exception:
                pass
            pyautogui.click(cx, cy)
            time.sleep(1.5)
            closed_after = _is_login_window_still_visible(login_win) is None
            if closed_after:
                closed = True
                print("  [关闭] 点击关闭按钮成功")
            else:
                print("  [关闭] 点击关闭按钮后窗口仍存在（可能是坐标偏了，可重录 CLOSE_BUTTON_COORDS）")
        except Exception as e:
            print(f"  [关闭] 点击关闭按钮异常: {e}")
    elif not closed:
        print("  [关闭] 未配置 CLOSE_BUTTON_COORDS（当前为 (0,0)），跳过坐标点击")

    # 步骤3: taskkill 强杀进程
    if not closed and FORCE_KILL_ON_FAIL:
        print("  [关闭] 尝试 taskkill 强杀进程 RohonServerRiskControl.exe")
        import subprocess
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "RohonServerRiskControl.exe", "/T"],
                capture_output=True, timeout=8
            )
            time.sleep(1.5)
            if _is_login_window_still_visible(login_win) is None:
                closed = True
                print("  [关闭] taskkill 强杀成功")
            else:
                print("  [关闭] taskkill 后窗口仍存在，请手动关闭")
        except Exception as e:
            print(f"  [关闭] taskkill 异常: {e}")

    return closed


def _launch_program():
    """
    启动融航风控端。
    关键：必须把「工作目录」切换到 exe 所在目录后再启动，否则软件会读取脚本运行目录下的配置，
    导致用户名/参数等一切都不对（例如出现用户名是 123）。
    """
    if PROGRAM_EXE_PATH and os.path.exists(PROGRAM_EXE_PATH):
        exe_dir = os.path.dirname(os.path.abspath(PROGRAM_EXE_PATH))
        print(f"[启动] 切换工作目录到: {exe_dir}")
        print(f"[启动] os.startfile: {PROGRAM_EXE_PATH}")
        try:
            # 保存原始 CWD，启动后恢复（避免脚本后续工作目录被污染）
            original_cwd = os.getcwd()
            try:
                os.chdir(exe_dir)
            except Exception as e:
                print(f"  [调试] chdir 失败(非致命): {e}")
            # 用 cwd=exe_dir 语义启动，等价于在文件夹里双击
            os.startfile(PROGRAM_EXE_PATH)
            # 启动完立刻把 CWD 切回原目录，防止影响脚本后续路径
            try:
                os.chdir(original_cwd)
            except Exception:
                pass
            return True
        except Exception as e:
            print(f"  [警告] 路径启动失败: {e}，尝试图标双击")

    # 兜底：桌面图标双击（这种方式 Windows 会以快捷方式/图标所在目录为 CWD，通常不会有配置读取问题）
    if DESKTOP_ICON_COORDS and DESKTOP_ICON_COORDS != (0, 0):
        print(f"[启动] 尝试双击桌面图标 {DESKTOP_ICON_COORDS}")
        try:
            pyautogui.hotkey("win", "d")
            time.sleep(1.2)
            pyautogui.doubleClick(*DESKTOP_ICON_COORDS)
            return True
        except Exception as e:
            print(f"  [警告] 图标双击失败: {e}")
    return False


def _activate_login_window():
    """按关键字找登录窗口，拉到前台并激活。返回窗口对象或 None。

    与 run_pipeline.py 风格一致：使用 pyautogui.getWindowsWithTitle(...)
    """
    deadline = time.time() + WAIT_PROGRAM_LAUNCH
    kws_to_try = [LOGIN_WINDOW_TITLE_KEYWORD, "融航", "登录", "Risk", "综合交易平台"]
    kws_to_try = [k for k in kws_to_try if k]  # 去空串
    last_try_windows = []
    while time.time() < deadline:
        valid = []
        for kw in kws_to_try:
            try:
                wins = pyautogui.getWindowsWithTitle(kw)
            except Exception:
                wins = []
            valid = [w for w in wins
                     if w and getattr(w, "title", "")
                     and not getattr(w, "isMinimized", False)]
            if valid:
                break
        if not valid:
            # 第一轮没匹配到时，缓存一份当前所有窗口供调试打印
            if not last_try_windows:
                try:
                    # getAllWindows 在有些版本 pyautogui 上是 pygetwindow，不保证存在
                    try:
                        all_wins = pyautogui.getAllWindows()
                    except AttributeError:
                        import pygetwindow as gw
                        all_wins = gw.getAllWindows()
                    last_try_windows = [w.title for w in all_wins if getattr(w, "title", "")][:25]
                except Exception:
                    pass
            time.sleep(0.4)
            continue

        win = valid[0]
        try:
            if getattr(win, "isMinimized", False):
                win.restore()
                time.sleep(0.3)
            # 先 activate，失败就兜底
            try:
                win.activate()
            except Exception:
                # 有些 pyautogui 版本 activate 可能失败，用 restore+置顶代替
                try:
                    win.restore()
                except Exception:
                    pass
            time.sleep(WAIT_WINDOW_ACTIVATE)
        except Exception as e:
            print(f"  [调试] 激活窗口非致命异常: {e}")
        try:
            title = getattr(win, "title", "")
            l = getattr(win, "left", "?"); t = getattr(win, "top", "?")
            w = getattr(win, "width", "?"); h = getattr(win, "height", "?")
            print(f"[窗口] 已激活: '{title}'  位置=({l},{t})  尺寸={w}x{h}")
        except Exception:
            print("[窗口] 已激活（窗口元信息读取失败，不影响后续）")
        return win

    # 超时
    print("[窗口] 超时未匹配到登录窗口。当前可见窗口标题（前25个）:")
    for t in last_try_windows:
        print(f"  '{t}'")
    if not last_try_windows:
        print("  (未获取到窗口列表)")
    print("  提示：把上面实际出现的登录窗口标题填到 LOGIN_WINDOW_TITLE_KEYWORD 即可精确匹配")
    return None


def _input_password():
    """按配置的方式把密码输入到当前焦点控件"""
    print(f"[密码] 输入方式={PASSWORD_INPUT_METHOD}  长度={len(PASSWORD)} 位")
    if PASSWORD_INPUT_METHOD == "clipboard":
        import subprocess
        # 用 clip 命令写入剪贴板（Windows）
        try:
            subprocess.run("clip", input=PASSWORD.encode("utf-16-le"), check=True, shell=True)
        except Exception:
            # 兜底用 tkinter 写剪贴板
            try:
                import tkinter as tk
                r = tk.Tk()
                r.withdraw()
                r.clipboard_clear()
                r.clipboard_append(PASSWORD)
                r.update()
                r.destroy()
            except Exception as e:
                raise RuntimeError(f"写入剪贴板失败: {e}")
        time.sleep(0.1)
        pyautogui.hotkey("ctrl", "v")
        # 清剪贴板
        try:
            import tkinter as tk
            r = tk.Tk(); r.withdraw(); r.clipboard_clear(); r.update(); r.destroy()
        except Exception:
            pass
    elif PASSWORD_INPUT_METHOD == "press_each":
        for ch in PASSWORD:
            pyautogui.press(ch)
            time.sleep(0.04)
    else:
        # typewrite：只能处理可打印字符，密码是纯数字/字母时最稳
        pyautogui.typewrite(PASSWORD, interval=0.05)


def main():
    print("=" * 50)
    print("融航风控端自动登录（云主机 100% 缩放版本）")
    print("=" * 50)
    print(f"  程序路径  : {PROGRAM_EXE_PATH}")
    print(f"  窗口关键字: {LOGIN_WINDOW_TITLE_KEYWORD}")
    print(f"  密码框坐标: {PASSWORD_INPUT_COORDS}")
    print(f"  确定按钮  : {CONFIRM_BUTTON_COORDS}")
    print(f"  关闭按钮× : {CLOSE_BUTTON_COORDS}"
          f"{'   (未配置，会跳过坐标点击)' if CLOSE_BUTTON_COORDS == (0, 0) else ''}")
    print()

    # 快速校验坐标：如果坐标超过当前屏幕分辨率就提醒
    sw, sh = pyautogui.size()
    print(f"[屏幕] 当前分辨率: {sw} x {sh}")
    for name, (x, y) in [("密码框", PASSWORD_INPUT_COORDS),
                         ("确定按钮", CONFIRM_BUTTON_COORDS),
                         ("关闭按钮×", CLOSE_BUTTON_COORDS),
                         ("桌面图标", DESKTOP_ICON_COORDS)]:
        if (x, y) == (0, 0):
            continue
        if not (0 <= x < sw and 0 <= y < sh):
            print(f"  [警告] {name} 坐标 ({x},{y}) 超出屏幕范围！屏幕最大 ({sw-1},{sh-1})")
            print(f"          请在当前云主机屏幕上重新运行 record_coordinates.py 记录坐标")
    print()

    # ----------------------------------------------------------
    # 步骤1：启动程序 + 激活登录窗口
    # ----------------------------------------------------------
    print("[步骤1/4] 启动程序并等待登录窗口 ...")
    _launch_program()
    win = _activate_login_window()
    if win is None:
        resp = input("  未自动找到登录窗口，是否继续（密码框仍会按坐标点击）？(y/N): ").strip().lower()
        if resp != "y":
            print("用户中止")
            sys.exit(1)
    print()

    # ----------------------------------------------------------
    # 步骤2：点击密码框
    # ----------------------------------------------------------
    print(f"[步骤2/4] 点击密码输入框 {PASSWORD_INPUT_COORDS} ...")
    if PASSWORD_INPUT_COORDS and PASSWORD_INPUT_COORDS != (0, 0):
        # 第一次点击
        pyautogui.click(*PASSWORD_INPUT_COORDS)
        time.sleep(WAIT_CLICK_SETTLE)
        # 第二次点击保险：避免点到标签而不是输入框内部
        pyautogui.click(*PASSWORD_INPUT_COORDS)
        time.sleep(0.3)
        print(f"  点击完成，光标应在密码框内闪烁")
    else:
        input("  未配置密码框坐标，请手动点击密码框使其聚焦，完成后按 Enter 继续 ...")
    print()

    # ----------------------------------------------------------
    # 步骤3：输入密码
    # ----------------------------------------------------------
    print("[步骤3/4] 输入密码 ...")
    _input_password()
    time.sleep(WAIT_TYPE_SETTLE)
    print("  输入完成")
    print()

    # ----------------------------------------------------------
    # 步骤4：点确定（或按 Enter）
    # ----------------------------------------------------------
    print(f"[步骤4/4] 点击确定按钮 {CONFIRM_BUTTON_COORDS} 并按 Enter 兜底 ...")
    if CONFIRM_BUTTON_COORDS and CONFIRM_BUTTON_COORDS != (0, 0):
        pyautogui.click(*CONFIRM_BUTTON_COORDS)
    # 无论坐标点没点中，最后再按一次 Enter（登录窗口默认按钮通常就是确定）
    time.sleep(0.2)
    pyautogui.press("enter")
    time.sleep(WAIT_AFTER_CONFIRM)
    print("  确定完成")
    print()

    # ----------------------------------------------------------
    # 步骤5：检测登录结果，失败则 10s 超时后自动关闭登录窗口
    # ----------------------------------------------------------
    login_succeeded = False
    if AUTO_CLOSE_ON_LOGIN_FAIL:
        print(f"[登录检测] 等待 {WAIT_LOGIN_RESULT} 秒观察登录窗口是否消失 ...")
        deadline = time.time() + WAIT_LOGIN_RESULT
        checked_win = win   # 用之前 _activate_login_window 保存下来的登录窗口对象
        last_reported = None
        while time.time() < deadline:
            remaining = int(deadline - time.time())
            still = _is_login_window_still_visible(checked_win)
            if still is None:
                login_succeeded = True
                print(f"  ✓ 登录窗口已消失，判定登录成功（剩余 {remaining}s）")
                # 如果对象是新命中的（返回了新窗口句柄），记录一下方便后续关闭
                break
            # 如果本轮返回的是「新的窗口对象」而不是旧的，更新引用
            if still is not None and still is not checked_win:
                checked_win = still
            # 每 2 秒或状态变化时打一条状态
            status = (still is not None)
            if status != last_reported or remaining % 2 == 0:
                title = getattr(still, "title", "")
                print(f"  · 还剩 {remaining}s → 登录窗口仍存在: '{title[:30]}'")
                last_reported = status
            time.sleep(0.5)
        print()

        if not login_succeeded:
            print(f"[登录检测] ✗ {WAIT_LOGIN_RESULT} 秒内登录窗口未消失，判定为登录失败（非交易日/密码错误/网络等原因）")
            print("  开始自动关闭登录窗口 ...")
            ok = _close_login_window(checked_win)
            if ok:
                print("[关闭] ✓ 登录窗口已成功关闭")
            else:
                print("[关闭] ✗ 未能成功关闭登录窗口，请手动处理")
            print()
            # 标记为失败，退出码非 0 便于上游脚本感知
            final_exit_code = 5
        else:
            final_exit_code = 0
    else:
        final_exit_code = 0

    print("=" * 50)
    if login_succeeded or not AUTO_CLOSE_ON_LOGIN_FAIL:
        print("自动登录流程结束，请确认风控端是否成功进入主界面")
    else:
        print("自动登录流程结束：登录未成功，已触发自动关闭（可能是非交易日/网络/密码等原因）")
    print("=" * 50)
    print("常见问题：")
    print("  1) 坐标超出范围：在云主机里重新运行 record_coordinates.py 录一次，直接填进来即可")
    print("  2) 密码没输进去：把 PASSWORD_INPUT_METHOD 改为 'clipboard' 再试")
    print("  3) 窗口没激活：把 LOGIN_WINDOW_TITLE_KEYWORD 改成日志里打印的实际标题")
    print("  4) 登录成功但被判定为失败：确认登录成功后原窗口标题是否还含'风控/登录'等关键字；若含，把超时 WAIT_LOGIN_RESULT 调大或加新关键字排除")
    print("  5) 关闭按钮×没点中/没生效：用 record_coordinates.py 重新录一次 CLOSE_BUTTON_COORDS（一定要点到登录窗口右上角那个红色的 ×）")
    print("  6) 不想手动录关闭按钮：把 CLOSE_BUTTON_COORDS 保持 (0,0)，脚本会用 .close() 和 taskkill 自动兜底")
    print("  7) 紧急中止：把鼠标指针甩到屏幕最左上角（pyautogui FAILSAFE）")
    if final_exit_code != 0:
        sys.exit(final_exit_code)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户 Ctrl+C 中断")
        sys.exit(0)
    except pyautogui.FailSafeException:
        print("\n检测到 pyautogui FAILSAFE（鼠标撞到屏幕左上角），已紧急中止")
        sys.exit(130)
