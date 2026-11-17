#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""控制台会话保活：把被 RDP 抢占的桌面切回控制台(console)。

背景
----
导出步骤（automate_export.py / account_export.py）用 pyautogui 按「固定桌面绝对坐标」
点击，依赖一个「活跃、分辨率固定」的桌面。一旦有人用微软远程桌面(RDP)登录：
  1) Windows 会新建/接管一个 RDP 会话，原本的控制台(VNC)会话被踢成「断开」；
  2) RDP 会话的分辨率由客户端（手机端）决定，且会被该会话「记住」。
结果：VNC 里看到的桌面尺寸/坐标全变，点击全部错位、导出失败。

解决
----
用 `tscon <会话ID> /dest:console` 把桌面「归还」给控制台。之后即使断开 RDP，
桌面仍按控制台的固定分辨率持续渲染，GUI 自动化/导出继续可用。

需要处理的两种形态
------------------
  A. 进程本身跑在 RDP 会话里（单会话系统，或计划任务被启动进了 RDP 会话）：
     自身会话名形如 `rdp-tcp#0`，不是 `console`。
     -> 直接 `tscon <自身会话ID> /dest:console`。
  B. 进程跑在原来的控制台会话里，但该会话已被新来的 RDP 会话挤成「断开」
     （Windows Server 上「同一账号 RDP 登录」的典型表现）：
     自身会话名仍是 `console`，但桌面上没人、画面不渲染，抓屏全黑/点击无效。
     -> 同样 `tscon <自身会话ID> /dest:console`，把自身会话重新挂回控制台。

注意
----
  - tscon 需要管理员权限，否则报「拒绝访问」；计划任务需勾选「使用最高权限运行」；
  - 切换瞬间你当前的 RDP 连接会被断开（提示「另一个用户已连接」），属正常现象；
  - 判定与结果都会以 INFO/WARNING 级别写日志，不会静默跳过；
  - 实际切换后有冷却时间，避免每个导出周期都重复执行。
"""

import os
import time
import logging
import subprocess

# 两次「实际切换」之间的最小间隔（秒）
CONSOLE_KEEPALIVE_COOLDOWN = 60

_last_switch_ts = 0.0


def _get_process_session_id():
    """获取当前进程所在的 Windows 会话 ID"""
    try:
        import ctypes
        sid = ctypes.c_ulong(0)
        ok = ctypes.windll.kernel32.ProcessIdToSessionId(
            ctypes.c_ulong(os.getpid()), ctypes.byref(sid)
        )
        return sid.value if ok else None
    except Exception:
        return None


def _list_sessions():
    """列出本机所有会话 -> [(会话名, 会话ID, 状态), ...]

    用 qwinsta 输出解析：按「首个纯数字 token 即会话 ID」定位，兼容有无用户名
    （无用户名时列会左移）以及中英文系统。
    """
    exe = "qwinsta"
    try:
        out = subprocess.check_output([exe], stderr=subprocess.STDOUT)
    except Exception:
        system_root = os.environ.get("SystemRoot", r"C:\Windows")
        out = subprocess.check_output(
            [os.path.join(system_root, "System32", "qwinsta.exe")],
            stderr=subprocess.STDOUT,
        )

    text = out.decode("gbk", errors="ignore")
    sessions = []
    for line in text.splitlines():
        tokens = line.replace(">", " ").split()
        if len(tokens) < 3:
            continue
        sid_idx = None
        for i, t in enumerate(tokens):
            if t.isdigit():
                sid_idx = i
                break
        if sid_idx is None:
            continue
        state = tokens[sid_idx + 1] if sid_idx + 1 < len(tokens) else ""
        sessions.append((tokens[0], int(tokens[sid_idx]), state))
    return sessions


def ensure_console_session(logger=None, force=False):
    """若当前桌面被 RDP 占用，则把会话切回控制台(console)。

    Args:
        logger: 调用方的 logger，保证日志写进各自的日志文件
        force:  True 时忽略冷却时间，强制执行

    Returns:
        bool: 本次是否真正执行了切换
    """
    log = logger or logging.getLogger(__name__)

    if os.name != "nt":
        return False

    try:
        sessions = _list_sessions()
    except Exception as e:
        log.warning("[会话] qwinsta 执行失败，跳过保活: %s", e)
        return False

    if not sessions:
        log.warning("[会话] qwinsta 无有效输出，跳过保活")
        return False

    my_id = _get_process_session_id()
    my_name = None
    for name, sid, _state in sessions:
        if my_id is not None and sid == my_id:
            my_name = name
            break

    rdp_sessions = [s for s in sessions if s[0].lower().startswith("rdp-tcp#")]

    log.info(
        "[会话] 自身会话 id=%s 名称=%s；RDP会话=%s；全部会话=%s",
        my_id, my_name or "?", rdp_sessions or "无", sessions,
    )

    # ---------- 决定动作 ----------
    if my_id is None:
        if not rdp_sessions:
            log.warning("[会话] 无法获取自身会话 ID 且无 RDP 会话，跳过保活")
            return False
        target = rdp_sessions[0][1]
        reason = "无法定位自身会话，改为把 RDP 会话切回控制台"
    elif my_name and my_name.lower().startswith("console") and not rdp_sessions:
        log.info("[会话] 已在控制台会话(%s)且无 RDP 会话，无需切换", my_name)
        return False
    else:
        target = my_id
        if my_name and not my_name.lower().startswith("console"):
            reason = "自身运行在 RDP 会话 %s" % my_name
        else:
            reason = "存在 RDP 会话 %s，自身控制台会话可能已被挤成断开" % (rdp_sessions,)

    # ---------- 冷却控制 ----------
    global _last_switch_ts
    if not force and (time.time() - _last_switch_ts) < CONSOLE_KEEPALIVE_COOLDOWN:
        log.info(
            "[会话] 需要切换（%s），但距上次切换不足 %ds，跳过",
            reason, CONSOLE_KEEPALIVE_COOLDOWN,
        )
        return False

    # ---------- 执行 tscon ----------
    log.warning("[会话] %s，执行 tscon %s /dest:console", reason, target)
    try:
        proc = subprocess.run(
            ["tscon.exe", str(target), "/dest:console"],
            capture_output=True, encoding="gbk", errors="ignore", timeout=15,
        )
        msg = (proc.stdout or proc.stderr or "").strip()
        if proc.returncode == 0:
            _last_switch_ts = time.time()
            log.info("[会话] 已切回控制台会话 (target=%s, rc=0) %s", target, msg)
            return True
        log.error(
            "[会话] 切换失败 rc=%s %s（tscon 需要管理员权限，请以管理员身份运行）",
            proc.returncode, msg,
        )
    except Exception as e:
        log.error("[会话] 执行 tscon 失败: %s", e)
    return False
