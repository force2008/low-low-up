#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账户资金监控程序 - 定时导出CSV并发送飞书通知

使用方式:
    python account_monitor.py [online|simu|7x24]

说明:
    - 每5分钟自动导出CTP账户数据
    - 读取CSV中的账号、动态权益、保证金、持盈、平盈
    - 发送飞书通知
    - 收盘后自动退出
    - 02:30退出前删除今天导出的文件
"""

import sys
import os
import time
import datetime
import json
import logging
import glob
import csv
import subprocess
import faulthandler
from logging.handlers import RotatingFileHandler

# 启用 C 级崩溃转储
faulthandler.enable()

# ==================== 路径配置 ====================
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_CURR_DIR)
sys.path.insert(0, _CURR_DIR)
sys.path.insert(0, PROJECT_ROOT)

# ==================== 日志配置 ====================
LOG_DIR = os.path.join(_CURR_DIR, "logs", "account_monitor")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "account_monitor.log")

logger = logging.getLogger("account_monitor")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

# ==================== 飞书Webhook配置 ====================
try:
    from compare_orders import FEISHU_WEBHOOK_ACCOUNT_MONITOR_URL
except ImportError:
    FEISHU_WEBHOOK_ACCOUNT_MONITOR_URL = ""


def send_feishu_text(text):
    """发送纯文本飞书通知"""
    if not FEISHU_WEBHOOK_ACCOUNT_MONITOR_URL:
        logger.warning("[飞书] 未配置Webhook，跳过发送")
        return
    try:
        import requests
        payload = {"msg_type": "text", "content": {"text": text}}
        resp = requests.post(FEISHU_WEBHOOK_ACCOUNT_MONITOR_URL, json=payload, timeout=10)
        logger.info("[飞书] 通知发送状态: %s", resp.status_code)
    except Exception as e:
        logger.error("[飞书] 通知发送失败: %s", e)


# ==================== 会话保活：把 RDP 会话切回控制台 ====================
# 背景：
#   导出步骤(automate_export.py)用 pyautogui 按「固定桌面坐标」点击，依赖一个
#   活跃且分辨率固定的桌面。用微软远程桌面(RDP)登录后：
#     1) Windows 会新建/接管一个 RDP 会话，控制台(VNC)会话被断开；
#     2) RDP 会话的分辨率由客户端(手机端)决定，且会话会「记住」该分辨率。
#   于是 VNC 里看到的桌面尺寸/坐标全变了，导出点击全部错位、失败。
#
# 解决：
#   用 tscon 把当前会话「归还」给控制台(console)，系统相当于「有人正坐在机器前
#   使用桌面」——桌面持续在虚拟显示器上按固定分辨率渲染，即使断开 RDP 也不会
#   黑屏，GUI 自动化/导出继续可用。
#
# 注意：
#   - tscon 需要管理员权限，否则报「拒绝访问」；
#   - 切换瞬间你当前的 RDP 连接会被断开（提示「另一个用户已连接」），属正常现象；
#   - CONSOLE_KEEPALIVE_INTERVAL <= 0 时只在启动瞬间检查一次，不做周期检查。
CONSOLE_KEEPALIVE_INTERVAL = 60  # 秒


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


def _query_session_name(session_id):
    """用 qwinsta 查询指定会话的「会话名 / 状态」"""
    try:
        out = subprocess.check_output(["qwinsta"], stderr=subprocess.STDOUT)
    except Exception as e:
        logger.debug("[会话] qwinsta 执行失败: %s", e)
        return None, None

    text = out.decode("gbk", errors="ignore")
    target = str(session_id)
    for line in text.splitlines():
        tokens = line.replace(">", " ").split()
        if target in tokens:
            idx = tokens.index(target)
            name = tokens[0]
            state = tokens[idx + 1] if idx + 1 < len(tokens) else ""
            return name, state
    return None, None


def ensure_console_session():
    """若程序当前运行在 RDP 会话中，则把该会话切回控制台(console)。

    Returns:
        bool: 本次是否真正执行了切换
    """
    if os.name != "nt":
        return False

    session_id = _get_process_session_id()
    if session_id is None:
        logger.debug("[会话] 未能获取当前会话 ID，跳过切换")
        return False

    name, state = _query_session_name(session_id)
    if not name:
        logger.debug("[会话] 未能解析当前会话(id=%s)，跳过切换", session_id)
        return False

    if name.lower().startswith("console"):
        logger.debug("[会话] 当前已在控制台会话(%s)，无需切换", name)
        return False

    logger.warning(
        "[会话] 当前运行在 RDP 会话 %s (id=%s, 状态=%s)，切换到控制台...",
        name, session_id, state,
    )
    try:
        proc = subprocess.run(
            ["tscon.exe", str(session_id), "/dest:console"],
            capture_output=True, text=True, timeout=15,
        )
        msg = (proc.stdout or proc.stderr or "").strip()
        if proc.returncode == 0:
            logger.info("[会话] 已切回控制台会话 (rc=0) %s", msg)
            return True
        logger.error("[会话] 切换失败 rc=%s %s（请以管理员身份运行）", proc.returncode, msg)
    except Exception as e:
        logger.error("[会话] 执行 tscon 失败: %s", e)
    return False


# ==================== 交易时间段配置 ====================
# 与 run_pipeline.py 保持一致
TRADING_SESSIONS = [
    ("09:00:15", "11:30:00"),    # 日盘上午
    ("13:00:15", "15:15:00"),    # 日盘下午
    ("21:00:15", "23:59:15"),    # 夜盘上半场
    ("00:00:05", "02:30:00"),    # 夜盘下半场
]

# 导出间隔（秒）
EXPORT_INTERVAL = 300  # 5分钟


def is_in_trading_time():
    """判断当前是否在配置的交易时间段内"""
    now = datetime.datetime.now().time()
    for start_str, end_str in TRADING_SESSIONS:
        start = datetime.datetime.strptime(start_str, "%H:%M:%S").time()
        end = datetime.datetime.strptime(end_str, "%H:%M:%S").time()
        if start <= end:
            if start <= now <= end:
                return True
        else:
            if now >= start or now <= end:
                return True
    return False


def get_session_end_time(now_time):
    """根据当前时间，返回本次任务应负责到的时段结束时间。

    多时段定时任务模式：
      - 08:59/12:59/20:59 各启动一次
      - 上午盘窗口：02:30 ~ 11:30 → 退出 11:30
      - 下午盘窗口：11:30 ~ 15:15 → 退出 15:15
      - 夜盘窗口：15:15 ~ 02:30 → 退出 02:30
    """
    morning_end = datetime.time(11, 30, 0)
    afternoon_end = datetime.time(15, 15, 0)
    night_end = datetime.time(2, 30, 0)

    # 02:30 ~ 11:30：上午盘任务窗口
    if night_end <= now_time < morning_end:
        return morning_end
    # 11:30 ~ 15:15：下午盘任务窗口
    if morning_end <= now_time < afternoon_end:
        return afternoon_end
    # 15:15 ~ 02:30：夜盘任务窗口
    if afternoon_end <= now_time or now_time < night_end:
        return night_end

    return morning_end


def is_after_session_end(now_time, session_end):
    """判断当前时间是否已超过目标时段结束时间"""
    if session_end == datetime.time(2, 30, 0):
        return datetime.time(2, 30, 0) <= now_time < datetime.time(9, 0, 0)
    return now_time >= session_end


def seconds_until_next_session():
    """计算距离下一个交易时段开始还有多少秒"""
    now = datetime.datetime.now()
    now_time = now.time()
    candidates = []
    for start_str, _ in TRADING_SESSIONS:
        start_time = datetime.datetime.strptime(start_str, "%H:%M:%S").time()
        start_dt = datetime.datetime.combine(now.date(), start_time)
        if start_time > now_time:
            candidates.append(start_dt)
        else:
            candidates.append(start_dt + datetime.timedelta(days=1))
    if candidates:
        next_dt = min(candidates)
        return int((next_dt - now).total_seconds())
    return 3600


# 根据本实例启动时间确定负责的交易时段结束时间
_RESPONSIBLE_SESSION_END = get_session_end_time(datetime.datetime.now().time())


# ==================== 导出功能 ====================
def run_export():
    """执行自动导出，使用 local_account_config.py 的配置"""
    try:
        import account_export
        success = account_export.main()
        return success
    except Exception as e:
        logger.error("[导出] 导出步骤异常: %s", e)
        return False


# ==================== 读取CSV并解析 ====================
def get_latest_csv_file(folder_path, prefix):
    """获取指定文件夹下，以prefix开头的最新CSV文件"""
    if not os.path.isdir(folder_path):
        logger.warning("[CSV] 文件夹不存在: %s", folder_path)
        return None

    pattern = os.path.join(folder_path, f"{prefix}*.csv")
    files = glob.glob(pattern)

    if not files:
        logger.warning("[CSV] 未找到匹配文件: %s", pattern)
        return None

    # 按修改时间排序，返回最新的
    latest = max(files, key=os.path.getmtime)
    logger.info("[CSV] 最新文件: %s", os.path.basename(latest))
    return latest


def read_account_data_from_csv(csv_path):
    """从CSV文件读取账户数据

    Returns:
        list: [{账号, 动态权益, 保证金, 持盈, 平盈}, ...]
    """
    results = []
    # 尝试多种编码
    encodings = ['gbk', 'gb2312', 'gb18030', 'utf-8']

    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                logger.info("[CSV] 使用编码 %s 读取成功，列名: %s", encoding, headers)

                for row in reader:
                    # 尝试多种可能的列名
                    account = row.get('账号') or row.get('帐号') or row.get('账户') or ''
                    dynamic_equity = row.get('动态权益') or row.get('权益') or '0'
                    margin = row.get('保证金') or row.get('占用保证金') or row.get('保证金占用') or '0'
                    position_profit = row.get('持盈') or row.get('持仓盈亏') or row.get('浮动盈亏') or '0'
                    close_profit = row.get('平盈') or row.get('平仓盈亏') or '0'

                    # 过滤空行
                    if account and account.strip():
                        results.append({
                            '账号': account.strip(),
                            '动态权益': dynamic_equity.strip(),
                            '保证金': margin.strip(),
                            '持盈': position_profit.strip(),
                            '平盈': close_profit.strip(),
                        })

            # 成功读取后跳出循环
            logger.info("[CSV] 读取到 %d 条账户记录", len(results))
            break
        except UnicodeDecodeError:
            logger.debug("[CSV] 编码 %s 读取失败，尝试下一个", encoding)
            continue
        except Exception as e:
            logger.error("[CSV] 读取CSV失败: %s", e)
            break

    return results


# ==================== 飞书通知 ====================
def format_account_message(accounts_data):
    """格式化账户数据为飞书消息

    - 只显示持盈和平盈不都为0的账号
    - 头部优先展示 CSV 末尾自带的「合 计」行（精确匹配，中间一个空格），
      找不到该行则 fallback 到代码自己累加的汇总
    """
    if not accounts_data:
        return "未读取到账户数据"

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    # fallback 用的手动累加汇总（只有当 CSV 不带合计行时才展示）
    total_equity = 0
    total_margin = 0
    total_position_profit = 0
    total_close_profit = 0
    valid_accounts = []
    total_row = None  # CSV 原始的「合 计」行（中间一个半角空格）

    def _is_total_account(account_name):
        """匹配 CSV 的合计行：

        实测 CSV 里账号形如『合 计:』（中间一个半角空格、尾部带冒号）。
        兼容：前后空白、中间半角/全角空格、尾部冒号、TAB —— 规整后等于『合计』即命中。
        """
        if not account_name:
            return False
        s = account_name.strip()
        # 先去掉尾部冒号（全角/半角），再去掉所有空白字符
        if s.endswith(':'):
            s = s[:-1].rstrip()
        if s.endswith('\uff1a'):  # 全角冒号
            s = s[:-1].rstrip()
        s = (s.replace(' ', '')
              .replace('\u3000', '')
              .replace('\t', ''))
        return s == '合计'

    for acc in accounts_data:
        try:
            equity = float(acc['动态权益'].replace(',', '')) if acc['动态权益'] else 0
            margin = float(acc['保证金'].replace(',', '')) if acc['保证金'] else 0
            pos_profit = float(acc['持盈'].replace(',', '')) if acc['持盈'] else 0
            close_profit = float(acc['平盈'].replace(',', '')) if acc['平盈'] else 0
        except (ValueError, AttributeError):
            equity = margin = pos_profit = close_profit = 0

        # 识别 CSV 原始合计行：精确匹配「合 计」(去掉空白 == '合计')
        if _is_total_account(acc.get('账号', '')):
            total_row = {
                **acc,
                '_pos_profit': pos_profit,
                '_close_profit': close_profit,
            }
            # 合计行仍然累加到 fallback 总计数里，保持 fallback 一致
            total_equity += equity
            total_margin += margin
            total_position_profit += pos_profit
            total_close_profit += close_profit
            continue

        total_equity += equity
        total_margin += margin
        total_position_profit += pos_profit
        total_close_profit += close_profit

        # 只保留持盈和平盈不都为0的账号
        if pos_profit != 0 or close_profit != 0:
            valid_accounts.append({
                **acc,
                '_pos_profit': pos_profit,
                '_close_profit': close_profit
            })

    # 头部：优先用 CSV 原始合计行；找不到才 fallback 到代码自己累加的
    lines = [f"📊 账户资金监控 ({now_str})", ""]
    if total_row is not None:
        # 持盈、平盈各自独立判断盈亏图标（用户关注点：合计行两项不一定同向）
        pos_icon = "📈" if total_row['_pos_profit'] >= 0 else "📉"
        close_icon = "📈" if total_row['_close_profit'] >= 0 else "📉"
        lines.append("📋 合 计")
        lines.append(f"  权益: {total_row['动态权益']} | 保证金: {total_row['保证金']}")
        lines.append(f"  持盈: {pos_icon} {total_row['持盈']} | 平盈: {close_icon} {total_row['平盈']}")
        lines.append("")
    else:
        total_profit = total_position_profit + total_close_profit
        total_icon = "📈" if total_profit >= 0 else "📉"
        lines.append("📋 汇总（合计）")
        lines.append(f"  权益: {total_equity:,.2f} | 保证金: {total_margin:,.2f}")
        lines.append(f"  持盈: {total_icon} {total_position_profit:,.2f} | 平盈: {total_icon} {total_close_profit:,.2f}")
        lines.append("")

    # 各账号详情（注意：合计行已经在头部展示过，这里不再重复）
    if valid_accounts:
        lines.append("─" * 20)
        for acc in valid_accounts:
            pos_icon = "📈" if acc['_pos_profit'] >= 0 else "📉"
            close_icon = "📈" if acc['_close_profit'] >= 0 else "📉"
            lines.append(f"【{acc['账号']}】")
            lines.append(f"  权益: {acc['动态权益']} | 保证金: {acc['保证金']}")
            lines.append(f"  持盈: {pos_icon} {acc['持盈']} | 平盈: {close_icon} {acc['平盈']}")
            lines.append("")

    return "\n".join(lines)


# ==================== 清理导出文件 ====================
def cleanup_export_files():
    """删除导出文件夹下的所有CSV文件"""
    try:
        from local_account_config import DEFAULT_SAVE_PATH
        if not DEFAULT_SAVE_PATH or not os.path.isdir(DEFAULT_SAVE_PATH):
            logger.warning("[清理] DEFAULT_SAVE_PATH 不存在: %s", DEFAULT_SAVE_PATH)
            return

        removed_count = 0
        failed_count = 0
        for entry in os.scandir(DEFAULT_SAVE_PATH):
            if entry.is_file() and entry.name.endswith('.csv'):
                try:
                    os.remove(entry.path)
                    removed_count += 1
                except Exception as e:
                    logger.warning("[清理] 删除失败: %s - %s", entry.path, e)
                    failed_count += 1

        logger.info("[清理] 已删除 %d 个导出文件", removed_count)
        if failed_count > 0:
            logger.warning("[清理] 删除失败 %d 个文件", failed_count)
    except Exception as e:
        logger.warning("[清理] 清理导出文件失败: %s", e)


# ==================== 主程序 ====================
# ==================== 交易日检查 ====================
_TRADE_DATE_FILE = os.path.join(
    PROJECT_ROOT, "config", "trade_date.json"
)

# 解析强制运行参数（用于非交易日测试）
_FORCE_RUN = "--force" in sys.argv
if _FORCE_RUN:
    sys.argv.remove("--force")
    print("[提示] 强制运行模式，即使非交易日也会执行")


# 解析运行环境：argv[1] or CTP_ENV（7x24/simu → 所有时段判断全部跳过，和 online 实盘隔离）
def _get_account_monitor_env():
    env = None
    if len(sys.argv) > 1 and str(sys.argv[1]).lower() in ("online", "simu", "7x24"):
        env = str(sys.argv[1]).lower()
    if not env:
        env = str(os.getenv("CTP_ENV") or "").lower() or "online"
    return env.strip().lower()


_ACCOUNT_MONITOR_ENV = _get_account_monitor_env()
# 统一兜（--force / 7x24 / simu 任一成立 → 跳过交易时段判断 + 段结束时间判断，7x24 小时可运行）
_SKIP_TRADING_CHECK = bool(_FORCE_RUN) or (_ACCOUNT_MONITOR_ENV in ("simu", "7x24"))


def _is_trading_day():
    """检查今天是否为交易日"""
    today = datetime.date.today().isoformat()

    if _FORCE_RUN:
        logger.info("[交易日] 强制运行模式，今日(%s)作为交易日处理", today)
        return True

    if not os.path.exists(_TRADE_DATE_FILE):
        logger.warning("[交易日] 交易日文件不存在: %s，跳过检查", _TRADE_DATE_FILE)
        return True

    try:
        with open(_TRADE_DATE_FILE, 'r', encoding='utf-8') as f:
            trade_dates = json.load(f)
        trade_dates_set = set(trade_dates)

        if today in trade_dates_set:
            logger.info("[交易日] 今日(%s)是交易日，可以运行", today)
            return True
        else:
            logger.warning("[交易日] 今日(%s)不是交易日，程序退出", today)
            print(f"[交易日] 今日({today})不是交易日，程序退出")
            return False
    except Exception as e:
        logger.error("[交易日] 读取交易日文件失败: %s", e)
        return True  # 出错时允许运行


def main():
    # 交易日检查
    if not _is_trading_day():
        return

    # 启动即做一次会话保活：若当前在 RDP 会话中，切回控制台，避免桌面坐标失效
    switched_to_console = ensure_console_session()

    logger.info("=" * 50)
    logger.info("账户资金监控程序已启动")
    logger.info("当前时段结束时间: %s", _RESPONSIBLE_SESSION_END)
    logger.info("导出间隔: %d 秒", EXPORT_INTERVAL)
    logger.info("=" * 50)

    # 启动时发送通知
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    session_end_str = _RESPONSIBLE_SESSION_END.strftime('%H:%M')
    startup_msg = (
        f"🚀 账户监控已启动\n"
        f"时间: {now_str}\n"
        f"时段结束: {session_end_str}"
    )
    if switched_to_console:
        startup_msg += "\n🖥️ 已从RDP切回控制台（桌面保活）"
    send_feishu_text(startup_msg)

    last_export_time = 0
    last_session_check = time.time()
    consecutive_failures = 0
    max_consecutive_failures = 3

    while True:
        try:
            now = datetime.datetime.now()
            now_time = now.time()

            # 周期性会话保活：期间若有人用 RDP 登录，会话会被抢占/切换，
            # 导致桌面坐标失效，这里自动切回控制台
            if CONSOLE_KEEPALIVE_INTERVAL > 0 and \
                    now.timestamp() - last_session_check >= CONSOLE_KEEPALIVE_INTERVAL:
                last_session_check = now.timestamp()
                ensure_console_session()

            # 检查是否超过时段结束时间（7x24/simu/--force 模式跳过，允许非交易时段持续运行）
            _need_exit_session_end = bool(is_after_session_end(now_time, _RESPONSIBLE_SESSION_END)) and not _SKIP_TRADING_CHECK
            if _need_exit_session_end:
                logger.info("[主循环] 已到达时段结束时间 %s，准备退出", _RESPONSIBLE_SESSION_END)

                # 夜盘结束，清理文件
                if _RESPONSIBLE_SESSION_END == datetime.time(2, 30, 0):
                    logger.info("[主循环] 夜盘结束，清理导出文件...")
                    send_feishu_text("🌙 夜盘结束，账户监控退出")
                    cleanup_export_files()
                elif _RESPONSIBLE_SESSION_END == datetime.time(15, 15, 0):
                    send_feishu_text("☀️ 日盘结束，账户监控退出")
                else:
                    send_feishu_text("☀️ 上午盘结束，账户监控退出")

                break

            # 检查是否在交易时间（7x24/simu/--force 模式也当做交易时段，正常导出）
            _in_trading = bool(is_in_trading_time()) or _SKIP_TRADING_CHECK
            if _in_trading:
                current_time = time.time()

                # 检查是否需要执行导出
                if current_time - last_export_time >= EXPORT_INTERVAL:
                    logger.info("[主循环] 开始导出...")
                    export_ok = run_export()

                    if export_ok:
                        consecutive_failures = 0
                        logger.info("[主循环] 导出成功，读取CSV...")

                        # 读取最新CSV文件
                        try:
                            from local_account_config import DEFAULT_SAVE_PATH
                            csv_path = get_latest_csv_file(DEFAULT_SAVE_PATH, "操作账户")

                            if csv_path:
                                accounts_data = read_account_data_from_csv(csv_path)
                                if accounts_data:
                                    message = format_account_message(accounts_data)
                                    send_feishu_text(message)
                                    logger.info("[主循环] 飞书通知已发送")
                                else:
                                    logger.warning("[主循环] 未读取到账户数据")
                            else:
                                logger.warning("[主循环] 未找到CSV文件")

                        except Exception as e:
                            logger.error("[主循环] 读取CSV失败: %s", e)

                        last_export_time = current_time
                    else:
                        consecutive_failures += 1
                        logger.warning("[主循环] 导出失败 (连续失败 %d/%d)", consecutive_failures, max_consecutive_failures)

                        if consecutive_failures >= max_consecutive_failures:
                            send_feishu_text(f"⚠️ 连续{consecutive_failures}次导出失败，程序退出")
                            logger.error("[主循环] 连续导出失败次数过多，退出程序")
                            break
                else:
                    # 计算距离下次导出的时间
                    next_export_in = EXPORT_INTERVAL - (current_time - last_export_time)
                    logger.debug("[主循环] 距下次导出还有 %.0f 秒", next_export_in)
            else:
                # 非交易时间
                wait_sec = seconds_until_next_session()
                logger.info("[主循环] 非交易时间，距离下次开盘还有 %d 分 %d 秒", wait_sec // 60, wait_sec % 60)

                # 如果是夜盘结束后的早晨（02:30-09:00），直接退出
                if _RESPONSIBLE_SESSION_END == datetime.time(11, 30, 0):
                    now_time_obj = datetime.time(9, 0, 0)
                    if datetime.time(2, 30, 0) <= now_time < now_time_obj:
                        logger.info("[主循环] 夜盘已结束，早盘未开始，退出")
                        send_feishu_text("🌅 夜盘结束，早盘未开始，账户监控退出")
                        break

            # 休眠一段时间
            time.sleep(10)

        except KeyboardInterrupt:
            logger.info("[主循环] 收到键盘中断，准备退出...")
            send_feishu_text("用户中断，账户监控退出")
            break
        except Exception as e:
            logger.error("[主循环] 异常: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(30)

    logger.info("账户资金监控程序已退出")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("[顶层] 未捕获异常，程序退出: %s", e)
        send_feishu_text(f"❌ 账户监控异常退出: {e}")
        sys.exit(1)
