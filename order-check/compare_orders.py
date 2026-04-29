import os
import glob
import csv
import json
import sys
import datetime

# 从本地配置文件导入保存路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from local_config import DEFAULT_SAVE_PATH as DATA_DIR, ACCOUNT
except ImportError:
    DATA_DIR = r"E:\personal files\data"
    ACCOUNT = "wangk0402"

# ==================== 配置区域 ====================
PREFIX = f"{ACCOUNT} 所有委托"

# 飞书机器人 webhook（请替换为实际的 webhook 地址）
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/6afaaa96-9685-4de8-8136-4de3b7eb4b42"
# 示例: FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxx"
# ==================================================


def find_latest_two_files():
    """按修改时间找出最新的两个文件"""
    pattern = os.path.join(DATA_DIR, f"{PREFIX} *.csv")
    files = glob.glob(pattern)
    if len(files) < 2:
        return None
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files[0], files[1]


def read_csv_as_dicts(filepath):
    """用 csv.DictReader 读取，返回字典列表"""
    with open(filepath, 'r', encoding='gbk', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)


def detect_keys(rows):
    """自动检测时间字段和编号字段"""
    if not rows:
        return None, None
    keys = list(rows[0].keys())

    time_key = None
    id_key = None

    for key in keys:
        if time_key is None and '时间' in key:
            time_key = key
        if id_key is None and '编号' in key:
            id_key = key

    # 如果找不到，打印字段名供用户排查
    if time_key is None or id_key is None:
        print("可用字段:", keys)

    return time_key, id_key


def _time_sort_key(row, time_key):
    """处理期货交易时间排序：夜盘(20:00-05:00)在前，日盘(09:00-15:00)在后"""
    t = row.get(time_key, '')
    if not t:
        return (999, 0, 0)
    try:
        parts = t.split(':')
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        if 20 <= h <= 23:
            # 晚上夜盘，保持 20-23，排最前
            pass
        elif h < 6:
            # 凌晨夜盘，变为 24-29，排中间
            h += 24
        else:
            # 日盘及盘前盘后(06-19)，变为 30-43，排最后
            h += 24
        return (h, m, s)
    except (ValueError, IndexError):
        return (999, 0, 0)


def _send_feishu(text):
    """发送飞书文本通知的通用函数"""
    if not FEISHU_WEBHOOK_URL:
        print("提示: 未配置 FEISHU_WEBHOOK_URL，跳过飞书通知")
        return
    try:
        import requests
    except ImportError:
        print("缺少 requests 库，请先安装: pip install requests")
        return
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=10)
        print(f"飞书通知发送状态: {resp.status_code}")
    except Exception as e:
        print(f"飞书通知发送失败: {e}")


def send_feishu_notification(rows):
    """发送委托更新飞书通知（简要列出前20条）"""
    lines = []
    for row in rows[:20]:
        lines.append(json.dumps(row, ensure_ascii=False))
    text = f"委托数据有更新（共 {len(rows)} 条）:\n" + "\n".join(lines)
    if len(rows) > 20:
        text += f"\n... 等共 {len(rows)} 条"
    _send_feishu(text)


def _should_send_hold_notify_today():
    """判断今天是否已经发送过持仓通知，每天只发一次"""
    flag_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.hold_notify_date')
    today = datetime.date.today().isoformat()
    if os.path.exists(flag_path):
        with open(flag_path, 'r', encoding='utf-8') as f:
            last_date = f.read().strip()
        if last_date == today:
            return False
    with open(flag_path, 'w', encoding='utf-8') as f:
        f.write(today)
    return True


def send_feishu_hold_notification(rows):
    """发送持仓汇总飞书通知（每天只发一次）"""
    if not _should_send_hold_notify_today():
        print("持仓通知今天已发送过，跳过")
        return
    if not rows:
        _send_feishu("当前无持仓")
        return

    lines = ["当前持仓汇总:"]
    total_margin = 0.0
    total_profit = 0.0
    total_volume = 0

    for row in rows:
        contract = row.get("合约", row.get("合约名", ""))
        direction = row.get("买/卖", row.get("多空", ""))
        volume = row.get("手数", "0")
        margin = row.get("占用保证金", "0")
        profit = row.get("持仓盈亏", "0")

        try:
            v = int(str(volume).strip())
        except ValueError:
            v = 0
        try:
            m = float(str(margin).strip())
        except ValueError:
            m = 0.0
        try:
            p = float(str(profit).strip())
        except ValueError:
            p = 0.0

        total_volume += v
        total_margin += m
        total_profit += p

        lines.append(f"{contract} {direction} {v}手  保证金{m:.2f}  盈亏{p:+.2f}")

    lines.append(f"\n汇总: 共{len(rows)}个合约  {total_volume}手  保证金{total_margin:.2f}  盈亏{total_profit:+.2f}")
    _send_feishu("\n".join(lines))


def generate_hold_std():
    """从最新的持仓明细 CSV 生成 hold-std.json（只保留最新一次）"""
    # 兼容不同命名习惯：如 "jm0310当前持仓 *.csv" 或 "jm0310 持仓明细 *.csv"
    patterns = [
        os.path.join(DATA_DIR, f"{ACCOUNT}*持仓*.csv"),
        os.path.join(DATA_DIR, f"{ACCOUNT} 持仓明细 *.csv"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    if not files:
        print(f"未找到 '{ACCOUNT}' 持仓明细文件")
        return False

    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    latest_file = files[0]
    print(f"最新持仓文件: {os.path.basename(latest_file)}")

    rows = read_csv_as_dicts(latest_file)
    if rows is None:
        print("持仓文件读取失败")
        return False

    hold_std_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold-std.json')
    with open(hold_std_path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"持仓标准文件已写入: {hold_std_path}（共 {len(rows)} 条）")

    return True


def main():
    # 先生成持仓标准文件
    generate_hold_std()
    print("=" * 60)

    result = find_latest_two_files()
    if result is None:
        print(f"在 {DATA_DIR} 中找不到至少两个 '{PREFIX}' CSV 文件")
        return False

    latest_file, prev_file = result
    print(f"最近文件: {os.path.basename(latest_file)}")
    print(f"对比文件: {os.path.basename(prev_file)}")
    print("-" * 60)

    latest_rows = read_csv_as_dicts(latest_file)
    prev_rows = read_csv_as_dicts(prev_file)

    if not latest_rows or not prev_rows:
        print("文件为空或读取失败")
        return False

    # 检测字段
    time_key, id_key = detect_keys(latest_rows)
    if not time_key or not id_key:
        print(f"未能自动检测字段，请检查 CSV 表头。time_key={time_key}, id_key={id_key}")
        return False

    print(f"排序字段: '{time_key}',  对比字段: '{id_key}'")
    print("-" * 60)

    # 按报单时间排序（兼容夜盘跨天：夜盘在前、日盘在后）
    latest_rows.sort(key=lambda r: _time_sort_key(r, time_key))
    prev_rows.sort(key=lambda r: _time_sort_key(r, time_key))

    # 找出新增记录：以报单编号为键，最新文件里有但旧文件里没有的
    prev_ids = {row.get(id_key) for row in prev_rows}
    new_rows = [row for row in latest_rows if row.get(id_key) not in prev_ids]

    if not new_rows:
        print("结果: 委托数据无变化（无新增委托）")
        return False

    # 有变化：只输出新增的委托数据
    print(f"结果: 委托数据有变化（新增 {len(new_rows)} 条）")
    print("\n新增委托数据:")
    for row in new_rows:
        print(json.dumps(row, ensure_ascii=False))

    # 写入信号文件（只保留新增委托）
    signal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'signal.json')
    with open(signal_path, 'w', encoding='utf-8') as f:
        json.dump(new_rows, f, ensure_ascii=False, indent=2)
    print(f"信号已写入: {signal_path}")

    # 发送飞书通知（只通知新增委托）
    send_feishu_notification(new_rows)
    return True


if __name__ == "__main__":
    main()
