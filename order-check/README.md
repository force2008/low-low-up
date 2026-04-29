# order-check 委托与持仓管理流水线

## 功能概述

本目录负责从外部交易客户端（融航）自动导出数据，并与 CTP 账户进行持仓对比和委托同步。

核心流程：
```
导出 CSV（持仓明细 + 所有委托）
    ↓
生成 hold-std.json（标准持仓）
    ↓
委托对比 → 生成 signal.json → 飞书通知
    ↓
CTP 持仓对比 → 不一致则告警（不自动交易）
    ↓
执行 signal.json 中的新增委托（限价单）
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `run_pipeline.py` | **主流水线入口**。每 20 秒循环执行：导出 → 生成 hold-std → 委托对比 → 持仓对比 → 委托执行 |
| `automate_export.py` | 用 `pyautogui` 自动点击融航客户端，导出持仓明细 CSV 和所有委托 CSV |
| `compare_orders.py` | 读取委托 CSV，对比找出新增委托，写入 `signal.json`，发送飞书通知 |
| `local_config.py` | 本地配置（账号名、窗口标题、鼠标坐标、保存路径）。每台机器独立维护 |
| `hold-std.json` | 标准持仓文件，由持仓明细 CSV 生成，用于和 CTP 实际持仓对比 |
| `signal.json` | 新增委托信号文件，由 `compare_orders.py` 生成，`PositionSyncManager` 读取并执行 |
| `logs/pipeline.log` | 流水线运行日志 |

## 快速开始

### 1. 配置 local_config.py

复制 `local_config.py`（已存在），修改以下关键项：

```python
ACCOUNT = "你的账号名"           # 用于匹配导出文件名前缀
APP_TITLE = "用户: xxx"          # 融航客户端窗口标题
DEFAULT_SAVE_PATH = r"C:\ronghang\data"  # CSV 导出保存路径
```

鼠标坐标根据你当前机器的分辨率重新采集（见 `record_coordinates.py`）。

### 2. 配置 CTP 环境

默认使用 `7x24` 模拟环境。切换到线上：

```bash
python run_pipeline.py online
# 或
set CTP_ENV=online && python run_pipeline.py
```

### 3. 首次运行（建仓）

首次运行时账户为空，程序按以下流程建仓：

```
1. 导出 CSV
   ├── 持仓明细 CSV  → 有 IC2606 5手（外部系统已有持仓）
   └── 所有委托 CSV  → 空（外部系统还没发新委托）

2. generate_hold_std()
   └── 从持仓明细 CSV 生成 hold-std.json
       └── hold-std.json = [{"合约":"IC2606","买/卖":"买","手数":5}]

3. PositionSyncManager.sync_and_trade()
   ├── 查 CTP 持仓 → 空
   ├── 加载 hold-std.json → 有 IC2606 5手
   ├── 判断：账户空 && hold-std 有数据 → 走首次建仓
   └── _build_positions()
          ├── 查 IC2606 行情 → AskPrice1
          ├── 下限价单：买入开仓 5手
          ├── 等成交（30秒）
          └── 成交后重新查持仓、更新 hold-std.json
```

**关键说明：**

| 问题 | 答案 |
|------|------|
| 委托 CSV 为空会影响建仓吗？ | **不会**。首次建仓只看 `hold-std.json`（来自持仓明细 CSV），不看委托 CSV。 |
| 建仓后 hold-std.json 会变吗？ | **会**。建仓成功后，程序会重新查 CTP 持仓，把实际持仓写回 `hold-std.json`。 |
| 建仓失败怎么办？ | 30秒未成交则撤单，`_build_positions()` 返回失败，程序进入主循环，下次再尝试。 |

**如果导出的持仓 CSV 也为空**（外部系统无持仓），程序会尝试读取：
- `../data/initial_positions.json` — 填入目标合约和手数，作为首次建仓来源

示例：
```json
[
  {"合约": "IC2606", "买/卖": "买", "手数": 1},
  {"合约": "IF2606", "买/卖": "买", "手数": 1}
]
```

### 4. 后续运行（循环模式）

```bash
python run_pipeline.py
```

程序进入每 20 秒循环：
- 导出最新数据
- 对比委托，有新增则发飞书通知
- 对比 CTP 持仓与 `hold-std.json`，不一致则**飞书告警**
- 执行 `signal.json` 中的新增委托（限价单）

### 5. 停止

按 `Ctrl+C` 即可。

## 交易逻辑说明

| 场景 | 行为 |
|------|------|
| 首次运行，账户空仓 | 按 `hold-std.json` / `initial_positions.json` 买入建仓 |
| 持仓对比不一致 | 发飞书告警，**不自动交易** |
| 委托 CSV 有新增 | 生成 `signal.json`，发飞书通知，并在 CTP 上执行限价单 |
| 30秒未成交 | 自动撤单（委托执行模式下不重发） |

## 注意事项

- `automate_export.py` 依赖融航客户端窗口处于可见状态，且不被其他窗口遮挡
- 持仓对比只告警、不自动交易；所有交易必须通过委托 CSV 触发
- 7x24 模拟环境跳过交易时段检查，任意时间均可执行
- 线上环境只在配置的交易时段内执行
