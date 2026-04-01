# 环境配置说明

## 概述

系统现在支持自动根据运行环境选择合适的 CTP 配置，无需手动修改代码。

## 配置优先级

系统按照以下优先级选择环境配置：

1. **命令行参数**（最高优先级）
2. **环境变量 `CTP_ENV`**
3. **操作系统自动判断**
4. **默认配置**（7x24）

## 使用方式

### 方式一：命令行参数（最简单，推荐）

直接在命令行中指定环境参数：

```bash
# Windows - 使用线上环境
python ArbitrageTrading.py online

# Windows - 使用开发环境（默认）
python ArbitrageTrading.py

# Linux - 使用开发环境
python ArbitrageTrading.py 7x24

# Linux - 使用线上环境（默认）
python ArbitrageTrading.py
```

### 方式二：使用环境变量

#### Windows（开发环境）

```powershell
# 设置环境变量为开发环境
$env:CTP_ENV="7x24"

# 运行程序
python ArbitrageTrading.py
```

或者在代码中设置：

```python
import os
os.environ["CTP_ENV"] = "7x24"
```

#### Linux（线上环境）

```bash
# 设置环境变量为线上环境
export CTP_ENV="online"

# 运行程序
python ArbitrageTrading.py
```

或者在代码中设置：

```python
import os
os.environ["CTP_ENV"] = "online"
```

### 方式二：自动判断（无需配置）

系统会根据操作系统自动选择环境：

- **Windows 系统** → 自动使用 `7x24` 环境
- **Linux 系统** → 自动使用 `online` 环境

### 方式三：手动指定配置

如果需要手动指定配置，可以在初始化时传入：

```python
import config

# 使用线上环境
td_spi = CTdSpiBase(conf=config.envs["online"])

# 使用开发环境
md_spi = CMdSpiBase(conf=config.envs["7x24"])

# 使用仿真环境
td_spi = CTdSpiBase(conf=config.envs["simu"])
```

## 可用环境

| 环境名称 | 说明 | 适用场景 |
|---------|------|---------|
| `7x24` | 7x24 测试环境 | Windows 开发环境 |
| `online` | 线上环境 | Linux 生产环境 |
| `simu` | 仿真环境 | SimNow 仿真测试 |
| `simu-vip` | 仿真 VIP 环境 | SimNow VIP 仿真测试 |

## 配置文件

所有配置定义在 `config.py` 文件中：

```python
envs = {
    "7x24": {
        "td": "tcp://724.openctp.cn:30001",
        "md": "tcp://724.openctp.cn:30011",
        "user_id": "17156",
        "password": "123456",
        "broker_id": "9999",
        "authcode": "",
        "appid": "",
        "user_product_info": "",
    },
    "online": {
        "td": "tcp://116.62.52.86:11001",
        "md": "tcp://101.226.253.150:41213",
        "user_id": "yqj0929",
        "password": "041354",
        "broker_id": "0268",
        "authcode": "zTvCzT7YVtrJwdvs",
        "appid": "client_yqj_v1.0",
        "user_product_info": "",
    },
    # ... 其他环境配置
}
```

## 代码示例

### 示例 1：自动选择环境（推荐）

```python
from base_tdapi import CTdSpiBase
from base_mdapi import CMdSpiBase

# 不传入配置，系统自动选择
td_spi = CTdSpiBase()  # Windows -> 7x24, Linux -> online
md_spi = CMdSpiBase()  # Windows -> 7x24, Linux -> online
```

### 示例 2：使用环境变量

```python
import os
from base_tdapi import CTdSpiBase

# 设置环境变量
os.environ["CTP_ENV"] = "online"

# 系统会自动使用 online 配置
td_spi = CTdSpiBase()
```

### 示例 3：手动指定配置

```python
import config
from base_tdapi import CTdSpiBase

# 手动指定使用线上环境
td_spi = CTdSpiBase(conf=config.envs["online"])
```

## 部署建议

### Windows 开发环境

#### 方式一：使用默认配置（推荐）

无需任何配置，直接运行即可：

```bash
python ArbitrageTrading.py
```

系统会自动使用 `7x24` 环境。

#### 方式二：使用命令行参数

如果需要使用线上环境：

```bash
python ArbitrageTrading.py online
```

### Linux 生产环境

#### 方式一：使用默认配置（推荐）

直接运行即可，无需任何配置：

```bash
python ArbitrageTrading.py
```

系统会自动使用 `online` 环境。

#### 方式二：使用命令行参数

如果需要使用开发环境：

```bash
python ArbitrageTrading.py 7x24
```

或者使用 systemd 服务：

```ini
[Unit]
Description=Arbitrage Trading Service
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/project
Environment="CTP_ENV=online"
ExecStart=/usr/bin/python /path/to/project/ArbitrageTrading.py
Restart=always

[Install]
WantedBy=multi-user.target
```

## 优势

1. **自动化**：无需手动修改代码，系统自动选择环境
2. **灵活性**：支持环境变量覆盖，方便测试
3. **安全性**：避免将生产环境配置提交到代码仓库
4. **可维护性**：集中管理所有环境配置
5. **兼容性**：保持向后兼容，不影响现有代码

## 注意事项

1. 确保生产环境（Linux）的配置信息正确
2. 不要将包含敏感信息（密码、authcode）的配置文件提交到代码仓库
3. 在生产环境中，建议使用环境变量或配置管理工具（如 Ansible、Kubernetes Secrets）
4. 定期检查和更新配置信息
