# 期货日报功能说明

## 功能概述

每天自动生成期货日报，包含：
- **板块行情与情绪**：基于 K线数据库计算主要板块（黑色/有色/贵金属/能源化工/农产品/金融）的涨跌幅与情绪标签
- **同花顺新闻摘要**：抓取同花顺商品期货频道新闻
- **研报要点**：抓取同花顺期货研报（当前数据源更新较慢，建议后续接入更稳定的研报源）
- **AI 总结**：调用大模型生成自然语言日报（需配置 LLM_API_KEY）

输出渠道：
- Web 页面：`http://localhost:5000/daily_news`
- API：`http://localhost:5000/api/daily_news/latest`
- 飞书机器人通知

## 文件结构

```
low-low-up/
├── config/sector_config.json           # 板块分类、模型参数、路径、阈值
├── news_collector/
│   ├── __init__.py
│   ├── config.py                       # 配置加载
│   ├── ths_collector.py                # 同花顺新闻/研报采集
│   ├── sector_analyzer.py              # 板块行情统计
│   ├── llm_summarizer.py               # LLM 摘要生成
│   ├── report_generator.py             # 编排、输出、飞书通知
│   └── daily_news_runner.py            # CLI 入口
└── scripts/crontab_daily_news          # crontab 示例

trades-analysis-web/
├── static/daily_news.html              # 日报展示页
├── static/news/                        # 日报 JSON 输出目录（运行时创建）
└── app.py                              # 新增 /daily_news、/api/daily_news/* 路由
```

## 快速开始

### 1. 安装依赖

```bash
/home/ubuntu/miniconda3/envs/python310/bin/pip install openai requests beautifulsoup4
# akshare 为可选降级源
/home/ubuntu/miniconda3/envs/python310/bin/pip install akshare
```

### 2. 配置环境变量

```bash
export LLM_API_KEY="你的大模型 API Key"
export FEISHU_WEBHOOK="你的飞书机器人 Webhook 地址"
```

建议写入 `~/.bashrc` 或启动脚本中。

支持的 LLM：任何 OpenAI 兼容接口，如 DeepSeek、Qwen、OpenAI 等。默认配置为 DeepSeek：
```json
{
  "llm": {
    "base_url": "https://api.deepseek.com/v1",
    "model": "deepseek-chat"
  }
}
```

### 3. 手动生成测试

```bash
cd /home/ubuntu/low-low-up
# 测试模式：不写入文件、不发通知
/home/ubuntu/miniconda3/envs/python310/bin/python -m news_collector.daily_news_runner --session day --date 2026-07-31 --force --test

# 正式生成一次
/home/ubuntu/miniconda3/envs/python310/bin/python -m news_collector.daily_news_runner --session day --date 2026-07-31 --force
```

### 4. 配置定时任务

```bash
crontab /home/ubuntu/low-low-up/scripts/crontab_daily_news
```

### 5. 访问 Web 页面

确保 Flask 服务已启动：
```bash
cd /home/ubuntu/web/trades-analysis-web
/home/ubuntu/miniconda3/envs/python310/bin/python app.py
```

打开：
- 页面：`http://localhost:5000/daily_news`
- API：`http://localhost:5000/api/daily_news/latest`
- 历史列表：`http://localhost:5000/api/daily_news/list`

## 配置说明

`config/sector_config.json` 主要配置项：

| 配置项 | 说明 |
|---|---|
| `llm.base_url` | 大模型 API 基础地址 |
| `llm.model` | 模型名称 |
| `sectors` | 板块分类及产品代码 |
| `sentiment_thresholds` | 情绪判定阈值 |
| `news.ths.news_url` | 同花顺商品期货新闻页 |
| `news.ths.report_url` | 同花顺期货研报页 |
| `news.report_days_lookback` | 研报保留天数（同花顺源更新较慢，建议设大） |
| `output.web_static_dir` | Web 项目静态目录 |
| `output.archive_dir` | 本地归档目录 |

## 已知问题与注意事项

1. **板块数据不完整**：当前 `data/contracts/main_contracts.json` 主力合约更新不及时，导致部分品种（贵金属、农产品、金融等）无法匹配到有效 K 线。需要定期更新主力合约或补充 K 线数据。
2. **同花顺反爬**：若同花顺页面抓取失败，系统会尝试 AKShare 降级。如仍失败，新闻/研报字段将为空，日报仍会用板块统计生成。
3. **研报数据源**：同花顺期货研报页当前更新较慢，返回的多为历史研报。建议后续接入期货公司研报 API 或付费数据服务。
4. **跨项目写权限**：生成器需要写入 `trades-analysis-web/static/news/`，请确保运行用户有写权限。

## 测试命令

```bash
# 测试同花顺新闻采集
/home/ubuntu/miniconda3/envs/python310/bin/python -m news_collector.ths_collector --type news

# 测试板块统计
/home/ubuntu/miniconda3/envs/python310/bin/python -m news_collector.sector_analyzer --date 2026-07-31 --session day

# 端到端测试（测试模式）
/home/ubuntu/miniconda3/envs/python310/bin/python -m news_collector.daily_news_runner --session day --date 2026-07-31 --force --test
```
