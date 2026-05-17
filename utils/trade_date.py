import akshare as ak
import json
from datetime import datetime, timedelta

# 获取所有交易日
df = ak.tool_trade_date_hist_sina()

# 转换为日期字符串列表
all_dates = df['trade_date'].tolist()
print(df.tail(10))  # 打印最后10个日期以验证
# 计算两年前的日期
two_years_ago = (datetime.now() - timedelta(days=1500)).date()
two_years_ago_str = two_years_ago.strftime('%Y-%m-%d')

# 过滤出近两年的交易日
recent_dates = []
for d in all_dates:
    if isinstance(d, (datetime,)):
        date_str = d.strftime('%Y-%m-%d')
        date_obj = d.date() if hasattr(d, 'date') else d
    else:
        date_str = str(d)
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date() if '-' in date_str else None

    if date_obj and date_obj >= two_years_ago:
        recent_dates.append(date_str)

# 按日期排序（从早到晚）
recent_dates.sort()

# 保存到JSON文件
with open('../config/trade_date.json', 'w', encoding='utf-8') as f:
    json.dump(recent_dates, f, ensure_ascii=False, indent=2)

print(f"已保存 {len(recent_dates)} 个交易日到 trade_date.json")
print(f"日期范围: {recent_dates[0]} ~ {recent_dates[-1]}")