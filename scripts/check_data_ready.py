#!/usr/bin/env python3
"""检查数据是否满足训练要求"""

import pandas as pd
from datetime import datetime, timedelta

print("=" * 60)
print("数据完整性检查")
print("=" * 60)

# 检查各文件
files = {
    'historical_a_stock.csv': 'OHLCV行情',
    'moneyflow_all.csv': '资金流向',
    'north_money_all.csv': '北向资金',
    'margin_all.csv': '融资融券',
    'daily_basic.csv': '每日估值',
}

for file, name in files.items():
    try:
        df = pd.read_csv(f'data/{file}')
        print(f"\n✅ {name} ({file}):")
        print(f"   记录数: {len(df):,}")
        if 'trade_date' in df.columns:
            dates = df['trade_date'].unique()
            print(f"   日期数: {len(dates)}")
            print(f"   范围: {min(dates)} ~ {max(dates)}")
        elif 'date' in df.columns:
            dates = df['date'].unique()
            print(f"   日期数: {len(dates)}")
            print(f"   范围: {min(dates)} ~ {max(dates)}")
    except Exception as e:
        print(f"\n❌ {name}: 文件不存在或读取失败")

print("\n" + "=" * 60)
print("结论:")
print("  - 如果核心数据(行情+资金+估值)都有2年数据 → 可以训练")
print("  - 如果有数据缺失严重 → 需要继续补采")
print("=" * 60)
