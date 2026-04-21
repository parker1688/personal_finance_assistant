#!/usr/bin/env python3
"""
一键补采全部A股最近1年数据
包括：行情、每日估值、资金流向、北向资金、融资融券、龙虎榜、新闻、研报
"""

import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("=" * 60)
print("一键补采全部A股最近1年数据")
print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# 日期范围
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
print(f"时间范围: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

# ==================== 1. 获取全部A股列表 ====================
print("\n[1/8] 获取全部A股列表...")
try:
    import tushare as ts
    from config import TUSHARE_TOKEN
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
    all_codes = stocks['ts_code'].tolist()
    print(f"✅ 获取到 {len(all_codes)} 只A股")
    
    # 保存标准股票池主文件
    stocks.to_csv('data/stock_basic.csv', index=False)
except Exception as e:
    print(f"❌ 获取股票列表失败: {e}")
    all_codes = []

# ==================== 2. 补采行情数据 ====================
print("\n[2/8] 补采行情数据（最近1年）...")
if all_codes:
    try:
        from collectors.stock_collector import StockCollector
        c = StockCollector()
        c.a_stock_pool = all_codes
        c.collect_batch(all_codes, market='A', years=1, delay=0.2)
        print("✅ 行情数据补采完成")
    except Exception as e:
        print(f"❌ 行情数据补采失败: {e}")

# ==================== 3. 补采每日估值 ====================
print("\n[3/8] 补采每日估值（最近1年）...")
try:
    from collectors.daily_basic_collector import DailyBasicCollector
    c = DailyBasicCollector()
    c.reset_progress()
    c.collect_all(days=250, max_stocks=None)
    print("✅ 每日估值补采完成")
except Exception as e:
    print(f"❌ 每日估值补采失败: {e}")

# ==================== 4. 补采资金流向 ====================
print("\n[4/8] 补采资金流向（最近1年）...")
try:
    from collectors.moneyflow_collector import MoneyflowCollector
    c = MoneyflowCollector()
    c.collect_by_date(start_date, end_date, max_stocks=None)
    print("✅ 资金流向补采完成")
except Exception as e:
    print(f"❌ 资金流向补采失败: {e}")

# ==================== 5. 补采北向资金 ====================
print("\n[5/8] 补采北向资金（最近1年）...")
try:
    from collectors.north_money_collector import NorthMoneyCollector
    c = NorthMoneyCollector()
    c.collect(start_date, end_date)
    print("✅ 北向资金补采完成")
except Exception as e:
    print(f"❌ 北向资金补采失败: {e}")

# ==================== 6. 补采融资融券 ====================
print("\n[6/8] 补采融资融券（最近1年）...")
try:
    from collectors.margin_collector import MarginCollector
    c = MarginCollector()
    c.collect_by_date(start_date, end_date)
    print("✅ 融资融券补采完成")
except Exception as e:
    print(f"❌ 融资融券补采失败: {e}")

# ==================== 7. 补采龙虎榜 ====================
print("\n[7/8] 补采龙虎榜（最近1年）...")
try:
    from collectors.top_list_collector import TopListCollector
    c = TopListCollector()
    c.collect(start_date, end_date)
    print("✅ 龙虎榜补采完成")
except Exception as e:
    print(f"❌ 龙虎榜补采失败: {e}")

# ==================== 8. 补采新闻和研报 ====================
print("\n[8/8] 补采新闻和研报...")
try:
    from collectors.news_collector import NewsCollector
    c = NewsCollector()
    c.collect_historical(days=30)
    print("✅ 新闻补采完成")
except Exception as e:
    print(f"❌ 新闻补采失败: {e}")

try:
    from collectors.research_collector import ResearchCollector
    c = ResearchCollector()
    c.collect_latest(days=5)
    print("✅ 研报补采完成")
except Exception as e:
    print(f"❌ 研报补采失败: {e}")

print("\n" + "=" * 60)
print(f"补采完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)
