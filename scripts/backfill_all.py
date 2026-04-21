#!/usr/bin/env python3
"""
统一补采脚本 - 采集最近2年的所有数据
支持断点续传，只采集缺失的数据
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    print("=" * 60)
    print("统一数据补采（最近2年）")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 时间范围：最近2年
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # 2年
    
    print(f"时间范围: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print()
    
    results = {}
    
    # 1. 北向资金
    print("\n[1/8] 采集北向资金...")
    try:
        from collectors.north_money_collector import NorthMoneyCollector
        c = NorthMoneyCollector()
        df = c.collect(start_date, end_date)
        results['north_money'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['north_money'] = 0
    
    # 2. 融资融券
    print("\n[2/8] 采集融资融券...")
    try:
        from collectors.margin_collector import MarginCollector
        c = MarginCollector()
        df = c.collect_by_date(start_date, end_date)
        results['margin'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['margin'] = 0
    
    # 3. 龙虎榜
    print("\n[3/8] 采集龙虎榜...")
    try:
        from collectors.top_list_collector import TopListCollector
        c = TopListCollector()
        df = c.collect(start_date, end_date)
        results['top_list'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['top_list'] = 0
    
    # 4. 财务指标（最近2年报告期）
    print("\n[4/8] 采集财务指标...")
    try:
        from collectors.financial_collector import FinancialCollector
        c = FinancialCollector()
        # 采集最近2年报告期，前200只股票
        df = c.collect_by_period(years=2, max_stocks=200)
        results['financial'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['financial'] = 0
    
    # 5. 每日估值（需要先重置）
    print("\n[5/8] 采集每日估值（2年）...")
    try:
        from collectors.daily_basic_collector import DailyBasicCollector
        c = DailyBasicCollector()
        # 重置进度，重新采集
        c.reset_progress()
        df = c.collect_by_date(start_date, end_date, max_stocks=100)
        results['daily_basic'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['daily_basic'] = 0
    
    # 6. 新闻舆情
    print("\n[6/8] 采集新闻舆情（2年）...")
    try:
        from collectors.news_collector import NewsCollector
        c = NewsCollector()
        df = c.collect_by_date(start_date, end_date)
        results['news'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['news'] = 0
    
    # 7. 券商研报
    print("\n[7/8] 采集券商研报（2年）...")
    try:
        from collectors.research_collector import ResearchCollector
        c = ResearchCollector()
        df = c.collect(start_date, end_date)
        results['research'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['research'] = 0
    
    # 8. 个股资金流向（需要Tushare积分，耗时最长，放最后）
    print("\n[8/8] 采集个股资金流向（2年，仅前100只测试）...")
    try:
        from collectors.moneyflow_collector import MoneyflowCollector
        c = MoneyflowCollector()
        df = c.collect_by_date(start_date, end_date, max_stocks=100)
        results['moneyflow'] = len(df) if df is not None else 0
    except Exception as e:
        print(f"❌ 失败: {e}")
        results['moneyflow'] = 0
    
    # 汇总
    print("\n" + "=" * 60)
    print("补采完成汇总")
    print("=" * 60)
    for name, count in results.items():
        print(f"  {name}: {count} 条")
    
    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == '__main__':
    main()
