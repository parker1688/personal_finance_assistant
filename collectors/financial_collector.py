"""
财务指标采集模块 - collectors/financial_collector.py
采集上市公司财务指标数据（EPS、ROE、毛利率等）
支持断点续传、按报告期批量采集
"""

import os
import sys
import time
import json
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 直接导入tushare，避免循环导入
try:
    import tushare as ts
    from config import TUSHARE_TOKEN
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    print("✅ Tushare连接成功")
except Exception as e:
    print(f"❌ Tushare连接失败: {e}")
    pro = None

# 进度文件目录
PROGRESS_DIR = 'data/collect_progress'
os.makedirs(PROGRESS_DIR, exist_ok=True)

# 输出文件
OUTPUT_FILE = 'data/financial_indicator.csv'


class FinancialCollector:
    """财务指标采集器"""
    
    def __init__(self):
        self.pro = pro
        # 限流控制
        self.request_count_minute = 0
        self.last_request_time = None
    
    def _wait_for_rate_limit(self):
        """控制请求频率（每分钟最多200次）"""
        now = datetime.now()
        
        if self.last_request_time:
            seconds_since_last = (now - self.last_request_time).total_seconds()
            if seconds_since_last < 0.5:  # 每秒最多2次
                time.sleep(0.5 - seconds_since_last)
        
        self.last_request_time = datetime.now()
    
    def _get_progress_file(self):
        return os.path.join(PROGRESS_DIR, 'financial_progress.json')
    
    def _load_progress(self):
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'completed_codes': [], 'last_update': None}
    
    def _save_progress(self, progress):
        progress['last_update'] = datetime.now().isoformat()
        with open(self._get_progress_file(), 'w') as f:
            json.dump(progress, f, indent=2)
    
    def get_all_stocks(self, max_stocks=None):
        """获取所有A股股票列表"""
        if self.pro is None:
            print("❌ Tushare未连接")
            return None
        
        print("正在获取股票列表...")
        stocks = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
        
        if stocks is None or len(stocks) == 0:
            print("❌ 获取股票列表失败")
            return None
        
        if max_stocks:
            stocks = stocks.head(max_stocks)
        
        print(f"✅ 获取到 {len(stocks)} 只股票")
        return stocks
    
    def load_existing_data(self):
        """加载已采集的数据"""
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            print(f"📁 已有数据: {len(df)} 条, {df['ts_code'].nunique()} 只股票")
            return df
        return None
    
    def collect_all(self, max_stocks=None, resume=True):
        """
        采集所有股票的财务指标
        """
        print("\n" + "=" * 60)
        print("财务指标采集（全部股票）")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接")
            return None
        
        # 获取股票列表
        stocks = self.get_all_stocks(max_stocks)
        if stocks is None:
            return None
        
        # 加载进度
        progress = self._load_progress() if resume else {'completed_codes': []}
        completed_codes = set(progress.get('completed_codes', []))
        
        # 加载已有数据
        existing_df = self.load_existing_data()
        all_data = [existing_df] if existing_df is not None else []
        new_count = 0
        stock_count = 0
        
        for idx, (_, stock) in enumerate(stocks.iterrows()):
            ts_code = stock['ts_code']
            name = stock['name']
            
            if ts_code in completed_codes:
                print(f"[{idx+1}/{len(stocks)}] {ts_code} {name} - 已采集，跳过")
                continue
            
            print(f"[{idx+1}/{len(stocks)}] 采集 {ts_code} {name}...", end=' ', flush=True)
            
            try:
                self._wait_for_rate_limit()
                df = self.pro.fina_indicator(ts_code=ts_code)
                
                if df is not None and len(df) > 0:
                    all_data.append(df)
                    new_count += len(df)
                    stock_count += 1
                    print(f"✅ {len(df)}条")
                else:
                    print(f"⚠️ 无数据")
                
                completed_codes.add(ts_code)
                progress['completed_codes'] = list(completed_codes)
                self._save_progress(progress)
                
            except Exception as e:
                print(f"❌ {str(e)[:50]}")
            
            # 每50只保存一次
            if (idx + 1) % 50 == 0 and all_data:
                temp_df = pd.concat(all_data, ignore_index=True)
                temp_df.to_csv(OUTPUT_FILE, index=False)
                print(f"   💾 已保存，累计 {len(temp_df)} 条, {temp_df['ts_code'].nunique()} 只股票")
        
        # 最终保存
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result.to_csv(OUTPUT_FILE, index=False)
            print(f"\n" + "=" * 60)
            print(f"✅ 财务指标采集完成")
            print(f"   新增: {new_count} 条, {stock_count} 只股票")
            print(f"   累计: {len(result)} 条, {result['ts_code'].nunique()} 只股票")
            print(f"   保存至: {OUTPUT_FILE}")
            print("=" * 60)
            return result
        else:
            print("❌ 无数据")
            return None
    
    def show_progress(self):
        """显示采集进度"""
        progress = self._load_progress()
        print(f"\n📊 财务指标采集进度:")
        print(f"   已完成股票: {len(progress.get('completed_codes', []))} 只")
        
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            print(f"   数据文件记录数: {len(df)} 条, {df['ts_code'].nunique()} 只股票")
    
    def reset_progress(self):
        """重置采集进度"""
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("✅ 进度已重置")
        else:
            print("⚠️ 无进度文件")


if __name__ == '__main__':
    collector = FinancialCollector()
    collector.show_progress()
    
    # 采集全部股票（不限数量）
    collector.collect_all()
    
    collector.show_progress()
