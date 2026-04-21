"""
券商研报采集模块 - collectors/research_collector.py
"""

import os
import sys
import time
import json
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import tushare as ts
    from config import TUSHARE_TOKEN
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    print("✅ Tushare连接成功")
except Exception as e:
    print(f"❌ Tushare连接失败: {e}")
    pro = None

PROGRESS_DIR = 'data/collect_progress'
os.makedirs(PROGRESS_DIR, exist_ok=True)

OUTPUT_FILE = 'data/research_report.csv'


class ResearchCollector:
    """券商研报采集器"""
    
    def __init__(self):
        self.pro = pro
        self.request_count_today = 0
        self.last_reset_date = datetime.now().date()
        self.last_request_time = None
    
    def _check_rate_limit(self):
        """检查限流（每天最多5次）"""
        now = datetime.now()
        today = now.date()
        
        if today != self.last_reset_date:
            self.request_count_today = 0
            self.last_reset_date = today
            print(f"\n📅 新的一天，重置计数")
        
        if self.request_count_today >= 5:
            wait_seconds = 24 * 3600
            next_time = now + timedelta(seconds=wait_seconds)
            print(f"\n⏰ 已达每日限制(5次/天)，明天 {next_time.strftime('%Y-%m-%d')} 继续")
            return wait_seconds
        
        if self.last_request_time:
            seconds_since_last = (now - self.last_request_time).total_seconds()
            if seconds_since_last < 12:
                time.sleep(12 - seconds_since_last)
        
        return 0
    
    def _wait_and_increment(self):
        wait_time = self._check_rate_limit()
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.request_count_today += 1
        self.last_request_time = datetime.now()
    
    def _get_progress_file(self):
        return os.path.join(PROGRESS_DIR, 'research_progress.json')
    
    def _load_progress(self):
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'completed_dates': [], 'last_update': None}
    
    def _save_progress(self, progress):
        progress['last_update'] = datetime.now().isoformat()
        with open(self._get_progress_file(), 'w') as f:
            json.dump(progress, f, indent=2)
    
    def get_date_list(self, start_date, end_date):
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        date_list = []
        current = start_dt
        while current <= end_dt:
            date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        return date_list
    
    def collect(self, start_date, end_date, resume=True):
        """采集研报数据"""
        print("\n" + "=" * 60)
        print("券商研报采集")
        print(f"时间范围: {start_date} ~ {end_date}")
        print("⚠️ 限制: 每天最多5次接口调用（当前实现通常≈最多补5个日期）")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接")
            return None
        
        start_date = start_date.strftime('%Y%m%d') if hasattr(start_date, 'strftime') else start_date
        end_date = end_date.strftime('%Y%m%d') if hasattr(end_date, 'strftime') else end_date
        
        date_list = self.get_date_list(start_date, end_date)
        print(f"总日期数: {len(date_list)}")
        
        progress = self._load_progress() if resume else {'completed_dates': []}
        completed_dates = set(progress.get('completed_dates', []))
        
        # 优先补最近缺口，先让最新训练/推荐能用上今年数据。
        pending_dates = sorted([d for d in date_list if d not in completed_dates], reverse=True)
        
        if not pending_dates:
            print("✅ 所有日期已采集完成")
            return None
        
        print(f"待采集: {len(pending_dates)} 天")
        
        existing_df = None
        if os.path.exists(OUTPUT_FILE):
            try:
                existing_df = pd.read_csv(OUTPUT_FILE)
            except Exception:
                existing_df = None

        all_data = []
        new_count = 0
        
        for idx, publish_date in enumerate(pending_dates):
            if self.request_count_today >= 5:
                print(f"\n📅 今日已达5次限制，剩余 {len(pending_dates)-idx} 天明天继续")
                break
            
            print(f"[{idx+1}/{len(pending_dates)}] 采集 {publish_date}...", end=' ', flush=True)
            
            try:
                self._wait_and_increment()
                df = self.pro.research_report(publish_date=publish_date)
                
                if df is not None and len(df) > 0:
                    # 统一列名：将 trade_date 改为 publish_date
                    if 'trade_date' in df.columns and 'publish_date' not in df.columns:
                        df = df.rename(columns={'trade_date': 'publish_date'})
                    
                    all_data.append(df)
                    new_count += len(df)
                    print(f"✅ {len(df)}条")
                else:
                    print(f"⚠️ 无数据")
                
                completed_dates.add(publish_date)
                progress['completed_dates'] = list(completed_dates)
                self._save_progress(progress)
                
            except Exception as e:
                print(f"❌ {str(e)[:50]}")
            
            time.sleep(1)
        
        if all_data:
            parts = []
            if existing_df is not None and not existing_df.empty:
                parts.append(existing_df)
            parts.extend(all_data)
            result = pd.concat(parts, ignore_index=True)

            if 'trade_date' in result.columns and 'publish_date' not in result.columns:
                result = result.rename(columns={'trade_date': 'publish_date'})

            dedup_key = 'url' if 'url' in result.columns else ('title' if 'title' in result.columns else None)
            if dedup_key is not None:
                result = result.drop_duplicates(subset=[dedup_key], keep='last')

            result.to_csv(OUTPUT_FILE, index=False)
            print(f"\n✅ 研报采集完成，新增 {new_count} 条")
            print(f"   累计: {len(result)} 条")
            print(f"   数据列: {list(result.columns)}")
            return result
        
        return None
    
    def collect_latest(self, days=5):
        """采集最近N天（每天最多5天）"""
        days = min(days, 5)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return self.collect(start_date, end_date)


if __name__ == '__main__':
    collector = ResearchCollector()
    collector.collect_latest(days=5)