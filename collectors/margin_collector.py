"""
融资融券采集模块 - collectors/margin_collector.py
采集融资融券（两融）每日数据
只使用Tushare数据源，支持断点续传、按日期范围批量采集
"""

import os
import sys
import time
import json
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger, get_tushare_pro

logger = get_logger(__name__)

# 进度文件目录
PROGRESS_DIR = 'data/collect_progress'
os.makedirs(PROGRESS_DIR, exist_ok=True)

# 输出文件
OUTPUT_FILE = 'data/margin_all.csv'


class MarginCollector:
    """融资融券采集器（仅Tushare）"""
    
    def __init__(self):
        self.pro = None
        self._init_tushare()
    
    def _init_tushare(self):
        """初始化Tushare连接"""
        try:
            self.pro = get_tushare_pro()
            if self.pro:
                print("✅ Tushare连接成功")
            else:
                print("❌ Tushare连接失败，请检查Token配置")
        except Exception as e:
            print(f"❌ Tushare初始化失败: {e}")
            self.pro = None
    
    def _get_progress_file(self):
        """获取进度文件路径"""
        return os.path.join(PROGRESS_DIR, 'margin_progress.json')
    
    def _load_progress(self):
        """加载采集进度"""
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            'completed_dates': [],
            'last_date': None,
            'last_update': None
        }
    
    def _save_progress(self, progress):
        """保存采集进度"""
        progress['last_update'] = datetime.now().isoformat()
        progress_file = self._get_progress_file()
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def _is_date_completed(self, date_str, progress):
        """检查日期是否已采集"""
        return date_str in progress.get('completed_dates', [])
    
    def _mark_date_completed(self, date_str, progress):
        """标记日期采集完成"""
        if date_str not in progress.get('completed_dates', []):
            progress.setdefault('completed_dates', []).append(date_str)
            progress['last_date'] = date_str
            self._save_progress(progress)
    
    def load_existing_data(self):
        """加载已采集的数据"""
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            print(f"📁 已有数据: {len(df)} 条")
            return df
        return None

    def _get_output_row_count(self):
        """快速获取输出文件行数（不加载整表）。"""
        if not os.path.exists(OUTPUT_FILE):
            return 0
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                return max(sum(1 for _ in f) - 1, 0)
        except Exception:
            return 0

    def _append_to_output(self, df):
        """增量写入输出文件，避免频繁全量重写。"""
        if df is None or len(df) == 0:
            return 0

        write_header = not os.path.exists(OUTPUT_FILE)
        df.to_csv(
            OUTPUT_FILE,
            mode='a' if not write_header else 'w',
            index=False,
            header=write_header,
        )
        return len(df)
    
    def get_trading_dates(self, start_date, end_date):
        """获取交易日列表"""
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')
        
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        date_list = []
        current = start_dt
        while current <= end_dt:
            if current.weekday() < 5:
                date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        return date_list
    
    def collect_by_date(self, start_date, end_date, resume=True):
        """
        按日期维度采集融资融券数据（逐日采集市场汇总数据）
        Args:
            start_date: 开始日期 (YYYYMMDD 或 date对象)
            end_date: 结束日期
            resume: 是否断点续传
        Returns:
            DataFrame: 融资融券数据
        """
        print("\n" + "=" * 60)
        print("融资融券采集（按日期维度）")
        print(f"时间范围: {start_date} ~ {end_date}")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接，无法采集")
            return None
        
        # 格式化日期
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')
        
        # 获取交易日列表
        date_list = self.get_trading_dates(start_date, end_date)
        print(f"交易日数量: {len(date_list)}")
        
        # 加载进度
        progress = self._load_progress() if resume else {'completed_dates': []}
        completed_dates = set(progress.get('completed_dates', []))
        
        # 过滤已采集的日期
        pending_dates = [d for d in date_list if d not in completed_dates]
        
        if not pending_dates:
            print("✅ 所有日期已采集完成")
            existing_df = self.load_existing_data()
            return existing_df
        
        print(f"待采集日期: {len(pending_dates)} 天")
        
        existing_rows = self._get_output_row_count()
        if existing_rows > 0:
            print(f"📁 已有数据: {existing_rows} 条")

        total_rows = existing_rows
        new_count = 0
        
        for idx, trade_date in enumerate(pending_dates):
            day_start = time.perf_counter()
            print(f"[{idx+1}/{len(pending_dates)}] 采集 {trade_date}...", end=' ', flush=True)
            
            try:
                # 采集融资融券日数据
                df = self.pro.margin(trade_date=trade_date)
                
                if df is not None and len(df) > 0:
                    appended = self._append_to_output(df)
                    new_count += appended
                    total_rows += appended
                    print(f"✅ {appended}条")
                else:
                    print(f"⚠️ 无数据")
                
                # 标记完成
                self._mark_date_completed(trade_date, progress)

                elapsed = time.perf_counter() - day_start
                logger.info(
                    f"margin day={trade_date} rows={0 if df is None else len(df)} elapsed={elapsed:.2f}s"
                )
                print(f"   ⏱️ {elapsed:.2f}s, 累计 {total_rows} 条")
                
            except Exception as e:
                print(f"❌ {str(e)[:40]}")
                logger.error(f"margin day={trade_date} failed: {e}")
            
            time.sleep(0.3)
        
        # 最终返回
        result = self.load_existing_data()
        if result is not None and len(result) > 0:
            print(f"\n" + "=" * 60)
            print(f"✅ 融资融券采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {total_rows} 条")
            print(f"   保存至: {OUTPUT_FILE}")
            print("=" * 60)
            return result

        print("❌ 无数据")
        return None
    
    def collect_by_stock(self, start_date, end_date, max_stocks=None, resume=True):
        """
        按股票维度采集融资融券数据（逐只股票采集）
        Args:
            start_date: 开始日期
            end_date: 结束日期
            max_stocks: 最大股票数量
            resume: 是否断点续传
        Returns:
            DataFrame: 融资融券数据
        """
        print("\n" + "=" * 60)
        print("融资融券采集（按股票维度）")
        print(f"时间范围: {start_date} ~ {end_date}")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接")
            return None
        
        # 格式化日期
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')
        
        # 获取股票列表
        stocks = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        if stocks is None or len(stocks) == 0:
            print("❌ 获取股票列表失败")
            return None
        
        if max_stocks:
            stocks = stocks.head(max_stocks)
        
        print(f"股票数量: {len(stocks)}")
        
        # 进度文件（按股票）
        progress_file = os.path.join(PROGRESS_DIR, 'margin_stock_progress.json')
        if resume and os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
        else:
            progress = {'completed_codes': []}
        
        completed_codes = set(progress.get('completed_codes', []))
        
        existing_rows = self._get_output_row_count()
        if existing_rows > 0:
            print(f"📁 已有数据: {existing_rows} 条")

        total_rows = existing_rows
        new_count = 0
        
        for idx, (_, stock) in enumerate(stocks.iterrows()):
            ts_code = stock['ts_code']
            name = stock['name']
            
            if ts_code in completed_codes:
                print(f"[{idx+1}/{len(stocks)}] {ts_code} {name} - 已采集，跳过")
                continue
            
            print(f"[{idx+1}/{len(stocks)}] 采集 {ts_code} {name}...", end=' ', flush=True)
            
            try:
                df = self.pro.margin(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df is not None and len(df) > 0:
                    appended = self._append_to_output(df)
                    new_count += appended
                    total_rows += appended
                    print(f"✅ {appended}条")
                else:
                    print(f"⚠️ 无数据")
                
                # 标记完成
                completed_codes.add(ts_code)
                progress['completed_codes'] = list(completed_codes)
                with open(progress_file, 'w') as f:
                    json.dump(progress, f, indent=2)
                
            except Exception as e:
                print(f"❌ {str(e)[:40]}")

            print(f"   💾 累计 {total_rows} 条")
            
            time.sleep(0.3)
        
        # 最终返回
        result = self.load_existing_data()
        if result is not None and len(result) > 0:
            print(f"\n✅ 融资融券采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {total_rows} 条")
            return result

        print("❌ 无数据")
        return None
    
    def collect_latest(self, days=30):
        """
        采集最近N天的融资融券数据
        Args:
            days: 天数
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return self.collect_by_date(start_date, end_date)
    
    def show_progress(self):
        """显示采集进度"""
        progress = self._load_progress()
        print(f"\n📊 融资融券采集进度:")
        print(f"   已完成日期: {len(progress.get('completed_dates', []))} 天")
        print(f"   最后日期: {progress.get('last_date', '无')}")
        print(f"   最后更新: {progress.get('last_update', '从未')}")
        
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            print(f"   数据文件记录数: {len(df)} 条")
    
    def reset_progress(self):
        """重置采集进度"""
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("✅ 日期进度已重置")
        
        stock_progress_file = os.path.join(PROGRESS_DIR, 'margin_stock_progress.json')
        if os.path.exists(stock_progress_file):
            os.remove(stock_progress_file)
            print("✅ 股票进度已重置")


if __name__ == '__main__':
    collector = MarginCollector()
    
    # 显示进度
    collector.show_progress()
    
    # 采集最近30天
    collector.collect_latest(days=30)
    
    collector.show_progress()