"""
每日估值采集模块 - collectors/daily_basic_collector.py
采集每日估值数据（PE、PB、市值等）
支持断点续传、按日期范围批量采集
"""

import os
import sys
import time
import json
import pandas as pd
import random
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

from utils import get_logger

logger = get_logger(__name__)

PROGRESS_DIR = 'data/collect_progress'
os.makedirs(PROGRESS_DIR, exist_ok=True)

OUTPUT_FILE = 'data/daily_basic.csv'


class DailyBasicCollector:
    """每日估值采集器"""
    
    def __init__(self):
        self.pro = pro
        self.request_count_minute = 0
        self.last_request_time = None
        self.min_request_interval = 0.12
        self.max_retry_rounds = 2
        self.retry_backoff_base = 1.2
    
    def _wait_for_rate_limit(self):
        """控制请求频率"""
        now = datetime.now()
        if self.last_request_time:
            seconds_since_last = (now - self.last_request_time).total_seconds()
            if seconds_since_last < self.min_request_interval:
                time.sleep(self.min_request_interval - seconds_since_last)
        self.last_request_time = datetime.now()

    def _adaptive_sleep_on_error(self, attempt=1):
        """请求失败后的退避等待。"""
        delay = self.retry_backoff_base * attempt + random.uniform(0.1, 0.4)
        time.sleep(delay)
    
    def _get_progress_file(self):
        return os.path.join(PROGRESS_DIR, 'daily_basic_progress.json')
    
    def _load_progress(self):
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    data = json.load(f)
                    # 向后兼容旧进度格式
                    if 'completed_dates' not in data:
                        data['completed_dates'] = []
                    if 'mode' not in data:
                        data['mode'] = 'by_stock'
                    return data
            except:
                pass
        return {
            'completed_codes': [],
            'completed_dates': [],
            'mode': None,
            'last_date': None,
            'last_update': None
        }
    
    def _save_progress(self, progress):
        progress['last_update'] = datetime.now().isoformat()
        with open(self._get_progress_file(), 'w') as f:
            json.dump(progress, f, indent=2)
    
    def get_all_stocks(self, max_stocks=None):
        if self.pro is None:
            return None
        stocks = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        if max_stocks:
            stocks = stocks.head(max_stocks)
        return stocks
    
    def get_trading_dates(self, start_date, end_date):
        """获取交易日列表"""
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        date_list = []
        current = start_dt
        while current <= end_dt:
            if current.weekday() < 5:
                date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        return date_list
    
    def load_existing_data(self):
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            return df
        return None

    def _load_existing_completed_dates(self):
        """从已有CSV推断已完成交易日，避免重复请求。"""
        if not os.path.exists(OUTPUT_FILE):
            return set()

        try:
            df = pd.read_csv(OUTPUT_FILE, usecols=['trade_date'])
            if 'trade_date' not in df.columns:
                return set()
            dates = set(df['trade_date'].astype(str).str.strip().tolist())
            return {d for d in dates if len(d) == 8 and d.isdigit()}
        except Exception:
            return set()

    def _append_to_output(self, df):
        """增量写入输出文件，避免内存累积。"""
        if df is None or len(df) == 0:
            return 0

        write_header = not os.path.exists(OUTPUT_FILE)
        df.to_csv(
            OUTPUT_FILE,
            mode='a' if not write_header else 'w',
            index=False,
            header=write_header
        )
        return len(df)

    def _collect_by_date(self, start_date, end_date, max_stocks=None, resume=True):
        """按交易日采集（单日拉取全市场），速度显著高于逐股逐日模式。"""
        stocks = self.get_all_stocks(max_stocks)
        if stocks is None:
            return None

        ts_code_set = set(stocks['ts_code'].tolist()) if max_stocks else None
        date_list = self.get_trading_dates(start_date, end_date)
        print(f"交易日数量: {len(date_list)}")
        print("采集模式: 按交易日全市场拉取（加速）")

        progress = self._load_progress() if resume else {
            'completed_codes': [],
            'completed_dates': [],
            'failed_dates': [],
            'mode': 'by_date'
        }

        completed_dates = set(progress.get('completed_dates', []))
        # 若进度文件不全，从已有CSV推断已完成日期
        if resume:
            completed_dates |= self._load_existing_completed_dates()
            progress['completed_dates'] = sorted(list(completed_dates))

        failed_dates = []
        new_rows = 0
        completed_days = 0

        # 若已存在旧文件且不是新任务，避免重复写入已完成日期
        if not resume and os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

        for idx, trade_date in enumerate(date_list):
            if trade_date in completed_dates:
                print(f"[{idx+1}/{len(date_list)}] {trade_date} - 已采集，跳过")
                continue

            print(f"[{idx+1}/{len(date_list)}] 采集交易日 {trade_date} ...", flush=True)
            day_start = time.perf_counter()
            try:
                self._wait_for_rate_limit()
                df = self.pro.daily_basic(trade_date=trade_date)

                if df is not None and len(df) > 0:
                    if ts_code_set is not None:
                        df = df[df['ts_code'].isin(ts_code_set)]

                    if len(df) > 0:
                        appended = self._append_to_output(df)
                        new_rows += appended
                        print(f"   ✅ {appended}条")
                    else:
                        print("   ⚠️ 过滤后无数据")
                else:
                    print("   ⚠️ 无数据")

                completed_days += 1
                completed_dates.add(trade_date)
                progress['completed_dates'] = sorted(list(completed_dates))
                progress['failed_dates'] = []
                progress['mode'] = 'by_date'
                progress['last_date'] = trade_date
                self._save_progress(progress)

                elapsed = time.perf_counter() - day_start
                logger.info(
                    f"daily_basic day={trade_date} rows={0 if df is None else len(df)} elapsed={elapsed:.2f}s"
                )
                print(f"   ⏱️ {elapsed:.2f}s")

            except Exception as e:
                print(f"   ❌ 失败: {e}")
                logger.error(f"daily_basic day={trade_date} failed: {e}")
                failed_dates.append(trade_date)

        # 失败日期重试（仅重试失败日期，不重跑全部）
        for round_idx in range(1, self.max_retry_rounds + 1):
            if not failed_dates:
                break

            print(f"\n🔁 第{round_idx}轮失败日期重试: {len(failed_dates)} 天")
            retry_remaining = []
            for trade_date in failed_dates:
                try:
                    self._wait_for_rate_limit()
                    df = self.pro.daily_basic(trade_date=trade_date)

                    if df is not None and len(df) > 0:
                        if ts_code_set is not None:
                            df = df[df['ts_code'].isin(ts_code_set)]
                        if len(df) > 0:
                            appended = self._append_to_output(df)
                            new_rows += appended
                        completed_dates.add(trade_date)
                        completed_days += 1
                        progress['completed_dates'] = sorted(list(completed_dates))
                        progress['failed_dates'] = []
                        progress['last_date'] = trade_date
                        self._save_progress(progress)
                        print(f"   ✅ 重试成功 {trade_date}")
                    else:
                        retry_remaining.append(trade_date)
                except Exception as e:
                    retry_remaining.append(trade_date)
                    print(f"   ❌ 重试失败 {trade_date}: {e}")
                    self._adaptive_sleep_on_error(round_idx)

            failed_dates = retry_remaining

        if failed_dates:
            progress['failed_dates'] = failed_dates
            self._save_progress(progress)
            print(f"\n⚠️ 仍有失败交易日 {len(failed_dates)} 天，已写入进度文件")

        print(f"\n✅ 每日估值采集完成（按日期）")
        print(f"   完成交易日: {completed_days} 天")
        print(f"   新增记录: {new_rows} 条")

        return self.load_existing_data()

    def _collect_by_stock(self, start_date, end_date, max_stocks=None, resume=True):
        """旧版逐股逐日采集逻辑（兼容保留）。"""
        stocks = self.get_all_stocks(max_stocks)
        if stocks is None:
            return None

        print(f"股票数量: {len(stocks)}")

        date_list = self.get_trading_dates(start_date, end_date)
        print(f"交易日数量: {len(date_list)}")
        print("采集模式: 按股票逐日拉取（兼容模式）")

        progress = self._load_progress() if resume else {'completed_codes': [], 'completed_dates': [], 'mode': 'by_stock'}
        completed_codes = set(progress.get('completed_codes', []))

        if not resume and os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

        new_count = 0
        stock_count = 0

        for idx, (_, stock) in enumerate(stocks.iterrows()):
            ts_code = stock['ts_code']
            name = stock['name']

            if ts_code in completed_codes:
                print(f"[{idx+1}/{len(stocks)}] {ts_code} {name} - 已采集，跳过")
                continue

            print(f"[{idx+1}/{len(stocks)}] 采集 {ts_code} {name}...", flush=True)

            stock_data = []

            for trade_date in date_list:
                try:
                    self._wait_for_rate_limit()
                    df = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
                    if df is not None and len(df) > 0:
                        stock_data.append(df)
                except Exception:
                    pass

            if stock_data:
                day_df = pd.concat(stock_data, ignore_index=True)
                appended = self._append_to_output(day_df)
                new_count += appended
                stock_count += 1
                print(f"   ✅ {appended}条")
            else:
                print("   ⚠️ 无数据")

            completed_codes.add(ts_code)
            progress['completed_codes'] = list(completed_codes)
            progress['mode'] = 'by_stock'
            self._save_progress(progress)

        print(f"\n✅ 每日估值采集完成（按股票）")
        print(f"   新增: {new_count} 条, {stock_count} 只股票")
        return self.load_existing_data()
    
    def collect_all(self, start_date='20240101', end_date=None, max_stocks=None, resume=True, days=None, mode='by_date'):
        """采集全部股票的每日估值。

        参数:
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            max_stocks: 限制股票数量（用于测试）
            resume: 是否断点续传
            days: 回溯天数（兼容旧调用）
            mode: by_date(推荐, 快) / by_stock(兼容)
        """
        print("\n" + "=" * 60)
        print("每日估值采集（全部股票）")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接")
            return None
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        # 兼容旧接口：传入 days 时自动推算 start_date
        if days is not None:
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            start_date = (end_dt - timedelta(days=int(days))).strftime('%Y%m%d')
        
        print(f"时间范围: {start_date} ~ {end_date}")

        if mode == 'by_stock':
            return self._collect_by_stock(start_date, end_date, max_stocks=max_stocks, resume=resume)
        return self._collect_by_date(start_date, end_date, max_stocks=max_stocks, resume=resume)
    
    def show_progress(self):
        progress = self._load_progress()
        print(f"\n📊 每日估值采集进度:")
        print(f"   采集模式: {progress.get('mode') or 'unknown'}")
        print(f"   已完成股票: {len(progress.get('completed_codes', []))} 只")
        print(f"   已完成交易日: {len(progress.get('completed_dates', []))} 天")
        print(f"   待重试交易日: {len(progress.get('failed_dates', []))} 天")
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_csv(OUTPUT_FILE)
            print(f"   数据文件记录数: {len(df)} 条")
    
    def reset_progress(self):
        """重置采集进度"""
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("✅ 进度已重置")
        else:
            print("⚠️ 无进度文件")


if __name__ == '__main__':
    collector = DailyBasicCollector()
    collector.show_progress()
    # 采集全部股票（最近250天）
    collector.collect_all(max_stocks=None)