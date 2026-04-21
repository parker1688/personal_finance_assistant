"""
个股资金流向采集模块 - collectors/moneyflow_collector.py
采集个股主力资金流向数据（需要Tushare积分2000+）
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
OUTPUT_FILE = 'data/moneyflow_all.csv'


class MoneyflowCollector:
    """个股资金流向采集器（仅Tushare）"""
    
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
        return os.path.join(PROGRESS_DIR, 'moneyflow_progress.json')
    
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
            'completed_codes': [],      # 已完成的股票代码
            'completed_dates': [],      # 已完成的日期
            'last_date': None,
            'last_update': None
        }
    
    def _save_progress(self, progress):
        """保存采集进度"""
        progress['last_update'] = datetime.now().isoformat()
        progress_file = self._get_progress_file()
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def _is_code_completed(self, code, progress):
        """检查股票是否已采集完成"""
        return code in progress.get('completed_codes', [])
    
    def _mark_code_completed(self, code, progress):
        """标记股票采集完成"""
        if code not in progress.get('completed_codes', []):
            progress.setdefault('completed_codes', []).append(code)
            self._save_progress(progress)
    
    def _is_date_completed(self, date_str, progress):
        """检查日期是否已采集"""
        return date_str in progress.get('completed_dates', [])
    
    def _mark_date_completed(self, date_str, progress):
        """标记日期采集完成"""
        if date_str not in progress.get('completed_dates', []):
            progress.setdefault('completed_dates', []).append(date_str)
            progress['last_date'] = date_str
            self._save_progress(progress)
    
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
            print(f"📁 已有数据: {len(df)} 条")
            return df
        return None

    def _append_to_output(self, df):
        """增量写入输出文件，避免每天全量拼接/重写。"""
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
    
    def collect_by_date(self, start_date, end_date, max_stocks=None, resume=True):
        """
        按日期维度采集（逐日采集所有股票的资金流向）- 推荐方式
        Args:
            start_date: 开始日期 (YYYYMMDD 或 date对象)
            end_date: 结束日期
            max_stocks: 最大股票数量，None表示全部
            resume: 是否断点续传
        Returns:
            DataFrame: 资金流向数据
        """
        print("\n" + "=" * 60)
        print("个股资金流向采集（按日期维度）")
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
        
        # 获取股票列表
        stocks = self.get_all_stocks(max_stocks)
        if stocks is None:
            return None
        
        # 生成日期列表（只取交易日）
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        date_list = []
        current = start_dt
        while current <= end_dt:
            if current.weekday() < 5:
                date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        print(f"交易日数量: {len(date_list)}")
        
        # 加载进度
        progress = self._load_progress() if resume else {'completed_dates': []}
        completed_dates = set(progress.get('completed_dates', []))
        existing_df = self.load_existing_data()
        output_missing = existing_df is None or len(existing_df) == 0

        if output_missing and completed_dates.intersection(date_list):
            print("⚠️  检测到资金流CSV缺失，但当前区间存在历史进度记录，自动忽略旧进度并重建")
            progress['completed_dates'] = [d for d in progress.get('completed_dates', []) if d not in date_list]
            self._save_progress(progress)
            completed_dates = set(progress.get('completed_dates', []))
        
        # 过滤已采集的日期
        pending_dates = [d for d in date_list if d not in completed_dates]
        
        if not pending_dates:
            if output_missing:
                print("⚠️  进度文件显示已完成，但资金流CSV缺失或为空，自动重建当前区间")
                progress['completed_dates'] = [d for d in progress.get('completed_dates', []) if d not in date_list]
                self._save_progress(progress)
                pending_dates = list(date_list)
            else:
                print("✅ 所有日期已采集完成")
                return existing_df
        
        print(f"待采集日期: {len(pending_dates)} 天")
        
        new_count = 0

        stock_code_set = set(stocks['ts_code'].tolist()) if max_stocks else None
        use_fast_daily_pull = (max_stocks is None)
        if use_fast_daily_pull:
            print("采集策略: 单日一次全市场拉取（快速模式）")
        else:
            print("采集策略: 按股票逐只拉取（限量模式）")
        
        failed_dates = []

        for idx, trade_date in enumerate(pending_dates):
            print(f"[{idx+1}/{len(pending_dates)}] 采集 {trade_date}...", flush=True)
            
            day_df = None
            fetch_succeeded = False
            try:
                if use_fast_daily_pull:
                    # TuShare moneyflow 支持按 trade_date 获取当日全市场资金流向。
                    max_attempts = 3
                    for attempt in range(1, max_attempts + 1):
                        try:
                            day_df = self.pro.moneyflow(trade_date=trade_date)
                            fetch_succeeded = True
                            break
                        except Exception as e:
                            logger.warning(f"moneyflow快速模式超时/异常: date={trade_date}, attempt={attempt}/{max_attempts}, err={e}")
                            if attempt < max_attempts:
                                time.sleep(min(2 * attempt, 5))
                else:
                    date_data = []
                    for _, stock in stocks.iterrows():
                        ts_code = stock['ts_code']
                        try:
                            df = self.pro.moneyflow(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                            if df is not None and len(df) > 0:
                                date_data.append(df)
                        except Exception:
                            pass
                        time.sleep(0.02)
                    if date_data:
                        day_df = pd.concat(date_data, ignore_index=True)
                    fetch_succeeded = True
            except Exception:
                # 快速模式失败时，回退逐股模式兜底。
                if use_fast_daily_pull:
                    date_data = []
                    for _, stock in stocks.iterrows():
                        ts_code = stock['ts_code']
                        try:
                            df = self.pro.moneyflow(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                            if df is not None and len(df) > 0:
                                date_data.append(df)
                        except Exception:
                            pass
                        time.sleep(0.02)
                    if date_data:
                        day_df = pd.concat(date_data, ignore_index=True)
                    fetch_succeeded = True

            if day_df is not None and len(day_df) > 0:
                if stock_code_set is not None:
                    day_df = day_df[day_df['ts_code'].isin(stock_code_set)]

                appended = self._append_to_output(day_df)
                new_count += appended
                print(f"   ✅ {appended}条")
                # 仅在成功获取且有结果时标记完成，避免超时导致漏采。
                self._mark_date_completed(trade_date, progress)
            else:
                print("   ⚠️ 无数据")
                if fetch_succeeded:
                    # 源端确实返回空数据时允许标记完成，避免无限重试。
                    self._mark_date_completed(trade_date, progress)
                else:
                    failed_dates.append(trade_date)
                    logger.warning(f"moneyflow日期未完成（将留待下次重试）: {trade_date}")
            
            # 打印累计量（不再全量读写临时拼接）。
            if os.path.exists(OUTPUT_FILE):
                try:
                    total_rows = sum(1 for _ in open(OUTPUT_FILE, 'r', encoding='utf-8', errors='ignore')) - 1
                    total_rows = max(total_rows, 0)
                    print(f"   💾 已保存，累计 {total_rows} 条")
                except Exception:
                    pass

            time.sleep(0.2)

        if failed_dates:
            logger.warning(f"moneyflow仍有未完成日期: {len(failed_dates)} 天, 示例={failed_dates[:5]}")

        result = self.load_existing_data()
        if result is not None and len(result) > 0:
            print(f"\n" + "=" * 60)
            print("✅ 资金流向采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {len(result)} 条")
            print(f"   保存至: {OUTPUT_FILE}")
            print("=" * 60)
            return result

        print("❌ 无数据")
        return None
    
    def collect_by_stock(self, start_date, end_date, max_stocks=None, resume=True):
        """
        按股票维度采集（逐只股票采集）- 适合补全历史数据
        """
        print("\n" + "=" * 60)
        print("个股资金流向采集（按股票维度）")
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
        stocks = self.get_all_stocks(max_stocks)
        if stocks is None:
            return None
        
        # 加载进度
        progress = self._load_progress() if resume else {'completed_codes': []}
        
        # 加载已有数据
        existing_df = self.load_existing_data()
        all_data = [existing_df] if existing_df is not None else []
        new_count = 0
        
        for idx, (_, stock) in enumerate(stocks.iterrows()):
            ts_code = stock['ts_code']
            name = stock['name']
            
            if self._is_code_completed(ts_code, progress):
                print(f"[{idx+1}/{len(stocks)}] {ts_code} {name} - 已采集，跳过")
                continue
            
            print(f"[{idx+1}/{len(stocks)}] 采集 {ts_code} {name}...", end=' ', flush=True)
            
            try:
                df = self.pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df is not None and len(df) > 0:
                    all_data.append(df)
                    new_count += len(df)
                    print(f"✅ {len(df)}条")
                else:
                    print(f"⚠️ 无数据")
                
                self._mark_code_completed(ts_code, progress)
                
            except Exception as e:
                print(f"❌ {str(e)[:40]}")
            
            # 每10只保存一次
            if (idx + 1) % 10 == 0 and all_data:
                temp_df = pd.concat(all_data, ignore_index=True)
                temp_df.to_csv(OUTPUT_FILE, index=False)
                print(f"   💾 已保存，累计 {len(temp_df)} 条")
            
            time.sleep(0.3)
        
        # 最终保存
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result.to_csv(OUTPUT_FILE, index=False)
            print(f"\n✅ 资金流向采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {len(result)} 条")
            return result
        else:
            print("❌ 无数据")
            return None
    
    def show_progress(self):
        """显示采集进度"""
        progress = self._load_progress()
        print(f"\n📊 资金流向采集进度:")
        print(f"   已完成股票: {len(progress.get('completed_codes', []))} 只")
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
            print("✅ 进度已重置")
        else:
            print("⚠️ 无进度文件")


if __name__ == '__main__':
    collector = MoneyflowCollector()
    
    # 显示进度
    collector.show_progress()
    
    # 示例：采集最近30天，前10只股票测试
    end = datetime.now()
    start = end - timedelta(days=30)
    
    # 按日期采集（推荐）
    collector.collect_by_date(start, end, max_stocks=10)
    
    collector.show_progress()