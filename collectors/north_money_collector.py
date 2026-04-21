"""
北向资金采集模块 - collectors/north_money_collector.py
采集北向资金（沪深港通）每日净流入流出数据
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
OUTPUT_FILE = 'data/north_money_all.csv'


class NorthMoneyCollector:
    """北向资金采集器（仅Tushare）"""
    
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
        return os.path.join(PROGRESS_DIR, 'north_money_progress.json')
    
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

    def _fetch_north_money_by_day(self, trade_date):
        """按交易日获取北向资金，统一调用已验证可用接口。"""
        errors = []

        # 优先按 trade_date 单日拉取，兼容部分环境的参数校验。
        try:
            df = self.pro.moneyflow_hsgt(trade_date=trade_date)
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            errors.append(f"moneyflow_hsgt(trade_date)={e}")

        # 回退到 start/end 形式。
        try:
            df = self.pro.moneyflow_hsgt(start_date=trade_date, end_date=trade_date)
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            errors.append(f"moneyflow_hsgt(start/end)={e}")

        if errors:
            raise RuntimeError('; '.join(errors))
        return None
    
    def collect(self, start_date, end_date, resume=True, strict=False):
        """
        采集北向资金
        Args:
            start_date: 开始日期 (YYYYMMDD 或 date对象)
            end_date: 结束日期
            resume: 是否断点续传
        Returns:
            DataFrame: 北向资金数据
        """
        print("\n" + "=" * 60)
        print("北向资金采集")
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
        failed_dates = []
        
        for idx, trade_date in enumerate(pending_dates):
            day_start = time.perf_counter()
            print(f"[{idx+1}/{len(pending_dates)}] 采集 {trade_date}...", end=' ', flush=True)
            
            try:
                df = self._fetch_north_money_by_day(trade_date)
                
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
                    f"north_money day={trade_date} rows={0 if df is None else len(df)} elapsed={elapsed:.2f}s"
                )
                print(f"   ⏱️ {elapsed:.2f}s, 累计 {total_rows} 条")
                
            except Exception as e:
                print(f"❌ {str(e)[:40]}")
                logger.error(f"north_money day={trade_date} failed: {e}")
                failed_dates.append(trade_date)
            
            time.sleep(0.3)
        
        # 最终返回
        result = self.load_existing_data()
        if result is not None and len(result) > 0:
            print(f"\n" + "=" * 60)
            print(f"✅ 北向资金采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {total_rows} 条")
            print(f"   保存至: {OUTPUT_FILE}")
            print("=" * 60)

            if strict and failed_dates:
                raise RuntimeError(f"north_money failed_dates={failed_dates[:5]} total_failed={len(failed_dates)}")
            return result

        print("❌ 无数据")
        if strict and failed_dates:
            raise RuntimeError(f"north_money failed_dates={failed_dates[:5]} total_failed={len(failed_dates)}")
        return None
    
    def collect_latest(self, days=30):
        """
        采集最近N天的北向资金
        Args:
            days: 天数
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return self.collect(start_date, end_date)
    
    def show_progress(self):
        """显示采集进度"""
        progress = self._load_progress()
        print(f"\n📊 北向资金采集进度:")
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
    collector = NorthMoneyCollector()
    
    # 显示进度
    collector.show_progress()
    
    # 采集最近30天
    collector.collect_latest(days=30)
    
    collector.show_progress()