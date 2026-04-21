#!/usr/bin/env python3
"""
历史数据采集脚本 - 采集最近N年股票数据（优化版）
路径: scripts/collect_historical_data.py
支持命令行参数、断点续传、重试机制
"""

import sys
import os
import time
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).resolve().parent.parent))

# 导入配置
try:
    from config import (
        DATA_DIR, COLLECT_PROGRESS_FILE, HISTORICAL_A_STOCK_FILE,
        HISTORICAL_HK_STOCK_FILE, HISTORICAL_US_STOCK_FILE,
        REQUEST_DELAY, BATCH_SIZE, MAX_RETRIES,
        USE_FULL_MARKET, MAX_A_STOCKS, MAX_HK_STOCKS, MAX_US_STOCKS,
        HK_MAIN_BOARD_MIN, HK_MAIN_BOARD_MAX, STOCK_POOL_CACHE_FILE,
        STOCK_POOL_CACHE_TTL
    )
except ImportError:
    # 默认配置
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / 'data'
    COLLECT_PROGRESS_FILE = DATA_DIR / 'collect_progress' / 'collect_progress.json'
    HISTORICAL_A_STOCK_FILE = DATA_DIR / 'historical_a_stock.csv'
    HISTORICAL_HK_STOCK_FILE = DATA_DIR / 'historical_hk_stock.csv'
    HISTORICAL_US_STOCK_FILE = DATA_DIR / 'historical_us_stock.csv'
    REQUEST_DELAY = 0.3
    BATCH_SIZE = 100
    MAX_RETRIES = 3
    USE_FULL_MARKET = True
    MAX_A_STOCKS = 5000
    MAX_HK_STOCKS = 2500
    MAX_US_STOCKS = 500
    HK_MAIN_BOARD_MIN = 1
    HK_MAIN_BOARD_MAX = 3999
    STOCK_POOL_CACHE_FILE = DATA_DIR / 'stock_pool_cache.json'
    STOCK_POOL_CACHE_TTL = 7 * 24 * 3600

from collectors.stock_collector import StockCollector
from models import get_session, RawStockData
from utils import get_logger, ProgressManager, retry, ensure_dir

logger = get_logger(__name__)

# 确保目录存在
ensure_dir(DATA_DIR)
ensure_dir(COLLECT_PROGRESS_FILE.parent)

# 预设股票池（小范围测试用）
PRESET_STOCK_POOL = {
    'A': [
        '000858.SZ', '000333.SZ', '002415.SZ', '002594.SZ',
        '300750.SZ', '002475.SZ', '000001.SZ', '002352.SZ',
    ],
    'H': [
        '0700.HK', '9988.HK', '3690.HK', '1810.HK', '9618.HK',
        '9999.HK', '1024.HK', '2015.HK', '9888.HK', '6618.HK',
    ],
    'US': [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
        'BABA', 'PDD', 'JD', 'BIDU', 'NIO',
    ]
}


class HistoricalDataCollector:
    """历史数据采集器（优化版）"""

    @staticmethod
    def _apply_market_limit(codes: List[str], max_count: Optional[int]) -> List[str]:
        items = list(codes or [])
        try:
            limit = int(max_count or 0)
        except Exception:
            limit = 0
        if limit > 0:
            return items[:limit]
        return items
    
    def __init__(self, mode: Optional[str] = None, skip_invalid: bool = True, 
                 max_retries: Optional[int] = None):
        """
        Args:
            mode: 采集模式 ('full' 全市场, 'preset' 预设)，默认使用配置
            skip_invalid: 是否跳过无效股票
            max_retries: 最大重试次数，默认使用配置
        """
        # 如果未指定mode，使用配置
        if mode is None:
            mode = 'full' if USE_FULL_MARKET else 'preset'
        
        self.mode = mode
        self.skip_invalid = skip_invalid
        self.max_retries = max_retries if max_retries is not None else MAX_RETRIES
        self.collector = StockCollector()
        self.session = get_session()
        self.progress = ProgressManager(COLLECT_PROGRESS_FILE)
        
        # 有效性缓存
        self._valid_cache = {}
    
    def get_stock_pool(self) -> Dict[str, List[str]]:
        """获取股票池"""
        if self.mode == 'full':
            logger.info("使用全市场股票池模式")
            return self._get_full_stock_pool()
        else:
            logger.info("使用预设股票池模式（小范围）")
            return PRESET_STOCK_POOL
    
    def _get_full_stock_pool(self) -> Dict[str, List[str]]:
        """获取全市场股票池（带缓存）"""
        # 检查缓存有效期
        if STOCK_POOL_CACHE_FILE.exists():
            cache_mtime = STOCK_POOL_CACHE_FILE.stat().st_mtime
            if time.time() - cache_mtime < STOCK_POOL_CACHE_TTL:
                try:
                    with open(STOCK_POOL_CACHE_FILE, 'r', encoding='utf-8') as f:
                        pool_data = json.load(f)
                        logger.info(f"从缓存加载股票池: A股{len(pool_data.get('a_stock', []))}只")
                        return {
                            'A': self._apply_market_limit(pool_data.get('a_stock', []), MAX_A_STOCKS),
                            'H': self._apply_market_limit(self._filter_hk_main_board(pool_data.get('hk_stock', [])), MAX_HK_STOCKS),
                            'US': self._apply_market_limit(pool_data.get('us_stock', PRESET_STOCK_POOL['US']), MAX_US_STOCKS)
                        }
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"读取缓存失败: {e}")
        
        # 实时获取
        logger.info("正在获取全市场股票池...")
        
        a_stocks = self.collector.fetch_all_a_stocks_from_akshare()
        h_stocks = self.collector.fetch_all_hk_stocks_from_akshare()
        u_stocks = self.collector.fetch_all_us_stocks()
        
        a_codes = [s['code'] for s in a_stocks] if a_stocks else PRESET_STOCK_POOL['A']
        h_codes = [s['code'] for s in h_stocks] if h_stocks else PRESET_STOCK_POOL['H']
        u_codes = [s['code'] for s in u_stocks] if u_stocks else PRESET_STOCK_POOL['US']
        
        # 应用数量限制（MAX_* = 0 表示不设上限）
        a_codes = self._apply_market_limit(a_codes, MAX_A_STOCKS)
        u_codes = self._apply_market_limit(u_codes, MAX_US_STOCKS)
        
        # 过滤港股主板
        h_codes = self._apply_market_limit(self._filter_hk_main_board(h_codes), MAX_HK_STOCKS)
        logger.info(f"港股过滤后: {len(h_codes)}只（仅主板）")
        
        # 保存缓存
        try:
            with open(STOCK_POOL_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump({'a_stock': a_codes, 'hk_stock': h_codes, 'us_stock': u_codes}, f)
            logger.info(f"股票池缓存已保存: {STOCK_POOL_CACHE_FILE}")
        except IOError as e:
            logger.warning(f"保存缓存失败: {e}")
        
        return {'A': a_codes, 'H': h_codes, 'US': u_codes}
    
    def _filter_hk_main_board(self, codes: List[str]) -> List[str]:
        """过滤港股主板"""
        min_code = HK_MAIN_BOARD_MIN
        max_code = HK_MAIN_BOARD_MAX
        
        main_board = []
        for code in codes:
            # 提取数字部分
            num_part = code.split('.')[0].lstrip('0')
            if num_part.isdigit():
                num = int(num_part)
                if min_code <= num <= max_code:
                    main_board.append(code)
            else:
                # 非数字代码（如ETF），保留
                main_board.append(code)
        
        return main_board
    
    def _is_valid_stock_quick(self, code: str) -> bool:
        """快速检查股票是否有效（带缓存）"""
        # 检查内存缓存
        if code in self._valid_cache:
            return self._valid_cache[code]
        
        try:
            import yfinance as yf
            ticker = yf.Ticker(code)
            hist = ticker.history(period='5d')
            is_valid = len(hist) > 0
            self._valid_cache[code] = is_valid
            return is_valid
        except Exception:
            self._valid_cache[code] = False
            return False
    
    @retry(max_attempts=MAX_RETRIES, delay=REQUEST_DELAY)
    def _collect_single_stock(self, code: str, market: str, years: int) -> Optional[pd.DataFrame]:
        """采集单只股票（带重试）"""
        return self.collector.collect_history(code, period=f'{years}y')
    
    def _save_batch(self, code: str, df: pd.DataFrame, market: str) -> int:
        """批量保存数据"""
        count = 0
        batch = []
        
        for _, row in df.iterrows():
            # 检查是否已存在
            existing = self.session.query(RawStockData).filter(
                RawStockData.code == code,
                RawStockData.date == row['date']
            ).first()
            
            if not existing:
                record = RawStockData(
                    code=code,
                    name=code.split('.')[0],
                    date=row['date'],
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    volume=int(row['volume']) if row['volume'] else 0,
                    market=market
                )
                batch.append(record)
                count += 1
                
                # 批量提交
                if len(batch) >= BATCH_SIZE:
                    self.session.add_all(batch)
                    self.session.commit()
                    batch = []
        
        # 提交剩余
        if batch:
            self.session.add_all(batch)
            self.session.commit()
        
        return count
    
    def collect(self, years: int = 3, max_stocks: Optional[int] = None) -> int:
        """
        采集历史数据
        Args:
            years: 采集年数
            max_stocks: 最大采集数量（用于测试）
        Returns:
            int: 采集记录数
        """
        logger.info(f"开始采集历史数据: {years}年, 模式={self.mode}")
        
        stock_pool = self.get_stock_pool()
        
        # 限制数量
        if max_stocks:
            per_market = max_stocks // len(stock_pool)
            for market in stock_pool:
                stock_pool[market] = stock_pool[market][:per_market]
        
        total_records = self.progress.progress.get('total_records', 0)
        
        for market, codes in stock_pool.items():
            logger.info(f"采集 {market} 股市场，共 {len(codes)} 只股票")
            
            for i, code in enumerate(codes):
                # 断点续传检查
                if self.progress.is_completed(code):
                    logger.debug(f"{code} 已完成，跳过")
                    continue
                
                # 检查是否已有足够数据
                existing = self.session.query(RawStockData).filter(
                    RawStockData.code == code
                ).count()
                
                if existing > 200:
                    logger.info(f"{code} 已有 {existing} 条记录，跳过")
                    self.progress.mark_completed(code)
                    continue
                
                # 快速检查股票有效性
                if self.skip_invalid and not self._is_valid_stock_quick(code):
                    logger.warning(f"{code} 无效（无行情数据），跳过")
                    self.progress.mark_skipped(code, "无行情数据")
                    continue
                
                logger.info(f"[{i+1}/{len(codes)}] 采集 {code}...")
                
                try:
                    df = self._collect_single_stock(code, market, years)
                    
                    if df is not None and len(df) > 0:
                        count = self._save_batch(code, df, market)
                        total_records += count
                        logger.info(f"✅ {code} 采集完成，{count} 条新记录")
                        self.progress.mark_completed(code)
                        self.progress.add_records(count)
                    else:
                        logger.warning(f"⚠️ {code} 无数据")
                        self.progress.mark_failed(code, "无数据")
                    
                except Exception as e:
                    logger.error(f"❌ {code} 采集失败: {e}")
                    self.progress.mark_failed(code, str(e))
                    self.session.rollback()
                
                time.sleep(REQUEST_DELAY)
        
        self.session.close()
        logger.info(f"🎉 采集完成！共采集 {total_records} 条记录")
        
        summary = self.progress.get_summary()
        logger.info(f"   成功: {summary['completed']} 只")
        logger.info(f"   失败: {summary['failed']} 只")
        logger.info(f"   跳过: {summary['skipped']} 只")
        
        return total_records
    
    def export_to_csv(self) -> None:
        """导出数据到CSV"""
        for market, filename in [
            ('A', HISTORICAL_A_STOCK_FILE),
            ('H', HISTORICAL_HK_STOCK_FILE),
            ('US', HISTORICAL_US_STOCK_FILE)
        ]:
            stocks = self.session.query(RawStockData).filter(
                RawStockData.market == market
            ).all()
            
            if stocks:
                df = pd.DataFrame([{
                    'code': s.code,
                    'date': s.date,
                    'open': s.open,
                    'high': s.high,
                    'low': s.low,
                    'close': s.close,
                    'volume': s.volume
                } for s in stocks])
                df.to_csv(filename, index=False, encoding='utf-8')
                logger.info(f"✅ {market}股数据已导出: {len(df)} 条 -> {filename}")
            else:
                logger.warning(f"⚠️ {market}股无数据可导出")
    
    def show_progress(self) -> None:
        """显示采集进度"""
        summary = self.progress.get_summary()
        print(f"\n📊 采集进度:")
        print(f"   已完成: {summary['completed']} 只")
        print(f"   失败: {summary['failed']} 只")
        print(f"   跳过: {summary['skipped']} 只")
        print(f"   总记录数: {summary['total_records']} 条")
        print(f"   最后更新: {summary['last_update']}")
    
    def reset_progress(self) -> None:
        """重置采集进度"""
        self.progress.reset()
        logger.info("进度已重置")
    
    def clear_cache(self) -> None:
        """清空有效性缓存"""
        self._valid_cache.clear()
        logger.info("有效性缓存已清空")
    
    def close(self) -> None:
        """关闭资源"""
        if self.session:
            self.session.close()


def main():
    parser = argparse.ArgumentParser(description='历史数据采集脚本')
    parser.add_argument('--years', type=int, default=3, help='采集年数')
    parser.add_argument('--mode', choices=['full', 'preset'], default=None,
                        help='采集模式: full(全市场), preset(预设)，默认使用配置')
    parser.add_argument('--max', type=int, default=None, help='最大采集数量（测试用）')
    parser.add_argument('--export', action='store_true', help='导出CSV')
    parser.add_argument('--reset', action='store_true', help='重置进度')
    parser.add_argument('--progress', action='store_true', help='显示进度')
    parser.add_argument('--no-skip', action='store_true', help='不跳过无效股票')
    parser.add_argument('--clear-cache', action='store_true', help='清空有效性缓存')
    
    args = parser.parse_args()
    
    collector = HistoricalDataCollector(
        mode=args.mode,
        skip_invalid=not args.no_skip
    )
    
    if args.reset:
        collector.reset_progress()
    elif args.progress:
        collector.show_progress()
    elif args.clear_cache:
        collector.clear_cache()
    else:
        count = collector.collect(args.years, args.max)
        print(f"\n✅ 采集完成，共 {count} 条记录")
        
        if args.export:
            collector.export_to_csv()
    
    collector.close()


if __name__ == '__main__':
    main()