"""
股票数据采集模块 - collectors/stock_collector.py
采集A股、港股、美股的实时和历史行情数据
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import sys
import os
from functools import lru_cache
from sqlalchemy.exc import OperationalError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    MIN_MARKET_CAP_A, MIN_MARKET_CAP_HK, MIN_MARKET_CAP_US,
    MIN_PRICE_HK, MIN_PRICE_US, MIN_VOLUME_A, MIN_VOLUME_HK, MIN_VOLUME_US,
    USE_FULL_MARKET, MAX_A_STOCKS, MAX_HK_STOCKS, MAX_US_STOCKS,
    STOCK_POOL_CACHE_FILE, STOCK_BASIC_FILE, LEGACY_STOCK_POOL_FILE,
    HISTORICAL_HK_STOCK_FILE, HISTORICAL_US_STOCK_FILE, resolve_data_file
)
from models import get_session, RawStockData, RawFundData
from utils import get_logger, get_tushare_pro

logger = get_logger(__name__)


class StockCollector:
    """股票数据采集器"""
    
    def __init__(self):
        self.session = get_session()
        
        self.a_stock_pool = self._get_a_stock_pool()
        self.hk_stock_pool = self._get_hk_stock_pool()
        self.us_stock_pool = self._get_us_stock_pool()
        
        self.cache = {}
        self._local_history_cache = {}
    
    @staticmethod
    def _dedupe_codes(values):
        seen = set()
        result = []
        for item in values or []:
            code = str(item).strip()
            if not code or code.lower() == 'nan' or code in seen:
                continue
            seen.add(code)
            result.append(code)
        return result

    @staticmethod
    def _apply_configured_max(values, configured_max):
        items = list(values or [])
        try:
            max_n = int(configured_max)
        except Exception:
            max_n = 0
        if max_n > 0:
            return items[:max_n]
        return items

    @staticmethod
    def _normalize_symbol_for_yfinance(code):
        code = str(code or '').strip().upper()
        if code.endswith('.SH'):
            return code[:-3] + '.SS'
        if code.endswith('.SZ'):
            base = code[:-3]
            if base.startswith(('4', '8', '9')):
                return base + '.BJ'
            return code
        return code

    @staticmethod
    def _normalize_domestic_code(code):
        code = str(code or '').strip().upper()
        if not code:
            return code
        if '.' not in code and len(code) == 6 and code.isdigit():
            if code.startswith('6'):
                return f"{code}.SH"
            if code.startswith(('4', '8', '9')):
                return f"{code}.BJ"
            return f"{code}.SZ"
        if code.endswith('.SZ') and code[:-3].startswith(('4', '8', '9')):
            return code[:-3] + '.BJ'
        return code

    @staticmethod
    def _is_domestic_market(code, market='US'):
        code = str(code or '').strip().upper()
        market = str(market or '').strip().upper()
        return market in {'A', 'ASHARE', 'CN', 'BJ'} or code.endswith(('.SH', '.SZ', '.BJ'))

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_a_spot_snapshot():
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            return df if isinstance(df, pd.DataFrame) and not df.empty else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _collect_realtime_domestic(self, code, market='A'):
        normalized = self._normalize_domestic_code(code)
        base = normalized.split('.')[0]

        spot_df = self._get_a_spot_snapshot()
        if isinstance(spot_df, pd.DataFrame) and not spot_df.empty and '代码' in spot_df.columns:
            matched = spot_df[spot_df['代码'].astype(str).str.strip() == base]
            if not matched.empty:
                row = matched.iloc[0]
                close_price = pd.to_numeric(row.get('最新价'), errors='coerce')
                if pd.notna(close_price):
                    open_price = pd.to_numeric(row.get('今开', close_price), errors='coerce')
                    high_price = pd.to_numeric(row.get('最高', close_price), errors='coerce')
                    low_price = pd.to_numeric(row.get('最低', close_price), errors='coerce')
                    volume = pd.to_numeric(row.get('成交量', 0), errors='coerce')
                    return {
                        'code': normalized,
                        'name': str(row.get('名称') or base),
                        'market': 'A',
                        'date': datetime.now().date(),
                        'open': float(open_price if pd.notna(open_price) else close_price),
                        'high': float(high_price if pd.notna(high_price) else close_price),
                        'low': float(low_price if pd.notna(low_price) else close_price),
                        'close': float(close_price),
                        'volume': int(volume if pd.notna(volume) else 0),
                    }

        pro = get_tushare_pro()
        if pro is not None:
            try:
                trade_date = datetime.now().strftime('%Y%m%d')
                daily = pro.daily(ts_code=normalized, trade_date=trade_date)
                if daily is not None and not daily.empty:
                    row = daily.iloc[0]
                    return {
                        'code': normalized,
                        'name': base,
                        'market': 'A',
                        'date': datetime.now().date(),
                        'open': float(pd.to_numeric(row.get('open'), errors='coerce') or 0.0),
                        'high': float(pd.to_numeric(row.get('high'), errors='coerce') or 0.0),
                        'low': float(pd.to_numeric(row.get('low'), errors='coerce') or 0.0),
                        'close': float(pd.to_numeric(row.get('close'), errors='coerce') or 0.0),
                        'volume': int(pd.to_numeric(row.get('vol'), errors='coerce') or 0),
                    }
            except Exception as e:
                logger.debug(f"Tushare实时回退失败 {normalized}: {e}")

        logger.debug(f"国内数据源暂无 {normalized} 的行情数据")
        return None

    def _collect_history_domestic(self, code, period='2y'):
        normalized = self._normalize_domestic_code(code)
        base = normalized.split('.')[0]
        try:
            import akshare as ak
            history = ak.stock_zh_a_hist(symbol=base, period='daily', adjust='qfq')
            if history is not None and isinstance(history, pd.DataFrame) and not history.empty:
                rename_map = {
                    '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high',
                    '最低': 'low', '成交量': 'volume'
                }
                history = history.rename(columns=rename_map)
                required = ['date', 'open', 'high', 'low', 'close', 'volume']
                if all(col in history.columns for col in required):
                    history = history[required].copy()
                    history['code'] = normalized
                    history['date'] = pd.to_datetime(history['date']).dt.date
                    if isinstance(period, str):
                        cutoff = None
                        if period.endswith('y'):
                            cutoff = datetime.now().date() - timedelta(days=365 * int(period[:-1] or 1))
                        elif period.endswith('mo'):
                            cutoff = datetime.now().date() - timedelta(days=30 * int(period[:-2] or 1))
                        elif period.endswith('d'):
                            cutoff = datetime.now().date() - timedelta(days=int(period[:-1] or 1))
                        if cutoff is not None:
                            history = history[history['date'] >= cutoff]
                    history['volume'] = pd.to_numeric(history['volume'], errors='coerce').fillna(0).astype(int)
                    for col in ['open', 'high', 'low', 'close']:
                        history[col] = pd.to_numeric(history[col], errors='coerce')
                    history = history.dropna(subset=['close'])
                    if not history.empty:
                        return history.reset_index(drop=True)
        except Exception as e:
            logger.debug(f"AkShare历史回退失败 {normalized}: {e}")

        pro = get_tushare_pro()
        if pro is not None:
            try:
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')
                daily = pro.daily(ts_code=normalized, start_date=start_date, end_date=end_date)
                if daily is not None and not daily.empty:
                    daily = daily.rename(columns={'trade_date': 'date', 'vol': 'volume'})
                    daily['date'] = pd.to_datetime(daily['date']).dt.date
                    daily['code'] = normalized
                    daily = daily[['date', 'open', 'high', 'low', 'close', 'volume', 'code']]
                    daily = daily.sort_values('date').reset_index(drop=True)
                    return daily
            except Exception as e:
                logger.debug(f"Tushare历史回退失败 {normalized}: {e}")

        logger.debug(f"国内数据源暂无 {normalized} 的历史数据")
        return None

    def _load_pool_from_cache_key(self, key):
        cache_file = str(STOCK_POOL_CACHE_FILE)
        if not os.path.exists(cache_file):
            return []
        try:
            import json
            with open(cache_file, 'r') as f:
                data = json.load(f)
            return self._dedupe_codes(data.get(key, []))
        except Exception:
            return []

    def _load_codes_from_csv(self, csv_path, max_count=None):
        try:
            if not os.path.exists(csv_path):
                return []
            df = pd.read_csv(csv_path)
            for col in ['ts_code', 'code', 'symbol']:
                if col in df.columns:
                    codes = self._dedupe_codes(df[col].dropna().astype(str).tolist())
                    if max_count is not None:
                        try:
                            max_n = int(max_count)
                            if max_n > 0:
                                codes = codes[:max_n]
                        except Exception:
                            pass
                    return codes
        except Exception:
            return []
        return []

    def _get_a_stock_pool(self):
        fallback = [
            '000858.SZ', '000333.SZ', '002415.SZ', '002594.SZ',
            '300750.SZ', '002475.SZ', '000001.SZ', '002352.SZ',
        ]
        cached = self._load_pool_from_cache_key('a_stock')
        if cached:
            return self._apply_configured_max(cached, MAX_A_STOCKS)
        if USE_FULL_MARKET:
            stock_pool_path = resolve_data_file(STOCK_BASIC_FILE, LEGACY_STOCK_POOL_FILE)
            local_codes = self._load_codes_from_csv(str(stock_pool_path), max_count=MAX_A_STOCKS if int(MAX_A_STOCKS or 0) > 0 else None)
            if local_codes:
                return local_codes
        return self._apply_configured_max(fallback, MAX_A_STOCKS)
    
    def _get_hk_stock_pool(self):
        fallback = [
            '0700.HK', '9988.HK', '3690.HK', '1810.HK', '9618.HK', '9999.HK', '1024.HK', '2015.HK', '9888.HK', '6618.HK',
            '0388.HK', '0005.HK', '0939.HK', '1299.HK', '2318.HK', '2388.HK', '3968.HK', '3988.HK', '0941.HK', '0016.HK',
            '0011.HK', '0688.HK', '0883.HK', '1109.HK', '1093.HK', '1177.HK', '2269.HK', '6862.HK', '1928.HK', '2020.HK'
        ]
        cached = self._load_pool_from_cache_key('hk_stock')
        if len(cached) >= 30:
            return self._apply_configured_max(cached, MAX_HK_STOCKS)
        local_codes = self._load_codes_from_csv(HISTORICAL_HK_STOCK_FILE, max_count=MAX_HK_STOCKS if int(MAX_HK_STOCKS or 0) > 0 else None)
        if len(local_codes) >= 30:
            return local_codes
        return self._apply_configured_max(fallback, MAX_HK_STOCKS)
    
    def _get_us_stock_pool(self):
        fallback = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BABA', 'PDD', 'JD', 'BIDU', 'NIO',
            'AVGO', 'NFLX', 'AMD', 'INTC', 'QCOM', 'ORCL', 'CRM', 'ADBE', 'UBER', 'PYPL', 'BRK-B', 'JPM',
            'V', 'MA', 'WMT', 'COST', 'LLY', 'ABBV', 'JNJ', 'UNH', 'KO', 'PEP', 'DIS', 'MCD'
        ]
        cached = self._load_pool_from_cache_key('us_stock')
        if len(cached) >= 30:
            return self._apply_configured_max(cached, MAX_US_STOCKS)
        local_codes = self._load_codes_from_csv(HISTORICAL_US_STOCK_FILE, max_count=MAX_US_STOCKS if int(MAX_US_STOCKS or 0) > 0 else None)
        if len(local_codes) >= 30:
            return local_codes
        return self._apply_configured_max(fallback, MAX_US_STOCKS)
    
    def collect_realtime(self, code, market='US'):
        try:
            normalized_code = self._normalize_domestic_code(code)
            if self._is_domestic_market(normalized_code, market):
                return self._collect_realtime_domestic(normalized_code, market='A')

            query_code = self._normalize_symbol_for_yfinance(normalized_code)
            ticker = yf.Ticker(query_code)
            history = ticker.history(period='1d')
            if len(history) == 0:
                logger.debug(f"暂无 {normalized_code} 的实时行情数据")
                return None
            
            # 检查是否有有效数据
            if pd.isna(history['Close'].iloc[-1]):
                logger.debug(f"{normalized_code} 今日数据未完成，已跳过")
                return None
            
            current_price = float(history['Close'].iloc[-1])
            open_price = float(history['Open'].iloc[-1])
            high_price = float(history['High'].iloc[-1])
            low_price = float(history['Low'].iloc[-1])
            volume = int(history['Volume'].iloc[-1])
            
            return {
                'code': str(normalized_code),
                'name': str(normalized_code).split('.')[0],
                'market': str(market),
                'date': datetime.now().date(),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': current_price,
                'volume': volume,
            }
        except Exception as e:
            logger.error(f"采集 {code} 实时行情失败: {e}")
            return None
    
    def collect_history(self, code, period='2y', interval='1d'):
        try:
            normalized_code = self._normalize_domestic_code(code)
            if self._is_domestic_market(normalized_code):
                domestic_history = self._collect_history_domestic(normalized_code, period=period)
                if domestic_history is not None and len(domestic_history) > 0:
                    return domestic_history

            query_code = self._normalize_symbol_for_yfinance(normalized_code)
            ticker = yf.Ticker(query_code)
            history = ticker.history(period=period, interval=interval)

            # yfinance 在部分港股会返回异常结构，这里尝试 download 回退。
            if history is None or not isinstance(history, pd.DataFrame):
                history = yf.download(query_code, period=period, interval=interval, progress=False, auto_adjust=False, threads=False)
            
            if history is None or not isinstance(history, pd.DataFrame) or len(history) == 0:
                logger.debug(f"暂无 {normalized_code} 的历史数据")
                return None
            
            history = history.reset_index()
            history['code'] = str(code)
            history.columns = [col.lower() for col in history.columns]
            if 'date' in history.columns:
                history['date'] = pd.to_datetime(history['date']).dt.date
            elif 'datetime' in history.columns:
                history['date'] = pd.to_datetime(history['datetime']).dt.date
            elif 'index' in history.columns:
                history['date'] = pd.to_datetime(history['index']).dt.date
            else:
                logger.debug(f"{code} 历史数据缺少日期列，已跳过")
                return None
            
            # 确保数值列为 float，并过滤掉 NaN
            for col in ['open', 'high', 'low', 'close']:
                if col in history.columns:
                    history[col] = pd.to_numeric(history[col], errors='coerce')
            if 'volume' in history.columns:
                history['volume'] = pd.to_numeric(history['volume'], errors='coerce').fillna(0).astype(int)
            
            # 删除 close 为 NaN 的行（未完成的数据）
            history = history.dropna(subset=['close'])

            if history.empty:
                logger.debug(f"{code} 历史数据清洗后为空")
                return None
            
            return history
        except Exception as e:
            logger.error(f"采集 {code} 历史数据失败: {e}")
            return None
    
    def collect_all_realtime(self):
        results = []
        
        logger.info(f"开始采集A股实时行情，共 {len(self.a_stock_pool)} 只")
        for code in self.a_stock_pool:
            data = self.collect_realtime(code, market='A')
            if data:
                results.append(data)
                self._save_to_database(data)
            time.sleep(0.3)
        
        logger.info(f"开始采集港股实时行情，共 {len(self.hk_stock_pool)} 只")
        for code in self.hk_stock_pool:
            data = self.collect_realtime(code, market='H')
            if data:
                results.append(data)
                self._save_to_database(data)
            time.sleep(0.3)
        
        logger.info(f"开始采集美股实时行情，共 {len(self.us_stock_pool)} 只")
        for code in self.us_stock_pool:
            data = self.collect_realtime(code, market='US')
            if data:
                results.append(data)
                self._save_to_database(data)
            time.sleep(0.3)
        
        logger.info(f"采集完成，成功采集 {len(results)} 条数据")
        return results
    
    def _save_to_database(self, data):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                existing = self.session.query(RawStockData).filter(
                    RawStockData.code == data['code'],
                    RawStockData.date == data['date']
                ).first()

                if existing:
                    for key, value in data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    record = RawStockData(**data)
                    self.session.add(record)

                self.session.commit()
                return True
            except OperationalError as e:
                self.session.rollback()
                message = str(e).lower()
                if 'database is locked' in message and attempt < (max_retries - 1):
                    time.sleep(0.2 * (attempt + 1))
                    continue
                logger.error(f"保存数据失败: {e}")
                return False
            except Exception as e:
                self.session.rollback()
                logger.error(f"保存数据失败: {e}")
                return False

        return False

    def _get_local_history_sources(self):
        return [
            os.path.join('data', 'historical_a_stock.csv'),
            str(HISTORICAL_HK_STOCK_FILE),
            str(HISTORICAL_US_STOCK_FILE),
            os.path.join('data', 'historical_etf.csv'),
            os.path.join('data', 'gold_prices.csv'),
            os.path.join('data', 'silver_prices.csv'),
            os.path.join('data', 'precious_metals.csv'),
            os.path.join('data', 'fund_nav.csv'),
        ]

    def _load_local_history_for_code(self, code, start_date=None, end_date=None):
        normalized = str(code or '').strip().upper()
        if not normalized:
            return None

        variants = {normalized, self._normalize_domestic_code(normalized)}
        if normalized.endswith('.SS'):
            variants.add(normalized[:-3] + '.SH')
        if normalized.endswith('.BJ') and len(normalized) > 3:
            variants.add(normalized[:-3] + '.SZ')
        if '.' in normalized:
            variants.add(normalized.split('.')[0])

        for path in self._get_local_history_sources():
            try:
                if path not in self._local_history_cache:
                    if not os.path.exists(path):
                        self._local_history_cache[path] = pd.DataFrame()
                    else:
                        local_df = pd.read_csv(path, dtype={'code': str}, low_memory=False)
                        if local_df is None or local_df.empty:
                            self._local_history_cache[path] = pd.DataFrame()
                        else:
                            frame = local_df.copy()
                            if 'trade_date' in frame.columns and 'date' not in frame.columns:
                                frame['date'] = frame['trade_date']
                            if 'nav' in frame.columns and 'close' not in frame.columns:
                                frame['close'] = frame['nav']
                            for col in ['open', 'high', 'low']:
                                if col not in frame.columns and 'close' in frame.columns:
                                    frame[col] = frame['close']
                            if 'volume' not in frame.columns:
                                frame['volume'] = 0
                            if 'code' not in frame.columns:
                                self._local_history_cache[path] = pd.DataFrame()
                                continue
                            frame['code'] = frame['code'].astype(str).str.upper().str.strip()
                            frame['date'] = pd.to_datetime(frame['date'], errors='coerce')
                            frame = frame.dropna(subset=['date', 'code'])
                            keep_cols = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
                            for col in keep_cols:
                                if col not in frame.columns:
                                    frame[col] = 0
                            frame = frame[keep_cols].copy()
                            for col in ['open', 'high', 'low', 'close', 'volume']:
                                frame[col] = pd.to_numeric(frame[col], errors='coerce').fillna(0)
                            self._local_history_cache[path] = frame

                frame = self._local_history_cache.get(path)
                if frame is None or frame.empty:
                    continue

                matched = frame[frame['code'].isin(variants)].copy()
                if matched.empty:
                    continue
                if start_date is not None:
                    matched = matched[matched['date'] >= pd.to_datetime(start_date)]
                if end_date is not None:
                    matched = matched[matched['date'] <= pd.to_datetime(end_date)]
                if matched.empty:
                    continue

                matched = matched.sort_values('date').drop_duplicates(subset=['date']).copy()
                matched['date'] = pd.to_datetime(matched['date']).dt.date
                matched.set_index('date', inplace=True)
                return matched[['open', 'high', 'low', 'close', 'volume']]
            except Exception as e:
                logger.debug(f"本地历史回退失败 {normalized} @ {path}: {e}")
                continue
        return None
    
    def get_stock_data_from_db(self, code, start_date=None, end_date=None):
        """从数据库获取股票历史数据，确保数值类型正确"""
        query = self.session.query(RawStockData).filter(
            RawStockData.code == str(code)
        )
        
        if start_date:
            query = query.filter(RawStockData.date >= start_date)
        if end_date:
            query = query.filter(RawStockData.date <= end_date)
        
        results = query.order_by(RawStockData.date).all()

        if not results:
            fund_query = self.session.query(RawFundData).filter(
                RawFundData.code == str(code)
            )
            if start_date:
                fund_query = fund_query.filter(RawFundData.date >= start_date)
            if end_date:
                fund_query = fund_query.filter(RawFundData.date <= end_date)
            fund_results = fund_query.order_by(RawFundData.date).all()
            if not fund_results:
                local_df = self._load_local_history_for_code(code, start_date=start_date, end_date=end_date)
                if local_df is not None and not local_df.empty:
                    return local_df
                return None

            df = pd.DataFrame([{
                'date': r.date,
                'open': float(r.nav) if r.nav else 0.0,
                'high': float(r.accumulated_nav or r.nav) if (r.accumulated_nav or r.nav) else 0.0,
                'low': float(r.nav) if r.nav else 0.0,
                'close': float(r.nav) if r.nav else 0.0,
                'volume': 0,
            } for r in fund_results])
        else:
            df = pd.DataFrame([{
                'date': r.date,
                'open': float(r.open) if r.open else 0.0,
                'high': float(r.high) if r.high else 0.0,
                'low': float(r.low) if r.low else 0.0,
                'close': float(r.close) if r.close else 0.0,
                'volume': int(r.volume) if r.volume else 0
            } for r in results])
        
        df.set_index('date', inplace=True)
        return df
    
    # ==================== 全市场股票池获取 ====================
    
    def fetch_all_a_stocks_from_akshare(self):
        """
        从 AkShare 获取全部A股列表（5000+只）
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取全部A股...")
            
            df = ak.stock_zh_a_spot_em()
            
            if df is not None and len(df) > 0:
                stocks = []
                for _, row in df.iterrows():
                    code = self._normalize_domestic_code(row['代码'])
                    
                    stocks.append({
                        'code': code,
                        'name': row['名称'],
                        'market': 'A',
                        'is_st': 'ST' in row['名称'] or '*ST' in row['名称']
                    })
                
                logger.info(f"✅ 获取到 {len(stocks)} 只A股")
                # 更新股票池
                self.a_stock_pool = [s['code'] for s in stocks if not s['is_st']]
                return stocks
        except Exception as e:
            logger.error(f"获取A股列表失败: {e}")
        return []
    
    def fetch_all_hk_stocks_from_akshare(self):
        """
        从 AkShare 获取全部港股列表（2500+只）
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取全部港股...")
            
            df = ak.stock_hk_spot_em()
            
            if df is not None and len(df) > 0:
                stocks = []
                for _, row in df.iterrows():
                    code = row['代码']
                    if '.HK' not in code:
                        code = f"{code}.HK"
                    
                    stocks.append({
                        'code': code,
                        'name': row['名称'],
                        'market': 'H'
                    })
                
                logger.info(f"✅ 获取到 {len(stocks)} 只港股")
                self.hk_stock_pool = [s['code'] for s in stocks]
                return stocks
        except Exception as e:
            logger.error(f"获取港股列表失败: {e}")
        return []
    
    def fetch_all_us_stocks(self):
        """
        获取美股列表，优先走 AkShare 的全市场接口，失败时再回退到核心大盘股名单。
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取美股列表...")
            df = ak.stock_us_spot_em()
            if df is not None and len(df) > 0:
                code_col = '代码' if '代码' in df.columns else ('symbol' if 'symbol' in df.columns else None)
                name_col = '名称' if '名称' in df.columns else ('name' if 'name' in df.columns else code_col)
                stocks = []
                if code_col:
                    for _, row in df.iterrows():
                        raw_code = str(row.get(code_col) or '').strip()
                        if not raw_code:
                            continue
                        code = raw_code.split('.')[-1].strip().upper()
                        if not code:
                            continue
                        stocks.append({
                            'code': code,
                            'name': str(row.get(name_col) or code).strip(),
                            'market': 'US'
                        })
                if stocks:
                    deduped = []
                    seen = set()
                    for item in stocks:
                        code = item['code']
                        if code in seen:
                            continue
                        seen.add(code)
                        deduped.append(item)
                    self.us_stock_pool = self._apply_configured_max([s['code'] for s in deduped], MAX_US_STOCKS)
                    logger.info(f"✅ 获取到 {len(self.us_stock_pool)} 只美股")
                    return deduped if int(MAX_US_STOCKS or 0) <= 0 else deduped[:int(MAX_US_STOCKS)]
        except Exception as e:
            logger.warning(f"AkShare美股列表获取失败，回退到核心股票池: {e}")

        # 标普500主要成分股
        sp500_codes = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
            'BRK-B', 'UNH', 'JNJ', 'JPM', 'V', 'PG', 'HD', 'MA',
            'CVX', 'ABBV', 'PFE', 'TMO', 'COST', 'AVGO', 'NFLX',
            'DIS', 'ADBE', 'CRM', 'AMD', 'INTC', 'QCOM', 'TXN',
            'IBM', 'CSCO', 'ORCL', 'PYPL', 'UBER', 'NKE', 'SBUX',
            'MCD', 'WMT', 'KO', 'PEP', 'BAC', 'WFC', 'C', 'GS',
            'MS', 'SPGI', 'BLK', 'AXP', 'VZ', 'T', 'TMUS', 'LOW',
            'NEE', 'LIN', 'MDT', 'BMY', 'MRK', 'ABT', 'DHR', 'LLY',
            'TGT', 'CCI', 'PSA', 'MRNA', 'BA', 'CAT', 'GE', 'HON',
            'RTX', 'LMT', 'UPS', 'FDX', 'NOC', 'GD', 'DE', 'MMM', 'SMCI', 'PLTR'
        ]

        china_stocks = [
            'BABA', 'PDD', 'JD', 'BIDU', 'NIO', 'LI', 'XPEV',
            'TCEHY', 'NTES', 'BILI', 'IQ', 'VIPS', 'YUMC', 'ZTO',
            'BEKE', 'TAL', 'EDU', 'YY', 'WB', 'MNSO'
        ]

        all_codes = self._apply_configured_max(list(dict.fromkeys(sp500_codes + china_stocks)), MAX_US_STOCKS)
        stocks = [{'code': code, 'name': code, 'market': 'US'} for code in all_codes]

        logger.info(f"✅ 获取到 {len(stocks)} 只美股")
        self.us_stock_pool = all_codes
        return stocks
    
    def update_all_stock_pools(self):
        """
        更新所有股票池（每日定时调用）
        """
        logger.info("开始更新全市场股票池...")
        
        a_stocks = self.fetch_all_a_stocks_from_akshare()
        h_stocks = self.fetch_all_hk_stocks_from_akshare()
        u_stocks = self.fetch_all_us_stocks()
        
        # 保存到文件，避免每次都重新获取
        import json
        pool_data = {
            'a_stock': self.a_stock_pool,
            'hk_stock': self.hk_stock_pool,
            'us_stock': self.us_stock_pool,
            'updated_at': datetime.now().isoformat()
        }
        
        with open('data/stock_pool_cache.json', 'w') as f:
            json.dump(pool_data, f)
        
        logger.info(f"股票池更新完成: A股{len(self.a_stock_pool)}只, 港股{len(self.hk_stock_pool)}只, 美股{len(self.us_stock_pool)}只")
        
        return {
            'a_stock_count': len(self.a_stock_pool),
            'hk_stock_count': len(self.hk_stock_pool),
            'us_stock_count': len(self.us_stock_pool)
        }
    
    def load_stock_pool_from_cache(self):
        """
        从缓存加载股票池
        """
        import json
        cache_file = 'data/stock_pool_cache.json'
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    pool_data = json.load(f)
                    self.a_stock_pool = pool_data.get('a_stock', self.a_stock_pool)
                    self.hk_stock_pool = pool_data.get('hk_stock', self.hk_stock_pool)
                    self.us_stock_pool = pool_data.get('us_stock', self.us_stock_pool)
                    logger.info(f"从缓存加载股票池: A股{len(self.a_stock_pool)}只")
                    return True
            except:
                pass
        return False
    
    # ==================== 批量采集优化 ====================
    
    def collect_batch(self, codes, market='A', years=2, delay=0.5, callback=None):
        """
        批量采集股票历史数据
        Args:
            codes: 股票代码列表
            market: 市场类型
            years: 采集年数
            delay: 每次请求间隔（秒）
            callback: 进度回调函数
        Returns:
            dict: 采集结果统计
        """
        results = {'success': 0, 'failed': 0, 'total': len(codes), 'details': []}
        
        for i, code in enumerate(codes):
            try:
                if callback:
                    callback(i+1, len(codes), code, 'processing')
                
                # 检查是否已有足够数据
                existing = self.session.query(RawStockData).filter(
                    RawStockData.code == code
                ).count()
                
                if existing > 200:
                    if callback:
                        callback(i+1, len(codes), code, 'skipped', f'已有{existing}条')
                    results['success'] += 1
                    results['details'].append({'code': code, 'status': 'skipped', 'records': existing})
                    continue
                
                # 采集历史数据
                df = self.collect_history(code, period=f'{years}y')
                
                if df is not None and len(df) > 0:
                    count = self._save_history_batch(code, df, market)
                    results['success'] += 1
                    results['details'].append({'code': code, 'status': 'success', 'records': count})
                    if callback:
                        callback(i+1, len(codes), code, 'success', f'{count}条')
                else:
                    results['failed'] += 1
                    results['details'].append({'code': code, 'status': 'failed', 'reason': '无数据'})
                    if callback:
                        callback(i+1, len(codes), code, 'failed', '无数据')
                        
            except Exception as e:
                results['failed'] += 1
                results['details'].append({'code': code, 'status': 'failed', 'reason': str(e)})
                if callback:
                    callback(i+1, len(codes), code, 'failed', str(e)[:50])
            
            time.sleep(delay)
        
        return results
    
    def _save_history_batch(self, code, df, market):
        """批量保存历史数据"""
        count = 0
        for _, row in df.iterrows():
            existing = self.session.query(RawStockData).filter(
                RawStockData.code == code,
                RawStockData.date == row['date']
            ).first()
            
            if not existing:
                record = RawStockData(
                    code=code,
                    name=code.split('.')[0],
                    date=row['date'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume'],
                    market=market
                )
                self.session.add(record)
                count += 1
        
        self.session.commit()
        return count
    
    def collect_top_stocks(self, limit=100, years=2):
        """
        只采集前N只股票（用于测试或快速更新）
        Args:
            limit: 采集数量
            years: 年数
        """
        print(f"\n开始采集前 {limit} 只股票...")
        
        # 合并所有股票池
        all_stocks = []
        for code in self.a_stock_pool[:limit//3]:
            all_stocks.append((code, 'A'))
        for code in self.hk_stock_pool[:limit//3]:
            all_stocks.append((code, 'H'))
        for code in self.us_stock_pool[:limit//3]:
            all_stocks.append((code, 'US'))
        
        results = {'success': 0, 'failed': 0}
        
        for i, (code, market) in enumerate(all_stocks):
            print(f"[{i+1}/{len(all_stocks)}] 采集 {code}...", end=' ')
            try:
                df = self.collect_history(code, period=f'{years}y')
                if df is not None and len(df) > 0:
                    count = self._save_history_batch(code, df, market)
                    print(f"✅ {count}条")
                    results['success'] += 1
                else:
                    print(f"❌ 无数据")
                    results['failed'] += 1
            except Exception as e:
                print(f"❌ {e}")
                results['failed'] += 1
            time.sleep(0.5)
        
        print(f"\n采集完成: 成功{results['success']}, 失败{results['failed']}")
        return results

    # 验证股票是否有效
    def is_valid_stock(self, code):
        """
        快速检查股票是否有效（有行情数据）
        """
        try:
            import yfinance as yf
            ticker = yf.Ticker(code)
            # 获取最近1天数据
            hist = ticker.history(period='1d')
            if len(hist) > 0:
                return True
            # 尝试获取最近5天
            hist = ticker.history(period='5d')
            return len(hist) > 0
        except Exception as e:
            logger.debug(f"检查 {code} 有效性失败: {e}")
            return False
    
    def filter_valid_stocks(self, codes, max_check=10):
        """
        批量过滤有效股票
        Args:
            codes: 股票代码列表
            max_check: 每次检查的最大数量（避免太慢）
        Returns:
            list: 有效股票代码列表
        """
        valid = []
        for code in codes[:max_check]:
            if self.is_valid_stock(code):
                valid.append(code)
            time.sleep(0.1)
        return valid
    
    # ==================== 港股采集 ====================
    
    def fetch_hk_stocks_from_akshare(self):
        """
        从 AkShare 获取全部港股列表
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取全部港股...")
            
            df = ak.stock_hk_spot_em()
            
            if df is not None and len(df) > 0:
                stocks = []
                for _, row in df.iterrows():
                    code = row['代码']
                    if '.HK' not in code:
                        code = f"{code}.HK"
                    
                    stocks.append({
                        'code': code,
                        'name': row['名称'],
                        'market': 'H'
                    })
                
                logger.info(f"✅ 获取到 {len(stocks)} 只港股")
                self.hk_stock_pool = [s['code'] for s in stocks]
                return stocks
        except Exception as e:
            logger.error(f"获取港股列表失败: {e}")
        return []
    
    def collect_hk_stocks_batch(self, codes=None, years=3, limit=None):
        """
        批量采集港股历史数据
        """
        if codes is None:
            use_limit = len(self.hk_stock_pool) if limit is None else int(limit)
            max_cfg = int(MAX_HK_STOCKS or 0)
            if max_cfg > 0 and use_limit > 0:
                use_limit = min(use_limit, max_cfg)
            codes = self.hk_stock_pool[:use_limit]
        
        return self._collect_batch(codes, 'H', years)
    
    # ==================== 美股采集 ====================
    
    def fetch_us_stocks_from_yfinance(self):
        """
        获取美股主要成分股列表
        """
        # 标普500主要成分股 + 中概股
        sp500_codes = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
            'BRK-B', 'UNH', 'JNJ', 'JPM', 'V', 'PG', 'HD', 'MA',
            'CVX', 'ABBV', 'PFE', 'TMO', 'COST', 'AVGO', 'NFLX',
            'DIS', 'ADBE', 'CRM', 'AMD', 'INTC', 'QCOM', 'TXN',
            'IBM', 'CSCO', 'ORCL', 'PYPL', 'UBER', 'NKE', 'SBUX',
            'MCD', 'WMT', 'KO', 'PEP', 'BAC', 'WFC', 'C', 'GS',
            'MS', 'SPGI', 'BLK', 'AXP', 'VZ', 'T', 'TMUS'
        ]
        
        china_stocks = [
            'BABA', 'PDD', 'JD', 'BIDU', 'NIO', 'LI', 'XPEV',
            'NTES', 'BILI', 'IQ', 'VIPS', 'YUMC', 'ZTO', 'TAL'
        ]
        
        all_codes = list(set(sp500_codes + china_stocks))
        
        stocks = [{'code': code, 'name': code, 'market': 'US'} for code in all_codes]
        
        logger.info(f"✅ 获取到 {len(stocks)} 只美股")
        self.us_stock_pool = all_codes
        return stocks
    
    def collect_us_stocks_batch(self, codes=None, years=3, limit=None):
        """
        批量采集美股历史数据
        """
        if codes is None:
            use_limit = len(self.us_stock_pool) if limit is None else int(limit)
            max_cfg = int(MAX_US_STOCKS or 0)
            if max_cfg > 0 and use_limit > 0:
                use_limit = min(use_limit, max_cfg)
            codes = self.us_stock_pool[:use_limit]
        
        return self._collect_batch(codes, 'US', years)
    
    # ==================== 基金采集 ====================
    
    def fetch_funds_from_akshare(self):
        """
        从 AkShare 获取基金列表
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取基金列表...")
            
            funds = []
            
            # 获取全市场基金列表
            stock_funds = ak.fund_name_em()
            if stock_funds is not None and len(stock_funds) > 0:
                for _, row in stock_funds.iterrows():
                    fund_name = str(row.get('基金简称') or '').strip()
                    fund_type = str(row.get('基金类型') or '').strip()
                    funds.append({
                        'code': str(row.get('基金代码') or '').strip(),
                        'name': fund_name,
                        'type': 'etf' if 'ETF' in fund_name.upper() or 'ETF' in fund_type.upper() else 'active_fund'
                    })
                logger.info(f"获取到 {len(stock_funds)} 只基金")
            
            # 获取ETF
            try:
                etf_funds = ak.fund_etf_spot_em()
                if etf_funds is not None and len(etf_funds) > 0:
                    for _, row in etf_funds.iterrows():
                        funds.append({
                            'code': row['代码'],
                            'name': row['名称'],
                            'type': 'etf'
                        })
                    logger.info(f"获取到 {len(etf_funds)} 只ETF")
            except:
                pass
            
            deduped = []
            seen = set()
            for item in funds:
                code = str(item.get('code') or '').strip()
                if not code or code in seen:
                    continue
                seen.add(code)
                deduped.append(item)

            logger.info(f"✅ 共获取到 {len(deduped)} 只基金")
            self.fund_pool = deduped
            return deduped
            
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return self._get_default_funds()
    
    def _get_default_funds(self):
        """获取默认基金池"""
        return [
            {'code': '110011', 'name': '易方达中小盘', 'type': 'active_fund'},
            {'code': '519069', 'name': '汇添富价值精选', 'type': 'active_fund'},
            {'code': '163402', 'name': '兴全趋势投资', 'type': 'active_fund'},
            {'code': '260108', 'name': '景顺长城新兴成长', 'type': 'active_fund'},
            {'code': '161005', 'name': '富国天惠成长', 'type': 'active_fund'},
            {'code': '510300.SH', 'name': '沪深300ETF', 'type': 'etf'},
            {'code': '510500.SH', 'name': '中证500ETF', 'type': 'etf'},
            {'code': '510050.SH', 'name': '上证50ETF', 'type': 'etf'},
        ]
    
    def collect_funds_batch(self, funds=None, years=3, limit=None):
        """
        批量采集基金历史净值数据
        """
        if funds is None:
            # 获取更多基金
            all_funds = self.fetch_funds_from_akshare()
            if all_funds:
                if limit is None:
                    funds = all_funds
                else:
                    funds = all_funds[:int(limit)]
            else:
                funds = self._get_default_funds()
        
        results = []
        
        for fund in funds:
            code = fund['code']
            name = fund['name']
            fund_type = fund['type']
            
            logger.info(f"采集基金 {name} ({code})...")
            
            try:
                import akshare as ak
                df = ak.fund_open_fund_info_em(symbol=code, indicator='单位净值走势')
                
                if df is not None and len(df) > 0:
                    count = 0
                    for _, row in df.iterrows():
                        existing = self.session.query(RawStockData).filter(
                            RawStockData.code == code,
                            RawStockData.date == row['净值日期']
                        ).first()
                        
                        if not existing:
                            record = RawStockData(
                                code=code,
                                name=name,
                                date=row['净值日期'],
                                open=row['单位净值'],
                                high=row['单位净值'],
                                low=row['单位净值'],
                                close=row['单位净值'],
                                volume=0,
                                market='FUND'
                            )
                            self.session.add(record)
                            count += 1
                    
                    self.session.commit()
                    logger.info(f"✅ {name} 采集完成，{count} 条")
                    results.append({'code': code, 'name': name, 'records': count})
                else:
                    logger.warning(f"⚠️ {name} 无数据")
                    results.append({'code': code, 'name': name, 'records': 0})
                    
            except Exception as e:
                logger.error(f"❌ {name} 采集失败: {e}")
                results.append({'code': code, 'name': name, 'records': 0, 'error': str(e)})
        
        return results
    
    # ==================== 黄金白银采集 ====================
    
    def collect_precious_metals(self, years=3):
        """
        采集黄金和白银历史数据
        """
        assets = [
            {'code': 'GC=F', 'name': '黄金期货', 'market': 'COMEX'},
            {'code': 'SI=F', 'name': '白银期货', 'market': 'COMEX'},
            {'code': 'XAUUSD=X', 'name': '伦敦金现货', 'market': 'FX'},
            {'code': 'XAGUSD=X', 'name': '伦敦银现货', 'market': 'FX'},
            {'code': 'GLD', 'name': '黄金ETF', 'market': 'US'},
            {'code': 'IAU', 'name': 'iShares黄金ETF', 'market': 'US'},
            {'code': 'SLV', 'name': '白银ETF', 'market': 'US'},
            {'code': 'SIVR', 'name': '实物白银ETF', 'market': 'US'},
            {'code': '518880.SH', 'name': '华安黄金ETF', 'market': 'A'},
            {'code': '518800.SH', 'name': '国泰黄金ETF', 'market': 'A'},
            {'code': '159934.SZ', 'name': '黄金ETF联接', 'market': 'A'},
        ]
        
        results = []
        
        for asset in assets:
            code = asset['code']
            name = asset['name']
            market = asset['market']
            
            logger.info(f"采集 {name} ({code})...")
            
            try:
                import yfinance as yf
                ticker = yf.Ticker(code)
                df = ticker.history(period=f'{years}y')
                
                if df is not None and len(df) > 0:
                    count = 0
                    for date, row in df.iterrows():
                        existing = self.session.query(RawStockData).filter(
                            RawStockData.code == code,
                            RawStockData.date == date.date()
                        ).first()
                        
                        if not existing:
                            record = RawStockData(
                                code=code,
                                name=name,
                                date=date.date(),
                                open=row['Open'],
                                high=row['High'],
                                low=row['Low'],
                                close=row['Close'],
                                volume=int(row['Volume']) if row['Volume'] else 0,
                                market=market
                            )
                            self.session.add(record)
                            count += 1
                    
                    self.session.commit()
                    logger.info(f"✅ {name} 采集完成，新增 {count} 条")
                    results.append({'code': code, 'name': name, 'records': count})
                else:
                    logger.warning(f"⚠️ {name} 无数据")
                    results.append({'code': code, 'name': name, 'records': 0})
                    
            except Exception as e:
                logger.error(f"❌ {name} 采集失败: {e}")
                results.append({'code': code, 'name': name, 'records': 0, 'error': str(e)})
        
        return results
    
    # ==================== 通用批量采集 ====================
    
    def _collect_batch(self, codes, market, years=3, delay=0.5):
        """
        通用批量采集方法
        """
        results = {'success': 0, 'failed': 0, 'details': []}
        
        for i, code in enumerate(codes):
            logger.info(f"[{i+1}/{len(codes)}] 采集 {code}...")
            
            try:
                df = self.collect_history(code, period=f'{years}y')
                
                if df is not None and len(df) > 0:
                    count = 0
                    for _, row in df.iterrows():
                        existing = self.session.query(RawStockData).filter(
                            RawStockData.code == code,
                            RawStockData.date == row['date']
                        ).first()
                        
                        if not existing:
                            record = RawStockData(
                                code=code,
                                name=code.split('.')[0],
                                date=row['date'],
                                open=row['open'],
                                high=row['high'],
                                low=row['low'],
                                close=row['close'],
                                volume=row['volume'],
                                market=market
                            )
                            self.session.add(record)
                            count += 1
                    
                    self.session.commit()
                    logger.info(f"✅ {code} 完成，{count} 条")
                    results['success'] += 1
                    results['details'].append({'code': code, 'status': 'success', 'records': count})
                else:
                    logger.warning(f"⚠️ {code} 无数据")
                    results['failed'] += 1
                    results['details'].append({'code': code, 'status': 'failed', 'reason': '无数据'})
                    
            except Exception as e:
                logger.error(f"❌ {code} 失败: {e}")
                results['failed'] += 1
                results['details'].append({'code': code, 'status': 'failed', 'reason': str(e)})
                self.session.rollback()
            
            time.sleep(delay)
        
        return results
    
    # ==================== 一键采集所有资产 ====================
    
    def collect_all_assets(self, years=3, hk_limit=None, us_limit=None, fund_limit=None):
        """
        一键采集所有资产数据
        Args:
            years: 采集年数
            hk_limit: 港股采集数量（默认500）
            us_limit: 美股采集数量（默认500）
            fund_limit: 基金采集数量（默认200）
        """
        logger.info("=" * 50)
        logger.info("开始采集所有资产数据")
        logger.info("=" * 50)
        
        all_results = {}
        
        # 1. 黄金白银（全量）
        logger.info("\n[1/4] 采集黄金白银...")
        all_results['precious_metals'] = self.collect_precious_metals(years)
        
        # 2. 基金（增加数量）
        logger.info("\n[2/4] 采集基金数据...")
        all_results['funds'] = self.collect_funds_batch(limit=fund_limit)
        
        # 3. 港股（增加数量）
        logger.info("\n[3/4] 采集港股数据...")
        hk_use_limit = len(self.hk_stock_pool) if hk_limit is None else int(hk_limit)
        max_hk_cfg = int(MAX_HK_STOCKS or 0)
        if max_hk_cfg > 0 and hk_use_limit > 0:
            hk_use_limit = min(hk_use_limit, max_hk_cfg)
        hk_codes = self.hk_stock_pool[:hk_use_limit]
        all_results['hk_stocks'] = self._collect_batch(hk_codes, 'H', years)
        
        # 4. 美股（增加数量）
        logger.info("\n[4/4] 采集美股数据...")
        us_use_limit = len(self.us_stock_pool) if us_limit is None else int(us_limit)
        max_us_cfg = int(MAX_US_STOCKS or 0)
        if max_us_cfg > 0 and us_use_limit > 0:
            us_use_limit = min(us_use_limit, max_us_cfg)
        us_codes = self.us_stock_pool[:us_use_limit]
        all_results['us_stocks'] = self._collect_batch(us_codes, 'US', years)
        
        logger.info("=" * 50)
        logger.info("所有资产采集完成")
        logger.info("=" * 50)
        
        return all_results
    
    # ==================== 资金流向采集 ====================
    
    def get_money_flow(self, code, days=30):
        """
        获取资金流向数据
        Args:
            code: 股票代码
            days: 天数
        Returns:
            DataFrame: 每日资金流向
        """
        try:
            import akshare as ak
            
            if '.SZ' in code or '.SH' in code:
                # A股资金流向
                symbol = code.replace('.SH', '').replace('.SZ', '')
                df = ak.stock_individual_fund_flow(stock=symbol, market='sh' if code.endswith('.SH') else 'sz')
                
                if df is not None and len(df) > 0:
                    df = df.rename(columns={
                        '日期': 'date',
                        '主力净流入': 'main_flow',
                        '超大单净流入': 'super_flow',
                        '大单净流入': 'big_flow',
                        '中单净流入': 'mid_flow',
                        '小单净流入': 'small_flow'
                    })
                    df['date'] = pd.to_datetime(df['date'])
                    return df
        except Exception as e:
            logger.error(f"获取资金流向失败 {code}: {e}")
        
        return None
    
    def get_north_money(self, days=30):
        """
        获取北向资金数据
        """
        try:
            import akshare as ak
            df = ak.stock_hsgt_north_net_flow_in_em(symbol='北上')
            
            if df is not None and len(df) > 0:
                df = df.rename(columns={'日期': 'date', '净买入': 'north_flow'})
                df['date'] = pd.to_datetime(df['date'])
                return df
        except Exception as e:
            logger.error(f"获取北向资金失败: {e}")
        
        return None


if __name__ == '__main__':
    collector = StockCollector()
    data = collector.collect_realtime('AAPL', market='US')
    if data:
        print(f"AAPL: ${data['close']:.2f}")
        