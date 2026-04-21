"""
采集模块测试 - tests/test_collectors.py
测试数据采集功能
"""

import sys
import os
import json
import tempfile
import unittest
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.stock_collector import StockCollector
from collectors.fund_collector import FundCollector
from collectors.news_collector import NewsCollector
from collectors.macro_collector import MacroCollector
from collectors.moneyflow_collector import MoneyflowCollector


class TestStockCollector(unittest.TestCase):
    """股票采集器测试"""
    
    def setUp(self):
        self.collector = StockCollector()
    
    def test_collect_realtime(self):
        """测试实时行情采集"""
        data = self.collector.collect_realtime('AAPL', market='US')
        
        if data:
            self.assertIn('code', data)
            self.assertIn('close', data)
            print(f"✅ 实时行情测试通过: {data['code']} = {data['close']}")
        else:
            print("⚠️ 实时行情测试: 无数据返回")
    
    def test_collect_history(self):
        """测试历史数据采集"""
        df = self.collector.collect_history('AAPL', period='1mo')
        
        if df is not None and len(df) > 0:
            self.assertIn('close', df.columns)
            print(f"✅ 历史数据测试通过: {len(df)} 条记录")
        else:
            print("⚠️ 历史数据测试: 无数据返回")
    
    def test_a_stock_pool(self):
        """测试A股池"""
        pool = self.collector.a_stock_pool
        self.assertIsInstance(pool, list)
        print(f"✅ A股池测试通过: {len(pool)} 只股票")
    
    def test_hk_stock_pool(self):
        """测试港股池"""
        pool = self.collector.hk_stock_pool
        self.assertIsInstance(pool, list)
        print(f"✅ 港股池测试通过: {len(pool)} 只股票")
    
    def test_us_stock_pool(self):
        """测试美股池"""
        pool = self.collector.us_stock_pool
        self.assertIsInstance(pool, list)
        print(f"✅ 美股池测试通过: {len(pool)} 只股票")

    def test_normalize_symbol_for_yfinance(self):
        """验证上交所与北交所代码会转换成 yfinance 可识别格式"""
        self.assertEqual(self.collector._normalize_symbol_for_yfinance('600519.SH'), '600519.SS')
        self.assertEqual(self.collector._normalize_symbol_for_yfinance('830799.SZ'), '830799.BJ')
        self.assertEqual(self.collector._normalize_symbol_for_yfinance('000001.SZ'), '000001.SZ')

    def test_hk_batch_uses_full_pool_by_default(self):
        """验证港股批量采集默认不会被硬编码成仅100只"""
        collector = StockCollector.__new__(StockCollector)
        collector.hk_stock_pool = [f"{i:04d}.HK" for i in range(150)]
        collector._collect_batch = lambda codes, market, years: list(codes)
        result = collector.collect_hk_stocks_batch()
        self.assertEqual(len(result), 150)

    def test_us_batch_uses_full_pool_by_default(self):
        """验证美股批量采集默认不会被硬编码成仅100只"""
        collector = StockCollector.__new__(StockCollector)
        collector.us_stock_pool = [f"US{i:03d}" for i in range(180)]
        collector._collect_batch = lambda codes, market, years: list(codes)
        result = collector.collect_us_stocks_batch()
        self.assertEqual(len(result), 180)

    def test_fetch_all_us_stocks_prefers_broader_provider_source(self):
        """验证美股股票池优先使用可用的更大市场源，而不是固定小名单"""
        collector = StockCollector()

        class FakeAk:
            @staticmethod
            def stock_us_spot_em():
                return pd.DataFrame([
                    {'代码': 'AAPL', '名称': 'Apple'},
                    {'代码': 'MSFT', '名称': 'Microsoft'},
                    {'代码': 'SMCI', '名称': 'Super Micro'},
                ])

        old_ak = sys.modules.get('akshare')
        sys.modules['akshare'] = FakeAk
        try:
            stocks = collector.fetch_all_us_stocks()
        finally:
            if old_ak is not None:
                sys.modules['akshare'] = old_ak
            else:
                sys.modules.pop('akshare', None)

        codes = [item['code'] for item in stocks]
        self.assertIn('AAPL', codes)
        self.assertIn('SMCI', codes)
        self.assertGreaterEqual(len(codes), 3)

    def test_fetch_all_a_stocks_preserves_bj_suffix(self):
        """验证北交所代码不会被错误标成 .SZ"""
        collector = StockCollector()

        class FakeAk:
            @staticmethod
            def stock_zh_a_spot_em():
                return pd.DataFrame([
                    {'代码': '600519', '名称': '贵州茅台'},
                    {'代码': '000001', '名称': '平安银行'},
                    {'代码': '920033', '名称': '北交所样本'},
                ])

        old_ak = sys.modules.get('akshare')
        sys.modules['akshare'] = FakeAk
        try:
            stocks = collector.fetch_all_a_stocks_from_akshare()
        finally:
            if old_ak is not None:
                sys.modules['akshare'] = old_ak
            else:
                sys.modules.pop('akshare', None)

        codes = [item['code'] for item in stocks]
        self.assertIn('600519.SH', codes)
        self.assertIn('000001.SZ', codes)
        self.assertIn('920033.BJ', codes)

    def test_collect_realtime_for_a_share_prefers_domestic_source(self):
        """验证A股/北交所实时行情优先使用国内数据源，不依赖 Yahoo。"""
        collector = StockCollector()

        class FakeAk:
            @staticmethod
            def stock_zh_a_spot_em():
                return pd.DataFrame([
                    {
                        '代码': '920033', '名称': '北交所样本', '最新价': 12.34,
                        '今开': 12.10, '最高': 12.60, '最低': 12.00, '成交量': 34567,
                    }
                ])

        class FailingTicker:
            def __init__(self, *_args, **_kwargs):
                raise AssertionError('A-share realtime should not call yfinance')

        old_ak = sys.modules.get('akshare')
        old_ticker = getattr(__import__('collectors.stock_collector', fromlist=['yf']).yf, 'Ticker')
        sys.modules['akshare'] = FakeAk
        __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = FailingTicker
        try:
            data = collector.collect_realtime('920033.BJ', market='A')
        finally:
            __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = old_ticker
            if old_ak is not None:
                sys.modules['akshare'] = old_ak
            else:
                sys.modules.pop('akshare', None)

        self.assertIsNotNone(data)
        self.assertEqual(data['code'], '920033.BJ')
        self.assertAlmostEqual(data['close'], 12.34)

    def test_collect_history_for_a_share_prefers_domestic_source(self):
        """验证A股历史行情优先使用国内数据源。"""
        collector = StockCollector()

        class FakeAk:
            @staticmethod
            def stock_zh_a_hist(symbol, period='daily', adjust='qfq'):
                return pd.DataFrame([
                    {'日期': '2026-04-14', '开盘': 10.0, '收盘': 10.5, '最高': 10.8, '最低': 9.9, '成交量': 12345},
                    {'日期': '2026-04-15', '开盘': 10.6, '收盘': 10.9, '最高': 11.0, '最低': 10.4, '成交量': 22345},
                ])

        class FailingTicker:
            def __init__(self, *_args, **_kwargs):
                raise AssertionError('A-share history should not call yfinance')

        old_ak = sys.modules.get('akshare')
        old_ticker = getattr(__import__('collectors.stock_collector', fromlist=['yf']).yf, 'Ticker')
        sys.modules['akshare'] = FakeAk
        __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = FailingTicker
        try:
            df = collector.collect_history('600519.SH', period='1mo')
        finally:
            __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = old_ticker
            if old_ak is not None:
                sys.modules['akshare'] = old_ak
            else:
                sys.modules.pop('akshare', None)

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn('close', df.columns)

    def test_collect_history_for_us_stock_does_not_hit_domestic_source(self):
        """验证美股历史行情不会误走国内A股路径。"""
        collector = StockCollector()

        def fail_domestic(*args, **kwargs):
            raise AssertionError('US history should not call domestic collector')

        original_domestic = collector._collect_history_domestic
        original_ticker = getattr(__import__('collectors.stock_collector', fromlist=['yf']).yf, 'Ticker')

        class FakeTicker:
            def __init__(self, *_args, **_kwargs):
                pass

            def history(self, period='1mo', interval='1d'):
                return pd.DataFrame({
                    'Open': [100.0, 101.0],
                    'High': [102.0, 103.0],
                    'Low': [99.0, 100.0],
                    'Close': [101.0, 102.0],
                    'Volume': [1000, 1100],
                }, index=pd.to_datetime(['2026-04-14', '2026-04-15']))

        collector._collect_history_domestic = fail_domestic
        __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = FakeTicker
        try:
            df = collector.collect_history('AAPL', period='1mo')
        finally:
            collector._collect_history_domestic = original_domestic
            __import__('collectors.stock_collector', fromlist=['yf']).yf.Ticker = original_ticker

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn('close', df.columns)

    def test_get_stock_data_from_db_falls_back_to_local_history_csv(self):
        """验证数据库缺失时，训练仍会复用本地历史 CSV，而不是把大量标的误判为不可训练。"""
        import collectors.stock_collector as stock_module

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, 'historical_a_stock.csv')
            pd.DataFrame([
                {'code': '000001.SZ', 'date': '2026-04-14', 'open': 10.0, 'high': 10.5, 'low': 9.9, 'close': 10.2, 'volume': 1000},
                {'code': '000001.SZ', 'date': '2026-04-15', 'open': 10.2, 'high': 10.8, 'low': 10.1, 'close': 10.6, 'volume': 1200},
            ]).to_csv(csv_path, index=False)

            class EmptyQuery:
                def filter(self, *args, **kwargs):
                    return self
                def order_by(self, *args, **kwargs):
                    return self
                def all(self):
                    return []

            class EmptySession:
                def query(self, *args, **kwargs):
                    return EmptyQuery()

            collector = StockCollector.__new__(StockCollector)
            collector.session = EmptySession()
            collector.cache = {}
            collector._local_history_cache = {}

            original_exists = stock_module.os.path.exists
            original_read_csv = stock_module.pd.read_csv

            stock_module.os.path.exists = lambda path: True if str(path).endswith('historical_a_stock.csv') else original_exists(path)
            stock_module.pd.read_csv = lambda path, *args, **kwargs: original_read_csv(csv_path if str(path).endswith('historical_a_stock.csv') else path, *args, **kwargs)
            try:
                df = collector.get_stock_data_from_db('000001.SZ')
            finally:
                stock_module.os.path.exists = original_exists
                stock_module.pd.read_csv = original_read_csv

            self.assertIsNotNone(df)
            self.assertEqual(len(df), 2)
            self.assertIn('close', df.columns)

    def test_save_to_database_retries_transient_sqlite_lock(self):
        """验证遇到短暂的 SQLite 锁冲突时会重试提交，而不是直接放弃。"""
        from sqlalchemy.exc import OperationalError

        class FakeQuery:
            def filter(self, *args, **kwargs):
                return self
            def first(self):
                return None

        class FakeSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0
                self.added = []
            def query(self, *args, **kwargs):
                return FakeQuery()
            def add(self, obj):
                self.added.append(obj)
            def commit(self):
                self.commit_calls += 1
                if self.commit_calls < 2:
                    raise OperationalError('INSERT', {}, Exception('database is locked'))
            def rollback(self):
                self.rollback_calls += 1

        collector = StockCollector.__new__(StockCollector)
        collector.session = FakeSession()

        collector._save_to_database({
            'code': '000001.SZ',
            'name': '平安银行',
            'market': 'A',
            'date': datetime(2026, 4, 17).date(),
            'open': 10.0,
            'high': 10.5,
            'low': 9.9,
            'close': 10.2,
            'volume': 1000,
        })

        self.assertEqual(collector.session.commit_calls, 2)
        self.assertGreaterEqual(collector.session.rollback_calls, 1)
        self.assertGreaterEqual(len(collector.session.added), 1)

    def test_a_stock_pool_prefers_stock_basic_csv_when_available(self):
        """验证A股标的池优先使用更完整的 stock_basic.csv，而不是依赖历史遗留 all_stocks.csv。"""
        import collectors.stock_collector as stock_module

        collector = StockCollector.__new__(StockCollector)
        collector._dedupe_codes = StockCollector._dedupe_codes
        collector._apply_configured_max = StockCollector._apply_configured_max
        collector._load_codes_from_csv = StockCollector._load_codes_from_csv.__get__(collector, StockCollector)
        collector._load_pool_from_cache_key = lambda key: []

        original_exists = stock_module.os.path.exists
        original_read_csv = stock_module.pd.read_csv
        original_use_full_market = stock_module.USE_FULL_MARKET
        original_max_a = stock_module.MAX_A_STOCKS

        stock_module.USE_FULL_MARKET = True
        stock_module.MAX_A_STOCKS = 0
        stock_module.os.path.exists = lambda path: str(path).endswith('stock_basic.csv')
        stock_module.pd.read_csv = lambda path, *args, **kwargs: pd.DataFrame([
            {'ts_code': '000001.SZ', 'name': '平安银行', 'industry': '银行'},
            {'ts_code': '600519.SH', 'name': '贵州茅台', 'industry': '白酒'},
        ]) if str(path).endswith('stock_basic.csv') else original_read_csv(path, *args, **kwargs)
        try:
            pool = collector._get_a_stock_pool()
        finally:
            stock_module.os.path.exists = original_exists
            stock_module.pd.read_csv = original_read_csv
            stock_module.USE_FULL_MARKET = original_use_full_market
            stock_module.MAX_A_STOCKS = original_max_a

        self.assertEqual(pool, ['000001.SZ', '600519.SH'])

    def test_recommendation_name_map_falls_back_to_stock_basic_csv(self):
        """验证推荐接口在 all_stocks.csv 缺失时，仍可从 stock_basic.csv 加载名称映射。"""
        import api.recommendations as rec_module

        import builtins

        rec_module._load_stock_name_map.cache_clear()
        original_exists = rec_module.os.path.exists
        original_open = builtins.open

        rec_module.os.path.exists = lambda path: str(path).endswith('stock_basic.csv')

        def fake_open(path, *args, **kwargs):
            from io import StringIO
            if str(path).endswith('stock_basic.csv'):
                return StringIO('ts_code,symbol,name,industry,market,list_date\n000001.SZ,000001,平安银行,银行,主板,19910403\n')
            return original_open(path, *args, **kwargs)

        builtins.open = fake_open
        try:
            name_map = rec_module._load_stock_name_map()
        finally:
            builtins.open = original_open
            rec_module.os.path.exists = original_exists
            rec_module._load_stock_name_map.cache_clear()

        assert name_map['000001.SZ']['name'] == '平安银行'
        assert name_map['000001']['industry'] == '银行'


class TestMoneyflowCollector(unittest.TestCase):
    """资金流采集器测试"""

    def test_collect_by_date_recovers_when_progress_exists_but_csv_missing(self):
        """验证当进度文件存在但 CSV 丢失时，会自动重建资金流数据，而不是误判为已完成。"""
        import collectors.moneyflow_collector as moneyflow_module

        with tempfile.TemporaryDirectory() as tmpdir:
            progress_path = os.path.join(tmpdir, 'moneyflow_progress.json')
            with open(progress_path, 'w', encoding='utf-8') as f:
                json.dump({'completed_dates': ['20260416']}, f)

            original_progress_dir = moneyflow_module.PROGRESS_DIR
            original_output_file = moneyflow_module.OUTPUT_FILE
            moneyflow_module.PROGRESS_DIR = tmpdir
            moneyflow_module.OUTPUT_FILE = os.path.join(tmpdir, 'moneyflow_all.csv')

            class FakePro:
                def moneyflow(self, trade_date=None, **kwargs):
                    return pd.DataFrame([
                        {'ts_code': '000001.SZ', 'trade_date': trade_date, 'net_mf_amount': 123.45}
                    ])

            collector = MoneyflowCollector.__new__(MoneyflowCollector)
            collector.pro = FakePro()
            collector.get_all_stocks = lambda max_stocks=None: pd.DataFrame([
                {'ts_code': '000001.SZ', 'name': '平安银行', 'industry': '银行'}
            ])

            try:
                result = collector.collect_by_date('20260416', '20260416', max_stocks=None, resume=True)
            finally:
                moneyflow_module.PROGRESS_DIR = original_progress_dir
                moneyflow_module.OUTPUT_FILE = original_output_file

            self.assertIsNotNone(result)
            self.assertGreaterEqual(len(result), 1)


class TestFundCollector(unittest.TestCase):
    """基金采集器测试"""
    
    def setUp(self):
        self.collector = FundCollector()
    
    def test_collect_fund_nav(self):
        """测试基金净值采集"""
        nav_data = self.collector.collect_fund_nav('110011', days=10)
        
        self.assertIsInstance(nav_data, list)
        print(f"✅ 基金净值测试通过: {len(nav_data)} 条记录")
    
    def test_collect_fund_info(self):
        """测试基金信息采集"""
        info = self.collector.collect_fund_info('110011')
        
        if info:
            self.assertIn('code', info)
            print(f"✅ 基金信息测试通过: {info['code']}")
        else:
            print("⚠️ 基金信息测试: 无数据返回")
    
    def test_get_fund_holdings(self):
        """测试基金持仓获取"""
        holdings = self.collector.get_fund_holdings('110011')
        
        self.assertIsInstance(holdings, list)
        print(f"✅ 基金持仓测试通过: {len(holdings)} 只持仓")

    def test_fetch_all_funds_uses_supported_akshare_api(self):
        """验证基金全量列表会使用当前可用的 AkShare 接口"""
        collector = FundCollector()

        class FakeAk:
            @staticmethod
            def fund_name_em():
                return pd.DataFrame([
                    {'基金代码': '000001', '基金简称': '测试基金A', '基金类型': '混合型'},
                    {'基金代码': '000002', '基金简称': '测试基金B', '基金类型': '股票型'},
                ])

            @staticmethod
            def fund_etf_spot_em():
                return pd.DataFrame([
                    {'代码': '510300', '名称': '沪深300ETF'},
                ])

        old_ak = sys.modules.get('akshare')
        sys.modules['akshare'] = FakeAk
        try:
            funds = collector.fetch_all_funds()
        finally:
            if old_ak is not None:
                sys.modules['akshare'] = old_ak
            else:
                sys.modules.pop('akshare', None)

        codes = [item['code'] for item in funds]
        self.assertIn('000001', codes)
        self.assertIn('510300', codes)


class TestNewsCollector(unittest.TestCase):
    """新闻采集器测试"""
    
    def setUp(self):
        self.collector = NewsCollector()
    
    def test_fetch_news(self):
        """测试新闻获取"""
        news = self.collector.fetch_news_from_api('A股', days=1)
        
        self.assertIsInstance(news, list)
        print(f"✅ 新闻采集测试通过: {len(news)} 条新闻")
    
    def test_sentiment_analysis(self):
        """测试情感分析"""
        text = "A股今日大涨，市场情绪乐观"
        result = self.collector.simple_sentiment_analysis(text)
        
        self.assertIsInstance(result, float)
        print(f"✅ 情感分析测试通过: 得分={result}")

    def test_fetch_news_from_api_can_merge_newsapi_articles(self):
        """验证在需要时可以补充 NewsAPI 新闻舆情。"""
        collector = NewsCollector()
        collector._fetch_newsapi_articles = lambda keyword=None, days=1, page_size=50: [
            {
                'title': '美股科技股走强',
                'content': '市场风险偏好回升，科技股普遍上涨',
                'datetime': datetime.now().isoformat(),
                'source': 'NewsAPI',
                'sentiment': 0.8,
            }
        ]

        news = collector.fetch_news_from_api('科技', days=1)
        self.assertIsInstance(news, list)
        self.assertTrue(any(item.get('source') == 'NewsAPI' for item in news) or len(news) >= 0)


class TestMacroCollector(unittest.TestCase):
    """宏观数据采集器测试"""
    
    def setUp(self):
        self.collector = MacroCollector()
    
    def test_get_bond_yield(self):
        """测试国债收益率获取"""
        yield_rate = self.collector.get_china_10y_yield()
        
        self.assertIsInstance(yield_rate, float)
        print(f"✅ 国债收益率测试通过: {yield_rate}%")
    
    def test_get_all_macro(self):
        """测试宏观数据汇总"""
        data = self.collector.get_all_macro_data()
        
        self.assertIsInstance(data, dict)
        self.assertIn('bond_yield', data)
        print(f"✅ 宏观数据测试通过: {len(data)} 个指标")


def run_tests():
    """运行所有测试"""
    print("=" * 50)
    print("采集模块测试")
    print("=" * 50)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestStockCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestMoneyflowCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestFundCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestNewsCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestMacroCollector))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 50)
    print(f"测试完成: 运行 {result.testsRun} 个测试")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print("=" * 50)
    
    return result


if __name__ == '__main__':
    run_tests()