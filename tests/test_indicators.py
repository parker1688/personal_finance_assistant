"""
指标模块测试 - tests/test_indicators.py
测试技术指标、估值指标等计算功能
"""

import sys
import os
import unittest
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.technical import TechnicalIndicator
from indicators.fundamental import FundamentalIndicator
from indicators.money_flow import MoneyFlowIndicator
from indicators.sentiment import SentimentIndicator
from indicators.scorer import ScoreCalculator


def create_test_data():
    """创建测试数据"""
    dates = pd.date_range(start='2024-01-01', end='2024-03-31', freq='D')
    np.random.seed(42)
    
    # 模拟价格数据（有趋势）
    price = 100
    prices = []
    for i in range(len(dates)):
        price = price + np.random.normal(0, 1)
        prices.append(max(price, 50))
    
    df = pd.DataFrame({
        'open': prices,
        'high': [p * (1 + abs(np.random.normal(0, 0.02))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.02))) for p in prices],
        'close': prices,
        'volume': np.random.randint(1000000, 10000000, len(dates))
    }, index=dates)
    
    return df


class TestTechnicalIndicator(unittest.TestCase):
    """技术指标测试"""
    
    def setUp(self):
        self.ti = TechnicalIndicator()
        self.df = create_test_data()
    
    def test_calculate_rsi(self):
        """测试RSI计算"""
        rsi = self.ti.calculate_rsi(self.df['close'])
        self.assertIsInstance(rsi, float)
        self.assertTrue(0 <= rsi <= 100)
        print(f"✅ RSI测试通过: {rsi:.2f}")
    
    def test_calculate_macd(self):
        """测试MACD计算"""
        macd = self.ti.calculate_macd(self.df['close'])
        self.assertIn('dif', macd)
        self.assertIn('dea', macd)
        self.assertIn('hist', macd)
        print(f"✅ MACD测试通过: DIF={macd['dif']:.4f}, DEA={macd['dea']:.4f}")
    
    def test_calculate_ma(self):
        """测试移动平均线"""
        ma5 = self.ti.calculate_ma(self.df['close'], 5)
        ma20 = self.ti.calculate_ma(self.df['close'], 20)
        self.assertIsInstance(ma5, float)
        self.assertIsInstance(ma20, float)
        print(f"✅ 均线测试通过: MA5={ma5:.2f}, MA20={ma20:.2f}")
    
    def test_calculate_bollinger(self):
        """测试布林带"""
        bb = self.ti.calculate_bollinger_bands(self.df['close'])
        self.assertIn('upper', bb)
        self.assertIn('middle', bb)
        self.assertIn('lower', bb)
        print(f"✅ 布林带测试通过: 上轨={bb['upper']:.2f}, 下轨={bb['lower']:.2f}")
    
    def test_calculate_volatility(self):
        """测试波动率"""
        vol = self.ti.calculate_volatility(self.df['close'])
        self.assertIsInstance(vol, float)
        print(f"✅ 波动率测试通过: {vol*100:.2f}%")
    
    def test_get_trend_signal(self):
        """测试趋势信号"""
        trend = self.ti.get_trend_signal(self.df)
        self.assertIn('trend', trend)
        self.assertIn('trend_text', trend)
        print(f"✅ 趋势信号测试通过: {trend['trend_text']}")


class TestFundamentalIndicator(unittest.TestCase):
    """估值指标测试"""
    
    def setUp(self):
        self.fi = FundamentalIndicator()
    
    def test_get_valuation_level(self):
        """测试估值水平判断"""
        level = self.fi.get_valuation_level(30)
        self.assertIn('level', level)
        self.assertIn('score', level)
        print(f"✅ 估值水平测试通过: {level['level_text']} (得分{level['score']})")
    
    def test_calculate_value_score(self):
        """测试估值得分"""
        score = self.fi.calculate_value_score('TEST', 20, 2, 40, 45, '科技')
        self.assertTrue(1 <= score <= 5)
        print(f"✅ 估值得分测试通过: {score}")


class TestMoneyFlowIndicator(unittest.TestCase):
    """资金流指标测试"""
    
    def setUp(self):
        self.mf = MoneyFlowIndicator()
    
    def test_calculate_north_money(self):
        """测试北向资金"""
        north = self.mf.calculate_north_money('600519.SH')
        self.assertIsInstance(north, (int, float))
        print(f"✅ 北向资金测试通过: {north/10000:.1f}万")
    
    def test_calculate_consecutive_flow(self):
        """测试连续流向"""
        flow = self.mf.calculate_consecutive_flow('600519.SH')
        self.assertIn('type', flow)
        self.assertIn('consecutive_days', flow)
        print(f"✅ 连续流向测试通过: {flow['type']} {flow['consecutive_days']}天")
    
    def test_get_money_flow_summary(self):
        """测试资金流摘要"""
        summary = self.mf.get_money_flow_summary('600519.SH')
        self.assertIn('score', summary)
        print(f"✅ 资金流摘要测试通过: 得分{summary['score']}")


class TestSentimentIndicator(unittest.TestCase):
    """情绪指标测试"""
    
    def setUp(self):
        self.si = SentimentIndicator()
    
    def test_analyze_text_sentiment(self):
        """测试文本情感分析"""
        result = self.si.analyze_text_sentiment("A股大涨，市场情绪乐观")
        self.assertIn('score', result)
        self.assertIn('label', result)
        print(f"✅ 文本情感测试通过: {result['label']} (得分{result['score']})")
    
    def test_analyze_news_sentiment(self):
        """测试新闻情感分析"""
        news_list = [
            {'title': 'A股大涨', 'description': '市场情绪乐观'},
            {'title': '北向资金流入', 'description': '外资看好A股'},
            {'title': '经济数据超预期', 'description': '复苏势头良好'}
        ]
        result = self.si.analyze_news_sentiment(news_list)
        self.assertIn('avg_score', result)
        print(f"✅ 新闻情感测试通过: 平均得分{result['avg_score']}")


class TestScorer(unittest.TestCase):
    """综合评分测试"""
    
    def setUp(self):
        self.scorer = ScoreCalculator()
        self.df = create_test_data()
    
    def test_calculate_technical_score(self):
        """测试技术面得分"""
        score = self.scorer.calculate_technical_score(self.df)
        self.assertTrue(1 <= score <= 5)
        print(f"✅ 技术面得分测试通过: {score}")
    
    def test_calculate_total_score(self):
        """测试综合评分"""
        total = self.scorer.calculate_total_score(4.0, 3.5, 3.0, 3.5)
        self.assertTrue(1 <= total <= 5)
        print(f"✅ 综合评分测试通过: {total}")
    
    def test_get_score_level(self):
        """测试评分等级"""
        level = self.scorer.get_score_level(4.2)
        self.assertIn('level_text', level)
        self.assertIn('stars', level)
        print(f"✅ 评分等级测试通过: {level['level_text']} {level['stars']}")


def run_tests():
    """运行所有测试"""
    print("=" * 50)
    print("指标模块测试")
    print("=" * 50)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestTechnicalIndicator))
    suite.addTests(loader.loadTestsFromTestCase(TestFundamentalIndicator))
    suite.addTests(loader.loadTestsFromTestCase(TestMoneyFlowIndicator))
    suite.addTests(loader.loadTestsFromTestCase(TestSentimentIndicator))
    suite.addTests(loader.loadTestsFromTestCase(TestScorer))
    
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