"""
综合评分模块 - indicators/scorer.py
"""

import pandas as pd
import numpy as np


class Scorer:
    """综合评分器"""
    
    def __init__(self):
        self.weights = {
            'technical': 0.35,
            'fundamental': 0.25,
            'money_flow': 0.25,
            'sentiment': 0.15
        }
    
    def set_weights(self, technical=0.35, fundamental=0.25, money_flow=0.25, sentiment=0.15):
        """设置权重"""
        total = technical + fundamental + money_flow + sentiment
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"权重之和应为1.0，当前为{total}")
        
        self.weights = {
            'technical': technical,
            'fundamental': fundamental,
            'money_flow': money_flow,
            'sentiment': sentiment
        }
    
    def calculate_technical_score(self, df=None, rsi=None, macd=None, trend=None):
        """计算技术面得分"""
        # 如果传入了DataFrame，从DataFrame计算
        if df is not None:
            from indicators.technical import TechnicalIndicator
            ti = TechnicalIndicator()
            score = ti.get_technical_score(df)
            return score
        
        # 否则根据指标计算
        score = 3.0
        
        if rsi is not None:
            if 30 <= rsi <= 70:
                score += 0.2
            elif rsi < 30:
                score += 0.4
            elif rsi > 70:
                score -= 0.4
        
        if macd is not None:
            if macd.get('dif', 0) > macd.get('dea', 0):
                score += 0.3
        
        if trend is not None:
            if trend == 'bullish':
                score += 0.5
            elif trend == 'bearish':
                score -= 0.5
        
        return max(1.0, min(5.0, score))
    
    def calculate_fundamental_score(self, pe=None, pb=None, roe=None, eps_growth=None):
        """计算基本面得分"""
        score = 3.0
        
        if pe is not None and pe > 0:
            if pe < 15:
                score += 0.5
            elif pe < 25:
                score += 0.2
            elif pe > 40:
                score -= 0.5
            elif pe > 30:
                score -= 0.2
        
        if pb is not None and pb > 0:
            if pb < 1.5:
                score += 0.3
            elif pb < 2.5:
                score += 0.1
            elif pb > 5:
                score -= 0.3
        
        if roe is not None:
            if roe > 20:
                score += 0.5
            elif roe > 15:
                score += 0.3
            elif roe > 10:
                score += 0.1
            elif roe < 5:
                score -= 0.3
        
        if eps_growth is not None:
            if eps_growth > 30:
                score += 0.5
            elif eps_growth > 15:
                score += 0.3
            elif eps_growth < 0:
                score -= 0.3
        
        return max(1.0, min(5.0, score))
    
    def calculate_money_flow_score(self, main_flow=None, north_flow=None, volume_ratio=None):
        """计算资金面得分"""
        score = 3.0
        
        if main_flow is not None:
            if main_flow > 1e8:
                score += 0.8
            elif main_flow > 5e7:
                score += 0.5
            elif main_flow > 1e7:
                score += 0.3
            elif main_flow < -1e8:
                score -= 0.8
            elif main_flow < -5e7:
                score -= 0.5
            elif main_flow < -1e7:
                score -= 0.3
        
        if north_flow is not None:
            if north_flow > 5e7:
                score += 0.3
            elif north_flow > 1e7:
                score += 0.1
            elif north_flow < -5e7:
                score -= 0.3
        
        if volume_ratio is not None:
            if volume_ratio > 1.5:
                score += 0.2
            elif volume_ratio < 0.5:
                score -= 0.2
        
        return max(1.0, min(5.0, score))
    
    def calculate_sentiment_score(self, news_sentiment=None, volatility=None):
        """计算情绪面得分"""
        score = 3.0
        
        if news_sentiment is not None:
            if news_sentiment > 0.3:
                score += 0.5
            elif news_sentiment > 0.1:
                score += 0.2
            elif news_sentiment < -0.3:
                score -= 0.5
            elif news_sentiment < -0.1:
                score -= 0.2
        
        if volatility is not None:
            if volatility < 0.2:
                score += 0.2
            elif volatility > 0.4:
                score -= 0.2
        
        return max(1.0, min(5.0, score))
    
    def calculate_total_score(self, technical_score=None, fundamental_score=None,
                               money_flow_score=None, sentiment_score=None):
        """计算综合得分"""
        total = 0.0
        weight_sum = 0.0
        
        if technical_score is not None:
            total += technical_score * self.weights['technical']
            weight_sum += self.weights['technical']
        
        if fundamental_score is not None:
            total += fundamental_score * self.weights['fundamental']
            weight_sum += self.weights['fundamental']
        
        if money_flow_score is not None:
            total += money_flow_score * self.weights['money_flow']
            weight_sum += self.weights['money_flow']
        
        if sentiment_score is not None:
            total += sentiment_score * self.weights['sentiment']
            weight_sum += self.weights['sentiment']
        
        if weight_sum == 0:
            return 3.0
        
        return total / weight_sum
    
    def get_rating(self, score):
        """根据得分获取评级"""
        if score >= 4.5:
            return {'rating': 'A', 'text': '强烈推荐', 'confidence': '高'}
        elif score >= 4.0:
            return {'rating': 'A-', 'text': '推荐', 'confidence': '较高'}
        elif score >= 3.5:
            return {'rating': 'B+', 'text': '中性偏多', 'confidence': '中等'}
        elif score >= 3.0:
            return {'rating': 'B', 'text': '中性', 'confidence': '中等'}
        elif score >= 2.5:
            return {'rating': 'B-', 'text': '中性偏空', 'confidence': '中等'}
        elif score >= 2.0:
            return {'rating': 'C+', 'text': '谨慎', 'confidence': '较低'}
        else:
            return {'rating': 'C', 'text': '回避', 'confidence': '低'}
    
    def get_score_level(self, score):
        """根据得分返回等级描述"""
        if score >= 4.5:
            return 'excellent'
        elif score >= 4.0:
            return 'good'
        elif score >= 3.5:
            return 'above_average'
        elif score >= 3.0:
            return 'average'
        elif score >= 2.5:
            return 'below_average'
        else:
            return 'poor'


class ScoreCalculator(Scorer):
    """向后兼容旧测试与旧调用方。"""

    def get_score_level(self, score):
        level = super().get_score_level(score)
        level_map = {
            'excellent': {'level_text': '优秀', 'stars': '★★★★★'},
            'good': {'level_text': '良好', 'stars': '★★★★☆'},
            'above_average': {'level_text': '中上', 'stars': '★★★★'},
            'average': {'level_text': '中性', 'stars': '★★★'},
            'below_average': {'level_text': '偏弱', 'stars': '★★☆'},
            'poor': {'level_text': '较弱', 'stars': '★★'},
        }
        result = level_map.get(level, {'level_text': '未知', 'stars': '—'})
        return {
            'level': level,
            'level_text': result['level_text'],
            'stars': result['stars'],
        }


if __name__ == '__main__':
    scorer = Scorer()
    
    tech_score = scorer.calculate_technical_score(rsi=55, macd={'dif': 0.5, 'dea': 0.3}, trend='bullish')
    fund_score = scorer.calculate_fundamental_score(pe=18, pb=2.5, roe=16)
    money_score = scorer.calculate_money_flow_score(main_flow=2e8, north_flow=1e8)
    sent_score = scorer.calculate_sentiment_score(news_sentiment=0.2)
    
    total = scorer.calculate_total_score(
        technical_score=tech_score,
        fundamental_score=fund_score,
        money_flow_score=money_score,
        sentiment_score=sent_score
    )
    
    print(f"技术面得分: {tech_score:.2f}")
    print(f"基本面得分: {fund_score:.2f}")
    print(f"资金面得分: {money_score:.2f}")
    print(f"情绪面得分: {sent_score:.2f}")
    print(f"综合得分: {total:.2f}")
    print(f"评级: {scorer.get_rating(total)}")