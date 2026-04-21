"""
情绪指标模块 - indicators/sentiment.py
"""

import pandas as pd
import numpy as np


class SentimentIndicator:
    """情绪指标计算器"""
    
    def __init__(self):
        pass
    
    def calculate_news_sentiment_score(self, sentiment_value):
        """计算新闻情感得分"""
        if sentiment_value is None:
            return 3.0
        
        if sentiment_value > 0.5:
            return 5.0
        elif sentiment_value > 0.2:
            return 4.0
        elif sentiment_value > -0.2:
            return 3.0
        elif sentiment_value > -0.5:
            return 2.0
        else:
            return 1.0
    
    def calculate_volatility_sentiment(self, volatility, historical_vol=None):
        """计算波动率情绪得分"""
        if volatility is None:
            return 3.0
        
        if historical_vol:
            ratio = volatility / historical_vol
            if ratio < 0.8:
                return 4.0
            elif ratio < 1.2:
                return 3.0
            elif ratio < 1.5:
                return 2.0
            else:
                return 1.0
        
        if volatility < 0.2:
            return 4.0
        elif volatility < 0.3:
            return 3.0
        elif volatility < 0.4:
            return 2.0
        else:
            return 1.0
    
    def analyze_text_sentiment(self, text):
        """兼容旧接口：对文本做轻量情绪分析。"""
        txt = str(text or '').strip()
        if not txt:
            score = 0.0
        else:
            positive = ['涨', '乐观', '利好', '回暖', '增长', '突破', '买入']
            negative = ['跌', '悲观', '利空', '风险', '下滑', '卖出', '承压']
            pos = sum(txt.count(word) for word in positive)
            neg = sum(txt.count(word) for word in negative)
            score = 0.0 if (pos + neg) == 0 else (pos - neg) / (pos + neg)
        label = 'positive' if score > 0.2 else ('negative' if score < -0.2 else 'neutral')
        return {'score': float(score), 'label': label}

    def analyze_news_sentiment(self, news_list):
        """兼容旧接口：聚合多条新闻情绪。"""
        items = news_list or []
        if not items:
            return {'avg_score': 0.0, 'label': 'neutral', 'count': 0}
        scores = []
        for item in items:
            text = f"{(item or {}).get('title', '')} {(item or {}).get('description', '')}".strip()
            scores.append(float(self.analyze_text_sentiment(text)['score']))
        avg_score = float(sum(scores) / len(scores)) if scores else 0.0
        label = 'positive' if avg_score > 0.2 else ('negative' if avg_score < -0.2 else 'neutral')
        return {'avg_score': avg_score, 'label': label, 'count': len(items)}

    def get_sentiment_score(self, news_sentiment=None, volatility=None):
        """获取情绪综合得分"""
        scores = []
        
        if news_sentiment is not None:
            scores.append(self.calculate_news_sentiment_score(news_sentiment))
        
        if volatility is not None:
            scores.append(self.calculate_volatility_sentiment(volatility))
        
        if not scores:
            return 3.0
        
        return sum(scores) / len(scores)


if __name__ == '__main__':
    si = SentimentIndicator()
    print(f"新闻情感0.3, 得分: {si.calculate_news_sentiment_score(0.3)}")
    print(f"波动率0.25, 得分: {si.calculate_volatility_sentiment(0.25)}")
