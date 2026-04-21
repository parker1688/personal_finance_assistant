"""
技术指标计算模块 - indicators/technical.py
计算RSI、MACD、均线、布林带、波动率等技术指标
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 移除 utils 导入，避免循环依赖
# from utils import get_logger
# logger = get_logger(__name__)

# 使用简单的 print 替代 logger（或直接忽略）
def log_info(msg):
    pass  # 可以改为 print 用于调试


class TechnicalIndicator:
    """技术指标计算器"""
    
    def __init__(self):
        self.cache = {}
    
    def calculate_rsi(self, prices, period=14):
        """计算RSI"""
        if len(prices) < period + 1:
            return 50.0
        
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        result = rsi.iloc[-1]
        if pd.isna(result):
            return 50.0
        return float(result)
    
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """计算MACD（单值）"""
        if len(prices) < slow + signal:
            return {'dif': 0.0, 'dea': 0.0, 'hist': 0.0}
        
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = 2 * (dif - dea)
        
        return {
            'dif': float(dif.iloc[-1]) if not pd.isna(dif.iloc[-1]) else 0.0,
            'dea': float(dea.iloc[-1]) if not pd.isna(dea.iloc[-1]) else 0.0,
            'hist': float(hist.iloc[-1]) if not pd.isna(hist.iloc[-1]) else 0.0
        }
    
    def calculate_macd_series(self, prices, fast=12, slow=26, signal=9):
        """计算MACD序列"""
        if len(prices) < slow + signal:
            index = prices.index
            return {
                'dif': pd.Series([0] * len(prices), index=index),
                'dea': pd.Series([0] * len(prices), index=index),
                'hist': pd.Series([0] * len(prices), index=index)
            }
        
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = 2 * (dif - dea)
        
        return {'dif': dif, 'dea': dea, 'hist': hist}
    
    def calculate_ma(self, prices, period=20):
        """计算移动平均线"""
        if len(prices) < period:
            return float(prices.iloc[-1]) if len(prices) > 0 else 0.0
        
        ma = prices.rolling(window=period).mean()
        result = ma.iloc[-1]
        return float(result) if not pd.isna(result) else float(prices.iloc[-1])
    
    def calculate_bollinger_bands(self, prices, period=20, std_dev=2):
        """计算布林带"""
        if len(prices) < period:
            last_price = float(prices.iloc[-1]) if len(prices) > 0 else 100.0
            return {
                'upper': last_price * 1.05,
                'middle': last_price,
                'lower': last_price * 0.95,
                'position': 'middle'
            }
        
        middle = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        
        last_upper = float(upper.iloc[-1]) if not pd.isna(upper.iloc[-1]) else 0
        last_middle = float(middle.iloc[-1]) if not pd.isna(middle.iloc[-1]) else 0
        last_lower = float(lower.iloc[-1]) if not pd.isna(lower.iloc[-1]) else 0
        last_price = float(prices.iloc[-1])
        
        if last_price >= last_upper:
            position = 'upper'
        elif last_price <= last_lower:
            position = 'lower'
        else:
            position = 'middle'
        
        return {
            'upper': last_upper,
            'middle': last_middle,
            'lower': last_lower,
            'position': position
        }
    
    def calculate_volatility(self, prices, period=60):
        """计算年化波动率"""
        if len(prices) < period:
            return 0.3
        
        returns = prices.pct_change().dropna()
        
        if len(returns) < period:
            returns = returns.tail(period)
        
        daily_std = returns.std()
        
        if pd.isna(daily_std):
            return 0.3
        
        annual_volatility = float(daily_std) * np.sqrt(252)
        return annual_volatility if annual_volatility < 2 else 0.3
    
    def calculate_volume_ratio(self, volumes, period=20):
        """计算量比"""
        if len(volumes) < period:
            return 1.0
        
        avg_volume = volumes.tail(period).mean()
        current_volume = volumes.iloc[-1]
        
        if pd.isna(avg_volume) or avg_volume == 0:
            return 1.0
        
        return float(current_volume / avg_volume)
    
    def calculate_atr(self, high, low, close, period=14):
        """计算ATR"""
        if len(high) < period + 1:
            return 0.0
        
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        result = atr.iloc[-1]
        return float(result) if not pd.isna(result) else 0.0
    
    def calculate_all_indicators(self, df):
        """计算所有技术指标"""
        if df is None or len(df) < 60:
            return {
                'rsi': 50.0,
                'macd_dif': 0.0,
                'macd_dea': 0.0,
                'macd_hist': 0.0,
                'ma5': 0.0,
                'ma20': 0.0,
                'ma60': 0.0,
                'bb_upper': 0.0,
                'bb_middle': 0.0,
                'bb_lower': 0.0,
                'volatility': 0.3,
                'volume_ratio': 1.0,
                'atr': 0.0,
                'price_vs_ma20': 0.0,
                'price_vs_ma60': 0.0
            }
        
        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']
        
        ma5 = self.calculate_ma(close, 5)
        ma20 = self.calculate_ma(close, 20)
        ma60 = self.calculate_ma(close, 60) if len(close) >= 60 else ma20
        
        result = {
            'rsi': float(self.calculate_rsi(close)),
            'macd_dif': float(self.calculate_macd(close)['dif']),
            'macd_dea': float(self.calculate_macd(close)['dea']),
            'macd_hist': float(self.calculate_macd(close)['hist']),
            'ma5': float(ma5),
            'ma20': float(ma20),
            'ma60': float(ma60),
            'bb_upper': float(self.calculate_bollinger_bands(close)['upper']),
            'bb_middle': float(self.calculate_bollinger_bands(close)['middle']),
            'bb_lower': float(self.calculate_bollinger_bands(close)['lower']),
            'volatility': float(self.calculate_volatility(close)),
            'volume_ratio': float(self.calculate_volume_ratio(volume)),
            'atr': float(self.calculate_atr(high, low, close)),
            'price_vs_ma20': float((close.iloc[-1] / ma20 - 1) * 100) if ma20 != 0 else 0.0,
            'price_vs_ma60': float((close.iloc[-1] / ma60 - 1) * 100) if ma60 != 0 else 0.0
        }
        
        return result
    
    def get_trend_signal(self, df):
        """获取趋势信号"""
        close = df['close']
        
        ma5 = self.calculate_ma(close, 5)
        ma20 = self.calculate_ma(close, 20)
        ma60 = self.calculate_ma(close, 60) if len(close) >= 60 else ma20
        
        current_price = float(close.iloc[-1])
        
        if current_price > ma5 > ma20 > ma60:
            trend = 'strong_bullish'
            trend_text = '强势上涨'
        elif current_price > ma20 > ma60:
            trend = 'bullish'
            trend_text = '上涨趋势'
        elif current_price < ma5 < ma20 < ma60:
            trend = 'strong_bearish'
            trend_text = '强势下跌'
        elif current_price < ma20 < ma60:
            trend = 'bearish'
            trend_text = '下跌趋势'
        else:
            trend = 'neutral'
            trend_text = '震荡整理'
        
        macd = self.calculate_macd(close)
        macd_signal = 'neutral'
        
        if macd['dif'] > macd['dea'] and macd['hist'] > 0:
            macd_signal = 'bullish'
        elif macd['dif'] < macd['dea'] and macd['hist'] < 0:
            macd_signal = 'bearish'
        
        rsi = self.calculate_rsi(close)
        rsi_signal = 'neutral'
        if rsi > 70:
            rsi_signal = 'overbought'
        elif rsi < 30:
            rsi_signal = 'oversold'
        
        return {
            'trend': trend,
            'trend_text': trend_text,
            'macd_signal': macd_signal,
            'rsi_signal': rsi_signal,
            'rsi_value': float(rsi)
        }
    
    def get_technical_score(self, df):
        """计算技术面得分（1-5分）"""
        close = df['close']
        
        score = 3.0
        
        ma5 = self.calculate_ma(close, 5)
        ma20 = self.calculate_ma(close, 20)
        ma60 = self.calculate_ma(close, 60) if len(close) >= 60 else ma20
        
        if close.iloc[-1] > ma5 > ma20 > ma60:
            score += 0.6
        elif close.iloc[-1] > ma20 > ma60:
            score += 0.3
        elif close.iloc[-1] < ma5 < ma20 < ma60:
            score -= 0.6
        elif close.iloc[-1] < ma20 < ma60:
            score -= 0.3
        
        macd = self.calculate_macd(close)
        if macd['dif'] > macd['dea'] and macd['hist'] > 0:
            score += 0.5
        elif macd['dif'] < macd['dea'] and macd['hist'] < 0:
            score -= 0.5
        elif macd['dif'] > macd['dea']:
            score += 0.25
        
        rsi = self.calculate_rsi(close)
        if 30 <= rsi <= 70:
            score += 0.2
        elif rsi < 30:
            score += 0.4
        elif rsi > 70:
            score -= 0.4
        
        bb = self.calculate_bollinger_bands(close)
        if bb['position'] == 'lower':
            score += 0.3
        elif bb['position'] == 'upper':
            score -= 0.3
        
        volume_ratio = self.calculate_volume_ratio(df['volume'])
        if volume_ratio > 1.5 and close.iloc[-1] > close.iloc[-2]:
            score += 0.2
        elif volume_ratio > 1.5 and close.iloc[-1] < close.iloc[-2]:
            score -= 0.2
        
        return max(1.0, min(5.0, score))


if __name__ == '__main__':
    import yfinance as yf
    
    ticker = yf.Ticker('AAPL')
    df = ticker.history(period='6mo')
    df.columns = [col.lower() for col in df.columns]
    
    ti = TechnicalIndicator()
    
    print("=" * 50)
    print("技术指标计算测试")
    print("=" * 50)
    
    rsi = ti.calculate_rsi(df['close'])
    macd = ti.calculate_macd(df['close'])
    ma20 = ti.calculate_ma(df['close'], 20)
    bb = ti.calculate_bollinger_bands(df['close'])
    volatility = ti.calculate_volatility(df['close'])
    score = ti.get_technical_score(df)
    trend = ti.get_trend_signal(df)
    
    print(f"RSI(14): {rsi:.2f}")
    print(f"MACD: DIF={macd['dif']:.4f}, DEA={macd['dea']:.4f}, HIST={macd['hist']:.4f}")
    print(f"MA20: {ma20:.2f}")
    print(f"布林带位置: {bb['position']}")
    print(f"年化波动率: {volatility*100:.2f}%")
    print(f"趋势: {trend['trend_text']}")
    print(f"技术面得分: {score:.2f}")
