"""
统一特征提取模块 - indicators/feature_extractor.py
整合所有特征提取逻辑，避免代码重复
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.technical import TechnicalIndicator
from utils import get_logger

logger = get_logger(__name__)


class FeatureExtractor:
    """统一特征提取器"""
    
    def __init__(self):
        self.technical = TechnicalIndicator()
        
        # 特征列定义（与模型训练保持一致）
        self.feature_columns = [
            # 技术指标
            'rsi', 'macd_hist', 'price_ma20_ratio', 'price_ma60_ratio',
            'volume_ratio', 'volatility', 'return_5d', 'return_10d', 'return_20d',
            # 增强特征
            'ma_bullish_count', 'volatility_trend', 'price_position_60d',
            'momentum_5d', 'momentum_10d', 'bb_position',
            # 外部特征（默认值，调用时可覆盖）
            'net_mf_amount', 'north_money', 'cpi', 'pmi', 'shibor_1w'
        ]
    
    def extract_features_from_series(self, close, volume=None, high=None, low=None):
        """
        从价格序列提取特征
        Args:
            close: 收盘价序列
            volume: 成交量序列（可选）
            high: 最高价序列（可选）
            low: 最低价序列（可选）
        Returns:
            dict: 特征字典
        """
        if volume is None:
            volume = pd.Series([1] * len(close), index=close.index)
        if high is None:
            high = close
        if low is None:
            low = close
        
        features = {}
        
        # ========== 基础技术指标 ==========
        # RSI
        rsi = self.technical.calculate_rsi(close)
        features['rsi'] = rsi if not np.isnan(rsi) else 50
        
        # MACD柱
        macd = self.technical.calculate_macd(close)
        features['macd_hist'] = macd['hist'] if not np.isnan(macd['hist']) else 0
        
        # 均线比率
        ma20 = self.technical.calculate_ma(close, 20)
        ma60 = self.technical.calculate_ma(close, 60) if len(close) >= 60 else ma20
        features['price_ma20_ratio'] = (close.iloc[-1] / ma20 - 1) if ma20 != 0 else 0
        features['price_ma60_ratio'] = (close.iloc[-1] / ma60 - 1) if ma60 != 0 else 0
        
        # 量比
        avg_volume = volume.tail(20).mean() if len(volume) >= 20 else volume.iloc[-1]
        features['volume_ratio'] = volume.iloc[-1] / avg_volume if avg_volume != 0 else 1
        
        # 波动率
        features['volatility'] = self.technical.calculate_volatility(close)
        
        # 收益率
        features['return_5d'] = close.pct_change(5).iloc[-1] if len(close) >= 6 else 0
        features['return_10d'] = close.pct_change(10).iloc[-1] if len(close) >= 11 else 0
        features['return_20d'] = close.pct_change(20).iloc[-1] if len(close) >= 21 else 0
        
        # ========== 增强特征 ==========
        # 均线多头排列计数
        ma5 = self.technical.calculate_ma(close, 5)
        bullish_count = 0
        if close.iloc[-1] > ma5:
            bullish_count += 1
        if ma5 > ma20:
            bullish_count += 1
        if ma20 > ma60:
            bullish_count += 1
        features['ma_bullish_count'] = bullish_count
        
        # 波动率趋势
        returns = close.pct_change().dropna()
        if len(returns) >= 30:
            vol_short = returns.tail(10).std() if len(returns) >= 10 else returns.std()
            vol_long = returns.tail(30).std() if len(returns) >= 30 else returns.std()
            features['volatility_trend'] = vol_short - vol_long
        else:
            features['volatility_trend'] = 0
        
        # 价格位置
        if len(close) >= 60:
            price_min = close.tail(60).min()
            price_max = close.tail(60).max()
            if price_max != price_min:
                features['price_position_60d'] = (close.iloc[-1] - price_min) / (price_max - price_min)
            else:
                features['price_position_60d'] = 0.5
        else:
            features['price_position_60d'] = 0.5
        
        # 动量
        features['momentum_5d'] = features['return_5d']
        features['momentum_10d'] = features['return_10d']
        
        # 布林带位置
        bb = self.technical.calculate_bollinger_bands(close)
        if bb['upper'] != bb['lower']:
            features['bb_position'] = (close.iloc[-1] - bb['lower']) / (bb['upper'] - bb['lower'])
        else:
            features['bb_position'] = 0.5
        
        # ========== 外部特征（默认值） ==========
        features['net_mf_amount'] = 0
        features['north_money'] = 0
        features['cpi'] = 2.0
        features['pmi'] = 50.0
        features['shibor_1w'] = 1.8
        
        # 处理缺失值
        for k, v in features.items():
            if pd.isna(v) or np.isinf(v):
                features[k] = 0
        
        return features
    
    def extract_features_from_df(self, df, external_features=None):
        """
        从DataFrame提取特征
        Args:
            df: 包含OHLCV数据的DataFrame
            external_features: 外部特征字典（资金流向、宏观数据等）
        Returns:
            pd.DataFrame: 特征DataFrame
        """
        if df is None or len(df) < 60:
            return None
        
        close = df['close']
        volume = df['volume'] if 'volume' in df.columns else pd.Series([1] * len(close), index=close.index)
        high = df['high'] if 'high' in df.columns else close
        low = df['low'] if 'low' in df.columns else close
        
        # 提取特征
        features = self.extract_features_from_series(close, volume, high, low)
        
        # 合并外部特征
        if external_features:
            for key, value in external_features.items():
                if key in self.feature_columns:
                    features[key] = value
        
        # 确保所有特征列都存在
        for col in self.feature_columns:
            if col not in features:
                features[col] = 0
        
        # 转换为DataFrame
        X = pd.DataFrame([features])[self.feature_columns]
        X = X.replace([np.inf, -np.inf], 0)
        X = X.fillna(0)
        
        return X
    
    def extract_batch_features(self, df_list, external_features_list=None):
        """
        批量提取特征
        Args:
            df_list: DataFrame列表
            external_features_list: 外部特征列表
        Returns:
            pd.DataFrame: 特征DataFrame
        """
        X_list = []
        
        for i, df in enumerate(df_list):
            ext_features = external_features_list[i] if external_features_list else None
            X = self.extract_features_from_df(df, ext_features)
            if X is not None:
                X_list.append(X)
        
        if not X_list:
            return None
        
        return pd.concat(X_list, ignore_index=True)
    
    def extract_training_data(self, df, period_days=5, threshold=0.02):
        """
        提取训练数据（特征+标签）
        Args:
            df: 历史数据DataFrame
            period_days: 预测周期
            threshold: 上涨阈值
        Returns:
            tuple: (X, y)
        """
        if df is None or len(df) < 60 + period_days:
            return None, None
        
        close = df['close'].values
        volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))
        
        X_list = []
        y_list = []
        
        # 滑动窗口
        for i in range(60, len(close) - period_days, 5):
            window_df = df.iloc[:i+1]
            features = self.extract_features_from_df(window_df)
            
            if features is not None:
                # 计算标签
                future_return = (close[i + period_days] - close[i]) / close[i]
                label = 1 if future_return > threshold else 0
                
                X_list.append(features.iloc[0].to_dict())
                y_list.append(label)
        
        if not X_list:
            return None, None
        
        X = pd.DataFrame(X_list)[self.feature_columns]
        y = pd.Series(y_list)
        
        return X, y
    
    def get_feature_columns(self):
        """获取特征列名"""
        return self.feature_columns.copy()
    
    def get_feature_count(self):
        """获取特征数量"""
        return len(self.feature_columns)


# 全局实例
_feature_extractor = None


def get_feature_extractor():
    """获取特征提取器单例"""
    global _feature_extractor
    if _feature_extractor is None:
        _feature_extractor = FeatureExtractor()
    return _feature_extractor


if __name__ == '__main__':
    import yfinance as yf
    
    # 测试
    extractor = FeatureExtractor()
    
    ticker = yf.Ticker('AAPL')
    df = ticker.history(period='6mo')
    df.columns = [col.lower() for col in df.columns]
    
    X = extractor.extract_features_from_df(df)
    print(f"特征提取测试:")
    print(f"  特征列: {X.columns.tolist()}")
    print(f"  特征值: {X.iloc[0].to_dict()}")
    
    # 测试训练数据提取
    X_train, y_train = extractor.extract_training_data(df, period_days=5)
    if X_train is not None:
        print(f"\n训练数据:")
        print(f"  样本数: {len(X_train)}")
        print(f"  正样本率: {y_train.mean():.2%}")