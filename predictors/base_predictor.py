"""
预测模型基类 - predictors/base_predictor.py
定义所有预测模型的公共接口
"""

from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger

logger = get_logger(__name__)


class BasePredictor(ABC):
    """预测模型基类"""
    
    def __init__(self, period_days=5):
        """
        Args:
            period_days: 预测周期（天数）
        """
        self.period_days = period_days
        self.model = None
        self.feature_columns = []
        self.is_trained = False
    
    @abstractmethod
    def prepare_features(self, df):
        """
        准备特征数据
        Args:
            df: 包含OHLCV的DataFrame
        Returns:
            X: 特征矩阵
            y: 标签（可选）
        """
        pass
    
    @abstractmethod
    def train(self, X, y):
        """
        训练模型
        Args:
            X: 特征矩阵
            y: 标签
        """
        pass
    
    @abstractmethod
    def predict(self, X):
        """
        预测
        Args:
            X: 特征矩阵
        Returns:
            dict: 预测结果
        """
        pass
    
    def calculate_target(self, df, shift_days=None):
        """
        计算目标变量（未来N日收益率）
        Args:
            df: 数据框
            shift_days: 偏移天数，默认使用self.period_days
        Returns:
            Series: 未来收益率
        """
        if shift_days is None:
            shift_days = self.period_days
        
        future_price = df['close'].shift(-shift_days)
        current_price = df['close']
        
        # 未来收益率 = (未来价格 - 当前价格) / 当前价格
        future_return = (future_price - current_price) / current_price
        
        return future_return
    
    def calculate_direction_label(self, df, shift_days=None, threshold=0):
        """
        计算方向标签（分类任务）
        Args:
            df: 数据框
            shift_days: 偏移天数
            threshold: 阈值，收益率超过此值才算上涨
        Returns:
            Series: 1表示上涨，0表示下跌
        """
        future_return = self.calculate_target(df, shift_days)
        
        # 1: 上涨, 0: 下跌
        labels = (future_return > threshold).astype(int)
        
        return labels
    
    def calculate_price_range(self, df, shift_days=None, confidence=0.7):
        """
        计算价格区间预测（分位数回归）
        Args:
            df: 数据框
            shift_days: 偏移天数
            confidence: 置信度（0.7表示70%置信区间）
        Returns:
            tuple: (下限, 上限)
        """
        if shift_days is None:
            shift_days = self.period_days
        
        # 简化版：使用历史波动率估算
        # 实际应用中应使用分位数回归模型
        
        current_price = df['close'].iloc[-1]
        
        # 计算历史波动率
        returns = df['close'].pct_change().dropna()
        volatility = returns.std() * np.sqrt(252)  # 年化波动率
        
        # 估算未来波动
        future_volatility = volatility * np.sqrt(shift_days / 252)
        
        # 正态分布分位数
        from scipy import stats
        z_score = stats.norm.ppf((1 + confidence) / 2)
        
        # 使用历史平均收益率作为预期
        avg_return = returns.mean() * shift_days
        
        # 价格区间
        lower = current_price * (1 + avg_return - z_score * future_volatility)
        upper = current_price * (1 + avg_return + z_score * future_volatility)
        
        # 确保下限为正
        lower = max(lower, current_price * 0.8)
        
        return lower, upper
    
    def calculate_confidence(self, X):
        """
        计算预测置信度
        Args:
            X: 特征矩阵
        Returns:
            float: 置信度（0-100）
        """
        # 使用分类边际(|p-0.5|)近似不确定性，边际越大置信度越高。
        try:
            if self.model is not None and hasattr(self.model, 'predict_proba') and X is not None:
                proba = self.model.predict_proba(X)
                if proba is not None and len(proba) > 0 and len(proba[0]) >= 2:
                    p_up = float(proba[0][1])
                    margin = min(1.0, abs(p_up - 0.5) * 2.0)  # 0~1
                    return max(50.0, min(95.0, 50.0 + 45.0 * margin))
        except Exception:
            pass

        return 55.0
    
    def get_stop_loss(self, current_price, volatility_level='medium'):
        """
        计算止损价（保守止损-5%）
        Args:
            current_price: 当前价格
            volatility_level: 波动率等级（low/medium/high）
        Returns:
            float: 止损价
        """
        # 保守止损：-5%
        stop_loss_ratio = 0.95
        
        # 根据波动率调整
        adjustment = {
            'low': 0.97,      # 低波动，止损更紧
            'medium': 0.95,   # 中等波动
            'high': 0.93      # 高波动，止损更宽
        }
        
        ratio = adjustment.get(volatility_level, 0.95)
        
        return current_price * ratio
    
    def save_model(self, filepath):
        """保存模型"""
        import pickle
        with open(filepath, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'feature_columns': self.feature_columns,
                'period_days': self.period_days,
                'is_trained': self.is_trained
            }, f)
        logger.info(f"模型已保存到 {filepath}")
    
    def load_model(self, filepath):
        """加载模型"""
        import pickle
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            self.model = data['model']
            self.feature_columns = data['feature_columns']
            self.period_days = data['period_days']
            self.is_trained = data['is_trained']
        logger.info(f"模型已从 {filepath} 加载")
    
    def get_prediction_result(self, df, volatility_level='medium'):
        """
        获取完整的预测结果
        Args:
            df: 数据框
            volatility_level: 波动率等级
        Returns:
            dict: 预测结果
        """
        # 准备特征
        X = self.prepare_features(df)
        
        if X is None or len(X) == 0:
            logger.warning("特征准备失败，使用默认预测")
            up_probability = 50
        elif self.is_trained and self.model is not None:
            # 使用模型预测
            up_probability = self.predict(X)
        else:
            # 使用简单规则预测
            up_probability = self._simple_rule_predict(df)
        
        # 计算目标价区间
        target_low, target_high = self.calculate_price_range(df)
        
        # 计算置信度
        confidence = self.calculate_confidence(X) if X is not None else 50
        
        # 计算止损价
        current_price = df['close'].iloc[-1]
        stop_loss = self.get_stop_loss(current_price, volatility_level)
        
        # 计算到期日期
        from datetime import date, timedelta
        expiry_date = date.today() + timedelta(days=self.period_days)
        
        return {
            'period_days': self.period_days,
            'up_probability': round(up_probability, 1),
            'down_probability': round(100 - up_probability, 1),
            'target_low': round(target_low, 2),
            'target_high': round(target_high, 2),
            'confidence': round(confidence, 1),
            'stop_loss': round(stop_loss, 2),
            'expiry_date': expiry_date.isoformat()
        }
    
    def _simple_rule_predict(self, df):
        """
        简单规则预测（当模型不可用时）
        基于均线和RSI
        """
        close = df['close']
        
        # 计算简单指标
        ma5 = close.rolling(5).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        current = close.iloc[-1]
        
        # 计算RSI（简化）
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_value = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        # 基础概率50%
        prob = 50
        
        # 均线信号
        if current > ma5 > ma20:
            prob += 15
        elif current > ma20:
            prob += 8
        elif current < ma5 < ma20:
            prob -= 15
        elif current < ma20:
            prob -= 8
        
        # RSI信号
        if rsi_value < 30:
            prob += 10  # 超卖，反弹概率大
        elif rsi_value > 70:
            prob -= 10  # 超买，回调概率大
        
        # 限制范围
        return max(20, min(80, prob))