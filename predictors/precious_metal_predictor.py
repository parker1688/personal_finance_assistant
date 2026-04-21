"""
贵金属预测模型 - predictors/precious_metal_predictor.py
预测黄金、白银的涨跌概率
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from predictors.base_predictor import BasePredictor
from indicators.technical import TechnicalIndicator
from utils import get_logger

logger = get_logger(__name__)


class PreciousMetalPredictor(BasePredictor):
    """贵金属预测模型"""
    
    def __init__(self, metal_type='gold'):
        super().__init__(period_days=5)
        self.metal_type = metal_type
        self.technical = TechnicalIndicator()
        self.model = None
        self.is_trained = False
    
    def prepare_features(self, df):
        """准备特征数据（含宏观指标）"""
        if df is None or len(df) < 60:
            return None
        
        close = df['close']
        volume = df['volume']
        
        features = {}
        
        # 技术指标
        features['rsi'] = self.technical.calculate_rsi(close)
        features['macd_hist'] = self.technical.calculate_macd(close)['hist']
        features['price_ma20_ratio'] = close.iloc[-1] / self.technical.calculate_ma(close, 20) - 1
        features['volume_ratio'] = self.technical.calculate_volume_ratio(volume)
        features['volatility'] = self.technical.calculate_volatility(close)
        features['return_5d'] = close.pct_change(5).iloc[-1] if len(close) >= 6 else 0
        bb = self.technical.calculate_bollinger_bands(close)
        features['bb_position'] = (close.iloc[-1] - bb['lower']) / (bb['upper'] - bb['lower']) if bb['upper'] != bb['lower'] else 0.5
        
        # 加载宏观数据
        current_date = df.index[-1].date()
        
        try:
            # 美元指数
            dxy_df = pd.read_csv('data/dxy.csv', index_col=0, parse_dates=True)
            dxy_df.index = pd.to_datetime(dxy_df.index).date
            if current_date in dxy_df.index:
                features['dxy'] = dxy_df.loc[current_date, 'close']
                features['dxy_trend'] = dxy_df['close'].pct_change(5).iloc[-1] if len(dxy_df) >= 5 else 0
            else:
                features['dxy'] = 0
                features['dxy_trend'] = 0
        except:
            features['dxy'] = 0
            features['dxy_trend'] = 0
        
        try:
            # VIX
            vix_df = pd.read_csv('data/vix.csv', index_col=0, parse_dates=True)
            vix_df.index = pd.to_datetime(vix_df.index).date
            if current_date in vix_df.index:
                features['vix'] = vix_df.loc[current_date, 'close']
            else:
                features['vix'] = 0
        except:
            features['vix'] = 0
        
        try:
            # 原油
            oil_df = pd.read_csv('data/oil.csv', index_col=0, parse_dates=True)
            oil_df.index = pd.to_datetime(oil_df.index).date
            if current_date in oil_df.index:
                features['oil'] = oil_df.loc[current_date, 'close']
            else:
                features['oil'] = 0
        except:
            features['oil'] = 0
        
        try:
            # 美债收益率
            bond_df = pd.read_csv('data/bond.csv', index_col=0, parse_dates=True)
            bond_df.index = pd.to_datetime(bond_df.index).date
            if current_date in bond_df.index:
                features['bond_yield'] = bond_df.loc[current_date, 'close'] / 100
            else:
                features['bond_yield'] = 0
        except:
            features['bond_yield'] = 0
        
        # 处理缺失值
        for k, v in features.items():
            if pd.isna(v) or np.isinf(v):
                features[k] = 0
        
        X = pd.DataFrame([features])
        X = X.replace([np.inf, -np.inf], 0)
        X = X.fillna(0)
        
        return X
    
    def train(self, X, y):
        """训练模型"""
        try:
            import xgboost as xgb
            
            if isinstance(X, pd.DataFrame):
                X = X.values
            if isinstance(y, pd.Series):
                y = y.values
            
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            
            self.model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                eval_metric='logloss'
            )
            
            self.model.fit(X, y)
            self.is_trained = True
            
            train_pred = self.model.predict(X)
            accuracy = (train_pred == y).mean()
            logger.info(f"{self.metal_type} 模型训练完成，准确率: {accuracy:.2%}")
            return accuracy
            
        except Exception as e:
            logger.error(f"训练失败: {e}")
            self.is_trained = False
            return 0
    
    def predict(self, X):
        """预测上涨概率"""
        if self.is_trained and self.model is not None:
            try:
                if isinstance(X, pd.DataFrame):
                    X = X.values
                X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
                prob = self.model.predict_proba(X)[0]
                return prob[1] * 100
            except:
                pass
        
        return 50
    
    def get_prediction_result(self, df):
        """获取预测结果"""
        X = self.prepare_features(df)
        
        if X is None:
            up_probability = 50
        elif self.is_trained and self.model is not None:
            up_probability = self.predict(X)
        else:
            up_probability = 50
        
        current_price = df['close'].iloc[-1]
        
        return {
            'period_days': self.period_days,
            'up_probability': round(up_probability, 1),
            'down_probability': round(100 - up_probability, 1),
            'target_low': round(current_price * 0.97, 2),
            'target_high': round(current_price * 1.03, 2),
            'confidence': 60,
            'stop_loss': round(current_price * 0.95, 2),
            'expiry_date': (date.today() + timedelta(days=5)).isoformat()
        }


if __name__ == '__main__':
    import yfinance as yf
    
    # 测试黄金
    gold = yf.Ticker('GC=F')
    df = gold.history(period='1y')
    df.columns = [col.lower() for col in df.columns]
    
    predictor = PreciousMetalPredictor('gold')
    X = predictor.prepare_features(df)
    print(f"黄金特征: {X.columns.tolist() if X is not None else 'None'}")
