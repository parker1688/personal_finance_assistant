"""
60日长期预测模型 - predictors/long_term.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from predictors.base_predictor import BasePredictor
from indicators.technical import TechnicalIndicator
from utils import get_logger

logger = get_logger(__name__)


class LongTermPredictor(BasePredictor):
    """60日长期预测模型"""
    
    def __init__(self):
        super().__init__(period_days=60)
        self.technical = TechnicalIndicator()
        self.is_trained = False
        self.model = None
        self.feature_columns = None

    def _align_features_for_model(self, X, model=None):
        """按已训练模型的列定义对齐推理特征，兼容历史版本模型。"""
        if X is None or not isinstance(X, pd.DataFrame):
            return X

        target_model = model or self.model
        if target_model is None:
            return X

        raw_cols = getattr(target_model, 'feature_names_in_', None)
        if raw_cols is not None and len(raw_cols) > 0:
            model_cols = [str(col) for col in list(raw_cols)]
        elif self.feature_columns:
            model_cols = [str(col) for col in list(self.feature_columns)]
        else:
            expected = getattr(target_model, 'n_features_in_', None)
            if isinstance(expected, int) and expected > 0 and X.shape[1] >= expected:
                return X.iloc[:, :expected].copy()
            return X

        aligned = pd.DataFrame(index=X.index)
        for col in model_cols:
            aligned[col] = X[col] if col in X.columns else 0.0
        return aligned
    
    def prepare_features(self, df, valuation_data=None, market_data=None):
        """准备特征数据（返回25个特征）"""
        if df is None or len(df) < 60:
            return None
        
        close = df['close']
        volume = df['volume']
        high = df['high']
        low = df['low']
        
        features = {}
        
        # 技术指标
        features['rsi'] = self.technical.calculate_rsi(close)
        macd = self.technical.calculate_macd(close)
        features['macd_hist'] = macd['hist']
        
        ma20 = self.technical.calculate_ma(close, 20)
        ma60 = self.technical.calculate_ma(close, 60) if len(close) >= 60 else ma20
        features['price_ma20_ratio'] = close.iloc[-1] / ma20 - 1 if ma20 != 0 else 0
        features['price_ma60_ratio'] = close.iloc[-1] / ma60 - 1 if ma60 != 0 else 0
        
        features['volume_ratio'] = self.technical.calculate_volume_ratio(volume)
        features['volatility'] = self.technical.calculate_volatility(close)
        
        features['return_5d'] = close.pct_change(5).iloc[-1] if len(close) >= 6 else 0
        features['return_10d'] = close.pct_change(10).iloc[-1] if len(close) >= 11 else 0
        features['return_20d'] = close.pct_change(20).iloc[-1] if len(close) >= 21 else 0
        features['return_60d'] = close.pct_change(60).iloc[-1] if len(close) >= 61 else 0
        features['return_120d'] = close.pct_change(120).iloc[-1] if len(close) >= 121 else 0
        
        features['momentum_5d'] = features['return_5d']
        features['momentum_10d'] = features['return_10d']
        features['momentum_20d'] = features['return_20d']
        features['momentum_60d'] = features['return_60d']
        
        ma5 = self.technical.calculate_ma(close, 5)
        bullish_count = 0
        if close.iloc[-1] > ma5:
            bullish_count += 1
        if ma5 > ma20:
            bullish_count += 1
        if ma20 > ma60:
            bullish_count += 1
        features['ma_bullish_count'] = bullish_count
        
        returns = close.pct_change().dropna()
        vol_short = np.std(returns[-10:]) if len(returns) >= 10 else 0
        vol_long = np.std(returns[-30:]) if len(returns) >= 30 else vol_short
        features['volatility_trend'] = vol_short - vol_long
        
        if len(close) >= 60:
            y60 = close.tail(60).values.astype(float)
            x60 = np.arange(len(y60), dtype=float)
            features['trend_slope_60'] = float(np.polyfit(x60, y60, 1)[0] / max(abs(float(close.iloc[-1])), 1e-6))
            price_min = np.min(close[-60:])
            price_max = np.max(close[-60:])
            features['price_position_60d'] = (close.iloc[-1] - price_min) / (price_max - price_min) if price_max != price_min else 0.5
        else:
            features['trend_slope_60'] = 0.0
            features['price_position_60d'] = 0.5

        if len(close) >= 120:
            y120 = close.tail(120).values.astype(float)
            x120 = np.arange(len(y120), dtype=float)
            features['trend_slope_120'] = float(np.polyfit(x120, y120, 1)[0] / max(abs(float(close.iloc[-1])), 1e-6))
            price_min_120 = np.min(close[-120:])
            price_max_120 = np.max(close[-120:])
            features['price_position_120d'] = (close.iloc[-1] - price_min_120) / (price_max_120 - price_min_120) if price_max_120 != price_min_120 else 0.5
            features['close_to_high_120'] = (float(close.iloc[-1]) - float(price_max_120)) / max(abs(float(price_max_120)), 1e-6)
            features['close_to_low_120'] = (float(close.iloc[-1]) - float(price_min_120)) / max(abs(float(price_min_120)), 1e-6)
        else:
            features['trend_slope_120'] = features['trend_slope_60']
            features['price_position_120d'] = features['price_position_60d']
            features['close_to_high_120'] = 0.0
            features['close_to_low_120'] = 0.0
        
        # 外部特征（优先使用传入真实数据）
        valuation_data = valuation_data or {}
        market_data = market_data or {}

        features['net_mf_amount'] = market_data.get('net_mf_amount', valuation_data.get('net_mf_amount', 0))
        features['pe'] = valuation_data.get('pe', 0)
        features['pb'] = valuation_data.get('pb', 0)
        features['eps'] = valuation_data.get('eps', 0)
        features['roe'] = valuation_data.get('roe', 0)
        features['has_top_list'] = market_data.get('has_top_list', 0)
        features['north_money'] = market_data.get('north_money', 0)
        features['rzye'] = market_data.get('rzye', 0)
        features['rzmre'] = market_data.get('rzmre', 0)
        features['sentiment'] = market_data.get('sentiment', 0)
        features['has_report'] = market_data.get('has_report', 0)
        features['cpi_yoy'] = market_data.get('cpi_yoy', 0)
        features['pmi'] = market_data.get('pmi', 0)
        features['shibor_1m'] = market_data.get('shibor_1m', 0)
        features['shibor_3m'] = market_data.get('shibor_3m', 0)
        features['macro_regime_score'] = market_data.get('macro_regime_score', 0)
        features['risk_off_proxy'] = market_data.get('risk_off_proxy', 0)
        features['dollar_proxy'] = market_data.get('dollar_proxy', 0)
        features['gold_oil_ratio'] = market_data.get('gold_oil_ratio', 0)
        features['sentiment_ma3'] = market_data.get('sentiment_ma3', 0)
        features['report_decay_3d'] = market_data.get('report_decay_3d', 0)
        features['is_a_asset'] = market_data.get('is_a_asset', 0)
        features['is_hk_asset'] = market_data.get('is_hk_asset', 0)
        features['is_us_asset'] = market_data.get('is_us_asset', 0)
        features['is_fund_asset'] = market_data.get('is_fund_asset', 0)
        features['is_metal_asset'] = market_data.get('is_metal_asset', 0)
        features['is_foreign_asset'] = market_data.get('is_foreign_asset', 0)
        features['event_heat'] = float(features['has_report']) + float(features['has_top_list']) + (0.4 * abs(float(features['sentiment']))) + (0.8 * float(features['report_decay_3d']))
        
        # 处理缺失值
        for k, v in features.items():
            if pd.isna(v) or np.isinf(v):
                features[k] = 0
        
        # 按训练时的顺序返回25个特征
        feature_order = [
            'rsi', 'macd_hist', 'price_ma20_ratio', 'price_ma60_ratio',
            'volume_ratio', 'volatility', 'return_5d', 'return_10d', 'return_20d', 'return_60d', 'return_120d',
            'momentum_5d', 'momentum_10d', 'momentum_20d', 'momentum_60d', 'ma_bullish_count', 'volatility_trend',
            'trend_slope_60', 'trend_slope_120', 'price_position_60d', 'price_position_120d', 'close_to_high_120', 'close_to_low_120',
            'net_mf_amount', 'pe', 'pb', 'eps', 'roe',
            'has_top_list', 'north_money', 'rzye', 'rzmre', 'sentiment', 'has_report',
            'cpi_yoy', 'pmi', 'shibor_1m', 'shibor_3m', 'macro_regime_score',
            'risk_off_proxy', 'dollar_proxy', 'gold_oil_ratio', 'sentiment_ma3', 'report_decay_3d',
            'is_a_asset', 'is_hk_asset', 'is_us_asset', 'is_fund_asset', 'is_metal_asset', 'is_foreign_asset', 'event_heat'
        ]
        
        X = pd.DataFrame([[features[f] for f in feature_order]], columns=feature_order)
        return X
    
    def train(self, X, y, sample_weight=None, model_params=None):
        """训练模型"""
        try:
            import xgboost as xgb
            pos = int((y == 1).sum())
            neg = int((y == 0).sum())
            scale_pos_weight = (neg / pos) if pos > 0 else 1.0
            scale_pos_weight = max(1.0, min(5.0, scale_pos_weight))

            params = {
                'n_estimators': 220,
                'max_depth': 4,
                'learning_rate': 0.04,
                'subsample': 0.9,
                'colsample_bytree': 0.85,
                'min_child_weight': 6,
                'gamma': 0.15,
                'reg_lambda': 4.0,
                'reg_alpha': 1.0,
                'random_state': 42,
                'eval_metric': 'logloss',
                'scale_pos_weight': scale_pos_weight
            }
            if model_params:
                params.update(model_params)

            self.model = xgb.XGBClassifier(**params)
            self.feature_columns = list(X.columns)
            if sample_weight is not None:
                self.model.fit(X, y, sample_weight=sample_weight)
            else:
                self.model.fit(X, y)
            self.is_trained = True
            return self.model.score(X, y)
        except Exception as e:
            logger.error(f"训练失败: {e}")
            self.is_trained = False
            return 0
    
    def predict(self, X, valuation_data=None, market_data=None):
        """预测上涨概率，兼容原始行情DataFrame与已准备好的特征矩阵。"""
        if self.is_trained and self.model is not None:
            try:
                if isinstance(X, pd.DataFrame) and 'close' in X.columns:
                    feature_df = self.prepare_features(X, valuation_data, market_data)
                    if feature_df is None:
                        return self._fallback_predict(X, valuation_data)
                    X = feature_df
                X_infer = self._align_features_for_model(X, model=self.model)
                prob = self.model.predict_proba(X_infer)[0]
                return float(prob[1] * 100)
            except Exception as e:
                logger.error(f"预测失败: {e}")

        if isinstance(X, pd.DataFrame) and 'close' in X.columns:
            return self._fallback_predict(X, valuation_data)
        return 50
    
    def calculate_price_range(self, df, shift_days=None, confidence=0.7):
        """计算价格区间"""
        if shift_days is None:
            shift_days = self.period_days
        
        current_price = df['close'].iloc[-1]
        avg_annual_return = 0.08
        expected_return = avg_annual_return * (shift_days / 252)
        
        returns = df['close'].pct_change().dropna()
        volatility = returns.std() * np.sqrt(252)
        future_volatility = volatility * np.sqrt(shift_days / 252)
        
        from scipy import stats
        z_score = stats.norm.ppf((1 + confidence) / 2)
        
        lower = current_price * (1 + expected_return - z_score * future_volatility)
        upper = current_price * (1 + expected_return + z_score * future_volatility)
        
        lower = max(lower, current_price * 0.7)
        upper = min(upper, current_price * 1.5)
        
        return lower, upper
    
    def get_prediction_result(self, df, valuation_data=None, market_data=None, volatility_level='medium'):
        """获取完整的预测结果"""
        X = self.prepare_features(df, valuation_data, market_data)
        if X is None:
            up_probability = 50
            confidence = 55.0
        elif self.is_trained and self.model is not None:
            X_aligned = self._align_features_for_model(X, model=self.model)
            up_probability = self.predict(X_aligned)
            confidence = self.calculate_confidence(X_aligned)
        else:
            # 降级到估值回归方法
            up_probability = self._fallback_predict(df, valuation_data)
            confidence = 55.0
        
        target_low, target_high = self.calculate_price_range(df)
        current_price = df['close'].iloc[-1]
        stop_loss = current_price * 0.85
        expiry_date = datetime.now().date() + timedelta(days=self.period_days)
        
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
    
    def calculate_valuation_regression(self, pe, pb=None, pe_percentile=None, pb_percentile=None):
        """根据估值分位做长期回归判断，返回上涨概率。"""
        try:
            if pe_percentile is None:
                pe_percentile = min(max(float(pe or 0) / 50.0 * 100.0, 0.0), 100.0)
            if pb_percentile is None:
                pb_percentile = min(max(float((pb if pb is not None else pe) or 0) / 5.0 * 100.0, 0.0), 100.0)
        except Exception:
            pe_percentile = 50
            pb_percentile = 50

        avg_pct = (float(pe_percentile) + float(pb_percentile)) / 2.0
        if avg_pct < 20:
            return 70.0
        if avg_pct < 35:
            return 65.0
        if avg_pct < 50:
            return 55.0
        if avg_pct < 65:
            return 45.0
        if avg_pct < 80:
            return 35.0
        return 30.0

    def calculate_trend_regression(self, df):
        """根据长期均线趋势做回归判断，返回上涨概率。"""
        if df is None or len(df) < 20 or 'close' not in df.columns:
            return 50.0

        close = df['close']
        ma60 = self.technical.calculate_ma(close, 60)
        ma120 = self.technical.calculate_ma(close, 120) if len(close) >= 120 else ma60
        current_price = close.iloc[-1]

        if current_price > ma60 > ma120:
            return 65.0
        if current_price > ma60:
            return 55.0
        if current_price < ma60 < ma120:
            return 35.0
        if current_price < ma60:
            return 45.0
        return 50.0

    def _fallback_predict(self, df, valuation_data=None):
        """降级预测方法（当模型不可用时）"""
        valuation_data = valuation_data or {}
        valuation_prob = self.calculate_valuation_regression(
            valuation_data.get('pe', 0),
            valuation_data.get('pb', 0),
            valuation_data.get('pe_percentile', 50),
            valuation_data.get('pb_percentile', 50),
        )
        trend_prob = self.calculate_trend_regression(df)
        return valuation_prob * 0.6 + trend_prob * 0.4