"""
20日中期预测模型 - predictors/medium_term.py
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


class MediumTermPredictor(BasePredictor):
    """20日中期预测模型"""
    
    def __init__(self):
        super().__init__(period_days=20)
        self.technical = TechnicalIndicator()
        self.is_trained = False
        self.model = None
        self.feature_columns = None
        self.calibrator = None
        self.calibration_method = 'none'

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
    
    @staticmethod
    def _predict_model_up_proba(model, X):
        if model is None or X is None or len(X) == 0:
            return None
        if hasattr(model, 'predict_proba'):
            prob = model.predict_proba(X)[0]
            return float(prob[1])

        raw = model.predict(X)
        raw_v = float(np.asarray(raw).reshape(-1)[0])
        if 0.0 <= raw_v <= 1.0:
            return max(1e-6, min(1 - 1e-6, raw_v))

        z = max(-20.0, min(20.0, raw_v))
        p = 1.0 / (1.0 + np.exp(-z))
        return max(1e-6, min(1 - 1e-6, float(p)))

    def prepare_features(self, df, valuation_data=None, market_data=None):
        """准备特征数据（强化20日趋势/风险区分度）"""
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
        features['atr_ratio'] = self.technical.calculate_atr(high, low, close) / max(float(close.iloc[-1]), 1e-6)
        
        features['return_1d'] = close.pct_change(1).iloc[-1] if len(close) >= 2 else 0
        features['return_3d'] = close.pct_change(3).iloc[-1] if len(close) >= 4 else 0
        features['return_5d'] = close.pct_change(5).iloc[-1] if len(close) >= 6 else 0
        features['return_10d'] = close.pct_change(10).iloc[-1] if len(close) >= 11 else 0
        features['return_20d'] = close.pct_change(20).iloc[-1] if len(close) >= 21 else 0
        features['return_60d'] = close.pct_change(60).iloc[-1] if len(close) >= 61 else 0
        
        features['momentum_5d'] = features['return_5d']
        features['momentum_10d'] = features['return_10d']
        features['momentum_20d'] = features['return_20d']
        
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
        
        if len(close) >= 20:
            y20 = close.tail(20).values.astype(float)
            x20 = np.arange(len(y20), dtype=float)
            features['trend_slope_20'] = float(np.polyfit(x20, y20, 1)[0] / max(abs(float(close.iloc[-1])), 1e-6))
        else:
            features['trend_slope_20'] = 0.0

        if len(close) >= 60:
            y60 = close.tail(60).values.astype(float)
            x60 = np.arange(len(y60), dtype=float)
            features['trend_slope_60'] = float(np.polyfit(x60, y60, 1)[0] / max(abs(float(close.iloc[-1])), 1e-6))
            price_min = np.min(close[-60:])
            price_max = np.max(close[-60:])
            features['price_position_60d'] = (close.iloc[-1] - price_min) / (price_max - price_min) if price_max != price_min else 0.5
            features['close_to_high_60'] = (float(close.iloc[-1]) - float(price_max)) / max(abs(float(price_max)), 1e-6)
            features['close_to_low_60'] = (float(close.iloc[-1]) - float(price_min)) / max(abs(float(price_min)), 1e-6)
            features['drawdown_60d'] = (float(close.iloc[-1]) / max(abs(float(price_max)), 1e-6)) - 1.0
        else:
            features['trend_slope_60'] = 0.0
            features['price_position_60d'] = 0.5
            features['close_to_high_60'] = 0.0
            features['close_to_low_60'] = 0.0
            features['drawdown_60d'] = 0.0

        if len(close) >= 120:
            price_min_120 = np.min(close[-120:])
            price_max_120 = np.max(close[-120:])
            features['price_position_120d'] = (close.iloc[-1] - price_min_120) / (price_max_120 - price_min_120) if price_max_120 != price_min_120 else 0.5
        else:
            features['price_position_120d'] = features['price_position_60d']

        recent20 = close.pct_change().dropna().tail(20)
        if len(recent20) > 0:
            up_days = float((recent20 > 0).sum())
            down_days = float((recent20 < 0).sum())
            features['up_days_ratio_20d'] = up_days / len(recent20)
            features['trend_consistency_20d'] = abs(up_days - down_days) / len(recent20)
        else:
            features['up_days_ratio_20d'] = 0.5
            features['trend_consistency_20d'] = 0.0

        if len(volume) >= 20:
            recent_vol = float(volume.tail(10).mean())
            prior_vol = float(volume.tail(20).head(10).mean())
            features['volume_trend_20d'] = (recent_vol / max(prior_vol, 1e-6)) - 1.0
        else:
            features['volume_trend_20d'] = 0.0

        if len(close) >= 20:
            high20 = float(np.max(high.tail(20)))
            low20 = float(np.min(low.tail(20)))
            last_close = float(close.iloc[-1])
            features['breakout_20d'] = (last_close / max(high20, 1e-6)) - 1.0
            features['rebound_from_low_20d'] = (last_close / max(low20, 1e-6)) - 1.0
            bb = self.technical.calculate_bollinger_bands(close, period=20)
            upper = float(bb.get('upper') or last_close)
            lower = float(bb.get('lower') or last_close)
            middle = float(bb.get('middle') or last_close)
            features['bb_width_20'] = (upper - lower) / max(abs(middle), 1e-6)
        else:
            features['breakout_20d'] = 0.0
            features['rebound_from_low_20d'] = 0.0
            features['bb_width_20'] = 0.0

        if len(close) >= 60:
            ret20 = close.pct_change().dropna().tail(20)
            ret60 = close.pct_change().dropna().tail(60)
            vol20 = float(ret20.std()) if len(ret20) > 5 else 0.0
            vol60 = float(ret60.std()) if len(ret60) > 10 else max(vol20, 1e-6)
            features['volatility_ratio_20_60'] = (vol20 / max(vol60, 1e-6)) - 1.0
        else:
            features['volatility_ratio_20_60'] = 0.0
        
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
        features['sentiment_lag1'] = market_data.get('sentiment_lag1', 0)
        features['sentiment_ma3'] = market_data.get('sentiment_ma3', 0)
        features['sentiment_shock'] = market_data.get('sentiment_shock', 0)
        features['report_decay_3d'] = market_data.get('report_decay_3d', 0)
        features['is_a_asset'] = market_data.get('is_a_asset', 0)
        features['is_hk_asset'] = market_data.get('is_hk_asset', 0)
        features['is_us_asset'] = market_data.get('is_us_asset', 0)
        features['is_fund_asset'] = market_data.get('is_fund_asset', 0)
        features['is_metal_asset'] = market_data.get('is_metal_asset', 0)
        features['is_foreign_asset'] = market_data.get('is_foreign_asset', 0)
        features['event_heat'] = float(features['has_report']) + float(features['has_top_list']) + (0.5 * abs(float(features['sentiment']))) + (0.6 * float(features['report_decay_3d']))
        
        # 处理缺失值
        for k, v in features.items():
            if pd.isna(v) or np.isinf(v):
                features[k] = 0
        
        # 按训练时的顺序返回25个特征
        feature_order = [
            'rsi', 'macd_hist', 'price_ma20_ratio', 'price_ma60_ratio',
            'volume_ratio', 'volatility', 'atr_ratio',
            'return_1d', 'return_3d', 'return_5d', 'return_10d', 'return_20d', 'return_60d',
            'momentum_5d', 'momentum_10d', 'momentum_20d', 'ma_bullish_count', 'volatility_trend',
            'trend_slope_20', 'trend_slope_60', 'price_position_60d', 'price_position_120d', 'close_to_high_60', 'close_to_low_60',
            'drawdown_60d', 'breakout_20d', 'rebound_from_low_20d', 'trend_consistency_20d', 'up_days_ratio_20d',
            'volume_trend_20d', 'volatility_ratio_20_60', 'bb_width_20',
            'net_mf_amount', 'pe', 'pb', 'eps', 'roe',
            'has_top_list', 'north_money', 'rzye', 'rzmre', 'sentiment', 'has_report',
            'cpi_yoy', 'pmi', 'shibor_1m', 'shibor_3m', 'macro_regime_score',
            'risk_off_proxy', 'dollar_proxy', 'gold_oil_ratio',
            'sentiment_lag1', 'sentiment_ma3', 'sentiment_shock', 'report_decay_3d',
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
                'subsample': 0.85,
                'colsample_bytree': 0.8,
                'min_child_weight': 8,
                'gamma': 0.2,
                'reg_lambda': 4.0,
                'reg_alpha': 1.2,
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
    
    def _apply_calibration(self, proba_up):
        p = float(proba_up)
        if self.calibrator is None or self.calibration_method == 'none':
            return max(1e-6, min(1 - 1e-6, p))

        try:
            if self.calibration_method == 'platt':
                p = float(self.calibrator.predict_proba(np.array([[p]]))[:, 1][0])
            elif self.calibration_method == 'isotonic':
                p = float(self.calibrator.predict(np.array([p]))[0])
        except Exception:
            return max(1e-6, min(1 - 1e-6, float(proba_up)))

        return max(1e-6, min(1 - 1e-6, p))

    def predict(self, X):
        """预测上涨概率"""
        if self.is_trained and self.model is not None:
            try:
                X_infer = self._align_features_for_model(X, model=self.model)
                p_raw = self._predict_model_up_proba(self.model, X_infer)
                if p_raw is None:
                    return 50
                p_up = self._apply_calibration(p_raw)
                return float(p_up * 100)
            except Exception as e:
                logger.error(f"预测失败: {e}")
        return 50
    
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
            up_probability = 50
            confidence = 55.0
        
        target_low, target_high = self.calculate_price_range(df)
        current_price = df['close'].iloc[-1]
        stop_loss = self.get_stop_loss(current_price, volatility_level)
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