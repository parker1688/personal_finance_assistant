"""
5日短期预测模型 - predictors/short_term.py
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


class ShortTermPredictor(BasePredictor):
    """5日短期预测模型"""
    
    def __init__(self):
        super().__init__(period_days=5)
        self.technical = TechnicalIndicator()
        self.is_trained = False
        self.model = None
        self.feature_columns = None
        self.calibrator = None
        self.calibration_method = 'none'
        self.regime_models = {}
        self.volatility_split = None
        self.blend_model = None
        self.blend_weight = 0.65
        self.blend_enabled = False

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
        features['return_1d'] = close.pct_change(1).iloc[-1] if len(close) >= 2 else 0
        features['return_3d'] = close.pct_change(3).iloc[-1] if len(close) >= 4 else 0
        
        features['momentum_5d'] = features['return_5d']
        features['momentum_10d'] = features['return_10d']

        last_open = float(df['open'].iloc[-1]) if len(df) > 0 else 0.0
        last_close = float(close.iloc[-1]) if len(close) > 0 else 0.0
        last_high = float(high.iloc[-1]) if len(high) > 0 else 0.0
        last_low = float(low.iloc[-1]) if len(low) > 0 else 0.0
        features['oc_change_1d'] = (last_close - last_open) / last_open if last_open > 0 else 0.0
        features['hl_range_1d'] = (last_high - last_low) / last_close if last_close > 0 else 0.0

        upper_shadow = last_high - max(last_open, last_close)
        lower_shadow = min(last_open, last_close) - last_low
        features['upper_shadow_ratio'] = upper_shadow / last_close if last_close > 0 else 0.0
        features['lower_shadow_ratio'] = lower_shadow / last_close if last_close > 0 else 0.0

        if len(volume) >= 2:
            features['volume_change_1d'] = float(volume.iloc[-1] / max(volume.iloc[-2], 1e-6) - 1.0)
        else:
            features['volume_change_1d'] = 0.0

        volume_ma20 = float(volume.tail(20).mean()) if len(volume) >= 1 else 0.0
        features['volume_ma20_ratio'] = float(volume.iloc[-1] / volume_ma20 - 1.0) if volume_ma20 > 0 else 0.0

        if len(close) >= 20:
            y20 = close.tail(20).values.astype(float)
            x20 = np.arange(len(y20), dtype=float)
            slope = np.polyfit(x20, y20, 1)[0]
            features['trend_slope_20'] = float(slope / max(abs(last_close), 1e-6))
            high20 = float(np.max(high.tail(20)))
            low20 = float(np.min(low.tail(20)))
            features['close_to_high_20'] = (last_close - high20) / max(high20, 1e-6)
            features['close_to_low_20'] = (last_close - low20) / max(low20, 1e-6)
        else:
            features['trend_slope_20'] = 0.0
            features['close_to_high_20'] = 0.0
            features['close_to_low_20'] = 0.0
        
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
        recent10 = returns.tail(10)
        if len(recent10) > 0:
            up_days = float((recent10 > 0).sum())
            down_days = float((recent10 < 0).sum())
            features['up_days_ratio_10d'] = up_days / len(recent10)
            features['trend_consistency_10d'] = abs(up_days - down_days) / len(recent10)
            pos_r = recent10[recent10 > 0]
            neg_r = recent10[recent10 < 0]
            features['avg_gain_10d'] = float(pos_r.mean()) if len(pos_r) > 0 else 0.0
            features['avg_loss_10d'] = float(abs(neg_r.mean())) if len(neg_r) > 0 else 0.0
        else:
            features['up_days_ratio_10d'] = 0.5
            features['trend_consistency_10d'] = 0.0
            features['avg_gain_10d'] = 0.0
            features['avg_loss_10d'] = 0.0

        if len(returns) >= 25:
            rolling_std = returns.rolling(5).std().dropna()
            features['vol_of_vol_20d'] = float(rolling_std.tail(20).std()) if len(rolling_std) >= 20 else float(rolling_std.std())
        else:
            features['vol_of_vol_20d'] = 0.0

        vol_short = np.std(returns[-10:]) if len(returns) >= 10 else 0
        vol_long = np.std(returns[-30:]) if len(returns) >= 30 else vol_short
        features['volatility_trend'] = vol_short - vol_long
        
        if len(close) >= 60:
            price_min = np.min(close[-60:])
            price_max = np.max(close[-60:])
            features['price_position_60d'] = (close.iloc[-1] - price_min) / (price_max - price_min) if price_max != price_min else 0.5
        else:
            features['price_position_60d'] = 0.5
        
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
        features['vix'] = market_data.get('vix', 0)
        features['dxy'] = market_data.get('dxy', 0)
        features['tnx'] = market_data.get('tnx', 0)
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
        features['sentiment_abs'] = abs(float(features['sentiment']))
        features['report_sentiment_interaction'] = float(features['has_report']) * float(features['sentiment'])
        features['event_heat'] = float(features['has_report']) + float(features['has_top_list']) + (0.5 * float(features['sentiment_abs'])) + (0.7 * float(features['report_decay_3d']))
        
        # 处理缺失值
        for k, v in features.items():
            if pd.isna(v) or np.isinf(v):
                features[k] = 0
        
        # 按训练时顺序返回固定特征集
        feature_order = ['rsi', 'macd_hist', 'price_ma20_ratio', 'price_ma60_ratio',
                 'volume_ratio', 'volatility', 'return_5d', 'return_10d', 'return_20d',
                 'return_1d', 'return_3d', 'momentum_5d', 'momentum_10d',
                 'ma_bullish_count', 'volatility_trend', 'price_position_60d',
                 'oc_change_1d', 'hl_range_1d', 'upper_shadow_ratio', 'lower_shadow_ratio',
                 'volume_change_1d', 'volume_ma20_ratio', 'trend_slope_20',
                 'close_to_high_20', 'close_to_low_20',
                 'up_days_ratio_10d', 'trend_consistency_10d', 'avg_gain_10d', 'avg_loss_10d', 'vol_of_vol_20d',
                 'net_mf_amount', 'pe', 'pb', 'eps', 'roe',
             'has_top_list', 'north_money', 'rzye', 'rzmre', 'sentiment', 'has_report',
             'cpi_yoy', 'pmi', 'shibor_1m', 'shibor_3m', 'macro_regime_score',
             'risk_off_proxy', 'dollar_proxy', 'gold_oil_ratio',
             'vix', 'dxy', 'tnx',
             'sentiment_lag1', 'sentiment_ma3', 'sentiment_shock', 'report_decay_3d',
             'is_a_asset', 'is_hk_asset', 'is_us_asset', 'is_fund_asset', 'is_metal_asset', 'is_foreign_asset',
             'sentiment_abs', 'report_sentiment_interaction', 'event_heat']
        
        X = pd.DataFrame([[features[f] for f in feature_order]], columns=feature_order)
        return X

    def _align_features_for_model(self, X, model=None):
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

    def _predict_up_probability_raw(self, X):
        """返回未校准的上涨概率(0-1)，支持按波动率路由到分层子模型。"""
        if X is None:
            return None

        model_to_use = self.model
        if self.regime_models and self.volatility_split is not None and 'volatility' in X.columns:
            try:
                v = float(X.iloc[0]['volatility'])
                if v <= float(self.volatility_split):
                    model_to_use = self.regime_models.get('low_vol') or model_to_use
                else:
                    model_to_use = self.regime_models.get('high_vol') or model_to_use
            except Exception:
                pass

        X_infer = self._align_features_for_model(X, model=model_to_use)
        p_base = self._predict_model_up_proba(model_to_use, X_infer)
        if p_base is None:
            return None

        if self.blend_enabled and self.blend_model is not None:
            X_blend = self._align_features_for_model(X, model=self.blend_model)
            p_aux = self._predict_model_up_proba(self.blend_model, X_blend)
            if p_aux is not None:
                w = max(0.0, min(1.0, float(self.blend_weight)))
                p_base = (w * p_base) + ((1.0 - w) * p_aux)

        return max(1e-6, min(1 - 1e-6, float(p_base)))

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
    
    def train(self, X, y, sample_weight=None, model_params=None):
        """训练模型"""
        try:
            import xgboost as xgb
            pos = int((y == 1).sum())
            neg = int((y == 0).sum())
            scale_pos_weight = (neg / pos) if pos > 0 else 1.0
            scale_pos_weight = max(1.0, min(5.0, scale_pos_weight))

            params = {
                'n_estimators': 150,
                'max_depth': 5,
                'learning_rate': 0.05,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42,
                'eval_metric': 'logloss',
                'scale_pos_weight': scale_pos_weight,
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
    
    def predict(self, X):
        """预测上涨概率"""
        if self.is_trained and self.model is not None:
            try:
                p_raw = self._predict_up_probability_raw(X)
                if p_raw is None:
                    return 50
                p_up = self._apply_calibration(p_raw)
                return p_up * 100
            except Exception as e:
                logger.error(f"预测失败: {e}")
        return 50

    def _fallback_predict(self, df):
        """5日降级预测：基于短期技术信号的确定性规则。"""
        try:
            close = df['close']
            rsi = self.technical.calculate_rsi(close)
            macd = self.technical.calculate_macd(close)
            ma5 = self.technical.calculate_ma(close, 5)
            ma20 = self.technical.calculate_ma(close, 20)
            ret_5d = close.pct_change(5).iloc[-1] if len(close) >= 6 else 0.0

            score = 50.0

            # RSI: 超卖偏多，超买偏空
            if rsi < 30:
                score += 8
            elif rsi < 40:
                score += 4
            elif rsi > 70:
                score -= 8
            elif rsi > 60:
                score -= 4

            # MACD 柱
            hist = float(macd.get('hist', 0.0))
            if hist > 0:
                score += 5
            elif hist < 0:
                score -= 5

            # 均线关系
            if ma5 > ma20:
                score += 4
            else:
                score -= 4

            # 5日动量（防止追涨杀跌过度，设置裁剪）
            score += max(-6.0, min(6.0, float(ret_5d) * 100.0 * 0.6))

            return max(20.0, min(80.0, score))
        except Exception as e:
            logger.warning(f"5日降级预测失败: {e}")
            return 50.0
    
    def get_prediction_result(self, df, valuation_data=None, market_data=None, volatility_level='medium'):
        X = self.prepare_features(df, valuation_data=valuation_data, market_data=market_data)
        if X is None:
            up_probability = 50
            confidence = 55.0
        elif self.is_trained and self.model is not None:
            up_probability = self.predict(X)
            # 置信度与校准后概率保持一致
            margin = min(1.0, abs(up_probability / 100.0 - 0.5) * 2.0)
            confidence = max(50.0, min(95.0, 50.0 + 45.0 * margin))
        else:
            up_probability = self._fallback_predict(df)
            confidence = 52.0
        
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