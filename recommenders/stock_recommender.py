"""
股票推荐引擎 - recommenders/stock_recommender.py
基于综合评分筛选TOP推荐标的
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.technical import TechnicalIndicator
from indicators.scorer import Scorer as ScoreCalculator
from predictors.short_term import ShortTermPredictor
from predictors.medium_term import MediumTermPredictor
from predictors.long_term import LongTermPredictor
from predictors.model_manager import ModelManager
from collectors.stock_collector import StockCollector
from models import Indicator
from recommendation_probability import derive_unified_trend
from utils import get_logger

logger = get_logger(__name__)


class StockRecommender:
    """股票推荐引擎"""
    
    def __init__(self):
        self.technical = TechnicalIndicator()
        self.scorer = ScoreCalculator()
        self.short_predictor = ShortTermPredictor()
        self.medium_predictor = MediumTermPredictor()
        self.long_predictor = LongTermPredictor()
        self.model_manager = ModelManager()
        self.collector = StockCollector()
        self._macro_snapshot = self._load_latest_macro_snapshot()
        self._model_quality_snapshot = self._load_model_quality_snapshot()
        self._predictor_contexts = self._build_market_predictor_contexts()

        default_context = self._resolve_market_predictor_context('A')
        self.short_predictor = default_context.get('short_term') or self.short_predictor
        self.medium_predictor = default_context.get('medium_term') or self.medium_predictor
        self.long_predictor = default_context.get('long_term') or self.long_predictor
        self._model_quality_snapshot = default_context.get('quality_snapshot') or self._model_quality_snapshot

        self.a_stock_pool = self.collector.a_stock_pool
        self.hk_stock_pool = self.collector.hk_stock_pool
        self.us_stock_pool = self.collector.us_stock_pool

    def _has_sufficient_local_history(self, code, min_rows=60):
        try:
            df = self.collector.get_stock_data_from_db(code)
            return df is not None and len(df) >= int(min_rows)
        except Exception:
            return False

    def _load_latest_macro_snapshot(self):
        """加载本地宏观快照，供在线预测注入统一外部因子。"""
        snap = {
            'cpi_yoy': 0.0,
            'pmi': 0.0,
            'shibor_1m': 0.0,
            'shibor_3m': 0.0,
            'macro_regime_score': 0.0,
            'risk_off_proxy': 0.0,
            'dollar_proxy': 0.0,
            'gold_oil_ratio': 0.0,
            'vix': 0.0,
            'dxy': 0.0,
            'tnx': 0.0,
        }

        data_dir = 'data'
        try:
            cpi_path = os.path.join(data_dir, 'macro_cpi.csv')
            if os.path.exists(cpi_path):
                cpi_df = pd.read_csv(cpi_path)
                if not cpi_df.empty:
                    cpi_col = 'nt_yoy' if 'nt_yoy' in cpi_df.columns else ('cnt_yoy' if 'cnt_yoy' in cpi_df.columns else None)
                    if cpi_col is not None:
                        cpi_df['trade_date'] = pd.to_datetime(cpi_df.get('month'), format='%Y%m', errors='coerce')
                        cpi_df[cpi_col] = pd.to_numeric(cpi_df[cpi_col], errors='coerce')
                        cpi_df = cpi_df.dropna(subset=['trade_date']).sort_values('trade_date')
                        if not cpi_df.empty:
                            snap['cpi_yoy'] = float(cpi_df.iloc[-1][cpi_col])
        except Exception as e:
            logger.warning(f"宏观快照CPI读取失败: {e}")

        try:
            pmi_path = os.path.join(data_dir, 'macro_pmi.csv')
            if os.path.exists(pmi_path):
                pmi_df = pd.read_csv(pmi_path)
                if not pmi_df.empty:
                    month_col = 'MONTH' if 'MONTH' in pmi_df.columns else ('month' if 'month' in pmi_df.columns else None)
                    pmi_col = None
                    for cand in ['PMI010000', 'PMI010100', 'PMI010400']:
                        if cand in pmi_df.columns:
                            pmi_col = cand
                            break
                    if month_col is not None and pmi_col is not None:
                        pmi_df['trade_date'] = pd.to_datetime(pmi_df[month_col], format='%Y%m', errors='coerce')
                        pmi_df[pmi_col] = pd.to_numeric(pmi_df[pmi_col], errors='coerce')
                        pmi_df = pmi_df.dropna(subset=['trade_date']).sort_values('trade_date')
                        if not pmi_df.empty:
                            snap['pmi'] = float(pmi_df.iloc[-1][pmi_col])
        except Exception as e:
            logger.warning(f"宏观快照PMI读取失败: {e}")

        try:
            shibor_path = os.path.join(data_dir, 'macro_shibor.csv')
            if os.path.exists(shibor_path):
                s_df = pd.read_csv(shibor_path)
                if not s_df.empty and 'date' in s_df.columns:
                    s_df['trade_date'] = pd.to_datetime(s_df['date'], errors='coerce')
                    s_df['1m'] = pd.to_numeric(s_df.get('1m', 0.0), errors='coerce')
                    s_df['3m'] = pd.to_numeric(s_df.get('3m', 0.0), errors='coerce')
                    s_df = s_df.dropna(subset=['trade_date']).sort_values('trade_date')
                    if not s_df.empty:
                        snap['shibor_1m'] = float(s_df.iloc[-1]['1m']) if not pd.isna(s_df.iloc[-1]['1m']) else 0.0
                        snap['shibor_3m'] = float(s_df.iloc[-1]['3m']) if not pd.isna(s_df.iloc[-1]['3m']) else 0.0
        except Exception as e:
            logger.warning(f"宏观快照Shibor读取失败: {e}")

        score = 0.0
        if snap['pmi'] > 0:
            score += float(np.clip((snap['pmi'] - 50.0) / 4.0, -0.5, 0.5))
        if snap['cpi_yoy'] > 0:
            if 0.0 <= snap['cpi_yoy'] <= 3.0:
                score += 0.2
            elif snap['cpi_yoy'] >= 4.0:
                score -= 0.2
        if snap['shibor_1m'] > 0 and snap['shibor_3m'] > 0:
            score += float(np.clip(-(snap['shibor_3m'] - snap['shibor_1m']) / 3.0, -0.25, 0.25))
        snap['macro_regime_score'] = float(np.clip(score, -1.0, 1.0))

        try:
            cross_path = os.path.join(data_dir, 'cross_asset_daily.csv')
            if os.path.exists(cross_path):
                cdf = pd.read_csv(cross_path)
                if not cdf.empty and 'trade_date' in cdf.columns:
                    cdf['trade_date'] = pd.to_datetime(cdf['trade_date'], errors='coerce')
                    cdf = cdf.dropna(subset=['trade_date']).sort_values('trade_date')
                    if not cdf.empty:
                        row = cdf.iloc[-1]
                        snap['vix'] = self._safe_float(row.get('vix'), 0.0) or 0.0
                        snap['dxy'] = self._safe_float(row.get('dxy'), 0.0) or 0.0
                        snap['tnx'] = self._safe_float(row.get('tnx'), 0.0) or 0.0
                        snap['gold_oil_ratio'] = self._safe_float(row.get('gold_oil_ratio'), 0.0) or 0.0
                        snap['risk_off_proxy'] = self._safe_float(row.get('risk_off_proxy'), 0.0) or 0.0
                        snap['dollar_proxy'] = self._safe_float(row.get('dollar_proxy'), 0.0) or 0.0
                        return snap
        except Exception as e:
            logger.warning(f"跨资产快照读取失败: {e}")

        try:
            import yfinance as yf

            def _last_close(symbol, period='1mo'):
                try:
                    h = yf.Ticker(symbol).history(period=period)
                    if h is None or h.empty or 'Close' not in h.columns:
                        return None
                    v = float(h['Close'].dropna().iloc[-1])
                    return v
                except Exception:
                    return None

            vix = _last_close('^VIX', period='5d')
            dxy = _last_close('DX-Y.NYB')
            if dxy is None:
                dxy = _last_close('UUP')
            tnx = _last_close('^TNX')
            gold = _last_close('GC=F', period='3mo')
            oil = _last_close('CL=F', period='3mo')
            if oil is None:
                oil = _last_close('BZ=F', period='3mo')

            if vix is not None:
                snap['vix'] = float(vix)
            if dxy is not None:
                snap['dxy'] = float(dxy)
            if tnx is not None:
                snap['tnx'] = float(tnx)
            if gold is not None and oil is not None and abs(float(oil)) > 1e-8:
                snap['gold_oil_ratio'] = float(gold) / float(oil)

            snap['risk_off_proxy'] = float(np.clip((snap['vix'] - 20.0) / 15.0, -1.0, 1.0))
            dollar_part = float(np.clip((snap['dxy'] - 103.0) / 8.0, -1.0, 1.0))
            rate_part = float(np.clip((snap['tnx'] - 4.5) / 2.0, -1.0, 1.0))
            snap['dollar_proxy'] = float(np.clip((0.7 * dollar_part) + (0.3 * rate_part), -1.0, 1.0))
        except Exception as e:
            logger.warning(f"跨资产在线快照拉取失败: {e}")

        return snap

    def _load_model_quality_snapshot(self):
        """读取最近训练反思结果，给在线投顾一个可靠性护栏。"""
        models_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'models')
        mapping = {
            'short_term': 'short_term_training_reflection.json',
            'medium_term': 'medium_term_training_reflection.json',
            'long_term': 'long_term_training_reflection.json',
        }
        snapshot = {}
        for horizon, filename in mapping.items():
            path = os.path.join(models_dir, filename)
            payload = {}
            try:
                if os.path.exists(path):
                    with open(path, 'r', encoding='utf-8') as f:
                        raw = json.load(f)
                    if isinstance(raw, dict) and isinstance(raw.get('history'), list) and raw['history']:
                        payload = raw['history'][-1]
                    else:
                        payload = raw or {}
            except Exception as e:
                logger.warning(f"读取模型质量快照失败 {horizon}: {e}")
                payload = {}
            snapshot[horizon] = self._score_model_reliability(payload, horizon)
        return snapshot

    def _score_model_reliability(self, payload, horizon='short_term'):
        metrics = payload.get('metrics', {}) if isinstance(payload, dict) else {}
        bias = payload.get('prediction_bias', {}) if isinstance(payload, dict) else {}

        score = 55.0
        passed = bool(payload.get('passed', False)) if isinstance(payload, dict) else False
        if passed:
            score += 8.0

        acc = self._safe_float(metrics.get('accuracy'), 0.0) or 0.0
        f1 = self._safe_float(metrics.get('f1'), 0.0) or 0.0
        auc = self._safe_float(metrics.get('auc'), 0.0) or 0.0
        brier = self._safe_float(metrics.get('brier'), 0.25)
        fpr = self._safe_float(bias.get('false_positive_rate'), None)

        if acc >= 0.56:
            score += 6.0
        elif acc < 0.53:
            score -= 8.0

        if f1 >= 0.50:
            score += 6.0
        elif f1 < 0.47:
            score -= 8.0

        if auc >= 0.62:
            score += 8.0
        elif auc < 0.58:
            score -= 10.0

        if brier <= 0.24:
            score += 4.0
        elif brier > 0.25:
            score -= 5.0

        if fpr is not None:
            if fpr > 0.46:
                score -= 12.0
            elif fpr > 0.42:
                score -= 6.0

        score = float(np.clip(score, 20.0, 90.0))
        level = 'high' if score >= 70 else ('medium' if score >= 50 else 'low')
        label = 'supportive' if level == 'high' else ('stable' if level == 'medium' else 'guarded')
        return {
            'score': round(score, 1),
            'level': level,
            'label': label,
            'passed': passed,
            'metrics': {
                'accuracy': round(acc, 4),
                'f1': round(f1, 4),
                'auc': round(auc, 4),
                'brier': round(brier, 4),
            },
        }

    def _resolve_model_reliability(self, horizon='short_term', override=None, market=None):
        if isinstance(override, dict):
            score = self._safe_float(override.get('score'), 55.0) or 55.0
            level = override.get('level') or ('high' if score >= 70 else ('medium' if score >= 50 else 'low'))
            label = override.get('label') or ('supportive' if level == 'high' else ('stable' if level == 'medium' else 'guarded'))
            return {
                'score': round(float(score), 1),
                'level': level,
                'label': label,
                'passed': bool(override.get('passed', level != 'low')),
                'metrics': override.get('metrics', {}),
                'gate': override.get('gate'),
                'reason': override.get('reason', ''),
            }

        if market is not None:
            context = self._resolve_market_predictor_context(market)
            snapshot = context.get('quality_snapshot') or {}
        else:
            snapshot = getattr(self, '_model_quality_snapshot', {}) or {}

        base = snapshot.get(horizon) or snapshot.get('short_term') or {
            'score': 55.0,
            'level': 'medium',
            'label': 'stable',
            'passed': True,
            'metrics': {},
        }
        return dict(base)

    def _build_bundle_reliability(self, bundle, horizon='short_term', fallback=None):
        fallback = fallback or {}
        metadata = bundle.get('metadata') or {}
        payload = {
            'passed': bool(bundle.get('loaded')),
            'metrics': {
                'accuracy': self._safe_float(metadata.get('validation_accuracy'), fallback.get('metrics', {}).get('accuracy', 0.0)),
                'f1': self._safe_float(metadata.get('validation_f1'), fallback.get('metrics', {}).get('f1', 0.0)),
                'auc': self._safe_float(metadata.get('validation_auc'), fallback.get('metrics', {}).get('auc', 0.0)),
                'brier': self._safe_float(metadata.get('validation_brier'), fallback.get('metrics', {}).get('brier', 0.25)),
            }
        }
        reliability = self._score_model_reliability(payload, horizon)
        reliability['gate'] = bundle.get('gate')
        reliability['reason'] = bundle.get('reason', '')
        if not bundle.get('loaded'):
            reliability['score'] = min(float(reliability.get('score', 35.0) or 35.0), 35.0)
            reliability['level'] = 'low'
            reliability['label'] = 'guarded'
            reliability['passed'] = False
        return reliability

    def _build_market_predictor_contexts(self):
        model_paths = {
            'A': {
                'short_term': ('data/models/short_term_model.pkl', 'A股5日', 5),
                'medium_term': ('data/models/medium_term_model.pkl', 'A股20日', 20),
                'long_term': ('data/models/long_term_model.pkl', 'A股60日', 60),
            },
            'H': {
                'short_term': ('data/models/hk_stock_short_term_model.pkl', '港股5日', 5),
                'medium_term': ('data/models/hk_stock_medium_term_model.pkl', '港股20日', 20),
                'long_term': ('data/models/hk_stock_long_term_model.pkl', '港股60日', 60),
            },
            'US': {
                'short_term': ('data/models/us_stock_short_term_model.pkl', '美股5日', 5),
                'medium_term': ('data/models/us_stock_medium_term_model.pkl', '美股20日', 20),
                'long_term': ('data/models/us_stock_long_term_model.pkl', '美股60日', 60),
            },
        }

        base_snapshot = getattr(self, '_model_quality_snapshot', {}) or {}
        contexts = {}
        for market_code, spec in model_paths.items():
            short_predictor = ShortTermPredictor()
            medium_predictor = MediumTermPredictor()
            long_predictor = LongTermPredictor()

            short_bundle = self._load_runtime_model_if_valid(short_predictor, *spec['short_term'])
            medium_bundle = self._load_runtime_model_if_valid(medium_predictor, *spec['medium_term'])
            long_bundle = self._load_runtime_model_if_valid(long_predictor, *spec['long_term'])

            contexts[market_code] = {
                'market': market_code,
                'short_term': short_predictor,
                'medium_term': medium_predictor,
                'long_term': long_predictor,
                'quality_snapshot': {
                    'short_term': self._build_bundle_reliability(short_bundle, 'short_term', fallback=base_snapshot.get('short_term') or {}),
                    'medium_term': self._build_bundle_reliability(medium_bundle, 'medium_term', fallback=base_snapshot.get('medium_term') or {}),
                    'long_term': self._build_bundle_reliability(long_bundle, 'long_term', fallback=base_snapshot.get('long_term') or {}),
                }
            }
        return contexts

    def _resolve_market_predictor_context(self, market='A'):
        market_code = str(market or 'A').strip().upper()
        if market_code not in ('A', 'H', 'US'):
            market_code = 'A'
        contexts = getattr(self, '_predictor_contexts', {}) or {}
        return contexts.get(market_code) or contexts.get('A') or {
            'market': 'A',
            'short_term': getattr(self, 'short_predictor', None),
            'medium_term': getattr(self, 'medium_predictor', None),
            'long_term': getattr(self, 'long_predictor', None),
            'quality_snapshot': getattr(self, '_model_quality_snapshot', {}) or {},
        }

    def _load_runtime_model_if_valid(self, predictor, model_path, model_tag, period_days):
        bundle = self.model_manager.load_runtime_model_bundle(
            model_path=model_path,
            period_days=period_days
        )

        if not bundle.get('loaded'):
            predictor.model = None
            predictor.is_trained = False
            logger.warning(f"{model_tag}模型未加载: {bundle.get('reason')}")
            return bundle

        predictor.model = bundle.get('model')
        predictor.is_trained = predictor.model is not None
        if hasattr(predictor, 'calibrator'):
            predictor.calibrator = bundle.get('calibrator')
        if hasattr(predictor, 'calibration_method'):
            predictor.calibration_method = bundle.get('calibration_method', 'none')
        if hasattr(predictor, 'regime_models'):
            predictor.regime_models = bundle.get('regime_models', {}) or {}
        if hasattr(predictor, 'volatility_split'):
            predictor.volatility_split = bundle.get('volatility_split')
        if hasattr(predictor, 'feature_columns'):
            predictor.feature_columns = bundle.get('feature_columns')
        if hasattr(predictor, 'blend_model'):
            predictor.blend_model = bundle.get('blend_model')
        if hasattr(predictor, 'blend_weight'):
            predictor.blend_weight = bundle.get('blend_weight')
        if hasattr(predictor, 'blend_enabled'):
            predictor.blend_enabled = bool(bundle.get('blend_enabled', False))
        logger.info(f"已加载{model_tag}预测模型(通过阈值 gate={bundle.get('gate')})")
        return bundle

    @staticmethod
    def _safe_float(value, default=None):
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _build_code_candidates(self, code, market):
        base = str(code).split('.')[0]
        candidates = [str(code), base]

        if str(market).upper() == 'A':
            candidates.extend([f"{base}.SZ", f"{base}.SH"])
        elif str(market).upper() == 'H':
            candidates.append(f"{base}.HK")

        # 去重并保持顺序
        seen = set()
        result = []
        for c in candidates:
            if c and c not in seen:
                seen.add(c)
                result.append(c)
        return result

    def _get_latest_indicator_snapshot(self, code, market):
        try:
            session = self.collector.session
            candidates = self._build_code_candidates(code, market)
            row = (
                session.query(Indicator)
                .filter(Indicator.code.in_(candidates))
                .order_by(Indicator.date.desc())
                .first()
            )
            return row
        except Exception as e:
            logger.warning(f"读取指标快照失败 {code}: {e}")
            return None

    def _resolve_sub_scores(self, code, market, tech_indicators,
                            valuation_data=None, money_flow_data=None, sentiment_data=None):
        snapshot = self._get_latest_indicator_snapshot(code, market)

        # 1) 基本面
        valuation_score = None
        if isinstance(valuation_data, dict):
            valuation_score = self.scorer.calculate_fundamental_score(
                pe=valuation_data.get('pe'),
                pb=valuation_data.get('pb'),
                roe=valuation_data.get('roe'),
                eps_growth=valuation_data.get('eps_growth')
            )
        elif snapshot is not None and snapshot.value_score is not None:
            valuation_score = self._safe_float(snapshot.value_score)
        elif snapshot is not None:
            valuation_score = self.scorer.calculate_fundamental_score(
                pe=self._safe_float(snapshot.pe),
                pb=self._safe_float(snapshot.pb),
                roe=None,
                eps_growth=None
            )

        # 2) 资金面
        money_flow_score = None
        if isinstance(money_flow_data, dict):
            money_flow_score = self.scorer.calculate_money_flow_score(
                main_flow=money_flow_data.get('main_flow'),
                north_flow=money_flow_data.get('north_flow'),
                volume_ratio=money_flow_data.get('volume_ratio')
            )
        elif snapshot is not None and snapshot.money_score is not None:
            money_flow_score = self._safe_float(snapshot.money_score)
        elif snapshot is not None:
            money_flow_score = self.scorer.calculate_money_flow_score(
                main_flow=self._safe_float(snapshot.main_money),
                north_flow=self._safe_float(snapshot.north_money),
                volume_ratio=tech_indicators.get('volume_ratio')
            )

        # 3) 情绪面
        news_score = None
        if isinstance(sentiment_data, dict):
            news_score = self.scorer.calculate_sentiment_score(
                news_sentiment=sentiment_data.get('news_sentiment'),
                volatility=tech_indicators.get('volatility')
            )
        elif snapshot is not None and snapshot.sentiment_score is not None:
            # Indicator.sentiment_score 可能是 -0.5~0.5 或 1~5 两种刻度
            s = self._safe_float(snapshot.sentiment_score)
            if s is not None and -1.0 <= s <= 1.0:
                news_score = self.scorer.calculate_sentiment_score(
                    news_sentiment=s,
                    volatility=tech_indicators.get('volatility')
                )
            elif s is not None:
                news_score = max(1.0, min(5.0, s))

        # 最终兜底，避免再回到“隐式常量污染”
        if valuation_score is None:
            valuation_score = 3.0
        if money_flow_score is None:
            money_flow_score = 3.0
        if news_score is None:
            news_score = 3.0

        return valuation_score, money_flow_score, news_score

    def _build_predictor_feature_payload(self, code, market):
        snapshot = self._get_latest_indicator_snapshot(code, market)
        if snapshot is None:
            return {
                'pe': 0.0,
                'pb': 0.0,
                'eps': 0.0,
                'roe': 0.0,
            }, {
                'net_mf_amount': 0.0,
                'north_money': 0.0,
                'rzye': 0.0,
                'rzmre': 0.0,
                'has_top_list': 0,
                'sentiment': 0.0,
                'has_report': 0,
                'cpi_yoy': self._macro_snapshot.get('cpi_yoy', 0.0),
                'pmi': self._macro_snapshot.get('pmi', 0.0),
                'shibor_1m': self._macro_snapshot.get('shibor_1m', 0.0),
                'shibor_3m': self._macro_snapshot.get('shibor_3m', 0.0),
                'macro_regime_score': self._macro_snapshot.get('macro_regime_score', 0.0),
                'risk_off_proxy': self._macro_snapshot.get('risk_off_proxy', 0.0),
                'dollar_proxy': self._macro_snapshot.get('dollar_proxy', 0.0),
                'gold_oil_ratio': self._macro_snapshot.get('gold_oil_ratio', 0.0),
                'vix': self._macro_snapshot.get('vix', 0.0),
                'dxy': self._macro_snapshot.get('dxy', 0.0),
                'tnx': self._macro_snapshot.get('tnx', 0.0),
                'sentiment_lag1': 0.0,
                'sentiment_ma3': 0.0,
                'sentiment_shock': 0.0,
                'report_decay_3d': 0.0,
            }

        valuation_data = {
            'pe': self._safe_float(snapshot.pe, 0.0),
            'pb': self._safe_float(snapshot.pb, 0.0),
            'eps': 0.0,
            'roe': 0.0,
        }
        sentiment_raw = self._safe_float(snapshot.sentiment_score, 0.0)
        if sentiment_raw is None:
            sentiment_raw = 0.0
        if abs(sentiment_raw) > 1.0:
            sentiment_raw = 0.0

        market_data = {
            'net_mf_amount': self._safe_float(snapshot.main_money, 0.0),
            'north_money': self._safe_float(snapshot.north_money, 0.0),
            'rzye': 0.0,
            'rzmre': 0.0,
            'has_top_list': 0,
            'sentiment': sentiment_raw,
            'has_report': 0,
            'cpi_yoy': self._macro_snapshot.get('cpi_yoy', 0.0),
            'pmi': self._macro_snapshot.get('pmi', 0.0),
            'shibor_1m': self._macro_snapshot.get('shibor_1m', 0.0),
            'shibor_3m': self._macro_snapshot.get('shibor_3m', 0.0),
            'macro_regime_score': self._macro_snapshot.get('macro_regime_score', 0.0),
            'risk_off_proxy': self._macro_snapshot.get('risk_off_proxy', 0.0),
            'dollar_proxy': self._macro_snapshot.get('dollar_proxy', 0.0),
            'gold_oil_ratio': self._macro_snapshot.get('gold_oil_ratio', 0.0),
            'vix': self._macro_snapshot.get('vix', 0.0),
            'dxy': self._macro_snapshot.get('dxy', 0.0),
            'tnx': self._macro_snapshot.get('tnx', 0.0),
            'sentiment_lag1': 0.0,
            'sentiment_ma3': 0.0,
            'sentiment_shock': 0.0,
            'report_decay_3d': 0.0,
        }
        return valuation_data, market_data
    
    def get_stock_analysis(self, code, market, df=None, valuation_data=None, 
                           money_flow_data=None, sentiment_data=None):
        """获取单只股票的完整分析"""
        if df is None:
            df = self.collector.get_stock_data_from_db(code)
            if df is None or len(df) < 60:
                logger.debug(f"{code} 数据不足，已跳过")
                return None
        
        try:
            technical_score = self.scorer.calculate_technical_score(df)
            tech_indicators = self.technical.calculate_all_indicators(df)
            trend = self.technical.get_trend_signal(df)
            
            valuation_score, money_flow_score, news_score = self._resolve_sub_scores(
                code=code,
                market=market,
                tech_indicators=tech_indicators,
                valuation_data=valuation_data,
                money_flow_data=money_flow_data,
                sentiment_data=sentiment_data
            )
            
            total_score = self.scorer.calculate_total_score(
                technical_score, valuation_score, money_flow_score, news_score
            )
            rating = self.scorer.get_score_level(total_score)
            
            volatility_level = self._get_volatility_level(tech_indicators.get('volatility', 0.3))
            
            valuation_payload, market_payload = self._build_predictor_feature_payload(code, market)
            predictor_context = self._resolve_market_predictor_context(market)
            short_predictor = predictor_context.get('short_term') or self.short_predictor
            medium_predictor = predictor_context.get('medium_term') or self.medium_predictor
            long_predictor = predictor_context.get('long_term') or self.long_predictor
            quality_snapshot = predictor_context.get('quality_snapshot') or {}

            short_pred = short_predictor.get_prediction_result(
                df,
                valuation_data=valuation_payload,
                market_data=market_payload,
                volatility_level=volatility_level
            )
            medium_pred = medium_predictor.get_prediction_result(
                df,
                valuation_data=valuation_payload,
                market_data=market_payload,
                volatility_level=volatility_level
            )
            long_pred = long_predictor.get_prediction_result(
                df,
                valuation_data=valuation_payload,
                market_data=market_payload,
                volatility_level=volatility_level
            )
            
            current_price = float(df['close'].iloc[-1])
            
            reason = self._build_reason(tech_indicators, trend)
            if not short_predictor.is_trained:
                reason = f"{reason}；5日模型未通过验证，短期概率采用中性值"
            risks = self._build_risks(tech_indicators, trend)

            model_status = {
                'market': predictor_context.get('market', str(market)),
                'short_term_validated': bool(short_predictor.is_trained),
                'medium_term_validated': bool(medium_predictor.is_trained),
                'long_term_validated': bool(long_predictor.is_trained),
                'short_term_source': 'model' if short_predictor.is_trained else 'rule_fallback',
                'market_model_reliability': quality_snapshot.get('short_term') or {},
            }
            unified_trend = derive_unified_trend({
                'predictions': {
                    'short_term': short_pred,
                    'medium_term': medium_pred,
                    'long_term': long_pred,
                },
                'model_status': model_status,
                'total_score': total_score,
            })
            advisor_view = self._build_advisor_view(
                total_score=total_score,
                trend=trend,
                unified_trend=unified_trend,
                tech_indicators=tech_indicators,
                risks=risks,
                model_reliability=self._resolve_model_reliability('short_term', market=market),
            )

            return {
                'code': str(code),
                'market': str(market),
                'current_price': round(current_price, 2),
                'technical_score': round(technical_score, 2),
                'valuation_score': round(valuation_score, 2),
                'money_flow_score': round(money_flow_score, 2),
                'news_score': round(news_score, 2),
                'total_score': round(total_score, 2),
                'rating': rating,
                'trend': trend,
                'predictions': {
                    'short_term': short_pred,
                    'medium_term': medium_pred,
                    'long_term': long_pred
                },
                'unified_trend': unified_trend,
                'model_status': model_status,
                'advisor_view': advisor_view,
                'reason': reason,
                'risks': risks,
                'volatility_level': volatility_level
            }
        except Exception as e:
            logger.error(f"分析 {code} 时出错: {e}")
            return None
    
    def _get_volatility_level(self, volatility):
        if volatility < 0.2:
            return 'low'
        elif volatility < 0.35:
            return 'medium'
        else:
            return 'high'
    
    def _build_reason(self, tech_indicators, trend):
        reasons = []
        trend_text = trend.get('trend_text', '震荡')
        reasons.append(f"趋势: {trend_text}")
        
        rsi = tech_indicators.get('rsi', 50)
        if rsi < 30:
            reasons.append(f"RSI={rsi:.1f}，处于超卖区")
        elif rsi > 70:
            reasons.append(f"RSI={rsi:.1f}，处于超买区")
        else:
            reasons.append(f"RSI={rsi:.1f}，处于中性区")
        
        return "；".join(reasons)
    
    def _build_risks(self, tech_indicators, trend):
        risks = []
        rsi = tech_indicators.get('rsi', 50)
        if rsi > 70:
            risks.append("RSI处于超买区，短期回调风险较大")

        volatility = float(tech_indicators.get('volatility', 0.0) or 0.0)
        if volatility >= 0.35:
            risks.append("波动率偏高，仓位不宜过重")

        price_ma20_ratio = float(tech_indicators.get('price_ma20_ratio', 0.0) or 0.0)
        if price_ma20_ratio >= 0.08:
            risks.append("股价偏离20日均线较多，追高风险上升")

        trend_type = trend.get('trend', 'neutral')
        if trend_type in ('bearish', 'strong_bearish'):
            risks.append("处于下跌趋势中，逆势操作风险较高")

        risks.append("市场整体存在不确定性")
        risks.append("本建议不构成确定性投资建议")
        return risks

    def _build_advisor_view(self, total_score, trend, unified_trend, tech_indicators, risks=None, model_reliability=None):
        """构建更像高级理财师的动作建议：买入、加仓、持有、减仓、清仓。"""
        total_score = float(total_score or 0.0)
        trend = trend or {}
        unified_trend = unified_trend or {}
        tech_indicators = tech_indicators or {}
        risks = list(risks or [])

        trend_type = str(trend.get('trend', 'neutral'))
        trend_text = str(trend.get('trend_text', '震荡'))
        trend_direction = str(unified_trend.get('trend_direction', 'neutral'))
        trend_score = float(unified_trend.get('trend_score', 50.0) or 50.0)
        trend_confidence = float(unified_trend.get('trend_confidence', 20.0) or 20.0)

        rsi = float(tech_indicators.get('rsi', 50.0) or 50.0)
        volatility = float(tech_indicators.get('volatility', 0.0) or 0.0)
        price_ma20_ratio = float(tech_indicators.get('price_ma20_ratio', 0.0) or 0.0)
        model_reliability = self._resolve_model_reliability('short_term', override=model_reliability)
        reliability_score = float(model_reliability.get('score', 55.0) or 55.0)

        risk_points = 0
        if trend_type in ('bearish', 'strong_bearish'):
            risk_points += 2
        if volatility >= 0.35:
            risk_points += 2
        elif volatility >= 0.22:
            risk_points += 1
        if rsi >= 72:
            risk_points += 2
        elif rsi >= 65:
            risk_points += 1
        if price_ma20_ratio >= 0.08 or price_ma20_ratio <= -0.08:
            risk_points += 1

        if risk_points >= 4:
            risk_level = 'high'
        elif risk_points >= 2:
            risk_level = 'medium'
        else:
            risk_level = 'low'

        evidence_score = 42.0
        evidence_score += max(0.0, trend_score - 50.0) * 0.45
        evidence_score += max(0.0, total_score - 3.0) * 12.0
        evidence_score += max(0.0, trend_confidence - 40.0) * 0.30
        evidence_score += (reliability_score - 55.0) * 0.35
        evidence_score -= risk_points * 6.0
        if trend_direction == 'bearish':
            evidence_score -= 6.0
        evidence_score = float(np.clip(evidence_score, 5.0, 95.0))

        action = 'hold'
        summary = '当前信号偏中性，建议继续观察。'

        if trend_direction == 'bearish' and (trend_score <= 40 or total_score <= 2.6 or trend_type == 'strong_bearish'):
            action = 'sell'
            summary = '趋势与综合评分同步转弱，若当前持有可优先考虑清仓。'
        elif trend_direction == 'bearish' or risk_level == 'high':
            action = 'reduce'
            summary = '风险偏高或趋势走弱，若有仓位宜先减仓控制回撤。'
        elif (
            trend_direction == 'bullish'
            and trend_score >= 68
            and trend_confidence >= 65
            and total_score >= 4.0
            and rsi < 70
            and risk_level == 'low'
            and evidence_score >= 72
        ):
            action = 'buy'
            summary = '趋势、评分与置信度共振，适合分批买入。'
        elif (
            trend_direction == 'bullish'
            and trend_score >= 60
            and trend_confidence >= 50
            and total_score >= 3.5
            and risk_level != 'high'
            and evidence_score >= 60
        ):
            action = 'add'
            summary = '整体偏强，但仍需控制仓位，适合小幅加仓。'
        elif total_score < 2.8 or (trend_direction == 'bullish' and trend_confidence < 45):
            action = 'watch'
            summary = '优势不明显或信号置信度不足，暂不建议主动出手。'

        if reliability_score < 45 and action in ('buy', 'add'):
            action = 'watch' if trend_confidence < 85 or evidence_score < 82 else 'hold'
            summary = '模型近期稳定性偏弱，为提升建议准确性，当前先降级为观察。'
        elif reliability_score < 55 and action == 'buy':
            action = 'add'
            summary = '模型稳定性一般，建议从积极买入降级为小仓位试探。'

        position_size_pct = {
            'buy': 16 if risk_level == 'low' and trend_confidence >= 75 else 12,
            'add': 8,
            'hold': 0,
            'watch': 0,
            'reduce': -8,
            'sell': -20,
        }.get(action, 0)

        stop_loss_pct = 0.05 if risk_level == 'low' else (0.07 if risk_level == 'medium' else 0.09)
        take_profit_pct = 0.12 if action in ('buy', 'add') and trend_confidence < 75 else 0.16

        confidence_label = 'high' if trend_confidence >= 70 else ('medium' if trend_confidence >= 45 else 'low')

        if action in ('sell', 'reduce') or risk_level == 'high':
            review_in_days = 1
            review_focus = ['risk_control', 'trend_confirmation']
        elif action in ('buy', 'add'):
            review_in_days = 3
            review_focus = ['entry_timing', 'position_management']
        else:
            review_in_days = 5
            review_focus = ['signal_refresh', 'wait_for_breakout']

        if evidence_score >= 75:
            confidence_label = 'high'
        elif evidence_score >= 58:
            confidence_label = 'medium'
        else:
            confidence_label = 'low'

        return {
            'action': action,
            'confidence': confidence_label,
            'risk_level': risk_level,
            'position_size_pct': int(position_size_pct),
            'stop_loss_pct': round(float(stop_loss_pct), 3),
            'take_profit_pct': round(float(take_profit_pct), 3),
            'evidence_score': round(float(evidence_score), 1),
            'model_reliability': model_reliability,
            'review_in_days': int(review_in_days),
            'review_focus': review_focus,
            'summary': f"{summary} 当前趋势为{trend_text}。",
            'reason_tags': [
                f"trend={trend_direction}",
                f"score={round(total_score, 2)}",
                f"risk={risk_level}",
            ],
            'risks': risks,
        }

    def _build_portfolio_advice(self, analyses):
        """基于多个标的的投顾动作，给出组合层面的仓位与风险建议。"""
        analyses = list(analyses or [])
        if not analyses:
            return {
                'overall_risk': 'medium',
                'stance': 'balanced',
                'priority': 'medium',
                'recommended_cash_ratio_pct': 25,
                'action_breakdown': {},
                'target_allocation': {
                    'cash_pct': 25,
                    'core_pct': 45,
                    'satellite_pct': 20,
                    'defense_pct': 10,
                },
                'rebalance_actions': [
                    {'title': '保持均衡仓位', 'instruction': '当前信号不足，先维持核心仓与现金缓冲，不宜频繁调仓。'},
                    {'title': '等待新信号', 'instruction': '优先观察趋势和风险是否出现同步改善，再决定是否加仓。'},
                ],
                'review_cycle': '每周复核一次',
                'summary': '当前暂无足够标的用于生成组合建议，建议保持均衡仓位。',
            }

        action_counts = {'buy': 0, 'add': 0, 'hold': 0, 'watch': 0, 'reduce': 0, 'sell': 0}
        risk_counts = {'low': 0, 'medium': 0, 'high': 0}

        for item in analyses:
            if not isinstance(item, dict):
                continue
            advisor = item.get('advisor_view') if isinstance(item.get('advisor_view'), dict) else item
            action = str(advisor.get('action', item.get('advisor_action', 'hold')) or 'hold')
            risk = str(advisor.get('risk_level', item.get('risk_level', 'medium')) or 'medium')
            if action not in action_counts:
                action = 'hold'
            if risk not in risk_counts:
                risk = 'medium'
            action_counts[action] += 1
            risk_counts[risk] += 1

        defensive_score = (action_counts['sell'] * 2.0) + (action_counts['reduce'] * 1.2) + (risk_counts['high'] * 1.5)
        constructive_score = (action_counts['buy'] * 1.6) + (action_counts['add'] * 1.1) + (risk_counts['low'] * 0.5)

        if defensive_score >= max(2.5, constructive_score + 0.8):
            overall_risk = 'high'
            stance = 'defensive'
            priority = 'high'
            recommended_cash_ratio_pct = 40
            target_allocation = {
                'cash_pct': 40,
                'core_pct': 25,
                'satellite_pct': 10,
                'defense_pct': 25,
            }
            rebalance_actions = [
                {'title': '先处理高风险仓位', 'instruction': '优先对减仓/卖出信号标的分两步降仓，避免单日一次性踩踏。'},
                {'title': '提高防御与现金', 'instruction': '将现金提升到 35%-45%，其余仓位优先保留低波动或防御型资产。'},
                {'title': '暂停新增进攻仓', 'instruction': '在短中长期信号重新趋同前，不建议追涨扩仓。'},
            ]
            review_cycle = '1个交易日内复核'
            summary = '高风险信号较多，当前更适合防守，宜提高现金比例并控制回撤。'
        elif constructive_score >= defensive_score and risk_counts['high'] == 0 and (action_counts['buy'] + action_counts['add']) > 0:
            overall_risk = 'low'
            stance = 'constructive'
            priority = 'low'
            recommended_cash_ratio_pct = 15
            target_allocation = {
                'cash_pct': 15,
                'core_pct': 50,
                'satellite_pct': 25,
                'defense_pct': 10,
            }
            rebalance_actions = [
                {'title': '分批增加强势主线', 'instruction': '把新增仓位优先给买入/加仓信号最集中的行业，但保持分批进入。'},
                {'title': '保留机动现金', 'instruction': '仍保留约 10%-20% 现金，用于回撤补仓或应对突发波动。'},
                {'title': '弱势仓向核心仓切换', 'instruction': '将低胜率观察仓逐步换到中期与长期概率更一致的核心标的。'},
            ]
            review_cycle = '每3个交易日复核一次'
            summary = '偏多信号占优，组合可保持进攻但仍建议分批布局。'
        else:
            overall_risk = 'medium'
            stance = 'balanced'
            priority = 'medium'
            recommended_cash_ratio_pct = 25
            target_allocation = {
                'cash_pct': 25,
                'core_pct': 45,
                'satellite_pct': 15,
                'defense_pct': 15,
            }
            rebalance_actions = [
                {'title': '保留核心仓不动', 'instruction': '继续持有中长期逻辑较清晰的核心仓，减少频繁切换。'},
                {'title': '观察仓设置触发线', 'instruction': '对观察/减仓标的设置止损与复核条件，达到条件再调整。'},
                {'title': '控制单一主题集中度', 'instruction': '避免单一行业过重，逐步把仓位分散到 2-3 条主线。'},
            ]
            review_cycle = '每周复核两次'
            summary = '多空信号交织，建议维持均衡仓位并精选标的。'

        return {
            'overall_risk': overall_risk,
            'stance': stance,
            'priority': priority,
            'recommended_cash_ratio_pct': int(recommended_cash_ratio_pct),
            'action_breakdown': action_counts,
            'target_allocation': target_allocation,
            'rebalance_actions': rebalance_actions,
            'review_cycle': review_cycle,
            'summary': summary,
        }
    
    def get_top_recommendations(self, market='A', limit=20):
        if market == 'A':
            stock_pool = self.a_stock_pool
        elif market == 'H':
            stock_pool = self.hk_stock_pool
        else:
            stock_pool = self.us_stock_pool
        
        eligible_codes = [code for code in stock_pool if self._has_sufficient_local_history(code, min_rows=60)]
        if eligible_codes:
            logger.info(f"{market}市场可用推荐候选: {len(eligible_codes)}/{len(stock_pool)}")
        else:
            logger.warning(f"{market}市场暂无足够历史数据的候选标的")

        recommendations = []
        for code in eligible_codes:
            try:
                analysis = self.get_stock_analysis(str(code), market)
                if analysis:
                    recommendations.append({
                        'code': str(code),
                        'name': str(code).split('.')[0],
                        'current_price': analysis['current_price'],
                        'total_score': analysis['total_score'],
                        'up_probability_5d': analysis['predictions']['short_term']['up_probability'],
                        'up_probability_20d': analysis['predictions']['medium_term']['up_probability'],
                        'up_probability_60d': analysis['predictions']['long_term']['up_probability'],
                        'trend_direction': analysis.get('unified_trend', {}).get('trend_direction', 'neutral'),
                        'trend_score': analysis.get('unified_trend', {}).get('trend_score', 50.0),
                        'trend_confidence': analysis.get('unified_trend', {}).get('trend_confidence', 20.0),
                        'advisor_action': analysis.get('advisor_view', {}).get('action', 'hold'),
                        'advisor_confidence': analysis.get('advisor_view', {}).get('confidence', 'low'),
                        'risk_level': analysis.get('advisor_view', {}).get('risk_level', 'medium'),
                        'position_size_pct': analysis.get('advisor_view', {}).get('position_size_pct', 0),
                        'stop_loss_pct': analysis.get('advisor_view', {}).get('stop_loss_pct', 0.07),
                        'take_profit_pct': analysis.get('advisor_view', {}).get('take_profit_pct', 0.16),
                        'volatility_level': analysis['volatility_level'],
                        'reason_summary': analysis.get('advisor_view', {}).get('summary', analysis['reason'][:100] if analysis['reason'] else '')
                    })
            except Exception as e:
                logger.error(f"分析 {code} 失败: {e}")
                continue
        
        action_priority = {
            'buy': 5,
            'add': 4,
            'hold': 3,
            'watch': 2,
            'reduce': 1,
            'sell': 0,
        }
        recommendations.sort(
            key=lambda x: (
                action_priority.get(x.get('advisor_action', 'watch'), 2),
                float(x.get('up_probability_20d', 50.0) or 50.0),
                float(x.get('up_probability_5d', 50.0) or 50.0),
                float(x.get('total_score', 0.0) or 0.0),
            ),
            reverse=True,
        )
        for i, rec in enumerate(recommendations[:limit]):
            rec['rank'] = i + 1
        return recommendations[:limit]
    
    def get_all_recommendations(self):
        a_stock = self.get_top_recommendations('A', 20)
        hk_stock = self.get_top_recommendations('H', 20)
        us_stock = self.get_top_recommendations('US', 20)
        portfolio_advice = self._build_portfolio_advice(a_stock + hk_stock + us_stock)
        return {
            'a_stock': a_stock,
            'hk_stock': hk_stock,
            'us_stock': us_stock,
            'portfolio_advice': portfolio_advice,
            'updated_at': datetime.now().isoformat()
        }


if __name__ == '__main__':
    recommender = StockRecommender()
    recs = recommender.get_top_recommendations('US', 5)
    print(f"美股推荐: {len(recs)} 只")
    for r in recs:
        print(f"  {r['rank']}. {r['code']} - 评分: {r['total_score']}")