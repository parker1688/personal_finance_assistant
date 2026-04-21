"""
模型训练模块 - predictors/model_trainer.py
训练和更新预测模型
"""

import sys
import os
import json
from copy import deepcopy
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, brier_score_loss

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from predictors.short_term import ShortTermPredictor
from predictors.medium_term import MediumTermPredictor
from predictors.long_term import LongTermPredictor
from predictors.model_manager import ModelManager
from collectors.stock_collector import StockCollector
from collectors.macro_collector import MacroCollector
from models import get_session, RawStockData, RawFundData
from config import (
    MIN_MODEL_ACCURACY,
    MIN_MODEL_F1_SCORE,
    MIN_SHORT_HORIZON_AUC,
    MAX_SHORT_HORIZON_BRIER,
    MODEL_TRAIN_TEST_SPLIT,
    PREDICTION_THRESHOLD,
    STOCK_BASIC_FILE,
    LEGACY_STOCK_POOL_FILE,
    resolve_data_file,
    AUTO_ACTIVATE_BEST_MODEL,
)
from utils import get_logger

logger = get_logger(__name__)


class ModelTrainer:
    """模型训练器"""
    
    def __init__(self):
        self.short_predictor = ShortTermPredictor()
        self.medium_predictor = MediumTermPredictor()
        self.long_predictor = LongTermPredictor()
        self.model_manager = ModelManager()
        self.collector = StockCollector()
        self._external_feature_cache = None
        self._last_training_diagnostics = {}
        self._last_short_code_scores = {}
        self._short_term_optimization_history = []
        self._medium_term_optimization_history = []
        self._long_term_optimization_history = []
        self._continuous_improvement_history = []

    @staticmethod
    def _to_datetime(value):
        try:
            return pd.to_datetime(value)
        except Exception:
            return None

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            if pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _safe_read_csv(path, usecols):
        if not os.path.exists(path):
            return pd.DataFrame(columns=usecols)
        try:
            return pd.read_csv(path, usecols=usecols)
        except Exception as e:
            logger.warning(f"读取特征文件失败 {path}: {e}")
            return pd.DataFrame(columns=usecols)

    @staticmethod
    def _safe_read_csv_flexible(path):
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception as e:
            logger.warning(f"读取特征文件失败 {path}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _compute_macro_regime_score(cpi_yoy, pmi, shibor_1m, shibor_3m):
        """将宏观变量压缩为[-1,1]区间的景气分值，便于模型学习。"""
        score = 0.0
        cpi_v = ModelTrainer._to_float(cpi_yoy, 0.0)
        pmi_v = ModelTrainer._to_float(pmi, 0.0)
        s1 = ModelTrainer._to_float(shibor_1m, 0.0)
        s3 = ModelTrainer._to_float(shibor_3m, 0.0)

        if pmi_v > 0:
            score += float(np.clip((pmi_v - 50.0) / 4.0, -0.5, 0.5))

        if cpi_v > 0:
            if 0.0 <= cpi_v <= 3.0:
                score += 0.2
            elif cpi_v >= 4.0:
                score -= 0.2

        if s1 > 0 and s3 > 0:
            slope = s3 - s1
            score += float(np.clip(-slope / 3.0, -0.25, 0.25))

        return float(np.clip(score, -1.0, 1.0))

    @staticmethod
    def _build_cross_asset_df(vix, dxy, tnx, gold, oil):
        """将跨资产价格序列标准化为统一特征表。"""
        base = pd.DataFrame(index=vix.index.union(dxy.index).union(tnx.index).union(gold.index).union(oil.index))
        base = base.sort_index()
        base['vix'] = pd.to_numeric(vix.reindex(base.index), errors='coerce')
        base['dxy'] = pd.to_numeric(dxy.reindex(base.index), errors='coerce')
        base['tnx'] = pd.to_numeric(tnx.reindex(base.index), errors='coerce')
        base['gold'] = pd.to_numeric(gold.reindex(base.index), errors='coerce')
        base['oil'] = pd.to_numeric(oil.reindex(base.index), errors='coerce')

        base[['vix', 'dxy', 'tnx', 'gold', 'oil']] = base[['vix', 'dxy', 'tnx', 'gold', 'oil']].ffill()
        base = base.dropna(subset=['vix', 'dxy', 'tnx', 'gold', 'oil'], how='all')
        if base.empty:
            return pd.DataFrame(columns=['trade_date', 'vix', 'dxy', 'tnx', 'gold_oil_ratio', 'risk_off_proxy', 'dollar_proxy'])

        base['gold_oil_ratio'] = np.where(base['oil'].abs() > 1e-8, base['gold'] / base['oil'], 0.0)
        base['risk_off_proxy'] = np.clip((base['vix'].fillna(20.0) - 20.0) / 15.0, -1.0, 1.0)
        dollar_part = np.clip((base['dxy'].fillna(103.0) - 103.0) / 8.0, -1.0, 1.0)
        rate_part = np.clip((base['tnx'].fillna(4.5) - 4.5) / 2.0, -1.0, 1.0)
        base['dollar_proxy'] = np.clip(0.7 * dollar_part + 0.3 * rate_part, -1.0, 1.0)

        out = base.reset_index()
        if 'trade_date' not in out.columns:
            if 'index' in out.columns:
                out = out.rename(columns={'index': 'trade_date'})
            else:
                out = out.rename(columns={out.columns[0]: 'trade_date'})
        out['trade_date'] = pd.to_datetime(out['trade_date'], errors='coerce')
        out = out.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
        return out[['trade_date', 'vix', 'dxy', 'tnx', 'gold_oil_ratio', 'risk_off_proxy', 'dollar_proxy']]

    def _load_cross_asset_features(self, data_dir):
        """加载跨资产特征: 优先本地快照，缺失时按需从yfinance构建。"""
        path = os.path.join(data_dir, 'cross_asset_daily.csv')
        cols = ['trade_date', 'vix', 'dxy', 'tnx', 'gold_oil_ratio', 'risk_off_proxy', 'dollar_proxy']

        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                for c in cols:
                    if c not in df.columns:
                        df[c] = np.nan
                df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
                for c in ['vix', 'dxy', 'tnx', 'gold_oil_ratio', 'risk_off_proxy', 'dollar_proxy']:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
                df = df.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
                if not df.empty:
                    return df[cols]
            except Exception as e:
                logger.warning(f"读取跨资产快照失败 {path}: {e}")

        try:
            import yfinance as yf

            def _series(symbol, period='6y'):
                try:
                    h = yf.Ticker(symbol).history(period=period)
                    if h is None or h.empty or 'Close' not in h.columns:
                        return pd.Series(dtype=float)
                    s = h['Close'].copy()
                    s.index = pd.to_datetime(s.index, errors='coerce').tz_localize(None)
                    s = s.dropna()
                    s.name = symbol
                    return s
                except Exception:
                    return pd.Series(dtype=float)

            vix = _series('^VIX')
            dxy = _series('DX-Y.NYB')
            if dxy.empty:
                dxy = _series('UUP')
            tnx = _series('^TNX')
            gold = _series('GC=F')
            oil = _series('CL=F')
            if oil.empty:
                oil = _series('BZ=F')

            cross = self._build_cross_asset_df(vix, dxy, tnx, gold, oil)
            if not cross.empty:
                try:
                    cross.to_csv(path, index=False)
                except Exception:
                    pass
                return cross
        except Exception as e:
            logger.warning(f"跨资产在线拉取失败: {e}")

        return pd.DataFrame(columns=cols)

    @staticmethod
    def _group_by_code(df, code_col, date_col):
        result = {}
        if df is None or df.empty:
            return result
        for code, g in df.groupby(code_col):
            g = g.sort_values(date_col).reset_index(drop=True)
            result[str(code)] = g
        return result

    def _latest_row_by_code(self, grouped_map, code, asof_dt, date_col):
        if asof_dt is None:
            return None
        g = grouped_map.get(str(code))
        if g is None or g.empty:
            return None
        idx = g[date_col].searchsorted(asof_dt, side='right') - 1
        if idx < 0:
            return None
        return g.iloc[int(idx)]

    def _latest_row_by_date(self, df, asof_dt, date_col):
        if asof_dt is None or df is None or df.empty:
            return None
        idx = df[date_col].searchsorted(asof_dt, side='right') - 1
        if idx < 0:
            return None
        return df.iloc[int(idx)]

    def _load_external_feature_cache(self):
        if self._external_feature_cache is not None:
            return self._external_feature_cache

        data_dir = os.path.join('data')

        daily_basic = self._safe_read_csv(
            os.path.join(data_dir, 'daily_basic.csv'),
            ['ts_code', 'trade_date', 'pe', 'pb']
        )
        if not daily_basic.empty:
            daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'], format='%Y%m%d', errors='coerce')
            daily_basic = daily_basic.dropna(subset=['ts_code', 'trade_date'])

        financial = self._safe_read_csv(
            os.path.join(data_dir, 'financial_indicator.csv'),
            ['ts_code', 'ann_date', 'eps', 'roe']
        )
        if not financial.empty:
            financial['ann_date'] = pd.to_datetime(financial['ann_date'], format='%Y%m%d', errors='coerce')
            financial = financial.dropna(subset=['ts_code', 'ann_date'])

        moneyflow = self._safe_read_csv(
            os.path.join(data_dir, 'moneyflow_all.csv'),
            ['ts_code', 'trade_date', 'net_mf_amount']
        )
        if not moneyflow.empty:
            moneyflow['trade_date'] = pd.to_datetime(moneyflow['trade_date'], format='%Y%m%d', errors='coerce')
            moneyflow = moneyflow.dropna(subset=['ts_code', 'trade_date'])

        north_money = self._safe_read_csv(
            os.path.join(data_dir, 'north_money_all.csv'),
            ['trade_date', 'north_money']
        )
        if not north_money.empty:
            north_money['trade_date'] = pd.to_datetime(north_money['trade_date'], format='%Y%m%d', errors='coerce')
            north_money = north_money.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)

        margin = self._safe_read_csv(
            os.path.join(data_dir, 'margin_all.csv'),
            ['trade_date', 'rzye', 'rzmre']
        )
        if not margin.empty:
            margin['trade_date'] = pd.to_datetime(margin['trade_date'], format='%Y%m%d', errors='coerce')
            margin = margin.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)

        top_list = self._safe_read_csv(
            os.path.join(data_dir, 'top_list.csv'),
            ['trade_date', 'ts_code']
        )
        top_list_set = set()
        if not top_list.empty:
            top_list['trade_date'] = pd.to_datetime(top_list['trade_date'], format='%Y%m%d', errors='coerce')
            top_list = top_list.dropna(subset=['trade_date', 'ts_code'])
            top_list_set = {(str(r.ts_code), r.trade_date.date()) for r in top_list.itertuples(index=False)}

        research = self._safe_read_csv_flexible(
            os.path.join(data_dir, 'research_report.csv')
        )
        research_set = set()
        research_dates_by_code = {}
        if not research.empty:
            if 'trade_date' not in research.columns and 'publish_date' in research.columns:
                research = research.rename(columns={'publish_date': 'trade_date'})

            if 'trade_date' in research.columns:
                research['trade_date'] = pd.to_datetime(research['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
            else:
                research['trade_date'] = pd.NaT

            if 'ts_code' not in research.columns:
                research['ts_code'] = None

            research = research.dropna(subset=['trade_date', 'ts_code'])
            research_set = {(str(r.ts_code), r.trade_date.date()) for r in research.itertuples(index=False)}
            for c, g in research.groupby('ts_code'):
                research_dates_by_code[str(c)] = sorted(pd.to_datetime(g['trade_date'], errors='coerce').dropna().dt.date.unique().tolist())

        news = self._safe_read_csv(
            os.path.join(data_dir, 'news_all.csv'),
            ['datetime', 'sentiment']
        )
        news_by_date = {}
        news_decay_by_date = {}
        if not news.empty:
            news['datetime'] = pd.to_datetime(news['datetime'], errors='coerce')
            news['trade_date'] = news['datetime'].dt.date
            news = news.dropna(subset=['trade_date'])
            grp = news.groupby('trade_date')['sentiment'].mean()
            news_by_date = {k: self._to_float(v, 0.0) for k, v in grp.items()}
            for d, s in news_by_date.items():
                for lag in range(0, 4):
                    tgt = d + timedelta(days=lag)
                    w = float(np.exp(-lag / 1.5))
                    news_decay_by_date[tgt] = news_decay_by_date.get(tgt, 0.0) + (float(s) * w)
            for d in list(news_decay_by_date.keys()):
                news_decay_by_date[d] = float(np.clip(news_decay_by_date[d], -1.0, 1.0))

        macro_cpi = self._safe_read_csv_flexible(os.path.join(data_dir, 'macro_cpi.csv'))
        if not macro_cpi.empty and 'month' in macro_cpi.columns:
            macro_cpi['trade_date'] = pd.to_datetime(macro_cpi['month'], format='%Y%m', errors='coerce')
            cpi_col = 'nt_yoy' if 'nt_yoy' in macro_cpi.columns else ('cnt_yoy' if 'cnt_yoy' in macro_cpi.columns else None)
            if cpi_col is not None:
                macro_cpi['cpi_yoy'] = pd.to_numeric(macro_cpi[cpi_col], errors='coerce')
                macro_cpi = macro_cpi.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
                macro_cpi = macro_cpi[['trade_date', 'cpi_yoy']]
            else:
                macro_cpi = pd.DataFrame(columns=['trade_date', 'cpi_yoy'])
        else:
            macro_cpi = pd.DataFrame(columns=['trade_date', 'cpi_yoy'])

        macro_pmi = self._safe_read_csv_flexible(os.path.join(data_dir, 'macro_pmi.csv'))
        if not macro_pmi.empty and ('MONTH' in macro_pmi.columns or 'month' in macro_pmi.columns):
            month_col = 'MONTH' if 'MONTH' in macro_pmi.columns else 'month'
            macro_pmi['trade_date'] = pd.to_datetime(macro_pmi[month_col], format='%Y%m', errors='coerce')
            pmi_col = None
            for cand in ['PMI010000', 'PMI010100', 'PMI010400']:
                if cand in macro_pmi.columns:
                    pmi_col = cand
                    break
            if pmi_col is not None:
                macro_pmi['pmi'] = pd.to_numeric(macro_pmi[pmi_col], errors='coerce')
                macro_pmi = macro_pmi.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
                macro_pmi = macro_pmi[['trade_date', 'pmi']]
            else:
                macro_pmi = pd.DataFrame(columns=['trade_date', 'pmi'])
        else:
            macro_pmi = pd.DataFrame(columns=['trade_date', 'pmi'])

        macro_shibor = self._safe_read_csv_flexible(os.path.join(data_dir, 'macro_shibor.csv'))
        if not macro_shibor.empty and 'date' in macro_shibor.columns:
            macro_shibor['trade_date'] = pd.to_datetime(macro_shibor['date'], errors='coerce')
            macro_shibor['shibor_1m'] = pd.to_numeric(macro_shibor.get('1m', 0.0), errors='coerce')
            macro_shibor['shibor_3m'] = pd.to_numeric(macro_shibor.get('3m', 0.0), errors='coerce')
            macro_shibor = macro_shibor.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
            macro_shibor = macro_shibor[['trade_date', 'shibor_1m', 'shibor_3m']]
        else:
            macro_shibor = pd.DataFrame(columns=['trade_date', 'shibor_1m', 'shibor_3m'])

        # 宏观CSV为空时，降级为最新实时快照，避免训练侧特征全为0。
        if macro_cpi.empty or macro_pmi.empty or macro_shibor.empty:
            mc = MacroCollector()
            anchor_day = pd.Timestamp('2000-01-01')

            if macro_cpi.empty:
                try:
                    cpi = mc.get_cpi() or {}
                    cpi_val = self._to_float(cpi.get('value'), 0.0)
                    macro_cpi = pd.DataFrame([{'trade_date': anchor_day, 'cpi_yoy': cpi_val}])
                except Exception as e:
                    logger.warning(f"CPI实时快照降级失败: {e}")

            if macro_pmi.empty:
                try:
                    pmi = mc.get_pmi() or {}
                    pmi_val = self._to_float(pmi.get('manufacturing_pmi'), 0.0)
                    macro_pmi = pd.DataFrame([{'trade_date': anchor_day, 'pmi': pmi_val}])
                except Exception as e:
                    logger.warning(f"PMI实时快照降级失败: {e}")

            if macro_shibor.empty:
                try:
                    s = mc.get_shibor() or {}
                    s1m = self._to_float(s.get('1m'), 0.0)
                    s3m = self._to_float(s.get('3m'), 0.0)
                    macro_shibor = pd.DataFrame([{'trade_date': anchor_day, 'shibor_1m': s1m, 'shibor_3m': s3m}])
                except Exception as e:
                    logger.warning(f"Shibor实时快照降级失败: {e}")

        cross_asset = self._load_cross_asset_features(data_dir)

        cache = {
            'daily_basic_by_code': self._group_by_code(daily_basic, 'ts_code', 'trade_date'),
            'financial_by_code': self._group_by_code(financial, 'ts_code', 'ann_date'),
            'moneyflow_by_code': self._group_by_code(moneyflow, 'ts_code', 'trade_date'),
            'north_money': north_money,
            'margin': margin,
            'top_list_set': top_list_set,
            'research_set': research_set,
            'research_dates_by_code': research_dates_by_code,
            'news_by_date': news_by_date,
            'news_decay_by_date': news_decay_by_date,
            'macro_cpi': macro_cpi,
            'macro_pmi': macro_pmi,
            'macro_shibor': macro_shibor,
            'cross_asset': cross_asset,
        }
        self._external_feature_cache = cache
        return cache

    def _build_external_feature_payload(self, code, asof_date):
        cache = self._load_external_feature_cache()
        asof_dt = self._to_datetime(asof_date)
        asof_day = asof_dt.date() if asof_dt is not None else None

        valuation_data = {
            'pe': 0.0,
            'pb': 0.0,
            'eps': 0.0,
            'roe': 0.0,
        }
        market_data = {
            'net_mf_amount': 0.0,
            'north_money': 0.0,
            'rzye': 0.0,
            'rzmre': 0.0,
            'has_top_list': 0,
            'sentiment': 0.0,
            'has_report': 0,
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
            'sentiment_lag1': 0.0,
            'sentiment_ma3': 0.0,
            'sentiment_shock': 0.0,
            'report_decay_3d': 0.0,
            'is_a_asset': 0,
            'is_hk_asset': 0,
            'is_us_asset': 0,
            'is_fund_asset': 0,
            'is_metal_asset': 0,
            'is_foreign_asset': 0,
        }

        # A股特征按 ts_code 精确匹配，其他市场保留宏观与跨资产特征，并追加资产类别标记。
        code = str(code)
        market_data.update(self._infer_asset_flags(code))

        daily_row = self._latest_row_by_code(cache['daily_basic_by_code'], code, asof_dt, 'trade_date')
        if daily_row is not None:
            valuation_data['pe'] = self._to_float(daily_row.get('pe'))
            valuation_data['pb'] = self._to_float(daily_row.get('pb'))

        fin_row = self._latest_row_by_code(cache['financial_by_code'], code, asof_dt, 'ann_date')
        if fin_row is not None:
            valuation_data['eps'] = self._to_float(fin_row.get('eps'))
            valuation_data['roe'] = self._to_float(fin_row.get('roe'))

        mf_row = self._latest_row_by_code(cache['moneyflow_by_code'], code, asof_dt, 'trade_date')
        if mf_row is not None:
            market_data['net_mf_amount'] = self._to_float(mf_row.get('net_mf_amount'))

        north_row = self._latest_row_by_date(cache['north_money'], asof_dt, 'trade_date')
        if north_row is not None:
            market_data['north_money'] = self._to_float(north_row.get('north_money'))

        margin_row = self._latest_row_by_date(cache['margin'], asof_dt, 'trade_date')
        if margin_row is not None:
            market_data['rzye'] = self._to_float(margin_row.get('rzye'))
            market_data['rzmre'] = self._to_float(margin_row.get('rzmre'))

        if asof_day is not None:
            market_data['has_top_list'] = 1 if (code, asof_day) in cache['top_list_set'] else 0
            market_data['has_report'] = 1 if (code, asof_day) in cache['research_set'] else 0
            decay_map = cache.get('news_decay_by_date', {}) or {}
            raw_map = cache.get('news_by_date', {}) or {}
            market_data['sentiment'] = self._to_float(decay_map.get(asof_day, raw_map.get(asof_day, 0.0)))

            lag1_day = asof_day - timedelta(days=1)
            lag2_day = asof_day - timedelta(days=2)
            market_data['sentiment_lag1'] = self._to_float(decay_map.get(lag1_day, raw_map.get(lag1_day, 0.0)))
            s0 = self._to_float(decay_map.get(asof_day, raw_map.get(asof_day, 0.0)))
            s1 = market_data['sentiment_lag1']
            s2 = self._to_float(decay_map.get(lag2_day, raw_map.get(lag2_day, 0.0)))
            market_data['sentiment_ma3'] = float((s0 + s1 + s2) / 3.0)
            market_data['sentiment_shock'] = float(s0 - market_data['sentiment_ma3'])

            report_days = cache.get('research_dates_by_code', {}).get(code, [])
            report_decay = 0.0
            if report_days:
                try:
                    recent = [d for d in report_days if d <= asof_day]
                    if recent:
                        delta = (asof_day - recent[-1]).days
                        if 0 <= delta <= 3:
                            report_decay = float(np.exp(-delta / 1.5))
                except Exception:
                    report_decay = 0.0
            market_data['report_decay_3d'] = report_decay

        cpi_row = self._latest_row_by_date(cache.get('macro_cpi'), asof_dt, 'trade_date')
        if cpi_row is not None:
            market_data['cpi_yoy'] = self._to_float(cpi_row.get('cpi_yoy'))

        pmi_row = self._latest_row_by_date(cache.get('macro_pmi'), asof_dt, 'trade_date')
        if pmi_row is not None:
            market_data['pmi'] = self._to_float(pmi_row.get('pmi'))

        shibor_row = self._latest_row_by_date(cache.get('macro_shibor'), asof_dt, 'trade_date')
        if shibor_row is not None:
            market_data['shibor_1m'] = self._to_float(shibor_row.get('shibor_1m'))
            market_data['shibor_3m'] = self._to_float(shibor_row.get('shibor_3m'))

        market_data['macro_regime_score'] = self._compute_macro_regime_score(
            market_data.get('cpi_yoy'),
            market_data.get('pmi'),
            market_data.get('shibor_1m'),
            market_data.get('shibor_3m')
        )

        cross_row = self._latest_row_by_date(cache.get('cross_asset'), asof_dt, 'trade_date')
        if cross_row is not None:
            market_data['risk_off_proxy'] = self._to_float(cross_row.get('risk_off_proxy'), 0.0)
            market_data['dollar_proxy'] = self._to_float(cross_row.get('dollar_proxy'), 0.0)
            market_data['gold_oil_ratio'] = self._to_float(cross_row.get('gold_oil_ratio'), 0.0)
            market_data['vix'] = self._to_float(cross_row.get('vix'), 0.0)
            market_data['dxy'] = self._to_float(cross_row.get('dxy'), 0.0)
            market_data['tnx'] = self._to_float(cross_row.get('tnx'), 0.0)

        return valuation_data, market_data

    @staticmethod
    def _runtime_model_score(metadata=None):
        """将多项验证指标压缩为单一分值，便于判断是否应升级运行时模型。"""
        metadata = metadata or {}

        def _metric(*keys, default=0.0):
            for key in keys:
                value = metadata.get(key)
                if value is not None:
                    try:
                        return float(value)
                    except Exception:
                        continue
            return float(default)

        acc = _metric('validation_accuracy', 'accuracy')
        f1 = _metric('validation_f1', 'f1')
        auc = _metric('validation_auc', 'auc')
        brier = _metric('validation_brier', 'brier', default=1.0)
        return float((acc * 0.45) + (f1 * 0.35) + (auc * 0.20) - (brier * 0.10))

    @classmethod
    def _should_promote_runtime_model(cls, existing_metadata=None, candidate_metadata=None):
        """只有当新模型综合验证质量不弱于现网模型时，才允许升级。"""
        existing_metadata = existing_metadata or {}
        candidate_metadata = candidate_metadata or {}
        if not existing_metadata:
            return True

        current_score = cls._runtime_model_score(existing_metadata)
        candidate_score = cls._runtime_model_score(candidate_metadata)
        if candidate_score > current_score + 0.002:
            return True
        if candidate_score < current_score - 0.002:
            return False

        current_acc = float(existing_metadata.get('validation_accuracy') or existing_metadata.get('accuracy') or 0.0)
        candidate_acc = float(candidate_metadata.get('validation_accuracy') or candidate_metadata.get('accuracy') or 0.0)
        current_f1 = float(existing_metadata.get('validation_f1') or existing_metadata.get('f1') or 0.0)
        candidate_f1 = float(candidate_metadata.get('validation_f1') or candidate_metadata.get('f1') or 0.0)
        current_auc = float(existing_metadata.get('validation_auc') or existing_metadata.get('auc') or 0.0)
        candidate_auc = float(candidate_metadata.get('validation_auc') or candidate_metadata.get('auc') or 0.0)
        current_brier = float(existing_metadata.get('validation_brier') or existing_metadata.get('brier') or 1.0)
        candidate_brier = float(candidate_metadata.get('validation_brier') or candidate_metadata.get('brier') or 1.0)

        improvements = sum([
            candidate_acc >= current_acc,
            candidate_f1 >= current_f1,
            candidate_auc >= current_auc,
            candidate_brier <= current_brier,
        ])
        regressions = sum([
            candidate_acc < current_acc - 0.005,
            candidate_f1 < current_f1 - 0.005,
            candidate_auc < current_auc - 0.005,
            candidate_brier > current_brier + 0.005,
        ])
        return improvements >= 3 and regressions <= 1

    def _save_runtime_model_file(self, model, filename, metadata=None):
        """保存运行时标准模型文件，供预测链路直接加载。"""
        model_path = os.path.join('data', 'models', filename)
        os.makedirs(os.path.dirname(model_path), exist_ok=True)

        existing_metadata = {}
        if os.path.exists(model_path):
            try:
                with open(model_path, 'rb') as f:
                    existing_payload = pickle.load(f)
                if isinstance(existing_payload, dict):
                    existing_metadata = existing_payload.get('metadata') or {}
            except Exception:
                existing_metadata = {}

        candidate_metadata = metadata or {}
        if existing_metadata and not self._should_promote_runtime_model(existing_metadata, candidate_metadata):
            logger.info(f"运行时模型保持现有版本，跳过覆盖: {model_path}")
            return False

        payload = {
            'model': model,
            'saved_at': datetime.now().isoformat(),
            'metadata': candidate_metadata
        }
        # 兼容扩展字段（如概率校准器）
        if isinstance(candidate_metadata, dict) and '_runtime_extras' in candidate_metadata:
            extras = candidate_metadata.get('_runtime_extras') or {}
            if isinstance(extras, dict):
                payload.update(extras)

        with open(model_path, 'wb') as f:
            pickle.dump(payload, f)
        logger.info(f"已更新运行时模型文件: {model_path}")
        return True

    @staticmethod
    def _fit_probability_calibrator(method, y_calib, proba_calib):
        try:
            y_arr = np.asarray(y_calib).astype(int)
            p_arr = np.asarray(proba_calib).astype(float)
            if len(y_arr) < 80 or len(np.unique(y_arr)) < 2:
                return None

            if method == 'platt':
                from sklearn.linear_model import LogisticRegression
                clf = LogisticRegression(solver='lbfgs', max_iter=500)
                clf.fit(p_arr.reshape(-1, 1), y_arr)
                return clf
            if method == 'isotonic':
                from sklearn.isotonic import IsotonicRegression
                iso = IsotonicRegression(out_of_bounds='clip')
                iso.fit(p_arr, y_arr)
                return iso
        except Exception:
            return None
        return None

    @staticmethod
    def _apply_probability_calibrator(method, calibrator, proba):
        p_arr = np.asarray(proba).astype(float)
        if calibrator is None or method == 'none':
            return np.clip(p_arr, 1e-6, 1 - 1e-6)
        try:
            if method == 'platt':
                return np.clip(calibrator.predict_proba(p_arr.reshape(-1, 1))[:, 1], 1e-6, 1 - 1e-6)
            if method == 'isotonic':
                return np.clip(calibrator.predict(p_arr), 1e-6, 1 - 1e-6)
        except Exception:
            return np.clip(p_arr, 1e-6, 1 - 1e-6)
        return np.clip(p_arr, 1e-6, 1 - 1e-6)

    def _get_label_threshold(self, period_days):
        """按预测周期返回标签阈值，短周期适当降低阈值避免样本过稀。"""
        base = float(PREDICTION_THRESHOLD)
        if period_days <= 5:
            # 5日使用方向标签，避免短周期涨跌幅阈值引入过强噪声与样本稀疏。
            return 0.0
        if period_days >= 60:
            return min(0.05, base * 1.25)
        return base

    @staticmethod
    def _infer_asset_flags(code):
        code = str(code or '').strip().upper()
        plain_six_digit = bool(code.isdigit() and len(code) == 6)
        fund_prefix = plain_six_digit and code[:2] in {'15', '16', '18', '50', '51', '52', '56', '58'}
        is_hk = int(code.endswith('.HK'))
        is_a = int(code.endswith('.SH') or code.endswith('.SZ') or code.endswith('.BJ') or (plain_six_digit and not fund_prefix))
        metal_codes = {'GC=F', 'SI=F', 'XAUUSD=X', 'XAGUSD=X', 'GLD', 'IAU', 'SLV', 'SIVR', '518880.SH', '518800.SH', '159934.SZ'}
        is_metal = int(code in metal_codes)
        is_fund = int((code.endswith('.OF')) or fund_prefix)
        is_us = int((not is_a and not is_hk and not code.endswith('.OF')) and bool(code))
        is_foreign = int(bool(is_hk or is_us))
        return {
            'is_a_asset': is_a,
            'is_hk_asset': is_hk,
            'is_us_asset': is_us,
            'is_fund_asset': is_fund,
            'is_metal_asset': is_metal,
            'is_foreign_asset': is_foreign,
        }

    @staticmethod
    def _round_robin_merge_code_groups(groups, target_limit=None):
        merged = []
        seen = set()
        groups = [list(g or []) for g in groups if g]
        if not groups:
            return merged

        max_len = max(len(g) for g in groups)
        for idx in range(max_len):
            for group in groups:
                if idx >= len(group):
                    continue
                code = str(group[idx]).strip()
                if not code or code in seen:
                    continue
                seen.add(code)
                merged.append(code)
                if target_limit is not None and len(merged) >= target_limit:
                    return merged
        return merged

    def _load_extra_training_codes_from_db(self):
        codes = []
        session = None
        try:
            session = get_session()
            stock_codes = [str(row[0]).strip() for row in session.query(RawStockData.code).distinct().all() if row and row[0]]
            fund_codes = [str(row[0]).strip() for row in session.query(RawFundData.code).distinct().all() if row and row[0]]
            codes.extend(stock_codes)
            codes.extend(fund_codes)
        except Exception as e:
            logger.warning(f"读取多资产训练代码失败: {e}")
        finally:
            if session:
                session.close()

        return list(dict.fromkeys([c for c in codes if c]))

    def _get_default_training_codes(self, limit=None):
        """构建默认训练代码池：优先纳入A股、港股、美股，以及已采集的基金/贵金属代理标的。"""
        target_limit = None
        try:
            if limit is not None and int(limit) > 0:
                target_limit = int(limit)
        except Exception:
            target_limit = None

        a_codes = list(dict.fromkeys(self.collector.a_stock_pool or []))
        hk_codes = list(dict.fromkeys(self.collector.hk_stock_pool or []))
        us_codes = list(dict.fromkeys(self.collector.us_stock_pool or []))

        extra_codes = self._load_extra_training_codes_from_db()
        fund_like = [c for c in extra_codes if self._infer_asset_flags(c).get('is_fund_asset') or self._infer_asset_flags(c).get('is_metal_asset')]
        remaining = [c for c in extra_codes if c not in set(fund_like)]

        codes = self._round_robin_merge_code_groups(
            [a_codes, hk_codes, us_codes, fund_like, remaining],
            target_limit=target_limit,
        )

        if target_limit is None or len(codes) < target_limit:
            csv_path = resolve_data_file(STOCK_BASIC_FILE, LEGACY_STOCK_POOL_FILE)
            if os.path.exists(csv_path):
                try:
                    stock_df = pd.read_csv(csv_path, usecols=['ts_code'])
                    for ts_code in stock_df['ts_code'].dropna().astype(str):
                        if not (ts_code.endswith('.SZ') or ts_code.endswith('.SH')):
                            continue
                        if ts_code not in codes:
                            codes.append(ts_code)
                        if target_limit is not None and len(codes) >= target_limit:
                            break
                except Exception as e:
                    logger.warning(f"读取默认训练股票池失败: {e}")

        return codes[:target_limit] if target_limit is not None else codes

    @staticmethod
    def _slice_feature_window(df, end_idx, period_days=5):
        """截取最近必要历史窗口，避免为每个样本重复扫描整段历史。"""
        if df is None or len(df) == 0:
            return df

        try:
            end_i = int(end_idx)
        except Exception:
            end_i = len(df) - 1

        if period_days <= 5:
            max_window = 180
        elif period_days <= 20:
            max_window = 220
        else:
            max_window = 260

        start_i = max(0, end_i - max_window + 1)
        return df.iloc[start_i:end_i + 1].copy()
    
    def prepare_training_data(self, stock_codes, period_days=5, lookback_years=3, neutral_zone=0.0, return_meta=False, adaptive_label_zone=False, use_event_features=False, progress_label=None):
        """
        准备训练数据
        Args:
            stock_codes: 股票代码列表
            period_days: 预测周期
            lookback_years: 回溯年数
        Returns:
            tuple: (X, y)
        """
        X_list = []
        y_list = []
        meta_rows = []
        label_threshold = self._get_label_threshold(period_days)

        max_codes = len(stock_codes)
        total_codes = len(stock_codes[:max_codes])
        progress_interval = 100 if total_codes >= 1000 else (50 if total_codes >= 300 else 25)
        min_window = 60
        max_samples_per_code = None
        if len(stock_codes) >= 1000:
            if period_days <= 5:
                max_samples_per_code = 360
            elif period_days <= 20:
                max_samples_per_code = 260
            else:
                max_samples_per_code = 180

        for idx, code in enumerate(stock_codes[:max_codes], start=1):
            try:
                df = self.collector.get_stock_data_from_db(code)
                if df is None or len(df) < (min_window + period_days + 5):
                    continue

                df = df.reset_index()
                if 'date' not in df.columns:
                    # 兼容索引列名不固定的场景（如 index）
                    first_col = df.columns[0]
                    df = df.rename(columns={first_col: 'date'})
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').reset_index(drop=True)

                lookback_days = int(max(30, round(365 * float(lookback_years))))
                lookback_start = df['date'].max() - pd.Timedelta(days=lookback_days)
                start_idx = int(df['date'].searchsorted(lookback_start, side='left'))
                start_idx = max(min_window - 1, start_idx)
                max_end_idx = len(df) - period_days - 1
                if max_end_idx < start_idx:
                    continue

                sample_count = 0

                # 滚动构建样本：全市场模式下对过密窗口做均匀抽样，
                # 保留全量标的覆盖，避免高度重叠窗口导致训练耗时失控。
                total_windows = max_end_idx - start_idx + 1
                if max_samples_per_code is not None and total_windows > max_samples_per_code:
                    step = max(1, int(np.ceil(total_windows / max_samples_per_code)))
                    end_idx_iter = range(start_idx, max_end_idx + 1, step)
                else:
                    end_idx_iter = range(start_idx, max_end_idx + 1)

                for end_idx in end_idx_iter:
                    if max_samples_per_code is not None and sample_count >= max_samples_per_code:
                        break

                    window_df = self._slice_feature_window(df, end_idx=end_idx, period_days=period_days)
                    asof_date = pd.to_datetime(df.iloc[end_idx]['date'])
                    if period_days == 5:
                        # 5日训练也应始终接入真实宏观/市场特征；
                        # 仅将舆情/研报等事件特征按实验开关控制，避免“数据已补齐但训练未使用”。
                        valuation_data, market_data = self._build_external_feature_payload(code, asof_date)
                        market_data = dict(market_data or {})
                        if not use_event_features:
                            for k in ['sentiment', 'sentiment_lag1', 'sentiment_ma3', 'sentiment_shock', 'has_report', 'report_decay_3d']:
                                market_data[k] = 0.0 if k != 'has_report' else 0
                    else:
                        valuation_data, market_data = self._build_external_feature_payload(code, asof_date)

                    if period_days == 5:
                        X_row = self.short_predictor.prepare_features(
                            window_df,
                            valuation_data=valuation_data,
                            market_data=market_data
                        )
                    elif period_days == 20:
                        X_row = self.medium_predictor.prepare_features(
                            window_df,
                            valuation_data=valuation_data,
                            market_data=market_data
                        )
                    elif period_days == 60:
                        X_row = self.long_predictor.prepare_features(
                            window_df,
                            valuation_data=valuation_data,
                            market_data=market_data
                        )
                    else:
                        continue

                    if X_row is None or len(X_row) == 0:
                        continue

                    base_price = float(df.iloc[end_idx]['close'])
                    future_price = float(df.iloc[end_idx + period_days]['close'])
                    future_return = (future_price - base_price) / base_price if base_price > 0 else 0.0
                    effective_neutral_zone = float(neutral_zone)
                    if effective_neutral_zone > 0 and adaptive_label_zone:
                        # 按近期波动率动态调整中性带宽，降低不同周期下的标签噪声。
                        lookback_len = 20 if period_days <= 5 else (40 if period_days <= 20 else 60)
                        recent_ret = pd.to_numeric(window_df['close'], errors='coerce').pct_change().dropna().tail(lookback_len)
                        recent_vol = float(recent_ret.std()) if len(recent_ret) >= 5 else 0.0
                        base_vol = 0.02 if period_days <= 5 else (0.035 if period_days <= 20 else 0.05)
                        vol_scale = np.clip(recent_vol / base_vol, 0.7, 1.8) if recent_vol > 0 else 1.0
                        lower_band = 0.003 if period_days <= 5 else (0.006 if period_days <= 20 else 0.012)
                        upper_band = 0.016 if period_days <= 5 else (0.025 if period_days <= 20 else 0.040)
                        effective_neutral_zone = float(np.clip(effective_neutral_zone * vol_scale, lower_band, upper_band))

                    if effective_neutral_zone > 0 and abs(future_return) <= effective_neutral_zone:
                        # 中性带宽内的弱波动不进入方向学习，避免中长期模型被噪声样本稀释。
                        continue

                    y_value = 1 if future_return > label_threshold else 0

                    X_list.append(X_row)
                    y_list.append(y_value)
                    feature_snapshot = X_row.iloc[0].to_dict() if hasattr(X_row, 'iloc') and len(X_row) > 0 else {}
                    meta_rows.append({
                        'code': code,
                        'asof_date': pd.to_datetime(df.iloc[end_idx]['date']),
                        'future_return': float(future_return),
                        'effective_neutral_zone': float(effective_neutral_zone),
                        'sentiment': float(market_data.get('sentiment', 0.0)),
                        'sentiment_shock': float(market_data.get('sentiment_shock', 0.0)),
                        'has_report': int(market_data.get('has_report', 0)),
                        'has_top_list': int(market_data.get('has_top_list', 0)),
                        'report_decay_3d': float(market_data.get('report_decay_3d', 0.0)),
                        'cpi_yoy': float(market_data.get('cpi_yoy', 0.0)),
                        'pmi': float(market_data.get('pmi', 0.0)),
                        'shibor_1m': float(market_data.get('shibor_1m', 0.0)),
                        'shibor_3m': float(market_data.get('shibor_3m', 0.0)),
                        'macro_regime_score': float(market_data.get('macro_regime_score', 0.0)),
                        'risk_off_proxy': float(market_data.get('risk_off_proxy', 0.0)),
                        'dollar_proxy': float(market_data.get('dollar_proxy', 0.0)),
                        'gold_oil_ratio': float(market_data.get('gold_oil_ratio', 0.0)),
                        'volatility': float(feature_snapshot.get('volatility', 0.0)),
                        'rsi': float(feature_snapshot.get('rsi', 50.0)),
                        'volume_ratio': float(feature_snapshot.get('volume_ratio', 1.0)),
                        'price_ma20_ratio': float(feature_snapshot.get('price_ma20_ratio', 0.0)),
                        'return_5d': float(feature_snapshot.get('return_5d', 0.0)),
                        'return_20d': float(feature_snapshot.get('return_20d', 0.0)),
                        'momentum_5d': float(feature_snapshot.get('momentum_5d', 0.0)),
                        'atr_ratio': float(feature_snapshot.get('atr_ratio', 0.0)),
                        'drawdown_60d': float(feature_snapshot.get('drawdown_60d', 0.0)),
                        'trend_consistency_20d': float(feature_snapshot.get('trend_consistency_20d', 0.0)),
                        'volume_trend_20d': float(feature_snapshot.get('volume_trend_20d', 0.0)),
                        'event_heat': float(feature_snapshot.get('event_heat', 0.0)),
                    })
                    sample_count += 1

            except Exception as e:
                logger.error(f"处理 {code} 失败: {e}")
                continue
            finally:
                if idx % progress_interval == 0 or idx == total_codes:
                    progress_tag = f"[{progress_label}]" if progress_label else ""
                    logger.info(
                        f"{period_days}日训练样本构建进度{progress_tag}: {idx}/{total_codes}，累计样本={len(X_list)}"
                    )

        if not X_list:
            if return_meta:
                return None, None, pd.DataFrame(meta_rows)
            return None, None

        X = pd.concat(X_list, ignore_index=True)
        y = pd.Series(y_list)

        if return_meta:
            return X, y, pd.DataFrame(meta_rows)
        return X, y

    def _time_based_split(self, X, y, meta_df, split_ratio=MODEL_TRAIN_TEST_SPLIT):
        """按样本时间切分训练/验证集，降低时间泄漏风险。"""
        if meta_df is None or meta_df.empty or 'asof_date' not in meta_df.columns:
            split_idx = int(len(X) * split_ratio)
            return X[:split_idx], X[split_idx:], y[:split_idx], y[split_idx:], None

        meta_df = meta_df.reset_index(drop=True)
        cutoff_date = meta_df['asof_date'].sort_values().iloc[int(len(meta_df) * split_ratio)]
        train_mask = meta_df['asof_date'] <= cutoff_date

        # 保证验证集非空
        if train_mask.all() or (~train_mask).all():
            split_idx = int(len(X) * split_ratio)
            return X[:split_idx], X[split_idx:], y[:split_idx], y[split_idx:], None

        return (
            X[train_mask.values],
            X[(~train_mask).values],
            y[train_mask.values],
            y[(~train_mask).values],
            cutoff_date
        )

    @staticmethod
    def _summarize_prediction_bias(y_true, y_pred):
        """提取阈值下的方向偏置特征，识别过度看涨/看跌。"""
        y_true_arr = np.asarray(y_true).astype(int)
        y_pred_arr = np.asarray(y_pred).astype(int)

        tp = int(((y_true_arr == 1) & (y_pred_arr == 1)).sum())
        tn = int(((y_true_arr == 0) & (y_pred_arr == 0)).sum())
        fp = int(((y_true_arr == 0) & (y_pred_arr == 1)).sum())
        fn = int(((y_true_arr == 1) & (y_pred_arr == 0)).sum())

        pos = max(int((y_true_arr == 1).sum()), 1)
        neg = max(int((y_true_arr == 0).sum()), 1)

        precision = float(tp / max(tp + fp, 1))
        recall = float(tp / pos)
        specificity = float(tn / neg)
        balanced_accuracy = float((recall + specificity) / 2.0)
        predicted_positive_rate = float(np.mean(y_pred_arr)) if len(y_pred_arr) > 0 else 0.0
        actual_positive_rate = float(np.mean(y_true_arr)) if len(y_true_arr) > 0 else 0.0

        return {
            'tp': tp,
            'tn': tn,
            'fp': fp,
            'fn': fn,
            'precision': precision,
            'recall': recall,
            'specificity': specificity,
            'balanced_accuracy': balanced_accuracy,
            'predicted_positive_rate': predicted_positive_rate,
            'actual_positive_rate': actual_positive_rate,
            'rate_gap': abs(predicted_positive_rate - actual_positive_rate),
            'false_positive_rate': float(fp / neg),
            'false_negative_rate': float(fn / pos),
        }

    def _evaluate_binary(self, y_true, y_pred, y_proba=None):
        """统一二分类评估指标。"""
        bias = self._summarize_prediction_bias(y_true, y_pred)
        metrics = {
            'accuracy': float(accuracy_score(y_true, y_pred)),
            'precision': bias['precision'],
            'recall': bias['recall'],
            'f1': float(f1_score(y_true, y_pred, zero_division=0)),
            'balanced_accuracy': bias['balanced_accuracy'],
            'predicted_positive_rate': bias['predicted_positive_rate'],
            'actual_positive_rate': bias['actual_positive_rate'],
            'false_positive_rate': bias['false_positive_rate'],
            'false_negative_rate': bias['false_negative_rate'],
        }

        if y_proba is not None and len(np.unique(y_true)) > 1:
            try:
                metrics['auc'] = float(roc_auc_score(y_true, y_proba))
            except Exception:
                metrics['auc'] = None
            try:
                metrics['brier'] = float(brier_score_loss(y_true, y_proba))
            except Exception:
                metrics['brier'] = None
        else:
            metrics['auc'] = None
            metrics['brier'] = None

        return metrics

    def _find_best_threshold(self, y_true, y_proba, min_threshold=0.30, max_threshold=0.70, step=0.01):
        """在验证集上搜索兼顾F1、平衡准确率与误报约束的最优阈值。"""
        if y_proba is None or len(y_true) == 0:
            return 0.5, None

        y_true_arr = np.asarray(y_true).astype(int)
        p_arr = np.asarray(y_proba).astype(float)

        best_t = 0.5
        best_score = -1e9
        thresholds = np.arange(min_threshold, max_threshold + 1e-9, step)

        for t in thresholds:
            y_pred = (p_arr >= t).astype(int)
            f1 = float(f1_score(y_true_arr, y_pred, zero_division=0))
            acc = float(accuracy_score(y_true_arr, y_pred))
            bias = self._summarize_prediction_bias(y_true_arr, y_pred)

            score = (
                (0.30 * f1)
                + (0.25 * bias['balanced_accuracy'])
                + (0.20 * bias['precision'])
                + (0.15 * acc)
                + (0.10 * bias['recall'])
                - (0.20 * bias['rate_gap'])
                - (0.15 * bias['false_positive_rate'])
            )

            if (score > best_score + 1e-12) or (abs(score - best_score) <= 1e-12 and float(t) > best_t):
                best_score = float(score)
                best_t = float(t)

        return best_t, float(best_score)

    def _find_best_threshold_for_horizon(self, period_days, y_true, y_proba):
        """按周期与样本偏态动态放宽阈值搜索区间，减少中长期误报。"""
        y_true_arr = np.asarray(y_true).astype(int) if y_true is not None else np.array([])
        pos_rate = float(np.mean(y_true_arr)) if len(y_true_arr) > 0 else 0.5

        min_t, max_t, step = 0.30, 0.70, 0.01
        if int(period_days) >= 60:
            min_t, max_t = 0.38, 0.88
        elif int(period_days) >= 20:
            min_t, max_t = 0.35, 0.85

        if pos_rate <= 0.32:
            min_t = max(min_t, 0.45)
            max_t = max(max_t, 0.90)
        elif pos_rate <= 0.40:
            max_t = max(max_t, 0.80)

        return self._find_best_threshold(y_true, y_proba, min_threshold=min_t, max_threshold=max_t, step=step)

    def _pass_validation_gate(self, period_days, eval_metrics):
        """按周期执行上线门槛判断。"""
        passed, gate, _ = self.model_manager.evaluate_validation_gate(period_days, eval_metrics)
        return passed, gate

    def _build_recency_weights(self, dates, decay_days=180.0):
        """根据样本日期构建时间衰减权重，近期样本权重更高。"""
        if dates is None or len(dates) == 0:
            return None

        dt = pd.to_datetime(dates, errors='coerce')
        if dt.isna().all():
            return None

        latest = dt.max()
        age_days = (latest - dt).dt.days.fillna(0).clip(lower=0)
        w = np.exp(-age_days / float(decay_days))

        # 约束范围，防止个别样本权重过大/过小
        w = np.clip(w, 0.4, 2.0)
        # 归一化到均值约1，避免改变训练器的整体尺度
        mean_w = float(np.mean(w)) if len(w) > 0 else 1.0
        if mean_w > 0:
            w = w / mean_w

        return w

    def _build_move_strength_weights(self, future_returns, scale=0.02, neutral_band=0.003):
        """按未来收益绝对值构建样本强度权重，弱信号降权，强信号增权。"""
        if future_returns is None or len(future_returns) == 0:
            return None
        try:
            fr = np.asarray(future_returns, dtype=float)
            fr = np.nan_to_num(fr, nan=0.0, posinf=0.0, neginf=0.0)
            abs_fr = np.abs(fr)
            scale_v = max(float(scale), 1e-6)
            band = max(float(neutral_band), 0.0)

            # 对接近0的弱波动样本做死区降权，减少标签噪声。
            effective = np.clip(abs_fr - band, 0.0, None)
            strength = effective / scale_v
            w = 0.70 + 1.30 * np.clip(strength, 0.0, 1.6)
            w = np.clip(w, 0.7, 2.6)

            mean_w = float(np.mean(w)) if len(w) > 0 else 1.0
            if mean_w > 0:
                w = w / mean_w
            return w
        except Exception:
            return None

    @staticmethod
    def _build_direction_balance_weights(labels, target_positive_rate=0.5):
        """根据标签分布构建方向平衡权重，抑制单边牛市/熊市造成的偏置。"""
        if labels is None or len(labels) == 0:
            return None
        try:
            y = np.asarray(labels).astype(int)
            pos_mask = (y == 1)
            neg_mask = (y == 0)
            pos_count = int(pos_mask.sum())
            neg_count = int(neg_mask.sum())
            if pos_count == 0 or neg_count == 0:
                return np.ones(len(y), dtype=float)

            actual_pos_rate = float(pos_count / max(len(y), 1))
            target_pos = float(np.clip(target_positive_rate, 0.2, 0.8))
            target_neg = 1.0 - target_pos

            pos_weight = target_pos / max(actual_pos_rate, 1e-6)
            neg_weight = target_neg / max(1.0 - actual_pos_rate, 1e-6)
            w = np.where(pos_mask, pos_weight, neg_weight).astype(float)
            w = np.clip(w, 0.6, 1.8)

            mean_w = float(np.mean(w)) if len(w) > 0 else 1.0
            if mean_w > 0:
                w = w / mean_w
            return w
        except Exception:
            return None

    @classmethod
    def _build_asset_balance_weights(cls, codes, clip_range=(0.65, 2.2)):
        """按资产类别平衡样本权重，避免单一市场体量过大压制其他资产。"""
        if codes is None or len(codes) == 0:
            return None
        try:
            buckets = []
            for code in codes:
                flags = cls._infer_asset_flags(code)
                if flags.get('is_metal_asset'):
                    bucket = 'metal'
                elif flags.get('is_fund_asset'):
                    bucket = 'fund'
                elif flags.get('is_hk_asset'):
                    bucket = 'hk'
                elif flags.get('is_us_asset'):
                    bucket = 'us'
                elif flags.get('is_a_asset'):
                    bucket = 'a'
                else:
                    bucket = 'other'
                buckets.append(bucket)

            s = pd.Series(buckets, dtype='object')
            counts = s.value_counts()
            if counts.empty:
                return None

            target = float(len(s)) / float(len(counts))
            lo, hi = clip_range
            w = np.asarray([np.clip(target / max(float(counts.get(bucket, 1)), 1.0), lo, hi) for bucket in buckets], dtype=float)
            mean_w = float(np.mean(w)) if len(w) > 0 else 1.0
            if mean_w > 0:
                w = w / mean_w
            return w
        except Exception:
            return None

    @staticmethod
    def _build_regime_alignment_weights(X_train, X_val, focus_cols=None):
        """对更接近当前验证期市场状态的训练样本给予更高权重。"""
        if X_train is None or X_val is None or len(X_train) == 0 or len(X_val) == 0:
            return None
        try:
            focus_cols = focus_cols or ['macro_regime_score', 'risk_off_proxy', 'dollar_proxy', 'volatility', 'price_position_60d']
            usable_cols = [c for c in focus_cols if c in X_train.columns and c in X_val.columns]
            if not usable_cols:
                return None

            train_df = X_train[usable_cols].apply(pd.to_numeric, errors='coerce').fillna(0.0)
            val_center = X_val[usable_cols].apply(pd.to_numeric, errors='coerce').fillna(0.0).median(axis=0)
            scale = train_df.std(axis=0).replace(0, 1.0).fillna(1.0)
            z = (train_df - val_center) / scale
            distance = np.sqrt(np.square(z).sum(axis=1))
            w = np.exp(-0.55 * np.asarray(distance, dtype=float))
            w = np.clip(w, 0.45, 1.75)
            mean_w = float(np.mean(w)) if len(w) > 0 else 1.0
            if mean_w > 0:
                w = w / mean_w
            return w
        except Exception:
            return None

    def _estimate_adaptive_neutral_band(self, future_returns, base_band=0.003, quantile=0.25):
        """基于训练集收益分布估计降权死区，降低静态阈值在不同市场状态下的失配。"""
        if future_returns is None or len(future_returns) == 0:
            return float(base_band)
        try:
            arr = np.asarray(future_returns, dtype=float)
            arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
            abs_arr = np.abs(arr)
            q = float(np.quantile(abs_arr, np.clip(float(quantile), 0.05, 0.5)))
            band = max(float(base_band), q)
            return float(np.clip(band, 0.002, 0.010))
        except Exception:
            return float(base_band)

    @staticmethod
    def _build_event_weights(
        has_report,
        sentiment,
        sentiment_shock=None,
        report_decay=None,
        report_boost=0.35,
        sentiment_boost=0.25,
        shock_boost=0.18,
        decay_boost=0.22,
    ):
        """财报/舆情样本加权，增强事件驱动信号。"""
        try:
            r = np.asarray(has_report, dtype=float)
            s = np.asarray(sentiment, dtype=float)
            if len(r) == 0 or len(s) == 0 or len(r) != len(s):
                return None
            s_abs = np.clip(np.abs(np.nan_to_num(s, nan=0.0, posinf=0.0, neginf=0.0)), 0.0, 1.0)
            shock_abs = np.zeros_like(s_abs)
            if sentiment_shock is not None:
                sh = np.asarray(sentiment_shock, dtype=float)
                if len(sh) == len(s):
                    shock_abs = np.clip(np.abs(np.nan_to_num(sh, nan=0.0, posinf=0.0, neginf=0.0)), 0.0, 1.5)

            decay_arr = np.zeros_like(s_abs)
            if report_decay is not None:
                rd = np.asarray(report_decay, dtype=float)
                if len(rd) == len(s):
                    decay_arr = np.clip(np.nan_to_num(rd, nan=0.0, posinf=0.0, neginf=0.0), 0.0, 1.0)

            w = (
                1.0
                + (float(report_boost) * np.clip(r, 0.0, 1.0))
                + (float(sentiment_boost) * s_abs)
                + (float(shock_boost) * shock_abs)
                + (float(decay_boost) * decay_arr)
            )
            w = np.clip(w, 0.8, 1.8)
            m = float(np.mean(w)) if len(w) > 0 else 1.0
            if m > 0:
                w = w / m
            return w
        except Exception:
            return None

    @staticmethod
    def _evaluate_temporal_stability(y_true, proba):
        """评估验证期前后半段稳定性，降低偶然最优的候选优先级。"""
        try:
            y_arr = np.asarray(y_true).astype(int)
            p_arr = np.asarray(proba).astype(float)
            n = len(y_arr)
            if n < 240:
                return {
                    'enabled': False,
                    'stability_penalty': 0.0,
                }

            mid = n // 2
            y1, y2 = y_arr[:mid], y_arr[mid:]
            p1, p2 = p_arr[:mid], p_arr[mid:]
            if len(np.unique(y1)) < 2 or len(np.unique(y2)) < 2:
                return {
                    'enabled': False,
                    'stability_penalty': 0.0,
                }

            auc_1 = float(roc_auc_score(y1, p1))
            auc_2 = float(roc_auc_score(y2, p2))
            brier_1 = float(brier_score_loss(y1, p1))
            brier_2 = float(brier_score_loss(y2, p2))

            auc_gap = abs(auc_1 - auc_2)
            brier_gap = abs(brier_1 - brier_2)

            penalty = min(0.12, (auc_gap * 0.50) + (brier_gap * 0.30))
            return {
                'enabled': True,
                'auc_first_half': round(auc_1, 6),
                'auc_second_half': round(auc_2, 6),
                'brier_first_half': round(brier_1, 6),
                'brier_second_half': round(brier_2, 6),
                'auc_gap': round(auc_gap, 6),
                'brier_gap': round(brier_gap, 6),
                'stability_penalty': round(float(penalty), 6),
            }
        except Exception:
            return {
                'enabled': False,
                'stability_penalty': 0.0,
            }

    @staticmethod
    def _evaluate_secondary_time_split(y_true, proba, tail_ratio=0.35):
        """第二时间切分复验: 仅看验证期尾段，检查近期可迁移性。"""
        try:
            y_arr = np.asarray(y_true).astype(int)
            p_arr = np.asarray(proba).astype(float)
            n = len(y_arr)
            if n < 220:
                return {'enabled': False}

            tail_n = int(max(120, round(n * float(tail_ratio))))
            tail_n = min(tail_n, n - 20)
            y_tail = y_arr[-tail_n:]
            p_tail = p_arr[-tail_n:]
            if len(np.unique(y_tail)) < 2:
                return {'enabled': False}

            auc_tail = float(roc_auc_score(y_tail, p_tail))
            brier_tail = float(brier_score_loss(y_tail, p_tail))
            return {
                'enabled': True,
                'tail_size': int(tail_n),
                'auc_tail': round(auc_tail, 6),
                'brier_tail': round(brier_tail, 6),
            }
        except Exception:
            return {'enabled': False}

    def _score_short_term_code(self, df):
        """评估个股短周期可预测性（流动性+稳定性+趋势一致性）。"""
        if df is None or len(df) < 120:
            return None
        try:
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            volume = pd.to_numeric(df['volume'], errors='coerce').dropna()
            if len(close) < 120 or len(volume) < 30:
                return None

            ret = close.pct_change().dropna().tail(240)
            if len(ret) < 60:
                return None

            vol = float(ret.std())
            ac1 = float(ret.autocorr(lag=1) or 0.0)
            ac5 = float(ret.autocorr(lag=5) or 0.0)
            consistency = min(1.0, abs(ac1) + abs(ac5) * 0.6)

            avg_vol20 = float(volume.tail(20).mean())
            liq_score = min(1.0, np.log1p(max(avg_vol20, 0.0)) / 16.0)

            price = float(close.iloc[-1])
            if price <= 0:
                return None
            tradable_score = 1.0 if price >= 3.0 else 0.6

            score = (consistency * 0.45) + (liq_score * 0.35) + (tradable_score * 0.20) - min(0.5, vol * 4.0)
            return float(score)
        except Exception:
            return None

    def _score_general_training_code(self, df, period_days=20):
        """评估中长期训练样本质量，优先保留流动性较好、波动不过度、历史更完整的标的。"""
        min_len = 160 if int(period_days) <= 20 else 220
        if df is None or len(df) < min_len:
            return None
        try:
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            volume = pd.to_numeric(df['volume'], errors='coerce').dropna() if 'volume' in df.columns else pd.Series(dtype=float)
            if len(close) < min_len:
                return None

            ret = close.pct_change().dropna().tail(260 if int(period_days) <= 20 else 360)
            if len(ret) < 80:
                return None

            vol = float(ret.std())
            autocorr_5 = abs(float(ret.autocorr(lag=5) or 0.0))
            autocorr_10 = abs(float(ret.autocorr(lag=10) or 0.0))
            trend_consistency = float(np.clip((autocorr_5 * 0.7) + (autocorr_10 * 0.5), 0.0, 1.0))

            if len(close) >= 120:
                ma60 = float(close.tail(60).mean())
                ma120 = float(close.tail(120).mean())
                slope = (ma60 / max(ma120, 1e-6)) - 1.0
                trend_score = float(np.clip(abs(slope) * 8.0, 0.0, 1.0))
            else:
                trend_score = 0.4

            avg_vol20 = float(volume.tail(20).mean()) if len(volume) >= 20 else 0.0
            liq_score = min(1.0, np.log1p(max(avg_vol20, 0.0)) / 16.0) if avg_vol20 > 0 else 0.35
            history_score = float(np.clip(len(close) / (420 if int(period_days) <= 20 else 600), 0.45, 1.0))

            price = float(close.iloc[-1])
            if price <= 0:
                return None
            tradable_score = 1.0 if price >= 3.0 else 0.65
            volatility_penalty = min(0.55, vol * (5.0 if int(period_days) <= 20 else 4.2))

            score = (
                (liq_score * 0.26)
                + (history_score * 0.22)
                + (trend_consistency * 0.20)
                + (trend_score * 0.17)
                + (tradable_score * 0.15)
                - volatility_penalty
            )
            return float(score)
        except Exception:
            return None

    def _select_quality_training_codes(self, stock_codes, period_days=20, target_count=None):
        """为中长期训练优先筛选更稳定、更有交易意义的标的，减少噪声样本。"""
        codes = list(stock_codes or [])
        if not codes:
            return []

        scored_groups = {'a': [], 'hk': [], 'us': [], 'fund': [], 'metal': [], 'other': []}
        for code in codes:
            try:
                df = self.collector.get_stock_data_from_db(code)
                score = self._score_general_training_code(df, period_days=period_days)
                if score is None:
                    continue
                flags = self._infer_asset_flags(code)
                if flags.get('is_metal_asset'):
                    bucket = 'metal'
                elif flags.get('is_fund_asset'):
                    bucket = 'fund'
                elif flags.get('is_hk_asset'):
                    bucket = 'hk'
                elif flags.get('is_us_asset'):
                    bucket = 'us'
                elif flags.get('is_a_asset'):
                    bucket = 'a'
                else:
                    bucket = 'other'
                scored_groups[bucket].append((str(code), float(score)))
            except Exception:
                continue

        if not any(scored_groups.values()):
            return codes if target_count is None else codes[:target_count]

        for bucket in scored_groups:
            scored_groups[bucket].sort(key=lambda x: x[1], reverse=True)

        merged = self._round_robin_merge_code_groups(
            [[code for code, _ in scored_groups[b]] for b in ['a', 'hk', 'us', 'fund', 'metal', 'other']],
            target_limit=int(target_count) if target_count is not None else None,
        )
        if merged:
            return merged
        return codes if target_count is None else codes[:int(target_count)]

    def _select_short_term_codes(self, stock_codes, target_count=None):
        """为5日模型筛选可训练标的；默认返回全部可用标的。"""
        scored = []
        for code in stock_codes:
            try:
                df = self.collector.get_stock_data_from_db(code)
                if df is None:
                    continue
                df = df.reset_index()
                if 'date' not in df.columns and len(df.columns) > 0:
                    df = df.rename(columns={df.columns[0]: 'date'})
                score = self._score_short_term_code(df)
                if score is None:
                    continue
                scored.append((str(code), score))
            except Exception:
                continue

        if not scored:
            self._last_short_code_scores = {}
            return stock_codes if target_count is None else stock_codes[:target_count]

        scored.sort(key=lambda x: x[1], reverse=True)
        self._last_short_code_scores = {str(c): float(s) for c, s in scored}
        if target_count is None or int(target_count) <= 0 or int(target_count) >= len(scored):
            return [c for c, _ in scored]

        picked = [c for c, _ in scored[:max(30, min(int(target_count), len(scored)))]]
        return picked

    @staticmethod
    def _horizon_key(period_days):
        try:
            period = int(period_days)
        except Exception:
            period = 0
        return {5: 'short_term', 20: 'medium_term', 60: 'long_term'}.get(period, f'{period}d')

    def _horizon_reflection_path(self, period_days):
        return os.path.join('data', 'models', f"{self._horizon_key(period_days)}_training_reflection.json")

    @staticmethod
    def _merge_short_term_search_plan(candidate_params, experiment_grid, overrides=None):
        """将反思得到的额外搜索策略并入默认5日实验计划。"""
        if not overrides:
            return candidate_params, experiment_grid

        merged_params = list(candidate_params or [])
        merged_experiments = list(experiment_grid or [])

        seen_param_keys = {tuple(sorted((k, str(v)) for k, v in (p or {}).items())) for p in merged_params}
        for p in overrides.get('candidate_params', []) or []:
            key = tuple(sorted((k, str(v)) for k, v in (p or {}).items()))
            if key not in seen_param_keys:
                merged_params.append(dict(p))
                seen_param_keys.add(key)

        seen_exp_names = {str((exp or {}).get('name')) for exp in merged_experiments}
        for exp in overrides.get('experiment_grid', []) or []:
            name = str((exp or {}).get('name'))
            if name and name not in seen_exp_names:
                merged_experiments.append(dict(exp))
                seen_exp_names.add(name)

        preferred_names = [
            str(x) for x in (
                overrides.get('preferred_names')
                or [str((exp or {}).get('name')) for exp in (overrides.get('experiment_grid', []) or [])]
            ) if x
        ]
        if preferred_names:
            rank_map = {name: idx for idx, name in enumerate(preferred_names)}
            merged_experiments = sorted(
                merged_experiments,
                key=lambda exp: (rank_map.get(str((exp or {}).get('name')), 10_000), str((exp or {}).get('name')))
            )

        return merged_params, merged_experiments

    def _derive_short_term_strategy_overrides(self, reflection, round_idx=1):
        """根据上一轮复盘结果，自动生成下一轮5日优化策略。"""
        reflection = reflection or {}
        metrics = reflection.get('metrics') or {}
        prediction_bias = reflection.get('prediction_bias') or {}
        failure = reflection.get('failure_analysis') or {}

        acc = float(metrics.get('accuracy') or 0.0)
        auc = float(metrics.get('auc') or 0.0)
        fp_count = int(failure.get('false_positive_count') or 0)
        fn_count = int(failure.get('false_negative_count') or 0)
        fp_rate = float(prediction_bias.get('false_positive_rate') or 0.0)
        fn_rate = float(prediction_bias.get('false_negative_rate') or 0.0)

        notes = []
        preferred_names = []
        candidate_params = []
        experiment_grid = []

        penalty_map = self._build_code_penalty_map(failure)
        if penalty_map:
            notes.append('downweight_repeat_error_codes')

        segment_stats = failure.get('segment_stats') or {}
        vol_segments = segment_stats.get('volatility_bucket') or []
        high_vol_uplift = 0.0
        for seg in vol_segments:
            if str(seg.get('segment')) == 'high_vol':
                high_vol_uplift = float(seg.get('uplift_vs_baseline') or 0.0)
                break

        if fp_count > max(fn_count * 2, 50) or fp_rate >= 0.42:
            notes.append('reduce_false_positives')
            preferred_names.extend([f'fp_guard_q24_r{round_idx}', f'fp_guard_q26_r{round_idx}', 'event_adaptive_label_q18'])
            experiment_grid.extend([
                {
                    'name': f'fp_guard_q24_r{round_idx}',
                    'lookback_years': 0.9,
                    'neutral_zone': 0.012,
                    'decay_days': 45.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.24,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                },
                {
                    'name': f'fp_guard_q26_r{round_idx}',
                    'lookback_years': 0.8,
                    'neutral_zone': 0.014,
                    'decay_days': 40.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.26,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                },
            ])
            candidate_params.extend([
                {
                    'n_estimators': 180,
                    'max_depth': 2,
                    'learning_rate': 0.03,
                    'subsample': 0.92,
                    'colsample_bytree': 0.72,
                    'reg_lambda': 6.0,
                    'reg_alpha': 2.6,
                    'min_child_weight': 12,
                    'gamma': 0.8,
                    'max_delta_step': 2,
                    'eval_metric': 'auc',
                },
                {
                    'n_estimators': 240,
                    'max_depth': 3,
                    'learning_rate': 0.025,
                    'subsample': 0.88,
                    'colsample_bytree': 0.7,
                    'reg_lambda': 7.0,
                    'reg_alpha': 3.0,
                    'min_child_weight': 14,
                    'gamma': 1.0,
                    'max_delta_step': 3,
                    'eval_metric': 'auc',
                },
            ])

        if auc < MIN_SHORT_HORIZON_AUC or acc < MIN_MODEL_ACCURACY:
            notes.append('boost_short_signal_quality')
            preferred_names.extend([f'signal_focus_q20_r{round_idx}', f'signal_focus_q18_r{round_idx}'])
            experiment_grid.extend([
                {
                    'name': f'signal_focus_q20_r{round_idx}',
                    'lookback_years': 1.0,
                    'neutral_zone': 0.010,
                    'decay_days': 50.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.20,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                },
                {
                    'name': f'signal_focus_q18_r{round_idx}',
                    'lookback_years': 0.9,
                    'neutral_zone': 0.009,
                    'decay_days': 42.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.18,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                }
            ])
            candidate_params.extend([
                {
                    'n_estimators': 260,
                    'max_depth': 3,
                    'learning_rate': 0.028,
                    'subsample': 0.9,
                    'colsample_bytree': 0.72,
                    'reg_lambda': 6.5,
                    'reg_alpha': 2.2,
                    'min_child_weight': 10,
                    'gamma': 0.7,
                    'max_delta_step': 2,
                    'eval_metric': 'auc',
                }
            ])

        if high_vol_uplift >= 0.03:
            notes.append('control_high_volatility_noise')
            preferred_names.extend([f'volatility_guard_q24_r{round_idx}'])
            experiment_grid.extend([
                {
                    'name': f'volatility_guard_q24_r{round_idx}',
                    'lookback_years': 0.8,
                    'neutral_zone': 0.014,
                    'decay_days': 38.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.24,
                    'adaptive_label_zone': True,
                    'use_event_features': False,
                }
            ])
            candidate_params.extend([
                {
                    'n_estimators': 160,
                    'max_depth': 2,
                    'learning_rate': 0.028,
                    'subsample': 0.9,
                    'colsample_bytree': 0.68,
                    'reg_lambda': 7.5,
                    'reg_alpha': 3.2,
                    'min_child_weight': 16,
                    'gamma': 1.2,
                    'max_delta_step': 3,
                    'eval_metric': 'auc',
                }
            ])

        rsi_segments = segment_stats.get('rsi_bucket') or []
        extension_segments = segment_stats.get('price_extension_bucket') or []
        liquidity_segments = segment_stats.get('liquidity_bucket') or []
        overbought_uplift = max((float(x.get('uplift_vs_baseline') or 0.0) for x in rsi_segments if str(x.get('segment')) == 'overbought'), default=0.0)
        extended_uplift = max((float(x.get('uplift_vs_baseline') or 0.0) for x in extension_segments if str(x.get('segment')) == 'extended_above_ma20'), default=0.0)
        thin_volume_uplift = max((float(x.get('uplift_vs_baseline') or 0.0) for x in liquidity_segments if str(x.get('segment')) == 'thin_volume'), default=0.0)

        if overbought_uplift >= 0.03 or extended_uplift >= 0.03 or thin_volume_uplift >= 0.03:
            notes.append('avoid_chasing_extended_names')
            preferred_names.extend([f'extension_guard_q26_r{round_idx}', f'extension_guard_q28_r{round_idx}'])
            experiment_grid.extend([
                {
                    'name': f'extension_guard_q26_r{round_idx}',
                    'lookback_years': 0.75,
                    'neutral_zone': 0.016,
                    'decay_days': 35.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.26,
                    'adaptive_label_zone': True,
                    'use_event_features': False,
                },
                {
                    'name': f'extension_guard_q28_r{round_idx}',
                    'lookback_years': 0.7,
                    'neutral_zone': 0.018,
                    'decay_days': 32.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.28,
                    'adaptive_label_zone': True,
                    'use_event_features': False,
                }
            ])
            candidate_params.extend([
                {
                    'n_estimators': 180,
                    'max_depth': 2,
                    'learning_rate': 0.024,
                    'subsample': 0.9,
                    'colsample_bytree': 0.66,
                    'reg_lambda': 8.0,
                    'reg_alpha': 3.4,
                    'min_child_weight': 18,
                    'gamma': 1.4,
                    'max_delta_step': 3,
                    'eval_metric': 'auc',
                }
            ])

        if fn_rate >= 0.55 or fn_count > max(fp_count * 1.05, 50):
            notes.append('recover_missed_positives')
            preferred_names.extend([f'recall_recover_q18_r{round_idx}', f'recall_recover_q16_r{round_idx}'])
            experiment_grid.extend([
                {
                    'name': f'recall_recover_q18_r{round_idx}',
                    'lookback_years': 1.1,
                    'neutral_zone': 0.008,
                    'decay_days': 55.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.18,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                },
                {
                    'name': f'recall_recover_q16_r{round_idx}',
                    'lookback_years': 1.2,
                    'neutral_zone': 0.007,
                    'decay_days': 60.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.16,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                }
            ])
            candidate_params.extend([
                {
                    'n_estimators': 220,
                    'max_depth': 4,
                    'learning_rate': 0.032,
                    'subsample': 0.88,
                    'colsample_bytree': 0.78,
                    'reg_lambda': 4.5,
                    'reg_alpha': 1.4,
                    'min_child_weight': 7,
                    'gamma': 0.35,
                    'max_delta_step': 1,
                    'eval_metric': 'auc',
                }
            ])

        notes = list(dict.fromkeys(notes))
        preferred_names = list(dict.fromkeys(preferred_names))
        return {
            'notes': notes,
            'preferred_names': preferred_names,
            'candidate_params': candidate_params,
            'experiment_grid': experiment_grid,
            'penalty_map': penalty_map,
        }

    def _save_horizon_optimization_loop(self, period_days, target_accuracy, target_f1, max_rounds, history, status):
        """保存指定周期的自动反思与优化记录。"""
        try:
            path = os.path.join('data', 'models', f'{self._horizon_key(period_days)}_optimization_loop.json')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                'timestamp': datetime.now().isoformat(),
                'target_accuracy': float(target_accuracy),
                'target_f1': float(target_f1),
                'max_rounds': int(max_rounds),
                'status': status,
                'history': history or [],
            }
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存{period_days}日优化循环记录失败: {e}")

    def _save_short_term_optimization_loop(self, target_accuracy, target_f1, max_rounds, history, status):
        """保存5日模型按轮次自动反思与优化记录。"""
        self._save_horizon_optimization_loop(5, target_accuracy, target_f1, max_rounds, history, status)

    @staticmethod
    def _is_better_horizon_candidate(cur, prev):
        if prev is None:
            return True
        if bool(cur.get('passed')) != bool(prev.get('passed')):
            return bool(cur.get('passed'))
        cur_rank = float(cur.get('rank_score') or 0.0)
        prev_rank = float(prev.get('rank_score') or 0.0)
        if abs(cur_rank - prev_rank) >= 1e-6:
            return cur_rank > prev_rank
        cur_acc = float((cur.get('metrics') or {}).get('accuracy') or 0.0)
        prev_acc = float((prev.get('metrics') or {}).get('accuracy') or 0.0)
        return cur_acc > prev_acc

    def _derive_medium_term_strategy_overrides(self, reflection, round_idx=1):
        """根据上一轮20日复盘结果，自动生成下一轮优化策略。"""
        reflection = reflection or {}
        metrics = reflection.get('metrics') or {}
        data_profile = reflection.get('data_profile') or {}
        prediction_bias = reflection.get('prediction_bias') or {}
        failure_analysis = reflection.get('failure_analysis') or {}

        acc = float(metrics.get('accuracy') or 0.0)
        f1 = float(metrics.get('f1') or 0.0)
        auc = float(metrics.get('auc') or 0.0)
        train_pos = float(data_profile.get('train_pos_rate') or 0.0)
        val_pos = float(data_profile.get('val_pos_rate') or 0.0)
        drift = abs(train_pos - val_pos)
        fp_rate = float(prediction_bias.get('false_positive_rate') or 0.0)
        rate_gap = float(prediction_bias.get('rate_gap') or 0.0)

        notes = []
        candidate_params = []
        experiment_grid = []
        penalty_map = self._build_code_penalty_map(failure_analysis)
        if penalty_map:
            notes.append('downweight_repeat_error_codes')

        if drift >= 0.08:
            notes.append('rebalance_regime_drift')
            experiment_grid.append({
                'name': f'mt_recent_focus_r{round_idx}',
                'lookback_years': 1.2,
                'neutral_zone': 0.006,
                'decay_days': 75.0,
            })

        if f1 < float(MIN_MODEL_F1_SCORE):
            notes.append('improve_class_balance')
            experiment_grid.append({
                'name': f'mt_balance_guard_r{round_idx}',
                'lookback_years': 1.5,
                'neutral_zone': 0.008,
                'decay_days': 90.0,
            })
            candidate_params.append({
                'n_estimators': 180,
                'max_depth': 3,
                'learning_rate': 0.035,
                'subsample': 0.88,
                'colsample_bytree': 0.8,
                'reg_lambda': 3.5,
                'reg_alpha': 1.0,
            })

        if fp_rate >= 0.40 or rate_gap >= 0.14:
            notes.append('reduce_false_positives')
            experiment_grid.append({
                'name': f'mt_fp_guard_r{round_idx}',
                'lookback_years': 1.0,
                'neutral_zone': 0.012,
                'decay_days': 60.0,
            })
            candidate_params.append({
                'n_estimators': 160,
                'max_depth': 2,
                'learning_rate': 0.03,
                'subsample': 0.92,
                'colsample_bytree': 0.72,
                'reg_lambda': 5.0,
                'reg_alpha': 1.8,
                'min_child_weight': 10,
                'gamma': 0.6,
                'max_delta_step': 2,
            })

        if auc < 0.60 or acc < float(MIN_MODEL_ACCURACY):
            notes.append('tighten_recent_weighting')
            experiment_grid.append({
                'name': f'mt_signal_refine_r{round_idx}',
                'lookback_years': 1.0,
                'neutral_zone': 0.01,
                'decay_days': 65.0,
            })
            candidate_params.append({
                'n_estimators': 220,
                'max_depth': 2,
                'learning_rate': 0.03,
                'subsample': 0.9,
                'colsample_bytree': 0.75,
                'reg_lambda': 4.2,
                'reg_alpha': 1.4,
            })

        if not experiment_grid:
            experiment_grid.append({
                'name': f'mt_default_r{round_idx}',
                'lookback_years': 1.8,
                'neutral_zone': 0.004,
                'decay_days': 105.0,
            })

        if not candidate_params:
            candidate_params.append({
                'n_estimators': 160,
                'max_depth': 3,
                'learning_rate': 0.04,
                'subsample': 0.9,
                'colsample_bytree': 0.85,
                'reg_lambda': 3.0,
                'reg_alpha': 0.8,
            })

        return {
            'notes': list(dict.fromkeys(notes)),
            'candidate_params': candidate_params,
            'experiment_grid': experiment_grid,
            'penalty_map': penalty_map,
        }

    @staticmethod
    def _build_code_penalty_map(failure_analysis=None):
        """根据上一轮误判最严重的标的生成降权映射。"""
        failure_analysis = failure_analysis or {}
        segment_stats = failure_analysis.get('segment_stats') or {}
        top_error_codes = segment_stats.get('top_error_codes') or []

        penalty_map = {}
        for item in top_error_codes:
            code = str(item.get('code') or '').strip()
            if not code:
                continue
            err = float(item.get('error_rate') or 0.0)
            cnt = int(item.get('count') or 0)
            if cnt < 8 or err < 0.55:
                continue

            penalty = 1.0 - ((err - 0.55) * 1.4) - min(0.20, cnt / 160.0)
            penalty_map[code] = float(np.clip(penalty, 0.35, 0.92))

        return penalty_map

    @staticmethod
    def _filter_codes_by_penalty(codes, penalty_map=None, min_penalty=0.5, max_drop_ratio=0.08):
        """在后续轮次过滤最差的重复高误判标的，降低训练噪声。"""
        codes = [str(c) for c in (codes or [])]
        penalty_map = penalty_map or {}
        if not codes or not penalty_map:
            return codes

        flagged = [c for c in codes if float(penalty_map.get(c, 1.0)) < float(min_penalty)]
        if not flagged:
            return codes

        drop_cap = int(np.ceil(len(codes) * float(max_drop_ratio)))
        drop_cap = max(1, drop_cap)
        ranked = sorted(flagged, key=lambda c: (float(penalty_map.get(c, 1.0)), c))
        to_drop = set(ranked[:min(len(ranked), drop_cap)])

        kept = [c for c in codes if c not in to_drop]
        return kept if kept else codes

    @staticmethod
    def _build_code_quality_weights(codes, score_map=None, penalty_map=None):
        """根据标的稳定性/可预测性得分与历史误判记录构建样本权重。"""
        codes = [str(c) for c in (codes or [])]
        score_map = score_map or {}
        penalty_map = penalty_map or {}
        if not codes:
            return {}

        raw_scores = np.asarray([float(score_map.get(c, 0.0)) for c in codes], dtype=float)
        if len(raw_scores) == 0:
            return {c: float(np.clip(penalty_map.get(c, 1.0), 0.35, 1.2)) for c in codes}

        lo = float(np.min(raw_scores))
        hi = float(np.max(raw_scores))
        denom = max(hi - lo, 1e-6)

        weights = {}
        for code in codes:
            score = float(score_map.get(code, lo))
            norm = np.clip((score - lo) / denom, 0.0, 1.0)
            base_w = float(0.75 + (0.75 * norm))
            penalty = float(np.clip(penalty_map.get(code, 1.0), 0.35, 1.2))
            weights[code] = float(np.clip(base_w * penalty, 0.25, 1.6))
        return weights

    def _analyze_failure_factors(self, meta_eval, y_true, y_pred, y_proba=None, period_days=None):
        """分析误判集中出现在哪些因子分组下。"""
        y_true_arr = np.asarray(y_true).astype(int)
        y_pred_arr = np.asarray(y_pred).astype(int)
        n = int(min(len(y_true_arr), len(y_pred_arr)))
        error_count = int((y_true_arr[:n] != y_pred_arr[:n]).sum()) if n > 0 else 0

        base_result = {
            'period_days': int(period_days or 0),
            'sample_count': n,
            'error_count': error_count,
            'false_positive_count': int(((y_true_arr[:n] == 0) & (y_pred_arr[:n] == 1)).sum()) if n > 0 else 0,
            'false_negative_count': int(((y_true_arr[:n] == 1) & (y_pred_arr[:n] == 0)).sum()) if n > 0 else 0,
            'baseline_error_rate': round(float(error_count / max(n, 1)), 6),
            'dominant_factors': [],
            'segment_stats': {},
        }
        if meta_eval is None or n == 0:
            return base_result

        try:
            meta = meta_eval.reset_index(drop=True).iloc[:n].copy()
            meta['is_error'] = (y_true_arr[:n] != y_pred_arr[:n]).astype(int)

            if y_proba is not None:
                p = np.asarray(y_proba).astype(float)[:n]
                meta['error_confidence'] = np.where(meta['is_error'].values == 1, np.abs(p - 0.5) * 2.0, 0.0)

            baseline = float(meta['is_error'].mean()) if len(meta) > 0 else 0.0
            dominant = []
            segment_stats = {}

            def _record_segment(name, values):
                seg_df = pd.DataFrame({'segment': values.astype(str), 'is_error': meta['is_error']})
                stats = seg_df.groupby('segment').agg(count=('is_error', 'size'), error_rate=('is_error', 'mean')).reset_index()
                stats = stats.sort_values(['error_rate', 'count'], ascending=[False, False])
                segment_stats[name] = [
                    {
                        'segment': str(row['segment']),
                        'count': int(row['count']),
                        'error_rate': round(float(row['error_rate']), 6),
                        'uplift_vs_baseline': round(float(row['error_rate'] - baseline), 6),
                    }
                    for _, row in stats.iterrows()
                ]
                for _, row in stats.iterrows():
                    uplift = float(row['error_rate'] - baseline)
                    if int(row['count']) >= max(1, min(30, n // 20 if n >= 20 else 1)) and uplift >= 0.03:
                        dominant.append(f"{name}={row['segment']} (error_rate={float(row['error_rate']):.2%}, n={int(row['count'])})")

            if 'sentiment' in meta.columns:
                sentiment_bucket = pd.Series(np.where(meta['sentiment'] >= 0.2, 'positive', np.where(meta['sentiment'] <= -0.2, 'negative', 'neutral')))
                _record_segment('sentiment_bucket', sentiment_bucket)
            if 'macro_regime_score' in meta.columns:
                macro_bucket = pd.Series(np.where(meta['macro_regime_score'] >= 0.25, 'risk_on', np.where(meta['macro_regime_score'] <= -0.25, 'risk_off', 'balanced')))
                _record_segment('macro_bucket', macro_bucket)
            if 'volatility' in meta.columns:
                vol = pd.to_numeric(meta['volatility'], errors='coerce').fillna(0.0)
                q_hi = float(vol.quantile(0.67)) if len(vol) > 0 else 0.0
                q_lo = float(vol.quantile(0.33)) if len(vol) > 0 else 0.0
                volatility_bucket = pd.Series(np.where(vol >= q_hi, 'high_vol', np.where(vol <= q_lo, 'low_vol', 'mid_vol')))
                _record_segment('volatility_bucket', volatility_bucket)
            if 'rsi' in meta.columns:
                rsi = pd.to_numeric(meta['rsi'], errors='coerce').fillna(50.0)
                rsi_bucket = pd.Series(np.where(rsi >= 70, 'overbought', np.where(rsi <= 30, 'oversold', 'neutral_rsi')))
                _record_segment('rsi_bucket', rsi_bucket)
            if 'volume_ratio' in meta.columns:
                vr = pd.to_numeric(meta['volume_ratio'], errors='coerce').fillna(1.0)
                liquidity_bucket = pd.Series(np.where(vr >= 1.5, 'surge_volume', np.where(vr <= 0.8, 'thin_volume', 'normal_volume')))
                _record_segment('liquidity_bucket', liquidity_bucket)
            if 'price_ma20_ratio' in meta.columns:
                ma20r = pd.to_numeric(meta['price_ma20_ratio'], errors='coerce').fillna(0.0)
                extension_bucket = pd.Series(np.where(ma20r >= 0.05, 'extended_above_ma20', np.where(ma20r <= -0.05, 'below_ma20', 'near_ma20')))
                _record_segment('price_extension_bucket', extension_bucket)
            if 'has_report' in meta.columns:
                _record_segment('has_report', meta['has_report'].fillna(0).astype(int))
            if 'has_top_list' in meta.columns:
                _record_segment('has_top_list', meta['has_top_list'].fillna(0).astype(int))

            if 'code' in meta.columns:
                code_stats = meta.groupby(meta['code'].astype(str)).agg(count=('is_error', 'size'), error_rate=('is_error', 'mean')).reset_index()
                code_stats = code_stats.sort_values(['error_rate', 'count'], ascending=[False, False]).head(10)
                segment_stats['top_error_codes'] = [
                    {
                        'code': str(row['code']),
                        'count': int(row['count']),
                        'error_rate': round(float(row['error_rate']), 6),
                    }
                    for _, row in code_stats.iterrows()
                ]

            base_result['dominant_factors'] = dominant[:8]
            base_result['segment_stats'] = segment_stats
            return base_result
        except Exception:
            return base_result

    def _short_term_reflection_path(self):
        return self._horizon_reflection_path(5)

    def _save_horizon_reflection(self, period_days, entry):
        """持久化分周期训练反思记录，用于后续复盘与迭代。"""
        path = self._horizon_reflection_path(period_days)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        payload = {'history': []}
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                if not isinstance(payload, dict):
                    payload = {'history': []}
            except Exception:
                payload = {'history': []}

        history = payload.get('history', [])
        if not isinstance(history, list):
            history = []

        prev = history[-1] if history else None
        if prev and isinstance(prev, dict):
            prev_m = prev.get('metrics') or {}
            cur_m = entry.get('metrics') or {}
            delta = {}
            for k in ['accuracy', 'f1', 'auc', 'brier']:
                if k in prev_m and k in cur_m:
                    try:
                        delta[k] = round(float(cur_m[k]) - float(prev_m[k]), 6)
                    except Exception:
                        continue
            if delta:
                entry['delta_vs_prev'] = delta
                entry['delta_vs_previous'] = delta

        history.append(entry)
        payload['history'] = history[-50:]
        payload['latest'] = entry

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _save_short_term_reflection(self, entry):
        self._save_horizon_reflection(5, entry)

    def _build_horizon_reflection(self, period_days, passed, gate_name, eval_metrics, data_profile=None, experiment=None):
        """构建分周期训练诊断结论：失败归因、漂移识别与改进行动。"""
        data_profile = data_profile or {}
        experiment = experiment or {'name': 'default'}

        acc = float(eval_metrics.get('accuracy') or 0.0)
        f1 = float(eval_metrics.get('f1') or 0.0)
        auc = float(eval_metrics.get('auc') or 0.0)
        brier = float(eval_metrics.get('brier') or 1.0)

        reasons = []
        actions = []

        if acc < MIN_MODEL_ACCURACY:
            reasons.append(f"方向准确率不足: accuracy={acc:.4f} < {MIN_MODEL_ACCURACY}")
            actions.append("降低模型复杂度并强化近期样本权重，减少过拟合")
        if f1 < MIN_MODEL_F1_SCORE:
            reasons.append(f"正负样本识别平衡不足: f1={f1:.4f} < {MIN_MODEL_F1_SCORE}")
            actions.append("优化标签阈值与类别权重，避免单侧预测")
        if int(period_days) <= 5 and auc < MIN_SHORT_HORIZON_AUC:
            reasons.append(f"排序能力不足: AUC={auc:.4f} < {MIN_SHORT_HORIZON_AUC}")
            actions.append("增强短周期状态特征与分层训练，减少横截面噪声")
        if int(period_days) <= 5 and brier > MAX_SHORT_HORIZON_BRIER:
            reasons.append(f"概率校准不足: Brier={brier:.4f} > {MAX_SHORT_HORIZON_BRIER}")
            actions.append("继续优化概率校准与阈值选择")

        pos_rate = float(data_profile.get('pos_rate') or 0.0)
        if pos_rate < 0.35 or pos_rate > 0.65:
            reasons.append(f"标签分布偏斜: pos_rate={pos_rate:.4f}")
            actions.append("调整中性区与采样策略，保持标签分布稳定")

        train_pos_rate = data_profile.get('train_pos_rate')
        val_pos_rate = data_profile.get('val_pos_rate')
        try:
            if train_pos_rate is not None and val_pos_rate is not None:
                drift = abs(float(train_pos_rate) - float(val_pos_rate))
                if drift >= 0.08:
                    reasons.append(f"市场状态漂移明显: train_pos_rate={float(train_pos_rate):.4f}, val_pos_rate={float(val_pos_rate):.4f}")
                    actions.append("按市场状态分桶训练，并提升时间衰减权重")
        except Exception:
            pass

        lessons = []
        if passed:
            lessons.append(f"有效实验配置: {experiment.get('name', 'default')}")
            lessons.append(f"有效门槛路径: {gate_name}")
            if acc >= MIN_MODEL_ACCURACY:
                lessons.append("方向预测已达到基础上线阈值")

        if not reasons and not passed:
            reasons.append(f"{period_days}日模型综合验证未通过")
            actions.append("继续补强周期匹配特征并复验")

        # 去重并保持顺序
        actions = list(dict.fromkeys(actions))
        reasons = list(dict.fromkeys(reasons))

        return {
            'timestamp': datetime.now().isoformat(),
            'period_days': int(period_days),
            'passed': bool(passed),
            'gate': gate_name,
            'metrics': {
                'accuracy': round(acc, 6),
                'f1': round(f1, 6),
                'auc': round(auc, 6) if eval_metrics.get('auc') is not None else None,
                'brier': round(brier, 6) if eval_metrics.get('brier') is not None else None,
            },
            'experiment': experiment,
            'data_profile': data_profile,
            'reasons': reasons,
            'actions': actions,
            'lessons': lessons,
        }

    def _build_short_term_reflection(self, passed, gate_name, eval_metrics, experiment, data_profile):
        reflection = self._build_horizon_reflection(
            period_days=5,
            passed=passed,
            gate_name=gate_name,
            eval_metrics=eval_metrics,
            data_profile=data_profile,
            experiment=experiment,
        )
        return reflection

    @staticmethod
    def _extract_top_features(model, feature_names, topn=8):
        """提取模型Top特征重要性。"""
        try:
            if model is None or not hasattr(model, 'feature_importances_'):
                return []
            imps = np.asarray(model.feature_importances_).astype(float)
            if feature_names is None or len(feature_names) != len(imps):
                feature_names = [f'f{i}' for i in range(len(imps))]

            pairs = []
            for i, name in enumerate(feature_names):
                pairs.append((str(name), float(imps[i])))
            pairs.sort(key=lambda x: x[1], reverse=True)

            top = pairs[:max(1, int(topn))]
            total = sum(v for _, v in top) or 1.0
            return [
                {
                    'name': n,
                    'importance': round(v, 6),
                    'share': round(v / total, 6)
                }
                for n, v in top
            ]
        except Exception:
            return []

    @staticmethod
    def _build_pruned_feature_columns(model, feature_names, keep_topn=22):
        """基于重要性构建裁剪特征列。"""
        try:
            if model is None or not hasattr(model, 'feature_importances_'):
                return list(feature_names)

            imps = np.asarray(model.feature_importances_).astype(float)
            names = list(feature_names)
            if len(names) != len(imps) or len(names) <= keep_topn:
                return names

            pairs = [(names[i], float(imps[i])) for i in range(len(names))]
            pairs.sort(key=lambda x: x[1], reverse=True)

            selected = [n for n, _ in pairs[:keep_topn]]
            # 保留稳定核心特征，避免剪掉基础结构信号
            for core in ['rsi', 'macd_hist', 'price_ma20_ratio', 'volatility', 'momentum_5d']:
                if core in names and core not in selected:
                    selected.append(core)

            return [c for c in names if c in set(selected)]
        except Exception:
            return list(feature_names)

    @staticmethod
    def _build_interaction_variant(X_train, X_val, top_feature_names, max_pairs=4):
        """基于高重要性特征构造轻量交互特征，控制复杂度避免过拟合。"""
        try:
            if X_train is None or X_val is None or len(X_train.columns) < 4:
                return None, None, None

            names = []
            seen = set()
            for n in (top_feature_names or []):
                s = str(n)
                if s in X_train.columns and s not in seen:
                    names.append(s)
                    seen.add(s)

            for core in ['rsi', 'macd_hist', 'momentum_5d', 'price_ma20_ratio', 'volatility', 'volume_ratio']:
                if core in X_train.columns and core not in seen:
                    names.append(core)
                    seen.add(core)

            if len(names) < 3:
                return None, None, None

            names = names[:6]
            X_tr_i = X_train.copy()
            X_va_i = X_val.copy()

            created = []
            pair_candidates = []
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    pair_candidates.append((names[i], names[j]))

            for a, b in pair_candidates[:max_pairs]:
                col = f'int_{a}__{b}'
                X_tr_i[col] = pd.to_numeric(X_tr_i[a], errors='coerce').fillna(0.0) * pd.to_numeric(X_tr_i[b], errors='coerce').fillna(0.0)
                X_va_i[col] = pd.to_numeric(X_va_i[a], errors='coerce').fillna(0.0) * pd.to_numeric(X_va_i[b], errors='coerce').fillna(0.0)
                created.append(col)

            if len(created) < 2:
                return None, None, None

            return X_tr_i, X_va_i, list(X_tr_i.columns)
        except Exception:
            return None, None, None

    def _train_short_term_regime_models(self, X_train, y_train, sample_weight, model_params):
        """训练高/低波动分层子模型，返回 (models, volatility_split)。"""
        if X_train is None or len(X_train) < 400 or 'volatility' not in X_train.columns:
            return {}, None

        vol_split = float(np.nanmedian(X_train['volatility'].values.astype(float)))
        mask_low = X_train['volatility'] <= vol_split
        mask_high = ~mask_low

        regime_models = {}
        for key, mask in [('low_vol', mask_low), ('high_vol', mask_high)]:
            X_sub = X_train[mask.values]
            y_sub = y_train[mask.values]
            if len(X_sub) < 200 or len(np.unique(y_sub)) < 2:
                continue

            sub_weight = None
            if sample_weight is not None and len(sample_weight) == len(X_train):
                try:
                    sub_weight = np.asarray(sample_weight)[mask.values]
                except Exception:
                    sub_weight = None

            sub_predictor = ShortTermPredictor()
            score = sub_predictor.train(X_sub, y_sub, sample_weight=sub_weight, model_params=model_params)
            if score > 0 and sub_predictor.model is not None:
                regime_models[key] = sub_predictor.model

        return regime_models, vol_split

    @staticmethod
    def _predict_model_up_proba(model, X):
        """统一将模型输出映射为上涨概率(0-1)，兼容classifier/ranker。"""
        if model is None or X is None or len(X) == 0:
            return None
        try:
            if hasattr(model, 'predict_proba'):
                return np.asarray(model.predict_proba(X)[:, 1], dtype=float)

            raw = np.asarray(model.predict(X), dtype=float).reshape(-1)
            if raw.size == 0:
                return None

            if float(np.nanmin(raw)) >= 0.0 and float(np.nanmax(raw)) <= 1.0:
                p = raw
            else:
                z = np.clip(raw, -20.0, 20.0)
                p = 1.0 / (1.0 + np.exp(-z))
            return np.clip(p, 1e-6, 1 - 1e-6)
        except Exception:
            return None

    def _train_short_term_ranker(self, X_train, y_train, train_dates, sample_weight, model_params):
        """训练XGBoost排序模型，用于提升短周期AUC排序能力。"""
        try:
            import xgboost as xgb

            if X_train is None or len(X_train) < 250 or train_dates is None:
                return None, 0.0

            dt = pd.to_datetime(train_dates, errors='coerce')
            if dt.isna().all() or len(dt) != len(X_train):
                return None, 0.0

            ord_idx = np.argsort(dt.values.astype('datetime64[ns]'))
            X_ord = X_train.iloc[ord_idx]
            y_arr = np.asarray(y_train).astype(int)
            y_ord = y_arr[ord_idx]
            d_ord = dt.iloc[ord_idx].dt.strftime('%Y-%m-%d').tolist()

            group = []
            if d_ord:
                c = 1
                for i in range(1, len(d_ord)):
                    if d_ord[i] == d_ord[i - 1]:
                        c += 1
                    else:
                        group.append(c)
                        c = 1
                group.append(c)

            if len(group) < 20:
                return None, 0.0

            params = {
                'n_estimators': 220,
                'max_depth': 4,
                'learning_rate': 0.04,
                'subsample': 0.85,
                'colsample_bytree': 0.8,
                'random_state': 42,
                'objective': 'rank:pairwise',
                'eval_metric': 'auc',
            }
            if model_params:
                for k, v in model_params.items():
                    if k in ['scale_pos_weight', 'max_delta_step']:
                        continue
                    params[k] = v

            ranker = xgb.XGBRanker(**params)
            # 注意: 当前xgboost版本在rank任务下要求group级权重，
            # 逐样本sample_weight会触发维度不匹配错误，因此此路径不传sample_weight。
            ranker.fit(X_ord, y_ord, group=group)

            p_train = self._predict_model_up_proba(ranker, X_ord)
            if p_train is None or len(np.unique(y_ord)) < 2:
                return ranker, 0.0
            auc_train = float(roc_auc_score(y_ord, p_train))
            return ranker, auc_train
        except Exception as e:
            logger.warning(f"5日排序模型训练失败: {e}")
            return None, 0.0

    @staticmethod
    def _is_better_short_candidate(cur, prev):
        """5日候选比较: 优先选择综合表现更均衡、误报更少的方案。"""
        if prev is None:
            return True

        if bool(cur.get('passed')) != bool(prev.get('passed')):
            return bool(cur.get('passed'))

        cur_rank = float(cur.get('rank_score') or 0.0)
        prev_rank = float(prev.get('rank_score') or 0.0)
        if abs(cur_rank - prev_rank) >= 1e-6:
            return cur_rank > prev_rank

        cur_m = cur.get('metrics') or {}
        prev_m = prev.get('metrics') or {}

        cur_acc = float(cur_m.get('accuracy') or 0.0)
        prev_acc = float(prev_m.get('accuracy') or 0.0)
        if abs(cur_acc - prev_acc) >= 0.002:
            return cur_acc > prev_acc

        cur_auc = float(cur_m.get('auc') or 0.0)
        prev_auc = float(prev_m.get('auc') or 0.0)
        if abs(cur_auc - prev_auc) >= 0.002:
            return cur_auc > prev_auc

        cur_brier = float(cur_m.get('brier') or 1.0)
        prev_brier = float(prev_m.get('brier') or 1.0)
        if abs(cur_brier - prev_brier) >= 0.001:
            return cur_brier < prev_brier

        return False

    def _predict_short_term_with_regime(self, base_model, regime_models, vol_split, X_val):
        """按波动率路由预测验证集概率。"""
        if base_model is None or X_val is None or len(X_val) == 0:
            return None

        base_proba = self._predict_model_up_proba(base_model, X_val)
        if base_proba is None:
            return None
        if not regime_models or vol_split is None or 'volatility' not in X_val.columns:
            return base_proba

        proba = np.array(base_proba, dtype=float)
        v = X_val['volatility'].values.astype(float)

        low_model = regime_models.get('low_vol')
        if low_model is not None:
            mask_low = v <= float(vol_split)
            if mask_low.any():
                p_low = self._predict_model_up_proba(low_model, X_val.loc[mask_low])
                if p_low is not None:
                    proba[mask_low] = p_low

        high_model = regime_models.get('high_vol')
        if high_model is not None:
            mask_high = v > float(vol_split)
            if mask_high.any():
                p_high = self._predict_model_up_proba(high_model, X_val.loc[mask_high])
                if p_high is not None:
                    proba[mask_high] = p_high

        return proba
    
    def _get_short_term_search_plan(self, full_data_mode=False, overrides=None):
        """返回5日模型的参数与实验计划。"""
        candidate_params = [
            {'n_estimators': 120, 'max_depth': 3, 'learning_rate': 0.06, 'subsample': 0.9, 'colsample_bytree': 0.9, 'reg_lambda': 2.0, 'reg_alpha': 0.8},
            {'n_estimators': 180, 'max_depth': 4, 'learning_rate': 0.04, 'subsample': 0.85, 'colsample_bytree': 0.85, 'reg_lambda': 2.5, 'reg_alpha': 1.0},
            {'n_estimators': 220, 'max_depth': 5, 'learning_rate': 0.03, 'subsample': 0.8, 'colsample_bytree': 0.8, 'reg_lambda': 3.0, 'reg_alpha': 1.2},
            {'n_estimators': 140, 'max_depth': 3, 'learning_rate': 0.08, 'subsample': 0.95, 'colsample_bytree': 0.85, 'reg_lambda': 1.5, 'reg_alpha': 0.5},
            {'n_estimators': 260, 'max_depth': 2, 'learning_rate': 0.03, 'subsample': 0.9, 'colsample_bytree': 0.7, 'reg_lambda': 4.0, 'reg_alpha': 1.5, 'min_child_weight': 8, 'gamma': 0.3},
            {'n_estimators': 320, 'max_depth': 3, 'learning_rate': 0.025, 'subsample': 0.85, 'colsample_bytree': 0.75, 'reg_lambda': 5.0, 'reg_alpha': 2.0, 'min_child_weight': 10, 'gamma': 0.5},
            {'n_estimators': 180, 'max_depth': 4, 'learning_rate': 0.03, 'subsample': 0.8, 'colsample_bytree': 0.8, 'reg_lambda': 2.0, 'reg_alpha': 0.6, 'min_child_weight': 6, 'gamma': 0.2},
            {'n_estimators': 260, 'max_depth': 4, 'learning_rate': 0.025, 'subsample': 0.9, 'colsample_bytree': 0.75, 'reg_lambda': 5.0, 'reg_alpha': 2.0, 'min_child_weight': 9, 'gamma': 0.6, 'max_delta_step': 2, 'eval_metric': 'auc'},
            {'n_estimators': 340, 'max_depth': 2, 'learning_rate': 0.02, 'subsample': 0.92, 'colsample_bytree': 0.7, 'reg_lambda': 6.0, 'reg_alpha': 2.4, 'min_child_weight': 12, 'gamma': 0.7, 'eval_metric': 'auc'},
            {'n_estimators': 220, 'max_depth': 3, 'learning_rate': 0.03, 'subsample': 0.88, 'colsample_bytree': 0.78, 'reg_lambda': 4.5, 'reg_alpha': 1.8, 'min_child_weight': 8, 'gamma': 0.4, 'max_delta_step': 1, 'eval_metric': 'auc'},
        ]

        experiment_grid = [
            {'name': 'base_3y', 'lookback_years': 3.0, 'neutral_zone': 0.0, 'decay_days': 90.0, 'adaptive_band': False},
            {'name': 'recent_2y', 'lookback_years': 2.0, 'neutral_zone': 0.0, 'decay_days': 75.0, 'adaptive_band': False},
            {'name': 'recent_1y', 'lookback_years': 1.0, 'neutral_zone': 0.0, 'decay_days': 60.0, 'adaptive_band': False},
            {'name': 'neutral_0.4pct', 'lookback_years': 2.0, 'neutral_zone': 0.004, 'decay_days': 75.0, 'adaptive_band': False},
            {'name': 'neutral_0.6pct', 'lookback_years': 2.0, 'neutral_zone': 0.006, 'decay_days': 75.0, 'adaptive_band': False},
            {'name': 'neutral_0.8pct_q18', 'lookback_years': 1.5, 'neutral_zone': 0.008, 'decay_days': 65.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18},
            {'name': 'neutral_0.8pct_q20', 'lookback_years': 1.5, 'neutral_zone': 0.008, 'decay_days': 65.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.20},
            {'name': 'neutral_0.8pct_q22', 'lookback_years': 1.5, 'neutral_zone': 0.008, 'decay_days': 65.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.22},
            {'name': 'neutral_0.8pct_q24', 'lookback_years': 1.5, 'neutral_zone': 0.008, 'decay_days': 65.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.24},
            {'name': 'neutral_1.0pct_q18', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18},
            {'name': 'neutral_1.0pct_q20', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.20},
            {'name': 'neutral_1.0pct_q22', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.22},
            {'name': 'neutral_1.0pct_q24', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.24},
            {'name': 'neutral_1.0pct_q18_l14', 'lookback_years': 1.4, 'neutral_zone': 0.010, 'decay_days': 60.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18},
            {'name': 'adaptive_label_1.0pct_q18', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18, 'adaptive_label_zone': True},
            {'name': 'adaptive_label_1.0pct_q20', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.20, 'adaptive_label_zone': True},
            {'name': 'adaptive_label_1.0pct_q22', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.22, 'adaptive_label_zone': True},
            {'name': 'adaptive_label_1.0pct_q24', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.24, 'adaptive_label_zone': True},
            {'name': 'adaptive_label_1.0pct_q18_l14', 'lookback_years': 1.4, 'neutral_zone': 0.010, 'decay_days': 60.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18, 'adaptive_label_zone': True},
            {'name': 'event_enhanced_q18', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18, 'use_event_features': True},
            {'name': 'event_adaptive_label_q18', 'lookback_years': 1.2, 'neutral_zone': 0.010, 'decay_days': 55.0, 'adaptive_band': True, 'adaptive_band_quantile': 0.18, 'adaptive_label_zone': True, 'use_event_features': True},
        ]

        candidate_params, experiment_grid = self._merge_short_term_search_plan(
            candidate_params,
            experiment_grid,
            overrides=overrides,
        )

        if full_data_mode:
            picked_params = []
            seen_param_keys = set()
            for p in ((overrides or {}).get('candidate_params') or []):
                key = tuple(sorted((k, str(v)) for k, v in (p or {}).items()))
                if key not in seen_param_keys:
                    picked_params.append(dict(p))
                    seen_param_keys.add(key)
            for idx in [0, 4, 8]:
                if idx < len(candidate_params):
                    p = candidate_params[idx]
                    key = tuple(sorted((k, str(v)) for k, v in (p or {}).items()))
                    if key not in seen_param_keys:
                        picked_params.append(dict(p))
                        seen_param_keys.add(key)
            candidate_params = picked_params[:4] if picked_params else candidate_params[:3]

            preferred_names = [
                str(x) for x in (
                    ((overrides or {}).get('preferred_names') or [])
                    or [str((exp or {}).get('name')) for exp in (((overrides or {}).get('experiment_grid') or []))]
                ) if x
            ]
            if preferred_names:
                filtered = [exp for exp in experiment_grid if str(exp.get('name')) in set(preferred_names)]
                if filtered:
                    experiment_grid = filtered[:3]
                else:
                    experiment_grid = experiment_grid[:2]
            else:
                experiment_grid = [exp for exp in experiment_grid if exp.get('name') == 'event_adaptive_label_q18']

        return candidate_params, experiment_grid

    def train_short_term_model(self, stock_codes=None, plan_override=None, round_idx=None, max_rounds=None):
        """
        训练5日预测模型
        Args:
            stock_codes: 股票代码列表
        Returns:
            float: 训练准确率
        """
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        logger.info("开始训练5日预测模型...")
        if round_idx is not None and max_rounds is not None:
            logger.info(f"5日训练-反思优化轮次: {int(round_idx)}/{int(max_rounds)}")
        if plan_override and plan_override.get('notes'):
            logger.info(f"5日反思优化策略: {', '.join(plan_override.get('notes') or [])}")

        selected_codes = self._select_short_term_codes(stock_codes, target_count=len(stock_codes))
        penalty_map = (plan_override or {}).get('penalty_map') or {}
        before_penalty_count = len(selected_codes)
        if penalty_map:
            selected_codes = self._filter_codes_by_penalty(
                selected_codes,
                penalty_map=penalty_map,
                min_penalty=0.5,
                max_drop_ratio=0.08 if len(selected_codes) >= 1000 else 0.18,
            )
        code_quality_map = self._build_code_quality_weights(
            selected_codes,
            self._last_short_code_scores,
            penalty_map=penalty_map,
        )
        logger.info(f"5日训练标的筛选: 原始代码池={len(stock_codes)}，具备历史数据并可训练={len(selected_codes)}")
        if penalty_map and before_penalty_count != len(selected_codes):
            logger.info(f"5日训练降噪过滤: 基于复盘剔除高误判标的 {before_penalty_count - len(selected_codes)} 只")

        full_data_mode = len(selected_codes) >= 1000
        if full_data_mode:
            logger.info(
                f"5日模型启用全量数据快速搜索模式: selected_codes={len(selected_codes)}，使用单实验快速验证路径"
            )

        candidate_params, experiment_grid = self._get_short_term_search_plan(
            full_data_mode=full_data_mode,
            overrides=plan_override,
        )

        best_candidate = None
        best_data_profile = None
        tried_count = 0

        total_experiments = len(experiment_grid)
        for exp_idx, exp in enumerate(experiment_grid, start=1):
            logger.info(f"5日实验开始: {exp['name']} ({exp_idx}/{total_experiments})")
            X, y, meta_df = self.prepare_training_data(
                selected_codes,
                period_days=5,
                lookback_years=exp['lookback_years'],
                neutral_zone=exp['neutral_zone'],
                adaptive_label_zone=bool(exp.get('adaptive_label_zone', False)),
                use_event_features=bool(exp.get('use_event_features', False)),
                return_meta=True,
                progress_label=f"{exp['name']} {exp_idx}/{total_experiments}"
            )

            if X is None or len(X) < 200:
                logger.warning(f"5日实验 {exp['name']} 样本不足，跳过")
                continue

            data_profile = {
                'samples': int(len(y)),
                'pos_rate': round(float(y.mean()), 6),
                'selected_codes': int(len(selected_codes)),
                'adaptive_label_zone': bool(exp.get('adaptive_label_zone', False)),
                'use_event_features': bool(exp.get('use_event_features', False)),
                'date_min': str(meta_df['asof_date'].min()) if meta_df is not None and not meta_df.empty else None,
                'date_max': str(meta_df['asof_date'].max()) if meta_df is not None and not meta_df.empty else None,
            }

            if meta_df is not None and not meta_df.empty:
                macro_coverage = {}
                for col in ['cpi_yoy', 'pmi', 'shibor_1m', 'macro_regime_score']:
                    if col in meta_df.columns:
                        arr = pd.to_numeric(meta_df[col], errors='coerce')
                        macro_coverage[f'{col}_non_null_rate'] = round(float(arr.notna().mean()), 6)
                        macro_coverage[f'{col}_non_zero_rate'] = round(float((arr.fillna(0.0).abs() > 1e-12).mean()), 6)
                if macro_coverage:
                    data_profile['macro_feature_coverage'] = macro_coverage

            X_train, X_val, y_train, y_val, cutoff_date = self._time_based_split(X, y, meta_df)
            if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns:
                meta_val = meta_df.loc[(meta_df['asof_date'] > cutoff_date).values].reset_index(drop=True)
            else:
                meta_val = meta_df.iloc[len(X_train):].reset_index(drop=True) if meta_df is not None else None

            sample_weight = None
            recency_weight = None
            move_weight = None
            event_weight = None
            quality_weight = None
            asset_weight = None
            move_neutral_band = max(float(exp.get('neutral_zone', 0.0)), 0.003)
            train_dates_for_group = None
            if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns:
                train_mask = meta_df['asof_date'] <= cutoff_date
                train_dates = meta_df.loc[train_mask.values, 'asof_date']
                train_dates_for_group = train_dates
                recency_weight = self._build_recency_weights(train_dates, decay_days=exp['decay_days'])
                if 'future_return' in meta_df.columns:
                    train_future_ret = meta_df.loc[train_mask.values, 'future_return']
                    if exp.get('adaptive_band', False):
                        move_neutral_band = self._estimate_adaptive_neutral_band(
                            train_future_ret,
                            base_band=move_neutral_band,
                            quantile=float(exp.get('adaptive_band_quantile', 0.25))
                        )
                    move_weight = self._build_move_strength_weights(
                        train_future_ret,
                        scale=0.02,
                        neutral_band=move_neutral_band
                    )
                if exp.get('use_event_features', False) and 'has_report' in meta_df.columns and 'sentiment' in meta_df.columns:
                    train_has_report = meta_df.loc[train_mask.values, 'has_report']
                    train_sentiment = meta_df.loc[train_mask.values, 'sentiment']
                    train_shock = meta_df.loc[train_mask.values, 'sentiment_shock'] if 'sentiment_shock' in meta_df.columns else None
                    train_decay = meta_df.loc[train_mask.values, 'report_decay_3d'] if 'report_decay_3d' in meta_df.columns else None
                    event_weight = self._build_event_weights(train_has_report, train_sentiment, train_shock, train_decay)
                if 'code' in meta_df.columns:
                    train_codes = meta_df.loc[train_mask.values, 'code'].astype(str).tolist()
                    quality_weight = np.asarray([code_quality_map.get(c, 1.0) for c in train_codes], dtype=float)
            elif meta_df is not None and 'asof_date' in meta_df.columns:
                train_dates = meta_df.iloc[:len(X_train)]['asof_date']
                train_dates_for_group = train_dates
                recency_weight = self._build_recency_weights(train_dates, decay_days=exp['decay_days'])
                if 'future_return' in meta_df.columns:
                    train_future_ret = meta_df.iloc[:len(X_train)]['future_return']
                    if exp.get('adaptive_band', False):
                        move_neutral_band = self._estimate_adaptive_neutral_band(
                            train_future_ret,
                            base_band=move_neutral_band,
                            quantile=float(exp.get('adaptive_band_quantile', 0.25))
                        )
                    move_weight = self._build_move_strength_weights(
                        train_future_ret,
                        scale=0.02,
                        neutral_band=move_neutral_band
                    )
                if exp.get('use_event_features', False) and 'has_report' in meta_df.columns and 'sentiment' in meta_df.columns:
                    train_has_report = meta_df.iloc[:len(X_train)]['has_report']
                    train_sentiment = meta_df.iloc[:len(X_train)]['sentiment']
                    train_shock = meta_df.iloc[:len(X_train)]['sentiment_shock'] if 'sentiment_shock' in meta_df.columns else None
                    train_decay = meta_df.iloc[:len(X_train)]['report_decay_3d'] if 'report_decay_3d' in meta_df.columns else None
                    event_weight = self._build_event_weights(train_has_report, train_sentiment, train_shock, train_decay)
                if 'code' in meta_df.columns:
                    train_codes = meta_df.iloc[:len(X_train)]['code'].astype(str).tolist()
                    quality_weight = np.asarray([code_quality_map.get(c, 1.0) for c in train_codes], dtype=float)
                    asset_weight = self._build_asset_balance_weights(train_codes)

            if recency_weight is not None and move_weight is not None and len(recency_weight) == len(move_weight):
                sample_weight = np.asarray(recency_weight) * np.asarray(move_weight)
                mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                if mean_w > 0:
                    sample_weight = sample_weight / mean_w
            elif recency_weight is not None:
                sample_weight = recency_weight
            elif move_weight is not None:
                sample_weight = move_weight

            if sample_weight is not None and event_weight is not None and len(sample_weight) == len(event_weight):
                sample_weight = np.asarray(sample_weight) * np.asarray(event_weight)
                mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                if mean_w > 0:
                    sample_weight = sample_weight / mean_w
            elif sample_weight is None and event_weight is not None:
                sample_weight = event_weight

            if sample_weight is not None and quality_weight is not None and len(sample_weight) == len(quality_weight):
                sample_weight = np.asarray(sample_weight) * np.asarray(quality_weight)
                mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                if mean_w > 0:
                    sample_weight = sample_weight / mean_w
            elif sample_weight is None and quality_weight is not None:
                sample_weight = quality_weight

            if sample_weight is not None and asset_weight is not None and len(sample_weight) == len(asset_weight):
                sample_weight = np.asarray(sample_weight) * np.asarray(asset_weight)
                mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                if mean_w > 0:
                    sample_weight = sample_weight / mean_w
            elif sample_weight is None and asset_weight is not None:
                sample_weight = asset_weight

            for i, params in enumerate(candidate_params, start=1):
                tried_count += 1
                variant_specs = []

                # 变体1: 全量特征
                X_train_full = X_train
                X_val_full = X_val
                variant_specs.append(('full', X_train_full, X_val_full, list(X_train_full.columns)))

                # 先训练一次全量模型用于提取重要性，再生成裁剪特征变体
                train_score = self.short_predictor.train(
                    X_train_full,
                    y_train,
                    sample_weight=sample_weight,
                    model_params=params
                )
                if train_score <= 0 or self.short_predictor.model is None:
                    continue

                if not full_data_mode:
                    pruned_cols = self._build_pruned_feature_columns(self.short_predictor.model, list(X_train_full.columns), keep_topn=22)
                    if len(pruned_cols) < len(X_train_full.columns):
                        variant_specs.append(('pruned', X_train_full[pruned_cols], X_val_full[pruned_cols], pruned_cols))
                    top_feature_names = [t.get('name') for t in self._extract_top_features(self.short_predictor.model, list(X_train_full.columns), topn=6)]
                    X_train_inter, X_val_inter, inter_cols = self._build_interaction_variant(
                        X_train_full,
                        X_val_full,
                        top_feature_names,
                        max_pairs=4
                    )
                    if X_train_inter is not None and X_val_inter is not None and inter_cols is not None:
                        variant_specs.append(('interaction', X_train_inter, X_val_inter, inter_cols))

                local_best = None

                for variant_name, X_tr_var, X_va_var, feature_cols in variant_specs:
                    mode_outputs = {}
                    train_modes = ['classifier']
                    if train_dates_for_group is not None and len(train_dates_for_group) == len(X_train):
                        train_modes.append('ranker')

                    for model_type in train_modes:
                        model_obj = None
                        regime_models = {}
                        vol_split = None

                        if model_type == 'classifier':
                            train_score_var = self.short_predictor.train(
                                X_tr_var,
                                y_train,
                                sample_weight=sample_weight,
                                model_params=params
                            )
                            if train_score_var <= 0 or self.short_predictor.model is None:
                                continue
                            model_obj = self.short_predictor.model

                            regime_models, vol_split = self._train_short_term_regime_models(
                                X_tr_var,
                                y_train,
                                sample_weight,
                                params
                            )
                        else:
                            model_obj, train_score_var = self._train_short_term_ranker(
                                X_tr_var,
                                y_train,
                                train_dates_for_group,
                                sample_weight,
                                params
                            )
                            if train_score_var <= 0 or model_obj is None:
                                continue

                        val_proba_raw = self._predict_short_term_with_regime(
                            model_obj,
                            regime_models,
                            vol_split,
                            X_va_var
                        )
                        if val_proba_raw is None:
                            continue

                        mode_outputs[model_type] = {
                            'model': model_obj,
                            'regime_models': regime_models,
                            'volatility_split': vol_split,
                            'raw_proba': np.asarray(val_proba_raw).astype(float),
                        }

                        y_val_arr = np.asarray(y_val).astype(int)
                        raw_arr = np.asarray(val_proba_raw).astype(float)

                        calib_cut = int(len(y_val_arr) * 0.4)
                        can_calibrate = False
                        y_cal = y_val_arr
                        p_cal = raw_arr
                        y_eval = y_val_arr
                        p_eval = raw_arr
                        meta_eval = meta_val.reset_index(drop=True) if meta_val is not None else None
                        if len(y_val_arr) >= 240 and 80 <= calib_cut < (len(y_val_arr) - 80):
                            y_cal = y_val_arr[:calib_cut]
                            p_cal = raw_arr[:calib_cut]
                            y_eval = y_val_arr[calib_cut:]
                            p_eval = raw_arr[calib_cut:]
                            if meta_eval is not None:
                                meta_eval = meta_eval.iloc[calib_cut:].reset_index(drop=True)
                            if len(np.unique(y_cal)) > 1 and len(np.unique(y_eval)) > 1:
                                can_calibrate = True

                        method_candidates = ['none', 'platt', 'isotonic'] if can_calibrate else ['none']

                        for cal_method in method_candidates:
                            calibrator = None
                            if cal_method != 'none':
                                calibrator = self._fit_probability_calibrator(cal_method, y_cal, p_cal)
                                if calibrator is None:
                                    continue

                            eval_proba = self._apply_probability_calibrator(cal_method, calibrator, p_eval)
                            best_threshold, _ = self._find_best_threshold(y_eval, eval_proba)
                            eval_pred = (eval_proba >= best_threshold).astype(int)
                            eval_metrics = self._evaluate_binary(y_eval, eval_pred, eval_proba)

                            passed, gate_name = self._pass_validation_gate(5, eval_metrics)
                            auc_v = float(eval_metrics.get('auc') or 0.0)
                            brier_v = float(eval_metrics.get('brier') or 1.0)
                            auc_score = min(auc_v / max(MIN_SHORT_HORIZON_AUC, 1e-6), 1.2)
                            brier_score = min(MAX_SHORT_HORIZON_BRIER / max(brier_v, 1e-6), 1.2)
                            stability_info = self._evaluate_temporal_stability(y_eval, eval_proba)
                            secondary_info = self._evaluate_secondary_time_split(y_eval, eval_proba, tail_ratio=0.35)
                            stability_penalty = float(stability_info.get('stability_penalty', 0.0) or 0.0)
                            bias_info = self._summarize_prediction_bias(y_eval, eval_pred)
                            rank_score = (
                                (auc_score * 0.32)
                                + (brier_score * 0.18)
                                + (eval_metrics['f1'] * 0.18)
                                + (eval_metrics['accuracy'] * 0.14)
                                + (eval_metrics.get('precision', 0.0) * 0.10)
                                + (eval_metrics.get('balanced_accuracy', 0.0) * 0.08)
                                - (bias_info.get('false_positive_rate', 0.0) * 0.12)
                                - (bias_info.get('rate_gap', 0.0) * 0.08)
                            )
                            rank_score = rank_score - stability_penalty

                            failure_analysis = self._analyze_failure_factors(meta_eval, y_eval, eval_pred, eval_proba, period_days=5)

                            cand = {
                                'index': i,
                                'experiment': exp,
                                'params': params,
                                'model': model_obj,
                                'model_type': model_type,
                                'feature_variant': variant_name,
                                'feature_columns': list(feature_cols),
                                'top_features': self._extract_top_features(model_obj, list(feature_cols), topn=8),
                                'regime_models': regime_models,
                                'volatility_split': vol_split,
                                'threshold': best_threshold,
                                'metrics': eval_metrics,
                                'passed': passed,
                                'gate_name': gate_name,
                                'cutoff_date': cutoff_date,
                                'train_data_count': len(X_tr_var),
                                'val_data_count': len(X_va_var),
                                'rank_score': rank_score,
                                'calibration_method': cal_method,
                                'calibrator': calibrator,
                                'calibration_samples': int(len(y_cal)) if cal_method != 'none' else 0,
                                'weighting_mode': (
                                    'recency+move' if recency_weight is not None and move_weight is not None
                                    else ('recency' if recency_weight is not None else ('move' if move_weight is not None else 'none'))
                                ),
                                'use_event_features': bool(exp.get('use_event_features', False)),
                                'move_neutral_band': float(move_neutral_band),
                                'stability': stability_info,
                                'secondary_validation': secondary_info,
                                'prediction_bias': bias_info,
                                'failure_analysis': failure_analysis,
                            }

                            if self._is_better_short_candidate(cand, local_best):
                                local_best = cand

                    # 可部署融合候选: classifier + ranker 概率加权
                    if 'classifier' in mode_outputs and 'ranker' in mode_outputs:
                        clf_out = mode_outputs['classifier']
                        rnk_out = mode_outputs['ranker']
                        blend_weights = [0.65, 0.75]
                        for bw in blend_weights:
                            raw_arr = (float(bw) * clf_out['raw_proba']) + ((1.0 - float(bw)) * rnk_out['raw_proba'])
                            y_val_arr = np.asarray(y_val).astype(int)

                            calib_cut = int(len(y_val_arr) * 0.4)
                            can_calibrate = False
                            y_cal = y_val_arr
                            p_cal = raw_arr
                            y_eval = y_val_arr
                            p_eval = raw_arr
                            if len(y_val_arr) >= 240 and 80 <= calib_cut < (len(y_val_arr) - 80):
                                y_cal = y_val_arr[:calib_cut]
                                p_cal = raw_arr[:calib_cut]
                                y_eval = y_val_arr[calib_cut:]
                                p_eval = raw_arr[calib_cut:]
                                if len(np.unique(y_cal)) > 1 and len(np.unique(y_eval)) > 1:
                                    can_calibrate = True

                            method_candidates = ['none', 'platt', 'isotonic'] if can_calibrate else ['none']
                            for cal_method in method_candidates:
                                calibrator = None
                                if cal_method != 'none':
                                    calibrator = self._fit_probability_calibrator(cal_method, y_cal, p_cal)
                                    if calibrator is None:
                                        continue

                                eval_proba = self._apply_probability_calibrator(cal_method, calibrator, p_eval)
                                best_threshold, _ = self._find_best_threshold(y_eval, eval_proba)
                                eval_pred = (eval_proba >= best_threshold).astype(int)
                                eval_metrics = self._evaluate_binary(y_eval, eval_pred, eval_proba)
                                failure_analysis = self._analyze_failure_factors(meta_eval, y_eval, eval_pred, eval_proba, period_days=5)

                                passed, gate_name = self._pass_validation_gate(5, eval_metrics)
                                auc_v = float(eval_metrics.get('auc') or 0.0)
                                brier_v = float(eval_metrics.get('brier') or 1.0)
                                auc_score = min(auc_v / max(MIN_SHORT_HORIZON_AUC, 1e-6), 1.2)
                                brier_score = min(MAX_SHORT_HORIZON_BRIER / max(brier_v, 1e-6), 1.2)
                                stability_info = self._evaluate_temporal_stability(y_eval, eval_proba)
                                secondary_info = self._evaluate_secondary_time_split(y_eval, eval_proba, tail_ratio=0.35)
                                stability_penalty = float(stability_info.get('stability_penalty', 0.0) or 0.0)
                                bias_info = self._summarize_prediction_bias(y_eval, eval_pred)
                                rank_score = (
                                    (auc_score * 0.32)
                                    + (brier_score * 0.18)
                                    + (eval_metrics['f1'] * 0.18)
                                    + (eval_metrics['accuracy'] * 0.14)
                                    + (eval_metrics.get('precision', 0.0) * 0.10)
                                    + (eval_metrics.get('balanced_accuracy', 0.0) * 0.08)
                                    - (bias_info.get('false_positive_rate', 0.0) * 0.12)
                                    - (bias_info.get('rate_gap', 0.0) * 0.08)
                                )
                                rank_score = rank_score - stability_penalty

                                cand = {
                                    'index': i,
                                    'experiment': exp,
                                    'params': params,
                                    'model': clf_out['model'],
                                    'model_type': 'blend',
                                    'blend_model': rnk_out['model'],
                                    'blend_weight': float(bw),
                                    'feature_variant': variant_name,
                                    'feature_columns': list(feature_cols),
                                    'top_features': self._extract_top_features(clf_out['model'], list(feature_cols), topn=8),
                                    'regime_models': clf_out.get('regime_models', {}) or {},
                                    'volatility_split': clf_out.get('volatility_split'),
                                    'threshold': best_threshold,
                                    'metrics': eval_metrics,
                                    'passed': passed,
                                    'gate_name': gate_name,
                                    'cutoff_date': cutoff_date,
                                    'train_data_count': len(X_tr_var),
                                    'val_data_count': len(X_va_var),
                                    'rank_score': rank_score,
                                    'calibration_method': cal_method,
                                    'calibrator': calibrator,
                                    'calibration_samples': int(len(y_cal)) if cal_method != 'none' else 0,
                                    'weighting_mode': (
                                        'recency+move' if recency_weight is not None and move_weight is not None
                                        else ('recency' if recency_weight is not None else ('move' if move_weight is not None else 'none'))
                                    ),
                                    'move_neutral_band': float(move_neutral_band),
                                    'stability': stability_info,
                                    'secondary_validation': secondary_info,
                                    'prediction_bias': bias_info,
                                    'failure_analysis': failure_analysis,
                                }

                                if self._is_better_short_candidate(cand, local_best):
                                    local_best = cand

                if local_best is None:
                    continue

                candidate = local_best

                if self._is_better_short_candidate(candidate, best_candidate):
                    best_candidate = candidate
                    best_data_profile = data_profile

        if best_candidate is None:
            logger.warning("5日模型训练未产出可评估候选")
            reflection = {
                'timestamp': datetime.now().isoformat(),
                'passed': False,
                'gate': 'failed',
                'metrics': {},
                'experiment': {'name': 'none'},
                'data_profile': {'samples': 0},
                'reasons': ['无可用候选模型，可能是训练样本不足或特征缺失'],
                'actions': ['检查数据覆盖率与特征完整性，扩展可用标的和时间窗口'],
                'lessons': [],
            }
            self._save_short_term_reflection(reflection)
            return 0

        self.short_predictor.model = best_candidate['model']
        self.short_predictor.is_trained = True
        self.short_predictor.feature_columns = best_candidate.get('feature_columns')
        self.short_predictor.calibrator = best_candidate.get('calibrator')
        self.short_predictor.calibration_method = best_candidate.get('calibration_method', 'none')
        self.short_predictor.regime_models = best_candidate.get('regime_models', {}) or {}
        self.short_predictor.volatility_split = best_candidate.get('volatility_split')
        self.short_predictor.blend_model = best_candidate.get('blend_model')
        self.short_predictor.blend_weight = best_candidate.get('blend_weight', 0.65)
        self.short_predictor.blend_enabled = best_candidate.get('model_type') == 'blend'

        eval_metrics = best_candidate['metrics']
        best_threshold = best_candidate['threshold']
        val_accuracy = eval_metrics['accuracy']
        passed = best_candidate['passed']
        gate_name = best_candidate['gate_name']

        logger.info(
            f"5日候选最优: exp={best_candidate['experiment']['name']} #{best_candidate['index']} passed={passed} "
            f"acc={val_accuracy:.4f} f1={eval_metrics['f1']:.4f} auc={eval_metrics['auc']} brier={eval_metrics['brier']} "
            f"cal={best_candidate.get('calibration_method', 'none')} feat={best_candidate.get('feature_variant', 'full')} tried={tried_count}"
        )

        reflection = self._build_short_term_reflection(
            passed=passed,
            gate_name=gate_name,
            eval_metrics=eval_metrics,
            experiment=best_candidate['experiment'],
            data_profile=best_data_profile or {}
        )
        reflection['top_features'] = best_candidate.get('top_features', [])
        reflection['feature_variant'] = best_candidate.get('feature_variant', 'full')
        reflection['model_type'] = best_candidate.get('model_type', 'classifier')
        reflection['blend_weight'] = best_candidate.get('blend_weight')
        reflection['weighting_mode'] = best_candidate.get('weighting_mode', 'none')
        reflection['use_event_features'] = best_candidate.get('use_event_features', False)
        reflection['move_neutral_band'] = round(float(best_candidate.get('move_neutral_band', 0.0) or 0.0), 6)
        reflection['stability'] = best_candidate.get('stability', {})
        reflection['secondary_validation'] = best_candidate.get('secondary_validation', {})
        reflection['prediction_bias'] = best_candidate.get('prediction_bias', {})
        reflection['failure_analysis'] = best_candidate.get('failure_analysis', {})
        bias_info = reflection.get('prediction_bias') or {}
        failure_info = reflection.get('failure_analysis') or {}
        if float(bias_info.get('false_positive_rate') or 0.0) >= 0.40:
            reflection['reasons'].append(f"误报偏高: false_positive_rate={float(bias_info.get('false_positive_rate')):.4f}")
            reflection['actions'].append("下轮继续下调高波动与高扩展形态样本权重，压制短线误报")
        dominant_factors = failure_info.get('dominant_factors') or []
        if dominant_factors:
            reflection['actions'].append(f"针对高误差场景 {dominant_factors[0]} 做分层训练与降权")
        self._last_training_diagnostics['short_term'] = reflection
        if not passed and reflection['top_features']:
            top1 = reflection['top_features'][0]
            reflection['actions'].append(
                f"重点检查高权重特征 {top1.get('name')} 的稳定性与缺失分布"
            )
        self._save_short_term_reflection(reflection)

        if not passed:
            logger.warning(
                f"5日模型未达到上线阈值，跳过保存: accuracy={val_accuracy:.4f}, f1={eval_metrics['f1']:.4f}, "
                f"auc={eval_metrics['auc']}, brier={eval_metrics['brier']}, "
                f"要求 acc>={MIN_MODEL_ACCURACY} & f1>={MIN_MODEL_F1_SCORE}，"
                f"或(5日) auc>={MIN_SHORT_HORIZON_AUC} & brier<={MAX_SHORT_HORIZON_BRIER}"
            )
            return val_accuracy

        metadata = {
            'validation_accuracy': val_accuracy,
            'validation_precision': eval_metrics['precision'],
            'validation_recall': eval_metrics['recall'],
            'validation_f1': eval_metrics['f1'],
            'validation_auc': eval_metrics['auc'],
            'validation_brier': eval_metrics['brier'],
            'decision_threshold': best_threshold,
            'validation_gate': gate_name,
            'label_threshold': self._get_label_threshold(5),
            'neutral_zone': best_candidate['experiment'].get('neutral_zone', 0.0),
            'lookback_years': best_candidate['experiment'].get('lookback_years', 3.0),
            'decay_days': best_candidate['experiment'].get('decay_days', 90.0),
            'experiment_name': best_candidate['experiment'].get('name'),
            'calibration_method': best_candidate.get('calibration_method', 'none'),
            'calibration_samples': best_candidate.get('calibration_samples', 0),
            'feature_variant': best_candidate.get('feature_variant', 'full'),
            'model_type': best_candidate.get('model_type', 'classifier'),
            'blend_weight': best_candidate.get('blend_weight'),
            'weighting_mode': best_candidate.get('weighting_mode', 'none'),
            'use_event_features': best_candidate.get('use_event_features', False),
            'move_neutral_band': best_candidate.get('move_neutral_band'),
            'adaptive_band_quantile': best_candidate.get('experiment', {}).get('adaptive_band_quantile'),
            'stability': best_candidate.get('stability', {}),
            'secondary_validation': best_candidate.get('secondary_validation', {}),
            'feature_count': len(best_candidate.get('feature_columns') or []),
            'regime_model_count': len(best_candidate.get('regime_models', {}) or {}),
            'volatility_split': best_candidate.get('volatility_split'),
            'train_data_count': best_candidate['train_data_count'],
            'val_data_count': best_candidate['val_data_count'],
            'validation_cutoff_date': best_candidate['cutoff_date'].isoformat() if best_candidate['cutoff_date'] is not None else None,
            'params': self.short_predictor.model.get_params() if hasattr(self.short_predictor.model, 'get_params') else {},
            '_runtime_extras': {
                'calibrator': best_candidate.get('calibrator'),
                'calibration_method': best_candidate.get('calibration_method', 'none'),
                'regime_models': best_candidate.get('regime_models', {}),
                'volatility_split': best_candidate.get('volatility_split'),
                'feature_columns': best_candidate.get('feature_columns'),
                'blend_model': best_candidate.get('blend_model'),
                'blend_weight': best_candidate.get('blend_weight'),
                'blend_enabled': best_candidate.get('model_type') == 'blend'
            },
        }

        version = self.model_manager.save_model(
            self.short_predictor.model,
            'xgboost',
            5,
            metadata=metadata
        )

        promoted = self._save_runtime_model_file(
            self.short_predictor.model,
            'short_term_model.pkl',
            metadata={**metadata, 'version': version, 'period_days': 5}
        )
        if promoted and version and AUTO_ACTIVATE_BEST_MODEL:
            self.model_manager.activate_model(version, model_type='xgboost', period_days=5)

        logger.info(
            f"5日模型训练完成，验证指标: accuracy={val_accuracy:.2%}, "
            f"f1={eval_metrics['f1']:.4f}, auc={eval_metrics['auc']}"
        )
        return val_accuracy
    
    def train_short_term_until_target(self, stock_codes=None, target_accuracy=1.0, target_f1=1.0, max_rounds=6):
        """5日模型训练完成后立即复盘并继续自动优化，直到达到目标或达到最大轮次。"""
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        self._short_term_optimization_history = []
        best_result = 0.0
        best_accuracy = -1.0
        status = 'max_rounds_reached'
        plan_override = None

        for round_idx in range(1, int(max_rounds) + 1):
            result = self.train_short_term_model(
                stock_codes=stock_codes,
                plan_override=plan_override,
                round_idx=round_idx,
                max_rounds=max_rounds,
            )

            reflection = dict(self._last_training_diagnostics.get('short_term') or {})
            metrics = dict(reflection.get('metrics') or {})
            accuracy = float(metrics.get('accuracy') or result or 0.0)
            f1 = float(metrics.get('f1') or 0.0)

            self._short_term_optimization_history.append({
                'round': int(round_idx),
                'result': float(result or 0.0),
                'gate': reflection.get('gate'),
                'passed': bool(reflection.get('passed')),
                'metrics': metrics,
                'reasons': reflection.get('reasons') or [],
                'actions': reflection.get('actions') or [],
                'prediction_bias': reflection.get('prediction_bias') or {},
                'failure_analysis': reflection.get('failure_analysis') or {},
                'applied_strategy': plan_override or {},
            })

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_result = float(result or 0.0)

            if accuracy >= float(target_accuracy) and f1 >= float(target_f1):
                status = 'target_met'
                logger.info(
                    f"5日模型已达到自动优化目标: accuracy={accuracy:.4f}, f1={f1:.4f}, round={round_idx}"
                )
                break

            if round_idx < int(max_rounds):
                plan_override = self._derive_short_term_strategy_overrides(reflection, round_idx=round_idx + 1)
                if plan_override.get('notes'):
                    logger.info(f"5日模型下一轮优化方向: {', '.join(plan_override.get('notes') or [])}")
                else:
                    logger.info("5日模型下一轮优化方向: 延续默认搜索并保持反思复验")

        self._save_short_term_optimization_loop(
            target_accuracy=target_accuracy,
            target_f1=target_f1,
            max_rounds=max_rounds,
            history=self._short_term_optimization_history,
            status=status,
        )
        return best_result

    def train_medium_term_until_target(self, stock_codes=None, target_accuracy=0.55, target_f1=0.5, max_rounds=4):
        """20日模型训练后立即复盘并继续自动优化，直到达到目标或达到最大轮次。"""
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        self._medium_term_optimization_history = []
        best_result = 0.0
        best_accuracy = -1.0
        status = 'max_rounds_reached'
        plan_override = None

        for round_idx in range(1, int(max_rounds) + 1):
            result = self.train_medium_term_model(
                stock_codes=stock_codes,
                plan_override=plan_override,
                round_idx=round_idx,
                max_rounds=max_rounds,
            )

            reflection = dict(self._last_training_diagnostics.get('medium_term') or {})
            metrics = dict(reflection.get('metrics') or {})
            accuracy = float(metrics.get('accuracy') or result or 0.0)
            f1 = float(metrics.get('f1') or 0.0)

            self._medium_term_optimization_history.append({
                'round': int(round_idx),
                'result': float(result or 0.0),
                'gate': reflection.get('gate'),
                'passed': bool(reflection.get('passed')),
                'metrics': metrics,
                'reasons': reflection.get('reasons') or [],
                'actions': reflection.get('actions') or [],
                'failure_analysis': reflection.get('failure_analysis') or {},
                'data_profile': reflection.get('data_profile') or {},
                'applied_strategy': plan_override or {},
            })

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_result = float(result or 0.0)

            if accuracy >= float(target_accuracy) and f1 >= float(target_f1):
                status = 'target_met'
                logger.info(
                    f"20日模型已达到自动优化目标: accuracy={accuracy:.4f}, f1={f1:.4f}, round={round_idx}"
                )
                break

            if round_idx < int(max_rounds):
                plan_override = self._derive_medium_term_strategy_overrides(reflection, round_idx=round_idx + 1)
                if plan_override.get('notes'):
                    logger.info(f"20日模型下一轮优化方向: {', '.join(plan_override.get('notes') or [])}")
                else:
                    logger.info("20日模型下一轮优化方向: 延续默认搜索并保持反思复验")

        self._save_horizon_optimization_loop(
            20,
            target_accuracy=target_accuracy,
            target_f1=target_f1,
            max_rounds=max_rounds,
            history=self._medium_term_optimization_history,
            status=status,
        )
        return best_result

    def train_medium_term_model(self, stock_codes=None, plan_override=None, round_idx=None, max_rounds=None):
        """训练20日预测模型，并支持按轮次反思优化。"""
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        logger.info("开始训练20日预测模型...")
        if round_idx is not None and max_rounds is not None:
            logger.info(f"20日训练-反思优化轮次: {round_idx}/{max_rounds}")
            if plan_override and plan_override.get('notes'):
                logger.info(f"20日反思优化策略: {', '.join(plan_override.get('notes') or [])}")

        overrides = plan_override or {}
        selected_codes = list(stock_codes or [])
        quality_target_count = overrides.get('target_code_count')
        if len(selected_codes) > 2000 or quality_target_count:
            before_quality_count = len(selected_codes)
            selected_codes = self._select_quality_training_codes(selected_codes, period_days=20, target_count=quality_target_count)
            logger.info(f"20日训练标的筛选: 原始代码池={before_quality_count}，具备历史数据并可训练={len(selected_codes)}")
        penalty_map = overrides.get('penalty_map') or {}
        if penalty_map:
            before_penalty_count = len(selected_codes)
            selected_codes = self._filter_codes_by_penalty(
                selected_codes,
                penalty_map=penalty_map,
                min_penalty=0.48,
                max_drop_ratio=0.06 if len(selected_codes) >= 1000 else 0.12,
            )
            if before_penalty_count != len(selected_codes):
                logger.info(f"20日训练降噪过滤: 基于复盘剔除高误判标的 {before_penalty_count - len(selected_codes)} 只")
        code_quality_map = self._build_code_quality_weights(selected_codes, penalty_map=penalty_map)

        experiment_grid = (overrides.get('experiment_grid') or [
            {'name': 'default', 'lookback_years': 2.0, 'neutral_zone': 0.006, 'decay_days': 120.0, 'adaptive_label_zone': True, 'adaptive_band': True, 'adaptive_band_quantile': 0.20},
            {'name': 'recent_focus', 'lookback_years': 1.5, 'neutral_zone': 0.010, 'decay_days': 90.0, 'adaptive_label_zone': True, 'adaptive_band': True, 'adaptive_band_quantile': 0.22},
            {'name': 'drift_guard', 'lookback_years': 1.0, 'neutral_zone': 0.012, 'decay_days': 70.0, 'adaptive_label_zone': True, 'adaptive_band': True, 'adaptive_band_quantile': 0.24},
        ])[:3]
        candidate_params = (overrides.get('candidate_params') or [
            None,
            {'n_estimators': 180, 'max_depth': 3, 'learning_rate': 0.04, 'subsample': 0.9, 'colsample_bytree': 0.85, 'reg_lambda': 3.0, 'reg_alpha': 0.8},
            {'n_estimators': 220, 'max_depth': 2, 'learning_rate': 0.03, 'subsample': 0.88, 'colsample_bytree': 0.78, 'reg_lambda': 4.0, 'reg_alpha': 1.2},
        ])[:3]

        best_candidate = None

        for exp in experiment_grid:
            X, y, meta_df = self.prepare_training_data(
                selected_codes,
                period_days=20,
                lookback_years=float(exp.get('lookback_years', 2.0) or 2.0),
                neutral_zone=float(exp.get('neutral_zone', 0.0) or 0.0),
                return_meta=True,
                adaptive_label_zone=bool(exp.get('adaptive_label_zone', False)),
            )

            if X is None or len(X) < 100:
                continue

            X_train, X_val, y_train, y_val, cutoff_date = self._time_based_split(X, y, meta_df)

            train_mask = meta_df['asof_date'] <= cutoff_date if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns else None
            sample_weight = None
            quality_weight = None
            asset_weight = None
            decay_days = float(exp.get('decay_days', 120.0) or 120.0)
            if train_mask is not None and 'asof_date' in meta_df.columns:
                recency_weight = self._build_recency_weights(meta_df.loc[train_mask.values, 'asof_date'], decay_days=decay_days)
                move_neutral_band = float(exp.get('neutral_zone', 0.01) or 0.01)
                if exp.get('adaptive_band', False) and 'future_return' in meta_df.columns:
                    move_neutral_band = self._estimate_adaptive_neutral_band(
                        meta_df.loc[train_mask.values, 'future_return'],
                        base_band=move_neutral_band,
                        quantile=float(exp.get('adaptive_band_quantile', 0.22) or 0.22),
                    )
                move_weight = self._build_move_strength_weights(meta_df.loc[train_mask.values, 'future_return'], scale=0.05, neutral_band=move_neutral_band) if 'future_return' in meta_df.columns else None
                target_pos_rate = float(np.clip(float(y_val.mean()) if len(y_val) > 0 else 0.35, 0.25, 0.5))
                direction_weight = self._build_direction_balance_weights(y_train, target_positive_rate=target_pos_rate)
                regime_weight = self._build_regime_alignment_weights(X_train, X_val)
                event_weight = None
                if 'has_report' in meta_df.columns and 'sentiment' in meta_df.columns:
                    event_weight = self._build_event_weights(
                        meta_df.loc[train_mask.values, 'has_report'],
                        meta_df.loc[train_mask.values, 'sentiment'],
                        meta_df.loc[train_mask.values, 'sentiment_shock'] if 'sentiment_shock' in meta_df.columns else None,
                        meta_df.loc[train_mask.values, 'report_decay_3d'] if 'report_decay_3d' in meta_df.columns else None,
                    )
                if 'code' in meta_df.columns:
                    train_codes = meta_df.loc[train_mask.values, 'code'].astype(str).tolist()
                    quality_weight = np.asarray([code_quality_map.get(c, 1.0) for c in train_codes], dtype=float)
                    asset_weight = self._build_asset_balance_weights(train_codes)
                weight_parts = [w for w in (recency_weight, move_weight, direction_weight, regime_weight, event_weight, quality_weight, asset_weight) if w is not None]
                if weight_parts:
                    sample_weight = np.ones(len(y_train), dtype=float)
                    for part in weight_parts:
                        if len(part) == len(sample_weight):
                            sample_weight = sample_weight * np.asarray(part)
                    mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                    if mean_w > 0:
                        sample_weight = sample_weight / mean_w

            for params in candidate_params:
                accuracy = self.medium_predictor.train(X_train, y_train, sample_weight=sample_weight, model_params=params)
                if accuracy <= 0 or self.medium_predictor.model is None:
                    continue

                raw_proba = self.medium_predictor.model.predict_proba(X_val)[:, 1] if hasattr(self.medium_predictor.model, 'predict_proba') else None
                if raw_proba is None:
                    continue

                y_val_arr = np.asarray(y_val).astype(int)
                calib_cut = int(len(y_val_arr) * 0.4)
                can_calibrate = False
                y_cal = y_val_arr
                p_cal = raw_proba
                y_eval = y_val_arr
                p_eval = raw_proba
                if len(y_val_arr) >= 240 and 80 <= calib_cut < (len(y_val_arr) - 80):
                    y_cal = y_val_arr[:calib_cut]
                    p_cal = raw_proba[:calib_cut]
                    y_eval = y_val_arr[calib_cut:]
                    p_eval = raw_proba[calib_cut:]
                    if len(np.unique(y_cal)) > 1 and len(np.unique(y_eval)) > 1:
                        can_calibrate = True

                method_candidates = ['none', 'platt', 'isotonic'] if can_calibrate else ['none']
                for cal_method in method_candidates:
                    calibrator = None
                    if cal_method != 'none':
                        calibrator = self._fit_probability_calibrator(cal_method, y_cal, p_cal)
                        if calibrator is None:
                            continue

                    eval_proba = self._apply_probability_calibrator(cal_method, calibrator, p_eval)
                    best_threshold, _ = self._find_best_threshold_for_horizon(20, y_eval, eval_proba)
                    val_pred = (eval_proba >= best_threshold).astype(int)
                    eval_metrics = self._evaluate_binary(y_eval, val_pred, eval_proba)
                    passed, gate_name = self._pass_validation_gate(20, eval_metrics)
                    bias_info = self._summarize_prediction_bias(y_eval, val_pred)

                    data_profile = {
                        'samples': int(len(y)),
                        'pos_rate': round(float(y.mean()), 6),
                        'train_pos_rate': round(float(y_train.mean()), 6),
                        'val_pos_rate': round(float(y_val.mean()), 6),
                        'train_data_count': int(len(X_train)),
                        'val_data_count': int(len(X_val)),
                    }
                    drift = abs(float(data_profile['train_pos_rate']) - float(data_profile['val_pos_rate']))

                    if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns:
                        meta_val = meta_df.loc[(meta_df['asof_date'] > cutoff_date).values].reset_index(drop=True)
                        if len(meta_val) == len(y_val_arr) and len(y_eval) != len(y_val_arr):
                            meta_eval = meta_val.iloc[calib_cut:].reset_index(drop=True)
                        else:
                            meta_eval = meta_val
                    else:
                        meta_val = meta_df.iloc[len(X_train):].reset_index(drop=True) if meta_df is not None else None
                        meta_eval = meta_val.iloc[calib_cut:].reset_index(drop=True) if (meta_val is not None and len(y_eval) != len(y_val_arr)) else meta_val

                    failure_analysis = self._analyze_failure_factors(meta_eval, y_eval, val_pred, eval_proba, period_days=20)
                    stability_info = self._evaluate_temporal_stability(y_eval, eval_proba)
                    stability_penalty = float(stability_info.get('stability_penalty', 0.0) or 0.0)
                    rank_score = (
                        float(eval_metrics.get('accuracy') or 0.0) * 0.34
                        + float(eval_metrics.get('f1') or 0.0) * 0.30
                        + float(eval_metrics.get('auc') or 0.0) * 0.18
                        + float(eval_metrics.get('balanced_accuracy') or 0.0) * 0.10
                        + float(eval_metrics.get('precision') or 0.0) * 0.08
                        - float(eval_metrics.get('brier') or 1.0) * 0.10
                        - drift * 0.22
                        - float(bias_info.get('false_positive_rate') or 0.0) * 0.14
                        - float(bias_info.get('rate_gap') or 0.0) * 0.08
                        - stability_penalty
                    )

                    candidate = {
                        'model': self.medium_predictor.model,
                        'metrics': eval_metrics,
                        'passed': passed,
                        'gate_name': gate_name,
                        'threshold': best_threshold,
                        'data_profile': data_profile,
                        'failure_analysis': failure_analysis,
                        'prediction_bias': bias_info,
                        'top_features': self._extract_top_features(self.medium_predictor.model, list(X_train.columns), topn=8),
                        'experiment': exp,
                        'params': params or {},
                        'rank_score': rank_score,
                        'cutoff_date': cutoff_date,
                        'train_data_count': len(X_train),
                        'val_data_count': len(X_val),
                        'calibrator': calibrator,
                        'calibration_method': cal_method,
                        'calibration_samples': int(len(y_cal)) if cal_method != 'none' else 0,
                        'stability': stability_info,
                    }
                    if self._is_better_horizon_candidate(candidate, best_candidate):
                        best_candidate = candidate

        if best_candidate is None:
            logger.warning("20日模型训练未产出可评估候选")
            reflection = self._build_horizon_reflection(20, False, 'failed', {}, data_profile={'samples': 0}, experiment={'name': 'none'})
            self._save_horizon_reflection(20, reflection)
            self._last_training_diagnostics['medium_term'] = reflection
            return 0

        self.medium_predictor.model = best_candidate['model']
        self.medium_predictor.is_trained = self.medium_predictor.model is not None
        self.medium_predictor.calibrator = best_candidate.get('calibrator')
        self.medium_predictor.calibration_method = best_candidate.get('calibration_method', 'none')

        eval_metrics = best_candidate['metrics']
        val_accuracy = float(eval_metrics.get('accuracy') or 0.0)
        passed = bool(best_candidate['passed'])
        gate_name = best_candidate['gate_name']
        best_threshold = best_candidate['threshold']

        reflection = self._build_horizon_reflection(20, passed, gate_name, eval_metrics, data_profile=best_candidate['data_profile'], experiment=best_candidate.get('experiment'))
        reflection['top_features'] = best_candidate.get('top_features', [])
        reflection['failure_analysis'] = best_candidate.get('failure_analysis', {})
        reflection['prediction_bias'] = best_candidate.get('prediction_bias', {})
        reflection['params'] = best_candidate.get('params', {})
        reflection['calibration_method'] = best_candidate.get('calibration_method', 'none')
        reflection['stability'] = best_candidate.get('stability', {})
        self._save_horizon_reflection(20, reflection)
        self._last_training_diagnostics['medium_term'] = reflection

        if not passed:
            logger.warning(
                f"20日模型未达到上线阈值，跳过保存: accuracy={val_accuracy:.4f}, f1={eval_metrics['f1']:.4f}, "
                f"要求 accuracy>={MIN_MODEL_ACCURACY}, f1>={MIN_MODEL_F1_SCORE}"
            )
            return val_accuracy

        metadata = {
            'validation_accuracy': val_accuracy,
            'validation_precision': eval_metrics['precision'],
            'validation_recall': eval_metrics['recall'],
            'validation_f1': eval_metrics['f1'],
            'validation_auc': eval_metrics['auc'],
            'validation_brier': eval_metrics['brier'],
            'decision_threshold': best_threshold,
            'validation_gate': gate_name,
            'label_threshold': self._get_label_threshold(20),
            'experiment_name': (best_candidate.get('experiment') or {}).get('name'),
            'calibration_method': best_candidate.get('calibration_method', 'none'),
            'calibration_samples': best_candidate.get('calibration_samples', 0),
            'stability': best_candidate.get('stability', {}),
            'train_data_count': best_candidate['train_data_count'],
            'val_data_count': best_candidate['val_data_count'],
            'validation_cutoff_date': best_candidate['cutoff_date'].isoformat() if best_candidate['cutoff_date'] is not None else None,
            'params': self.medium_predictor.model.get_params() if hasattr(self.medium_predictor.model, 'get_params') else {},
            '_runtime_extras': {
                'calibrator': best_candidate.get('calibrator'),
                'calibration_method': best_candidate.get('calibration_method', 'none'),
            },
        }

        version = self.model_manager.save_model(
            self.medium_predictor.model,
            'xgboost',
            20,
            metadata=metadata
        )

        promoted = self._save_runtime_model_file(
            self.medium_predictor.model,
            'medium_term_model.pkl',
            metadata={**metadata, 'version': version, 'period_days': 20}
        )
        if promoted and version and AUTO_ACTIVATE_BEST_MODEL:
            self.model_manager.activate_model(version, model_type='xgboost', period_days=20)

        logger.info(
            f"20日模型训练完成，验证指标: accuracy={val_accuracy:.2%}, "
            f"f1={eval_metrics['f1']:.4f}, auc={eval_metrics['auc']}"
        )
        return val_accuracy

    def _derive_long_term_strategy_overrides(self, reflection, round_idx=1):
        """根据上一轮60日复盘结果，自动生成下一轮长期优化策略。"""
        reflection = reflection or {}
        metrics = reflection.get('metrics') or {}
        data_profile = reflection.get('data_profile') or {}
        prediction_bias = reflection.get('prediction_bias') or {}
        failure_analysis = reflection.get('failure_analysis') or {}

        acc = float(metrics.get('accuracy') or 0.0)
        f1 = float(metrics.get('f1') or 0.0)
        auc = float(metrics.get('auc') or 0.0)
        train_pos = float(data_profile.get('train_pos_rate') or 0.0)
        val_pos = float(data_profile.get('val_pos_rate') or 0.0)
        drift = abs(train_pos - val_pos)
        fp_rate = float(prediction_bias.get('false_positive_rate') or 0.0)
        rate_gap = float(prediction_bias.get('rate_gap') or 0.0)

        notes = []
        candidate_params = []
        experiment_grid = []
        penalty_map = self._build_code_penalty_map(failure_analysis)
        if penalty_map:
            notes.append('downweight_repeat_error_codes')

        if drift >= 0.08 or f1 < float(MIN_MODEL_F1_SCORE):
            notes.append('rebalance_long_term_class_mix')
            experiment_grid.append({
                'name': f'lt_balance_guard_r{round_idx}',
                'lookback_years': 2.0,
                'neutral_zone': 0.012,
                'decay_days': 140.0,
            })
            candidate_params.append({
                'n_estimators': 240,
                'max_depth': 3,
                'learning_rate': 0.035,
                'subsample': 0.88,
                'colsample_bytree': 0.8,
                'reg_lambda': 4.6,
                'reg_alpha': 1.2,
                'min_child_weight': 8,
            })

        if auc < 0.60 or acc < float(MIN_MODEL_ACCURACY):
            notes.append('tighten_long_term_signal_quality')
            experiment_grid.append({
                'name': f'lt_recent_focus_r{round_idx}',
                'lookback_years': 1.8,
                'neutral_zone': 0.014,
                'decay_days': 120.0,
            })
            candidate_params.append({
                'n_estimators': 260,
                'max_depth': 3,
                'learning_rate': 0.03,
                'subsample': 0.9,
                'colsample_bytree': 0.78,
                'reg_lambda': 5.0,
                'reg_alpha': 1.4,
                'gamma': 0.2,
            })

        if fp_rate >= 0.38 or rate_gap >= 0.12:
            notes.append('reduce_long_term_false_positives')
            experiment_grid.append({
                'name': f'lt_fp_guard_r{round_idx}',
                'lookback_years': 1.6,
                'neutral_zone': 0.016,
                'decay_days': 110.0,
            })
            candidate_params.append({
                'n_estimators': 180,
                'max_depth': 2,
                'learning_rate': 0.028,
                'subsample': 0.92,
                'colsample_bytree': 0.72,
                'reg_lambda': 6.2,
                'reg_alpha': 1.8,
                'min_child_weight': 10,
            })

        if not experiment_grid:
            experiment_grid.append({
                'name': f'lt_default_r{round_idx}',
                'lookback_years': 2.4,
                'neutral_zone': 0.010,
                'decay_days': 160.0,
            })

        if not candidate_params:
            candidate_params.append({
                'n_estimators': 220,
                'max_depth': 4,
                'learning_rate': 0.04,
                'subsample': 0.9,
                'colsample_bytree': 0.85,
                'reg_lambda': 4.0,
                'reg_alpha': 1.0,
            })

        return {
            'notes': list(dict.fromkeys(notes)),
            'candidate_params': candidate_params,
            'experiment_grid': experiment_grid,
            'penalty_map': penalty_map,
            'target_code_count': 1600 if acc < float(MIN_MODEL_ACCURACY) else None,
        }

    def train_long_term_until_target(self, stock_codes=None, target_accuracy=0.58, target_f1=0.5, max_rounds=4):
        """60日模型训练后立即复盘并继续自动优化，直到达到目标或达到最大轮次。"""
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        self._long_term_optimization_history = []
        best_result = 0.0
        best_accuracy = -1.0
        status = 'max_rounds_reached'
        plan_override = None

        for round_idx in range(1, int(max_rounds) + 1):
            result = self.train_long_term_model(
                stock_codes=stock_codes,
                plan_override=plan_override,
                round_idx=round_idx,
                max_rounds=max_rounds,
            )

            reflection = dict(self._last_training_diagnostics.get('long_term') or {})
            metrics = dict(reflection.get('metrics') or {})
            accuracy = float(metrics.get('accuracy') or result or 0.0)
            f1 = float(metrics.get('f1') or 0.0)

            self._long_term_optimization_history.append({
                'round': int(round_idx),
                'result': float(result or 0.0),
                'gate': reflection.get('gate'),
                'passed': bool(reflection.get('passed')),
                'metrics': metrics,
                'reasons': reflection.get('reasons') or [],
                'actions': reflection.get('actions') or [],
                'prediction_bias': reflection.get('prediction_bias') or {},
                'failure_analysis': reflection.get('failure_analysis') or {},
                'applied_strategy': plan_override or {},
            })

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_result = float(result or 0.0)

            if accuracy >= float(target_accuracy) and f1 >= float(target_f1):
                status = 'target_met'
                logger.info(
                    f"60日模型已达到自动优化目标: accuracy={accuracy:.4f}, f1={f1:.4f}, round={round_idx}"
                )
                break

            if round_idx < int(max_rounds):
                plan_override = self._derive_long_term_strategy_overrides(reflection, round_idx=round_idx + 1)
                if plan_override.get('notes'):
                    logger.info(f"60日模型下一轮优化方向: {', '.join(plan_override.get('notes') or [])}")
                else:
                    logger.info("60日模型下一轮优化方向: 延续默认搜索并保持反思复验")

        self._save_horizon_optimization_loop(
            60,
            target_accuracy=target_accuracy,
            target_f1=target_f1,
            max_rounds=max_rounds,
            history=self._long_term_optimization_history,
            status=status,
        )
        return best_result

    def train_long_term_model(self, stock_codes=None, plan_override=None, round_idx=None, max_rounds=None):
        """训练60日预测模型，并支持按轮次复盘优化。"""
        if stock_codes is None:
            stock_codes = self._get_default_training_codes(limit=None)

        logger.info("开始训练60日预测模型...")
        if round_idx is not None and max_rounds is not None:
            logger.info(f"60日训练-反思优化轮次: {round_idx}/{max_rounds}")
            if plan_override and plan_override.get('notes'):
                logger.info(f"60日反思优化策略: {', '.join(plan_override.get('notes') or [])}")

        overrides = plan_override or {}
        selected_codes = list(stock_codes or [])
        quality_target_count = overrides.get('target_code_count')
        if len(selected_codes) > 1800 or quality_target_count:
            before_quality_count = len(selected_codes)
            selected_codes = self._select_quality_training_codes(selected_codes, period_days=60, target_count=quality_target_count)
            logger.info(f"60日训练标的筛选: 原始代码池={before_quality_count}，具备历史数据并可训练={len(selected_codes)}")

        penalty_map = overrides.get('penalty_map') or {}
        if penalty_map:
            before_penalty_count = len(selected_codes)
            selected_codes = self._filter_codes_by_penalty(
                selected_codes,
                penalty_map=penalty_map,
                min_penalty=0.45,
                max_drop_ratio=0.10,
            )
            logger.info(f"60日训练误差降噪筛选: {before_penalty_count} -> {len(selected_codes)} 只标的")

        experiment_grid = overrides.get('experiment_grid') or [{}]
        primary_experiment = dict(experiment_grid[0] or {})
        lookback_years = float(primary_experiment.get('lookback_years', overrides.get('lookback_years', 3.0)) or 3.0)
        neutral_zone = float(primary_experiment.get('neutral_zone', overrides.get('neutral_zone', 0.0)) or 0.0)
        decay_days = float(primary_experiment.get('decay_days', overrides.get('decay_days', 180.0)) or 180.0)

        candidate_params = overrides.get('candidate_params') or []
        model_params = dict(candidate_params[0] or {}) if candidate_params else None

        X, y, meta_df = self.prepare_training_data(
            selected_codes,
            period_days=60,
            lookback_years=lookback_years,
            neutral_zone=neutral_zone,
            return_meta=True,
        )

        if X is None or len(X) < 100:
            logger.warning("训练数据不足，跳过60日模型训练")
            return 0

        X_train, X_val, y_train, y_val, cutoff_date = self._time_based_split(X, y, meta_df)

        train_mask = meta_df['asof_date'] <= cutoff_date if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns else None
        sample_weight = None
        if train_mask is not None and 'asof_date' in meta_df.columns:
            recency_weight = self._build_recency_weights(meta_df.loc[train_mask.values, 'asof_date'], decay_days=decay_days)
            move_weight = self._build_move_strength_weights(meta_df.loc[train_mask.values, 'future_return'], scale=0.08, neutral_band=max(0.02, neutral_zone)) if 'future_return' in meta_df.columns else None
            asset_weight = None
            if 'code' in meta_df.columns:
                train_codes = meta_df.loc[train_mask.values, 'code'].astype(str).tolist()
                asset_weight = self._build_asset_balance_weights(train_codes)
            direction_weight = self._build_direction_balance_weights(
                np.asarray(y_train),
                target_positive_rate=float(np.clip(np.mean(y_val), 0.28, 0.45)) if len(y_val) else 0.35,
            )
            regime_weight = self._build_regime_alignment_weights(X_train, X_val)

            if recency_weight is not None and move_weight is not None and len(recency_weight) == len(move_weight):
                sample_weight = np.asarray(recency_weight) * np.asarray(move_weight)
                mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                if mean_w > 0:
                    sample_weight = sample_weight / mean_w
            elif recency_weight is not None:
                sample_weight = recency_weight

            for extra_weight in [asset_weight, direction_weight, regime_weight]:
                if sample_weight is not None and extra_weight is not None and len(sample_weight) == len(extra_weight):
                    sample_weight = np.asarray(sample_weight) * np.asarray(extra_weight)
                    mean_w = float(np.mean(sample_weight)) if len(sample_weight) > 0 else 1.0
                    if mean_w > 0:
                        sample_weight = sample_weight / mean_w
                elif sample_weight is None and extra_weight is not None:
                    sample_weight = np.asarray(extra_weight)

        accuracy = self.long_predictor.train(X_train, y_train, sample_weight=sample_weight, model_params=model_params)

        if accuracy > 0:
            val_proba = self.long_predictor.model.predict_proba(X_val)[:, 1] if hasattr(self.long_predictor.model, 'predict_proba') else None
            best_threshold, _ = self._find_best_threshold_for_horizon(60, y_val, val_proba)
            val_pred = (val_proba >= best_threshold).astype(int) if val_proba is not None else self.long_predictor.model.predict(X_val)
            eval_metrics = self._evaluate_binary(y_val, val_pred, val_proba)
            bias_info = self._summarize_prediction_bias(y_val, val_pred)
            val_accuracy = eval_metrics['accuracy']

            passed, gate_name = self._pass_validation_gate(60, eval_metrics)
            data_profile = {
                'samples': int(len(y)),
                'pos_rate': round(float(y.mean()), 6),
                'train_pos_rate': round(float(y_train.mean()), 6),
                'val_pos_rate': round(float(y_val.mean()), 6),
                'train_data_count': int(len(X_train)),
                'val_data_count': int(len(X_val)),
            }
            if cutoff_date is not None and meta_df is not None and 'asof_date' in meta_df.columns:
                meta_val = meta_df.loc[(meta_df['asof_date'] > cutoff_date).values].reset_index(drop=True)
            else:
                meta_val = meta_df.iloc[len(X_train):].reset_index(drop=True) if meta_df is not None else None
            failure_analysis = self._analyze_failure_factors(meta_val, y_val, val_pred, val_proba, period_days=60)
            reflection = self._build_horizon_reflection(60, passed, gate_name, eval_metrics, data_profile=data_profile)
            reflection['top_features'] = self._extract_top_features(self.long_predictor.model, list(X_train.columns), topn=8)
            reflection['failure_analysis'] = failure_analysis
            reflection['prediction_bias'] = bias_info
            reflection['experiment'] = primary_experiment
            reflection['applied_params'] = model_params or {}
            self._save_horizon_reflection(60, reflection)
            self._last_training_diagnostics['long_term'] = reflection
            if not passed:
                logger.warning(
                    f"60日模型未达到上线阈值，跳过保存: accuracy={val_accuracy:.4f}, f1={eval_metrics['f1']:.4f}, "
                    f"要求 accuracy>={MIN_MODEL_ACCURACY}, f1>={MIN_MODEL_F1_SCORE}"
                )
                return val_accuracy

            metadata = {
                'validation_accuracy': val_accuracy,
                'validation_precision': eval_metrics['precision'],
                'validation_recall': eval_metrics['recall'],
                'validation_f1': eval_metrics['f1'],
                'validation_auc': eval_metrics['auc'],
                'validation_brier': eval_metrics['brier'],
                'decision_threshold': best_threshold,
                'validation_gate': gate_name,
                'label_threshold': self._get_label_threshold(60),
                'lookback_years': lookback_years,
                'neutral_zone': neutral_zone,
                'decay_days': decay_days,
                'experiment_name': primary_experiment.get('name', 'lt_default'),
                'train_data_count': len(X_train),
                'val_data_count': len(X_val),
                'validation_cutoff_date': cutoff_date.isoformat() if cutoff_date is not None else None,
                'params': self.long_predictor.model.get_params() if hasattr(self.long_predictor.model, 'get_params') else {}
            }

            version = self.model_manager.save_model(
                self.long_predictor.model,
                'xgboost',
                60,
                metadata=metadata
            )

            promoted = self._save_runtime_model_file(
                self.long_predictor.model,
                'long_term_model.pkl',
                metadata={**metadata, 'version': version, 'period_days': 60}
            )
            if promoted and version and AUTO_ACTIVATE_BEST_MODEL:
                self.model_manager.activate_model(version, model_type='xgboost', period_days=60)

            logger.info(
                f"60日模型训练完成，验证指标: accuracy={val_accuracy:.2%}, "
                f"f1={eval_metrics['f1']:.4f}, auc={eval_metrics['auc']}"
            )
            return val_accuracy

        return 0
    
    def _save_training_backtest_summary(self, results):
        """保存本轮训练的统一回测与复盘摘要。"""
        try:
            path = os.path.join('data', 'models', 'training_backtest_summary.json')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                'timestamp': datetime.now().isoformat(),
                'results': results,
                'diagnostics': self._last_training_diagnostics,
            }
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存训练回测摘要失败: {e}")

    def _requested_targets_met(self, target_periods, diagnostics):
        """检查本轮请求的所有周期是否都通过验证门槛。"""
        period_to_key = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}
        for period in set(target_periods or [5, 20, 60]):
            diag = diagnostics.get(period_to_key.get(period, '')) or {}
            if not bool(diag.get('passed')):
                return False
        return True

    def _save_continuous_improvement_summary(self, summary):
        """保存统一的连续训练-复盘-再优化摘要。"""
        try:
            path = os.path.join('data', 'models', 'continuous_improvement_summary.json')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存连续优化摘要失败: {e}")

    def train_all_models(
        self,
        stock_codes=None,
        target_periods=None,
        auto_optimize_short_term=True,
        auto_optimize_medium_term=True,
        auto_optimize_long_term=True,
        short_term_target_accuracy=1.0,
        short_term_target_f1=1.0,
        short_term_max_rounds=6,
        medium_term_target_accuracy=0.55,
        medium_term_target_f1=0.5,
        medium_term_max_rounds=4,
        long_term_target_accuracy=0.58,
        long_term_target_f1=0.5,
        long_term_max_rounds=4,
        continuous_improvement_rounds=1,
    ):
        """
        训练模型（支持全模型多轮训练 → 复盘总结 → 自动优化 → 再训练）。
        """
        results = {}
        self._last_training_diagnostics = {}
        self._continuous_improvement_history = []

        target_periods = set(target_periods or [5, 20, 60])
        total_cycles = max(1, int(continuous_improvement_rounds or 1))
        final_status = 'max_cycles_reached'

        for cycle_idx in range(1, total_cycles + 1):
            cycle_results = {}
            logger.info(f"开始全模型连续优化循环: {cycle_idx}/{total_cycles}")

            if 5 in target_periods:
                if auto_optimize_short_term:
                    cycle_results['short_term'] = self.train_short_term_until_target(
                        stock_codes=stock_codes,
                        target_accuracy=short_term_target_accuracy,
                        target_f1=short_term_target_f1,
                        max_rounds=short_term_max_rounds,
                    )
                else:
                    cycle_results['short_term'] = self.train_short_term_model(stock_codes)
            if 20 in target_periods:
                if auto_optimize_medium_term:
                    cycle_results['medium_term'] = self.train_medium_term_until_target(
                        stock_codes=stock_codes,
                        target_accuracy=medium_term_target_accuracy,
                        target_f1=medium_term_target_f1,
                        max_rounds=medium_term_max_rounds,
                    )
                else:
                    cycle_results['medium_term'] = self.train_medium_term_model(stock_codes)
            if 60 in target_periods:
                if auto_optimize_long_term:
                    cycle_results['long_term'] = self.train_long_term_until_target(
                        stock_codes=stock_codes,
                        target_accuracy=long_term_target_accuracy,
                        target_f1=long_term_target_f1,
                        max_rounds=long_term_max_rounds,
                    )
                else:
                    cycle_results['long_term'] = self.train_long_term_model(stock_codes)

            diagnostics_snapshot = deepcopy(self._last_training_diagnostics)
            cycle_result_snapshot = deepcopy(cycle_results)
            self._continuous_improvement_history.append({
                'cycle': int(cycle_idx),
                'results': cycle_result_snapshot,
                'diagnostics': diagnostics_snapshot,
            })
            results = deepcopy(cycle_result_snapshot)

            if self._requested_targets_met(target_periods, diagnostics_snapshot):
                final_status = 'target_met'
                logger.info(f"全模型连续优化已提前完成，停止于第 {cycle_idx} 轮")
                break

        results['continuous_improvement'] = {
            'status': final_status,
            'requested_cycles': total_cycles,
            'completed_cycles': len(self._continuous_improvement_history),
            'cycles': self._continuous_improvement_history,
        }

        self._save_training_backtest_summary(results)
        self._save_continuous_improvement_summary({
            'timestamp': datetime.now().isoformat(),
            'target_periods': sorted(target_periods),
            'summary': results['continuous_improvement'],
        })
        logger.info(f"模型训练完成: {results}")
        return results


# 测试代码
if __name__ == '__main__':
    trainer = ModelTrainer()
    results = trainer.train_all_models()
    print(f"训练结果: {results}")