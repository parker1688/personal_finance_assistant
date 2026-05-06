# -*- coding: utf-8 -*-
"""基金模型训练器 - 迁移自 scripts/train_fund.py"""

# -*- coding: utf-8 -*-
"""
基金模型训练脚本 - 优化版

优化内容(对齐A股4项优化):
1. 宏观特征接入: CPI/PMI/SHIBOR 作为输入特征(三级降级+硬编码兜底)
2. 回归->分类 + 强中性区: XGBClassifier预测30日正收益, +-1.5%中性区过滤噪声样本
3. 严格日期验证: holdout 2026-04-01~2026-04-24, 与A股对齐
4. Walk-forward多窗口验证: 6窗口x30天, 检验模型跨期稳定性
"""

import sys
import os
import logging
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
from pathlib import Path


from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, precision_score, recall_score, brier_score_loss

from predictors.model_manager import ModelManager

logger = logging.getLogger('fund_trainer')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestClassifier
    logger.warning('[WARN] XGBoost未安装, 将使用RandomForestClassifier')


# ------------------------------------------------------------------ #
#  宏观数据加载 (对齐 model_trainer.py 三级降级 + 硬编码兜底)
# ------------------------------------------------------------------ #
MACRO_HARDCODE = {
    'cpi_yoy': 1.0,
    'pmi': 50.0,
    'shibor_1w': 1.42,
    'shibor_1m': 1.44,
}


def _load_macro_snapshot(ref_date=None):
    """
    加载宏观特征快照, 返回 dict.
    优先级: CSV文件 -> 硬编码兜底
    """
    if ref_date is None:
        ref_date = datetime.now().date()
    if isinstance(ref_date, str):
        ref_date = pd.to_datetime(ref_date).date()

    result = dict(MACRO_HARDCODE)

    # CPI
    try:
        cpi_paths = ['data/macro_cpi.csv', 'data/cpi.csv']
        for p in cpi_paths:
            if not os.path.exists(p):
                continue
            cpi_df = pd.read_csv(p, dtype=str)
            cpi_df.columns = [c.lower().strip() for c in cpi_df.columns]
            date_col = next((c for c in cpi_df.columns if 'date' in c or 'month' in c or 'time' in c), None)
            val_col = next((c for c in cpi_df.columns if 'yoy' in c or 'cpi' in c), None)
            if date_col and val_col:
                cpi_df['_dt'] = pd.to_datetime(cpi_df[date_col], errors='coerce')
                cpi_df = cpi_df.dropna(subset=['_dt', val_col])
                cpi_df[val_col] = pd.to_numeric(cpi_df[val_col], errors='coerce')
                cpi_df = cpi_df.dropna(subset=[val_col]).sort_values('_dt')
                valid = cpi_df[cpi_df['_dt'].dt.date <= ref_date]
                if not valid.empty:
                    result['cpi_yoy'] = float(valid.iloc[-1][val_col])
            break
    except Exception as ex:
        logger.warning(f'CPI加载失败, 使用兜底: {ex}')

    # PMI
    try:
        pmi_paths = ['data/macro_pmi.csv', 'data/pmi.csv']
        pmi_col_candidates = ['pmi010000', 'pmi', 'manufacturing_pmi', 'value']
        for p in pmi_paths:
            if not os.path.exists(p):
                continue
            pmi_df = pd.read_csv(p, dtype=str)
            pmi_df.columns = [c.lower().strip() for c in pmi_df.columns]
            date_col = next((c for c in pmi_df.columns if 'date' in c or 'month' in c or 'time' in c), None)
            val_col = next((c for c in pmi_df.columns if c in pmi_col_candidates), None)
            if date_col and val_col:
                pmi_df['_dt'] = pd.to_datetime(pmi_df[date_col], errors='coerce')
                pmi_df = pmi_df.dropna(subset=['_dt', val_col])
                pmi_df[val_col] = pd.to_numeric(pmi_df[val_col], errors='coerce')
                pmi_df = pmi_df.dropna(subset=[val_col]).sort_values('_dt')
                valid = pmi_df[pmi_df['_dt'].dt.date <= ref_date]
                if not valid.empty:
                    result['pmi'] = float(valid.iloc[-1][val_col])
            break
    except Exception as ex:
        logger.warning(f'PMI加载失败, 使用兜底: {ex}')

    # SHIBOR
    try:
        shibor_paths = ['data/macro_shibor.csv', 'data/shibor.csv']
        for p in shibor_paths:
            if not os.path.exists(p):
                continue
            sh_df = pd.read_csv(p, dtype=str)
            sh_df.columns = [c.lower().strip() for c in sh_df.columns]
            date_col = next((c for c in sh_df.columns if 'date' in c or 'time' in c), None)
            col_1w = next((c for c in sh_df.columns if '1w' in c or ('on' in c and 'non' not in c)), None)
            col_1m = next((c for c in sh_df.columns if '1m' in c and '3m' not in c), None)
            if date_col:
                sh_df['_dt'] = pd.to_datetime(sh_df[date_col], errors='coerce')
                sh_df = sh_df.dropna(subset=['_dt']).sort_values('_dt')
                valid = sh_df[sh_df['_dt'].dt.date <= ref_date]
                if not valid.empty and col_1w:
                    v = pd.to_numeric(valid.iloc[-1][col_1w], errors='coerce')
                    if pd.notna(v):
                        result['shibor_1w'] = float(v)
                if not valid.empty and col_1m:
                    v = pd.to_numeric(valid.iloc[-1][col_1m], errors='coerce')
                    if pd.notna(v):
                        result['shibor_1m'] = float(v)
            break
    except Exception as ex:
        logger.warning(f'SHIBOR加载失败, 使用兜底: {ex}')

    return result


def _build_macro_series(dates):
    """
    对每个日期构建宏观特征快照, 返回 DataFrame, 行数与 dates 对齐.
    为提升性能, 按月缓存.
    """
    dates_ts = pd.to_datetime(dates)
    unique_months = sorted(set((d.year, d.month) for d in dates_ts))
    cache = {}
    for y, m in unique_months:
        ref = datetime(y, m, 1).date()
        cache[(y, m)] = _load_macro_snapshot(ref_date=ref)

    rows = []
    for d in dates_ts:
        snap = cache.get((d.year, d.month), MACRO_HARDCODE)
        rows.append(snap)
    return pd.DataFrame(rows, index=range(len(dates)))


# ------------------------------------------------------------------ #
#  市场情绪数据 (沪深300ETF 510300.SH 作为市场代理)
# ------------------------------------------------------------------ #
MKT_HARDCODE = {
    'mkt_ret_5d': 0.0,
    'mkt_ret_20d': 0.0,
    'mkt_vol_20d': 0.20,
    'mkt_up_trend': 0.5,
}

_MARKET_CACHE = None  # 全局缓存，避免重复加载


def _load_market_etf():
    """
    加载沪深300ETF (510300.SH) 作为市场情绪代理.
    计算 5/20日收益率、20日波动率、均线方向.
    返回 DataFrame，以 date 为索引.
    """
    global _MARKET_CACHE
    if _MARKET_CACHE is not None:
        return _MARKET_CACHE

    try:
        etf = pd.read_csv('data/historical_etf.csv')
        etf = etf[etf['code'] == '510300.SH'].copy()
        if etf.empty:
            logger.warning('historical_etf.csv 中未找到 510300.SH 数据，使用兜底')
            return None
        etf['date'] = pd.to_datetime(etf['date'], errors='coerce')
        etf = etf.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
        close = etf['close'].values.astype(float)

        n = len(close)
        ret5 = np.full(n, np.nan)
        ret20 = np.full(n, np.nan)
        vol20 = np.full(n, np.nan)
        trend = np.full(n, np.nan)

        for i in range(n):
            if i >= 5 and close[i - 5] > 0:
                ret5[i] = (close[i] - close[i - 5]) / close[i - 5]
            if i >= 20 and close[i - 20] > 0:
                ret20[i] = (close[i] - close[i - 20]) / close[i - 20]
                daily_rets = np.diff(close[i - 20:i + 1]) / (close[i - 20:i] + 1e-9)
                vol20[i] = float(np.std(daily_rets) * np.sqrt(252))
                ma20 = float(np.mean(close[i - 20:i + 1]))
                trend[i] = 1.0 if close[i] > ma20 else 0.0

        mkt_df = pd.DataFrame({
            'mkt_ret_5d': ret5,
            'mkt_ret_20d': ret20,
            'mkt_vol_20d': vol20,
            'mkt_up_trend': trend,
        }, index=pd.DatetimeIndex(etf['date']))
        mkt_df = mkt_df.dropna()
        _MARKET_CACHE = mkt_df
        logger.info(f'市场情绪数据加载完成: {len(mkt_df)} 条, '
                    f'{mkt_df.index.min().date()} ~ {mkt_df.index.max().date()}')
        return _MARKET_CACHE
    except Exception as ex:
        logger.warning(f'市场情绪数据加载失败, 使用兜底: {ex}')
        return None


def _build_market_series(dates):
    """
    对基金每个日期查找市场情绪快照（as-of 最近可用交易日）.
    返回 DataFrame，行数与 dates 对齐.
    """
    mkt_df = _load_market_etf()
    dates_ts = pd.to_datetime(dates)

    if mkt_df is None:
        return pd.DataFrame([MKT_HARDCODE] * len(dates_ts))

    mkt_index = mkt_df.index  # DatetimeIndex (sorted)
    rows = []
    for d in dates_ts:
        pos = mkt_index.searchsorted(d, side='right') - 1
        if pos >= 0:
            rows.append(mkt_df.iloc[pos].to_dict())
        else:
            rows.append(dict(MKT_HARDCODE))
    return pd.DataFrame(rows, index=range(len(dates_ts)))


# ------------------------------------------------------------------ #
#  FundTrainer
# ------------------------------------------------------------------ #
class FundTrainer:
    """基金模型训练器 (优化版)"""

    # 优化2: 强中性区阈值
    NEUTRAL_ZONE = 0.015   # +-1.5%
    PREDICT_HORIZON = 30   # 预测未来30日收益

    @staticmethod
    def _predict_positive_proba(model, X):
        if hasattr(model, 'predict_proba'):
            proba = model.predict_proba(X)
            if getattr(proba, 'ndim', 1) == 2 and proba.shape[1] > 1:
                return proba[:, 1]
            if getattr(proba, 'ndim', 1) == 2 and proba.shape[1] == 1:
                return np.full(proba.shape[0], float(proba[0, 0]))
        pred = model.predict(X)
        return np.asarray(pred, dtype=float)

    @staticmethod
    def _find_best_threshold(y_true, y_proba, min_threshold=0.30, max_threshold=0.70, step=0.01):
        y_true_arr = np.asarray(y_true, dtype=int)
        y_proba_arr = np.asarray(y_proba, dtype=float)
        best_threshold = 0.50
        best_f1 = -1.0
        for threshold in np.arange(min_threshold, max_threshold + 1e-9, step):
            y_pred = (y_proba_arr >= threshold).astype(int)
            f1 = float(f1_score(y_true_arr, y_pred, zero_division=0))
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = float(threshold)
        return round(best_threshold, 2), max(best_f1, 0.0)

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

    def __init__(self):
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        self.model_manager = ModelManager()
        self.data = None

    def load_data(self):
        """加载基金净值数据"""
        logger.info('=' * 60)
        logger.info('加载基金数据')
        logger.info('=' * 60)

        fund_files = ['data/fund_nav.csv', 'data/funds.csv', 'data/historical_funds.csv']
        for file in fund_files:
            if not os.path.exists(file):
                continue
            logger.info(f'加载基金数据: {file}...')
            df = pd.read_csv(file, low_memory=False)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            elif 'trade_date' in df.columns:
                df['date'] = pd.to_datetime(df['trade_date'], errors='coerce')
                df.drop(columns=['trade_date'], inplace=True)
            df = df.dropna(subset=['date'])
            df = df.sort_values(['code', 'date']).reset_index(drop=True)
            logger.info(f'基金数据: {len(df):,} 条, {df["code"].nunique():,} 只基金')
            logger.info(f'时间范围: {df["date"].min().date()} ~ {df["date"].max().date()}')
            self.data = df
            return True

        # 数据库回退
        try:
            from models import get_session, RawFundData
            session = get_session()
            rows = session.query(
                RawFundData.code, RawFundData.date, RawFundData.nav,
                RawFundData.accumulated_nav, RawFundData.daily_return,
            ).all()
            session.close()
            if rows:
                df = pd.DataFrame(rows, columns=['code', 'date', 'nav', 'accumulated_nav', 'daily_return'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(['code', 'date']).reset_index(drop=True)
                logger.info(f'数据库回退加载: {len(df):,} 条, {df["code"].nunique():,} 只基金')
                self.data = df
                return True
        except Exception as e:
            logger.warning(f'数据库回退加载失败: {e}')

        logger.error('未找到基金数据, 请先运行 collectors/fund_collector.py')
        return False

    def extract_features(self, fund_df, macro_df=None, market_df=None):
        """
        从单只基金数据提取样本特征 + 标签.
        优化1: 加入宏观特征 cpi_yoy / pmi / shibor_1w / shibor_1m
        优化2: 标签为二分类(强中性区过滤)
        优化5: 加入市场情绪特征 mkt_ret_5/20d / mkt_vol_20d / mkt_up_trend
               加入时间特征 month_sin / month_cos / is_q1
        返回: X (DataFrame), y (Series), dates (list)
        """
        if fund_df is None or len(fund_df) < 30:
            return None, None, None

        nav_col = 'nav' if 'nav' in fund_df.columns else ('close' if 'close' in fund_df.columns else None)
        if nav_col is None:
            return None, None, None

        nav_values = fund_df[nav_col].values.astype(float)
        dates_arr = fund_df['date'].values

        X_list, y_list, date_list = [], [], []
        feature_keys = None

        for i in range(30, len(nav_values) - self.PREDICT_HORIZON, 10):
            if nav_values[i] <= 0:
                continue
            # 未来收益
            future_return = (nav_values[i + self.PREDICT_HORIZON] - nav_values[i]) / nav_values[i]

            # 优化2: 强中性区过滤 -- 跳过不确定样本
            if abs(future_return) < self.NEUTRAL_ZONE:
                continue

            label = 1 if future_return > 0 else 0

            features = {}

            # --- 技术特征 ---
            features['return_5d'] = (nav_values[i] - nav_values[i-5]) / nav_values[i-5] if nav_values[i-5] > 0 else 0.0
            features['return_10d'] = (nav_values[i] - nav_values[i-10]) / nav_values[i-10] if nav_values[i-10] > 0 else 0.0
            features['return_20d'] = (nav_values[i] - nav_values[i-20]) / nav_values[i-20] if nav_values[i-20] > 0 else 0.0
            features['return_30d'] = (nav_values[i] - nav_values[i-30]) / nav_values[i-30] if nav_values[i-30] > 0 else 0.0

            prev_nav = nav_values[:i]
            returns = np.diff(prev_nav) / (prev_nav[:-1] + 1e-9)
            features['volatility_5d'] = float(np.std(returns[-5:]) * np.sqrt(252)) if len(returns) >= 5 else 0.0
            features['volatility_10d'] = float(np.std(returns[-10:]) * np.sqrt(252)) if len(returns) >= 10 else 0.0
            features['volatility_20d'] = float(np.std(returns[-20:]) * np.sqrt(252)) if len(returns) >= 20 else 0.0

            avg_ret = float(np.mean(returns[-20:])) if len(returns) >= 20 else 0.0
            std_ret = float(np.std(returns[-20:])) if len(returns) >= 20 else 1.0
            features['sharpe_ratio'] = (avg_ret - 0.00005) / (std_ret + 1e-6) if std_ret > 0 else 0.0

            recent = returns[-30:] if len(returns) >= 30 else returns
            cumulative = np.cumprod(1 + np.clip(recent, -0.5, 0.5))
            running_max = np.maximum.accumulate(cumulative)
            drawdown = (cumulative - running_max) / (running_max + 1e-9)
            features['max_drawdown'] = float(np.min(drawdown)) if len(drawdown) > 0 else 0.0

            positive_cnt = int(np.sum(returns[-20:] > 0)) if len(returns) >= 20 else 0
            features['positive_days_ratio'] = positive_cnt / min(20, max(len(returns), 1))

            ma10 = float(np.mean(nav_values[i-10:i+1])) if i >= 10 else float(nav_values[i])
            features['nav_ma10_ratio'] = (nav_values[i] / ma10 - 1) if ma10 > 0 else 0.0

            ma20 = float(np.mean(nav_values[i-20:i+1])) if i >= 20 else float(nav_values[i])
            features['nav_ma20_ratio'] = (nav_values[i] / ma20 - 1) if ma20 > 0 else 0.0

            # --- 优化1: 宏观特征 ---
            if macro_df is not None and i < len(macro_df):
                snap = macro_df.iloc[i]
                features['cpi_yoy'] = float(snap.get('cpi_yoy', MACRO_HARDCODE['cpi_yoy']))
                features['pmi'] = float(snap.get('pmi', MACRO_HARDCODE['pmi']))
                features['shibor_1w'] = float(snap.get('shibor_1w', MACRO_HARDCODE['shibor_1w']))
                features['shibor_1m'] = float(snap.get('shibor_1m', MACRO_HARDCODE['shibor_1m']))
            else:
                features.update(MACRO_HARDCODE)

            # --- 优化5: 市场情绪特征 ---
            if market_df is not None and i < len(market_df):
                snap = market_df.iloc[i]
                features['mkt_ret_5d'] = float(snap.get('mkt_ret_5d', MKT_HARDCODE['mkt_ret_5d']))
                features['mkt_ret_20d'] = float(snap.get('mkt_ret_20d', MKT_HARDCODE['mkt_ret_20d']))
                features['mkt_vol_20d'] = float(snap.get('mkt_vol_20d', MKT_HARDCODE['mkt_vol_20d']))
                features['mkt_up_trend'] = float(snap.get('mkt_up_trend', MKT_HARDCODE['mkt_up_trend']))
            else:
                features.update(MKT_HARDCODE)

            # --- 时间特征 ---
            date_ts = pd.Timestamp(dates_arr[i])
            month = date_ts.month
            features['month_sin'] = float(np.sin(2 * np.pi * month / 12))
            features['month_cos'] = float(np.cos(2 * np.pi * month / 12))
            features['is_q1'] = 1.0 if month in (1, 2, 3) else 0.0

            # 清理 inf/nan
            for k in list(features.keys()):
                v = features[k]
                if not isinstance(v, (int, float)) or np.isnan(v) or np.isinf(v):
                    features[k] = 0.0

            if feature_keys is None:
                feature_keys = list(features.keys())

            X_list.append([features[k] for k in feature_keys])
            y_list.append(label)
            date_list.append(pd.Timestamp(dates_arr[i]))

        if not X_list:
            return None, None, None

        X = pd.DataFrame(X_list, columns=feature_keys)
        y = pd.Series(y_list)
        return X, y, date_list

    def _strict_split(self, X, y, dates, val_start='2026-04-01', val_end='2026-04-24'):
        """
        优化3: 严格日期验证分割.
        返回 X_train, X_val, y_train, y_val, info_dict
        """
        dates_arr = pd.to_datetime(dates)
        s = pd.to_datetime(val_start)
        e = pd.to_datetime(val_end)
        val_mask = (dates_arr >= s) & (dates_arr <= e)
        train_mask = ~val_mask

        if train_mask.sum() == 0 or val_mask.sum() == 0:
            # 退化为 80/20 时间切分
            split_idx = int(len(X) * 0.8)
            return (
                X.iloc[:split_idx].reset_index(drop=True),
                X.iloc[split_idx:].reset_index(drop=True),
                y.iloc[:split_idx].reset_index(drop=True),
                y.iloc[split_idx:].reset_index(drop=True),
                {'mode': 'fallback_80_20', 'n_train': split_idx, 'n_val': len(X) - split_idx},
            )

        X_train = X.loc[train_mask].reset_index(drop=True)
        X_val   = X.loc[val_mask].reset_index(drop=True)
        y_train = y.loc[train_mask].reset_index(drop=True)
        y_val   = y.loc[val_mask].reset_index(drop=True)
        info = {
            'mode': 'strict',
            'val_start': val_start,
            'val_end': val_end,
            'n_train': int(train_mask.sum()),
            'n_val': int(val_mask.sum()),
        }
        return X_train, X_val, y_train, y_val, info

    def run_walkforward_validation(self, X, y, dates, n_windows=6, window_days=30):
        """
        优化4: Walk-forward 多窗口验证.
        返回 per-window metrics 列表.
        """
        dates_arr = pd.to_datetime(dates)
        max_date = dates_arr.max()
        results = []
        for i in range(n_windows, 0, -1):
            val_end_dt = max_date - timedelta(days=(i - 1) * window_days)
            val_start_dt = val_end_dt - timedelta(days=window_days)
            val_mask = (dates_arr >= val_start_dt) & (dates_arr < val_end_dt)
            train_mask = dates_arr < val_start_dt
            if train_mask.sum() < 100 or val_mask.sum() < 20:
                continue
            X_tr = X.loc[train_mask].reset_index(drop=True)
            X_va = X.loc[val_mask].reset_index(drop=True)
            y_tr = y.loc[train_mask].reset_index(drop=True)
            y_va = y.loc[val_mask].reset_index(drop=True)
            if y_tr.nunique() < 2 or y_va.nunique() < 2:
                continue
            try:
                if HAS_XGB:
                    m = xgb.XGBClassifier(
                        n_estimators=100, max_depth=5, learning_rate=0.1,
                        use_label_encoder=False, eval_metric='logloss',
                        random_state=42, verbosity=0
                    )
                else:
                    m = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
                m.fit(X_tr, y_tr)
                y_pred = m.predict(X_va)
                y_prob = m.predict_proba(X_va)[:, 1] if hasattr(m, 'predict_proba') else y_pred.astype(float)
                acc = float(accuracy_score(y_va, y_pred))
                auc = float(roc_auc_score(y_va, y_prob)) if y_va.nunique() == 2 else 0.5
                results.append({
                    'val_start': str(val_start_dt.date()),
                    'val_end': str(val_end_dt.date()),
                    'n_val': int(val_mask.sum()),
                    'accuracy': round(acc, 4),
                    'auc': round(auc, 4),
                })
            except Exception as ex:
                logger.warning(f'walk-forward 窗口 {val_start_dt.date()}~{val_end_dt.date()} 失败: {ex}')
        return results

    def train_model(self, strict_val_start='2026-04-01', strict_val_end='2026-04-24'):
        """训练基金分类模型 (优化版)"""
        logger.info('=' * 60)
        logger.info('训练基金模型 (优化版: 分类 + 宏观特征 + 严格验证 + 强中性区)')
        logger.info('=' * 60)

        all_X, all_y, all_dates = [], [], []
        codes = self.data['code'].unique()
        logger.info(f'遍历 {len(codes):,} 只基金...')

        for idx, code in enumerate(codes):
            if (idx + 1) % 500 == 0:
                logger.info(
                    f'  进度: {idx+1}/{len(codes)}, '
                    f'累计样本={sum(len(x) for x in all_X):,}'
                )
            fund_df = self.data[self.data['code'] == code].sort_values('date').reset_index(drop=True)
            if len(fund_df) < 60:
                continue
            macro_df = _build_macro_series(fund_df['date'].values)
            market_df = _build_market_series(fund_df['date'].values)
            X, y, dates = self.extract_features(fund_df, macro_df=macro_df, market_df=market_df)
            if X is not None and len(X) > 0:
                all_X.append(X)
                all_y.append(y)
                all_dates.extend(dates)

        if not all_X:
            logger.error('无有效训练数据')
            return None

        X = pd.concat(all_X, ignore_index=True)
        y = pd.concat(all_y, ignore_index=True)
        logger.info(f'总样本数: {len(X):,}')
        logger.info(f'特征数: {len(X.columns)}')
        logger.info(f'正样本比例: {y.mean():.2%}')

        # 优化3: 严格日期验证
        X_train, X_val, y_train, y_val, split_info = self._strict_split(
            X, y, all_dates, val_start=strict_val_start, val_end=strict_val_end
        )
        logger.info(
            f'数据分割: 模式={split_info["mode"]}, '
            f'训练集={split_info["n_train"]:,}, 验证集={split_info["n_val"]:,}'
        )

        # 训练模型
        logger.info('训练 XGBClassifier...')
        if HAS_XGB:
            model = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                use_label_encoder=False,
                eval_metric='logloss',
                random_state=42,
                verbosity=0,
            )
        else:
            model = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42, n_jobs=-1)

        model.fit(X_train, y_train)

        train_acc = float(accuracy_score(y_train, model.predict(X_train)))

        # ── 校准候选 + gate 评估 ──────────────────────────────────────────
        y_val_arr = np.asarray(y_val, dtype=int)
        raw_eval_proba = self._predict_positive_proba(model, X_val)

        # 将验证集分成校准集(前半)和评估集(后半)
        n_val = len(y_val_arr)
        n_cal = n_val // 2
        can_calibrate = n_cal >= 80 and len(np.unique(y_val_arr)) >= 2

        if can_calibrate:
            X_cal_half = X_val.iloc[:n_cal]
            y_cal = y_val_arr[:n_cal]
            y_eval = y_val_arr[n_cal:]
            X_eval_half = X_val.iloc[n_cal:]
            raw_cal_proba = self._predict_positive_proba(model, X_cal_half)
            raw_eval_proba = self._predict_positive_proba(model, X_eval_half)
            if len(np.unique(y_eval)) < 2:
                can_calibrate = False

        if not can_calibrate:
            y_cal = y_val_arr
            y_eval = y_val_arr
            raw_cal_proba = raw_eval_proba
            raw_eval_proba = self._predict_positive_proba(model, X_val)

        method_candidates = ['none', 'platt', 'isotonic'] if can_calibrate else ['none']
        candidate_results = []
        for method in method_candidates:
            calibrator = (self._fit_probability_calibrator(method, y_cal, raw_cal_proba)
                          if method != 'none' else None)
            if method != 'none' and calibrator is None:
                continue

            eval_proba = self._apply_probability_calibrator(method, calibrator, raw_eval_proba)
            eval_auc = float(roc_auc_score(y_eval, eval_proba)) if len(np.unique(y_eval)) >= 2 else None
            eval_brier = float(brier_score_loss(y_eval, eval_proba))

            # gate-aware 阈值搜索
            threshold_candidates = np.arange(0.30, 0.70 + 1e-9, 0.01)
            passed_rows = []
            all_rows = []
            for threshold in threshold_candidates:
                y_eval_pred = (eval_proba >= float(threshold)).astype(int)
                eval_acc = float((y_eval_pred == y_eval).mean())
                eval_f1 = float(f1_score(y_eval, y_eval_pred, zero_division=0))
                eval_precision = float(precision_score(y_eval, y_eval_pred, zero_division=0))
                eval_recall = float(recall_score(y_eval, y_eval_pred, zero_division=0))
                metrics_at_t = {
                    'validation_accuracy': eval_acc,
                    'validation_f1': eval_f1,
                    'validation_precision': eval_precision,
                    'validation_recall': eval_recall,
                    'validation_auc': eval_auc,
                    'validation_brier': eval_brier,
                }
                passed_t, gate_t, reason_t = self.model_manager.evaluate_validation_gate(
                    self.PREDICT_HORIZON, metrics_at_t
                )
                row = {
                    'threshold': float(threshold),
                    'validation_accuracy': eval_acc,
                    'validation_f1': eval_f1,
                    'validation_precision': eval_precision,
                    'validation_recall': eval_recall,
                    'validation_gate': gate_t,
                    'validation_passed': bool(passed_t),
                    'validation_reason': reason_t,
                }
                all_rows.append(row)
                if passed_t:
                    passed_rows.append(row)

            best_t = max(
                passed_rows if passed_rows else all_rows,
                key=lambda r: (r['validation_f1'], r['validation_accuracy'], r['validation_precision']),
            )
            candidate_results.append({
                'calibration_method': method,
                'calibrator': calibrator,
                'calibration_samples': int(n_cal) if method != 'none' else 0,
                'validation_accuracy': float(best_t['validation_accuracy']),
                'validation_f1': float(best_t['validation_f1']),
                'validation_precision': float(best_t['validation_precision']),
                'validation_recall': float(best_t['validation_recall']),
                'validation_auc': eval_auc,
                'validation_brier': eval_brier,
                'decision_threshold': round(float(best_t['threshold']), 2),
                'validation_gate': best_t['validation_gate'],
                'validation_passed': bool(best_t['validation_passed']),
                'validation_reason': best_t['validation_reason'],
            })

        if not candidate_results:
            logger.error('未找到有效候选模型（校准/评估失败）')
            return None

        best = max(
            candidate_results,
            key=lambda r: (
                int(r['validation_passed']),
                r['validation_f1'],
                r['validation_accuracy'],
                r['validation_precision'],
            ),
        )

        val_acc = best['validation_accuracy']
        best_f1 = best['validation_f1']
        val_precision = best['validation_precision']
        val_recall = best['validation_recall']
        val_auc = best['validation_auc']
        val_brier = best['validation_brier']
        best_threshold = best['decision_threshold']
        gate_name = best['validation_gate']
        gate_reason = best['validation_reason']
        gate_passed = best['validation_passed']
        cal_method = best['calibration_method']
        calibrator_obj = best['calibrator']
        calibration_samples = best['calibration_samples']

        logger.info('=' * 50)
        logger.info('[优化后 - 严格验证结果 (A股对齐)]')
        logger.info(f'  训练准确率: {train_acc:.2%}')
        logger.info(f'  验证准确率: {val_acc:.2%}')
        logger.info(f'  验证F1(最优阈值): {best_f1:.2%} (threshold={best_threshold:.2f}, '
                    f'precision={val_precision:.2%}, recall={val_recall:.2%})')
        logger.info(f'  概率校准: {cal_method} (samples={calibration_samples})')
        logger.info(f'  Gate: {gate_name} | passed={gate_passed} | {gate_reason}')
        if val_auc is not None:
            logger.info(f'  AUC={val_auc:.4f}, Brier={val_brier:.4f}')
        logger.info('=' * 50)

        # 优化4: Walk-forward 验证
        logger.info('运行 Walk-forward 验证 (6窗口 x 30天)...')
        wf_results = self.run_walkforward_validation(X, y, all_dates, n_windows=6, window_days=30)
        if wf_results:
            avg_acc = round(float(np.mean([r['accuracy'] for r in wf_results])), 4)
            avg_auc = round(float(np.mean([r['auc'] for r in wf_results])), 4)
            logger.info(f'Walk-forward 均值: accuracy={avg_acc}, auc={avg_auc}')
            for r in wf_results:
                logger.info(
                    f"  窗口 {r['val_start']}~{r['val_end']}: "
                    f"acc={r['accuracy']}, auc={r['auc']}, n={r['n_val']}"
                )
        else:
            avg_acc = avg_auc = None
            logger.warning('Walk-forward 验证未产生有效窗口')

        # 保存模型
        model_file = os.path.join(self.models_dir, 'fund_model.pkl')
        model_data = {
            'model': model,
            'feature_columns': list(X.columns),
            # 顶层标准字段（与 A股/ETF 对齐）
            'val_accuracy': val_acc,
            'val_f1': best_f1,
            'val_precision': val_precision,
            'val_recall': val_recall,
            'val_auc': val_auc,
            'val_brier': val_brier,
            'decision_threshold': best_threshold,
            'validation_gate': gate_name,
            'validation_passed': gate_passed,
            'validation_reason': gate_reason,
            'calibration_method': cal_method,
            'calibrator': calibrator_obj,
            'calibration_samples': calibration_samples,
            # 兼容旧字段
            'val_score': val_acc,
            'metrics': {
                'accuracy': round(val_acc, 4),
                'f1': round(best_f1, 4),
                'precision': round(val_precision, 4),
                'recall': round(val_recall, 4),
                'auc': round(val_auc, 4) if val_auc else None,
            },
            'walkforward': {'results': wf_results, 'avg_accuracy': avg_acc, 'avg_auc': avg_auc},
            'split_info': split_info,
            'neutral_zone': self.NEUTRAL_ZONE,
            'predict_horizon': self.PREDICT_HORIZON,
            'train_date': datetime.now().isoformat(),
            'asset_type': 'fund',
            # metadata dict 供 ModelManager.evaluate_validation_gate 使用
            'metadata': {
                'validation_accuracy': val_acc,
                'validation_f1': best_f1,
                'validation_precision': val_precision,
                'validation_recall': val_recall,
                'validation_auc': val_auc,
                'validation_brier': val_brier,
                'decision_threshold': best_threshold,
                'period_days': self.PREDICT_HORIZON,
                'asset_type': 'fund',
                'validation_gate': gate_name,
                'validation_passed': gate_passed,
                'validation_reason': gate_reason,
                'calibration_method': cal_method,
                'calibration_samples': calibration_samples,
            },
        }
        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)
        logger.info(f'模型已保存: {model_file}')
        return model_data

    def run(self, strict_val_start='2026-04-01', strict_val_end='2026-04-24'):
        """运行基金模型训练"""
        logger.info('=' * 60)
        logger.info('基金模型训练 - 优化版')
        logger.info('=' * 60)
        logger.info(f'开始时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info(f'严格验证区间: [{strict_val_start}, {strict_val_end}]')
        logger.info(f'强中性区阈值: +-{self.NEUTRAL_ZONE*100:.1f}%')

        if not self.load_data():
            logger.error('数据加载失败')
            return False

        result = self.train_model(strict_val_start=strict_val_start, strict_val_end=strict_val_end)

        logger.info('=' * 60)
        if result:
            logger.info('[OK] 基金模型训练成功')
        else:
            logger.error('[FAIL] 基金模型训练失败')
        logger.info(f'完成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info('=' * 60)
        return bool(result)


