# -*- coding: utf-8 -*-
"""贵金属模型训练器 - 迁移自 scripts/train_gold.py"""

"""
黄金/贵金属模型训练脚本 - 独立训练

用途: 针对黄金、白银等贵金属单独训练预测模型
数据源: 历史黄金价格 (通常是国际现货价格 XAUUSD/XAGUSD)
模型输出: data/models/gold_short_term_model.pkl, gold_medium_term_model.pkl, gold_long_term_model.pkl
         data/models/silver_short_term_model.pkl, silver_medium_term_model.pkl, silver_long_term_model.pkl
         并兼容写回 gold_model.pkl / silver_model.pkl 作为5日默认模型

贵金属特点:
  - 全球24小时交易 (无涨跌停限制)
  - 以美元计价 (受汇率影响)
  - 强趋势性 (比较容易形成趋势)
  - 避险资产 (与股市相关性较弱)

使用方式:
  python3 scripts/train_gold.py               # 完整训练 (黄金+白银)
  python3 scripts/train_gold.py --asset gold  # 仅训练黄金
"""

import sys
import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from pathlib import Path


from indicators.technical import TechnicalIndicator
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import GradientBoostingClassifier, ExtraTreesClassifier
from sklearn.dummy import DummyClassifier
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score, brier_score_loss

from predictors.model_manager import ModelManager

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestClassifier
    print("⚠️  XGBoost未安装，将使用RandomForest")


class GoldTrainer:
    """贵金属模型训练器"""

    GOLD_CODES = {'GC=F', 'XAUUSD=X', 'GLD', 'IAU', 'GLDM', 'SGOL', '518880.SH', '518800.SH', '159934.SZ'}
    SILVER_CODES = {'SI=F', 'XAGUSD=X', 'SLV', 'SIVR', 'PSLV'}
    HORIZON_NAME_MAP = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}
    
    def __init__(self):
        self.ti = TechnicalIndicator()
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        self.model_manager = ModelManager()
        self.gold_data = None
        self.silver_data = None

    @staticmethod
    def _predict_proba_or_score(model, X):
        if hasattr(model, 'predict_proba'):
            proba = np.asarray(model.predict_proba(X), dtype=float)
            if proba.ndim == 2 and proba.shape[1] > 1:
                return proba[:, 1]
            if proba.ndim == 2 and proba.shape[1] == 1:
                cls = getattr(model, 'classes_', [1])[0]
                return np.full(proba.shape[0], 1.0 if int(cls) == 1 else 0.0, dtype=float)
        raw = np.asarray(model.predict(X), dtype=float).reshape(-1)
        return np.clip(raw, 0, 1)

    @staticmethod
    def _find_best_threshold(y_true, y_proba, min_threshold=0.30, max_threshold=0.65, step=0.01):
        """F1-optimal 阈值搜索（对齐 A股/ETF/基金标准）。"""
        y_arr = np.asarray(y_true, dtype=int)
        p_arr = np.asarray(y_proba, dtype=float)
        best_threshold = 0.50
        best_f1 = -1.0
        for threshold in np.arange(min_threshold, max_threshold + 1e-9, step):
            pred = (p_arr >= threshold).astype(int)
            f1 = float(f1_score(y_arr, pred, zero_division=0))
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

    def _normalize_price_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        if 'trade_date' in frame.columns and 'date' not in frame.columns:
            frame['date'] = frame['trade_date']
        frame['date'] = pd.to_datetime(frame['date'])
        if 'nav' in frame.columns and 'close' not in frame.columns:
            frame['close'] = frame['nav']
        for col in ['open', 'high', 'low']:
            if col not in frame.columns:
                frame[col] = frame['close']
        if 'volume' not in frame.columns:
            frame['volume'] = 1.0
        frame['code'] = frame.get('code', frame.get('asset', 'UNKNOWN')).astype(str).str.upper().str.strip()
        return frame[['code', 'date', 'open', 'high', 'low', 'close', 'volume']].dropna(subset=['code', 'date', 'close'])

    def load_data(self):
        """加载贵金属数据"""
        print("\n" + "=" * 60)
        print("加载贵金属数据")
        print("=" * 60)

        gold_frames = []
        silver_frames = []

        print("\n[1/2] 加载黄金数据...")
        gold_files = [
            'data/gold_prices.csv',
            'data/xauusd.csv',
            'data/precious_metals.csv'
        ]

        for file in gold_files:
            if not os.path.exists(file):
                continue
            df = pd.read_csv(file)
            if 'code' in df.columns:
                df['code'] = df['code'].astype(str).str.upper().str.strip()
                gold = df[df['code'].isin(self.GOLD_CODES)].copy()
            elif 'asset' in df.columns or 'name' in df.columns:
                asset_col = 'asset' if 'asset' in df.columns else 'name'
                gold = df[df[asset_col].astype(str).str.contains('gold|黄金|XAUUSD', case=False, na=False)].copy()
            else:
                gold = df.copy()
            if len(gold) > 0:
                gold_frames.append(self._normalize_price_frame(gold))

        if gold_frames:
            gold = pd.concat(gold_frames, ignore_index=True)
            gold = gold.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date']).reset_index(drop=True)
            print(f"   ✅ 黄金数据: {len(gold):,} 条")
            print(f"   ✅ 时间范围: {gold['date'].min().date()} ~ {gold['date'].max().date()}")
            self.gold_data = gold

        print("\n[2/2] 加载白银数据...")
        silver_files = [
            'data/silver_prices.csv',
            'data/xagusd.csv',
            'data/precious_metals.csv'
        ]

        for file in silver_files:
            if not os.path.exists(file):
                continue
            df = pd.read_csv(file)
            if 'code' in df.columns:
                df['code'] = df['code'].astype(str).str.upper().str.strip()
                silver = df[df['code'].isin(self.SILVER_CODES)].copy()
            elif 'asset' in df.columns or 'name' in df.columns:
                asset_col = 'asset' if 'asset' in df.columns else 'name'
                silver = df[df[asset_col].astype(str).str.contains('silver|白银|XAGUSD', case=False, na=False)].copy()
            else:
                silver = df.copy()
            if len(silver) > 0:
                silver_frames.append(self._normalize_price_frame(silver))

        if silver_frames:
            silver = pd.concat(silver_frames, ignore_index=True)
            silver = silver.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date']).reset_index(drop=True)
            print(f"   ✅ 白银数据: {len(silver):,} 条")
            print(f"   ✅ 时间范围: {silver['date'].min().date()} ~ {silver['date'].max().date()}")
            self.silver_data = silver
        
        if self.gold_data is None and self.silver_data is None:
            try:
                from models import get_session, RawStockData
                session = get_session()
                rows = session.query(
                    RawStockData.code,
                    RawStockData.date,
                    RawStockData.open,
                    RawStockData.high,
                    RawStockData.low,
                    RawStockData.close,
                    RawStockData.volume,
                ).all()
                session.close()
                if rows:
                    raw_df = pd.DataFrame(rows, columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])
                    raw_df['code'] = raw_df['code'].astype(str).str.upper().str.strip()
                    raw_df['date'] = pd.to_datetime(raw_df['date'])

                    gold = raw_df[raw_df['code'].isin(self.GOLD_CODES)].copy()
                    silver = raw_df[raw_df['code'].isin(self.SILVER_CODES)].copy()
                    if not gold.empty:
                        gold['asset'] = gold['code']
                        self.gold_data = gold.sort_values('date').reset_index(drop=True)
                    if not silver.empty:
                        silver['asset'] = silver['code']
                        self.silver_data = silver.sort_values('date').reset_index(drop=True)
            except Exception as e:
                print(f"   ⚠️  数据库回退加载贵金属失败: {e}")

        if self.gold_data is None and self.silver_data is None:
            print("   ⚠️  未找到贵金属数据文件")
            print("   提示: 请先运行 scripts/prepare_training_datasets.py 补齐数据")
            return False
        
        return True
    
    def extract_features(self, df, asset_type='gold', forecast_horizon=5):
        """从贵金属数据提取特征（优化版：去常量特征，加 ATR/BB/动量加速度/中性区过滤）"""
        if df is None or len(df) < max(65, 70 + int(forecast_horizon or 5)):
            return None, None

        close = df['close'].values if 'close' in df.columns else df['price'].values
        high  = df['high'].values  if 'high'  in df.columns else close
        low   = df['low'].values   if 'low'   in df.columns else close
        volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))

        X_list = []
        y_list = []
        horizon = int(forecast_horizon or 5)
        base_step = 5 if horizon <= 5 else (8 if horizon <= 20 else 10)
        step_size = base_step if asset_type == 'gold' else max(4, base_step - 1)

        # 中性区过滤：过滤掉绝对收益太小的噪声样本
        neutral_threshold = 0.0 if horizon <= 5 else (0.02 if horizon <= 20 else 0.03)

        for i in range(65, len(close) - forecast_horizon, step_size):
            if i + forecast_horizon >= len(close):
                continue
            future_return = (close[i + forecast_horizon] - close[i]) / close[i]

            # 中性区过滤（medium/long 仅保留有显著方向的样本）
            if neutral_threshold > 0 and abs(future_return) < neutral_threshold:
                continue

            label = 1 if future_return > 0 else 0

            window_close  = close[:i + 1]
            window_high   = high[:i + 1]
            window_low    = low[:i + 1]
            window_volume = volume[:i + 1]

            features = {}

            # ── RSI ──
            rsi = self.ti.calculate_rsi(pd.Series(window_close))
            features['rsi'] = rsi if not np.isnan(rsi) else 50.0

            # ── MACD ──
            macd = self.ti.calculate_macd(pd.Series(window_close))
            features['macd_hist'] = macd['hist'] if not np.isnan(macd['hist']) else 0.0

            # ── 移动均线比 ──
            ma5  = self.ti.calculate_ma(pd.Series(window_close), 5)
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20
            features['price_ma5_ratio']  = (window_close[-1] / ma5  - 1) if ma5  > 0 else 0.0
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 > 0 else 0.0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 > 0 else 0.0

            # ── 量价 ──
            avg_vol = np.mean(window_volume[-20:]) if len(window_volume) >= 20 else window_volume[-1]
            features['volume_ratio'] = window_volume[-1] / avg_vol if avg_vol > 0 else 1.0

            # ── 波动率 ──
            returns = np.diff(window_close) / (window_close[:-1] + 1e-9)
            features['volatility']    = float(np.std(returns[-20:]) * np.sqrt(252)) if len(returns) >= 20 else 0.3
            features['volatility_5d'] = float(np.std(returns[-5:])  * np.sqrt(252)) if len(returns) >= 5  else 0.3

            # ── 收益率动量 ──
            features['return_5d']  = (window_close[-1] - window_close[-6])  / window_close[-6]  if len(window_close) >= 6  else 0.0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0.0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0.0
            features['return_60d'] = (window_close[-1] - window_close[-61]) / window_close[-61] if len(window_close) >= 61 else 0.0

            # ── 动量加速度（短期动量 vs 中期动量，判断趋势提速/减速）──
            features['momentum_accel'] = features['return_5d'] - features['return_10d'] / 2.0

            # ── 60日内价格位置（百分比位置）──
            price_min = float(np.min(window_close[-60:])) if len(window_close) >= 60 else float(window_close[0])
            price_max = float(np.max(window_close[-60:])) if len(window_close) >= 60 else float(window_close[0])
            features['price_position'] = (window_close[-1] - price_min) / (price_max - price_min + 1e-9)

            # ── 20日高低通道位置（原 high_low_ratio，语义更清晰）──
            h20 = float(np.max(window_high[-20:])) if len(window_high) >= 20 else window_close[-1]
            l20 = float(np.min(window_low[-20:]))  if len(window_low)  >= 20 else window_close[-1]
            features['channel_position'] = (window_close[-1] - l20) / (h20 - l20 + 1e-9)

            # ── 布林带位置 ──
            if len(window_close) >= 20:
                bb_mid = float(np.mean(window_close[-20:]))
                bb_std = float(np.std(window_close[-20:]) + 1e-9)
                bb_upper = bb_mid + 2 * bb_std
                bb_lower = bb_mid - 2 * bb_std
                features['bb_position'] = (window_close[-1] - bb_lower) / (bb_upper - bb_lower + 1e-9)
            else:
                features['bb_position'] = 0.5

            # ── ATR 比率（真实波动幅度 / 价格，贵金属趋势信号）──
            if len(window_high) >= 14 and len(window_low) >= 14:
                tr_arr = np.maximum(
                    window_high[-14:] - window_low[-14:],
                    np.maximum(
                        np.abs(window_high[-14:] - np.roll(window_close[-15:], 1)[1:]),
                        np.abs(window_low[-14:]  - np.roll(window_close[-15:], 1)[1:])
                    )
                )
                atr = float(np.mean(tr_arr))
                features['atr_ratio'] = atr / (window_close[-1] + 1e-9)
            else:
                features['atr_ratio'] = 0.01

            # ── 趋势强度 ──
            up_days = int(np.sum(returns[-20:] > 0)) if len(returns) >= 20 else 10
            features['uptrend_strength'] = up_days / 20.0

            # ── NaN/Inf 清理 ──
            for k in list(features.keys()):
                v = features[k]
                if not isinstance(v, (int, float)) or np.isnan(v) or np.isinf(v):
                    features[k] = 0.0

            X_list.append(list(features.values()))
            y_list.append(label)

        if len(X_list) == 0:
            return None, None

        feature_keys = list(features.keys())
        X = pd.DataFrame(X_list, columns=feature_keys)
        y = pd.Series(y_list)
        return X, y
    
    @classmethod
    def _horizon_key(cls, period_days):
        return cls.HORIZON_NAME_MAP.get(int(period_days or 5), f"{int(period_days or 5)}d")

    def train_model(self, asset_type='gold', period_days=5):
        """训练贵金属模型"""
        asset_name = '黄金' if asset_type == 'gold' else '白银'
        horizon = int(period_days or 5)
        horizon_key = self._horizon_key(horizon)

        print(f"\n{'='*60}")
        print(f"训练{asset_name} {horizon}日模型")
        print(f"{'='*60}")

        if asset_type == 'gold':
            data = self.gold_data
        else:
            data = self.silver_data

        if data is None:
            print(f"  ❌ {asset_name}数据不可用")
            return None

        train_parts = []
        train_labels = []
        val_parts = []
        val_labels = []

        for code in sorted(data['code'].astype(str).unique()):
            asset_df = data[data['code'].astype(str) == code].copy().sort_values('date').reset_index(drop=True)
            if len(asset_df) < max(80, 70 + horizon):
                continue
            X, y = self.extract_features(asset_df, asset_type, forecast_horizon=horizon)
            min_samples = 20 if horizon <= 20 else 8
            if X is None or len(X) < min_samples:
                continue
            split_idx = max(1, int(len(X) * 0.8))
            split_idx = min(split_idx, len(X) - 1)
            train_parts.append(X.iloc[:split_idx])
            train_labels.append(y.iloc[:split_idx])
            val_parts.append(X.iloc[split_idx:])
            val_labels.append(y.iloc[split_idx:])

        if not train_parts or not val_parts:
            print(f"  ❌ 无有效训练数据")
            return None

        X_train = pd.concat(train_parts, ignore_index=True)
        y_train = pd.concat(train_labels, ignore_index=True)
        X_val = pd.concat(val_parts, ignore_index=True)
        y_val = pd.concat(val_labels, ignore_index=True)
        X = pd.concat([X_train, X_val], ignore_index=True)
        y = pd.concat([y_train, y_val], ignore_index=True)

        print(f"  ✅ 总样本数: {len(X):,}")
        print(f"  ✅ 特征数: {len(X.columns)}")
        print(f"  ✅ 正样本比例: {y.sum()/len(y):.2%}")
        print(f"  训练集: {len(X_train):,} 样本 | 验证集: {len(X_val):,} 样本")

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)

        print(f"  训练{asset_name}{horizon}日模型中...")
        if int(pd.Series(y_train).nunique()) < 2:
            baseline_label = int(pd.Series(y_train).iloc[0]) if len(y_train) else 0
            print(f"  ⚠️ 当前周期样本标签单一，使用稳定基线模型: label={baseline_label}")
            model = DummyClassifier(strategy='constant', constant=baseline_label)
        elif asset_type == 'gold':
            # 黄金数据量少（通常仅1个标的，约200-400个训练样本），需强正则化防过拟合
            if HAS_XGB:
                model = xgb.XGBClassifier(
                    n_estimators=80 if horizon <= 20 else 120,
                    max_depth=2,
                    learning_rate=0.05,
                    subsample=0.7,
                    colsample_bytree=0.7,
                    min_child_weight=8,
                    reg_lambda=10.0,
                    reg_alpha=1.0,
                    scale_pos_weight=1.0,
                    use_label_encoder=False,
                    eval_metric='logloss',
                    random_state=42,
                    n_jobs=-1,
                )
            else:
                model = ExtraTreesClassifier(
                    n_estimators=80 if horizon <= 20 else 120,
                    max_depth=3,
                    min_samples_leaf=10,
                    class_weight='balanced',
                    random_state=42,
                    n_jobs=-1,
                )
        else:
            # 白银同样数据量少，参数与黄金对齐
            if HAS_XGB:
                model = xgb.XGBClassifier(
                    n_estimators=80 if horizon <= 20 else 120,
                    max_depth=2,
                    learning_rate=0.04 if horizon <= 20 else 0.03,
                    subsample=0.7,
                    colsample_bytree=0.7,
                    min_child_weight=8,
                    reg_lambda=10.0,
                    reg_alpha=1.0,
                    use_label_encoder=False,
                    eval_metric='logloss',
                    random_state=42,
                    n_jobs=-1,
                )
            else:
                model = GradientBoostingClassifier(
                    n_estimators=50 if horizon <= 20 else 80,
                    learning_rate=0.04 if horizon <= 20 else 0.03,
                    max_depth=2,
                    min_samples_leaf=10,
                    random_state=42,
                )
        model.fit(X_train_scaled, y_train)

        train_acc = float(((self._predict_proba_or_score(model, X_train_scaled) >= 0.5).astype(int) ==
                            np.asarray(y_train, dtype=int)).mean())

        # ── 校准候选 + gate 评估（A股标准对齐）──────────────────────────
        y_val_arr = np.asarray(y_val, dtype=int)
        raw_eval_proba = self._predict_proba_or_score(model, X_val_scaled)

        n_val = len(y_val_arr)
        n_cal = n_val // 2
        can_calibrate = n_cal >= 80 and len(np.unique(y_val_arr)) >= 2

        if can_calibrate:
            X_cal_scaled_half = X_val_scaled[:n_cal]
            y_cal = y_val_arr[:n_cal]
            y_eval_arr = y_val_arr[n_cal:]
            X_eval_scaled_half = X_val_scaled[n_cal:]
            raw_cal_proba = self._predict_proba_or_score(model, X_cal_scaled_half)
            raw_eval_proba = self._predict_proba_or_score(model, X_eval_scaled_half)
            if len(np.unique(y_eval_arr)) < 2:
                can_calibrate = False

        if not can_calibrate:
            y_cal = y_val_arr
            y_eval_arr = y_val_arr
            raw_cal_proba = raw_eval_proba
            raw_eval_proba = self._predict_proba_or_score(model, X_val_scaled)

        method_candidates = ['none', 'platt', 'isotonic'] if can_calibrate else ['none']
        candidate_results = []
        for method in method_candidates:
            calibrator = (self._fit_probability_calibrator(method, y_cal, raw_cal_proba)
                          if method != 'none' else None)
            if method != 'none' and calibrator is None:
                continue

            eval_proba = self._apply_probability_calibrator(method, calibrator, raw_eval_proba)
            eval_auc = float(roc_auc_score(y_eval_arr, eval_proba)) if len(np.unique(y_eval_arr)) >= 2 else None
            eval_brier = float(brier_score_loss(y_eval_arr, eval_proba))

            threshold_candidates = np.arange(0.30, 0.65 + 1e-9, 0.01)
            passed_rows = []
            all_rows = []
            for threshold in threshold_candidates:
                y_eval_pred = (eval_proba >= float(threshold)).astype(int)
                eval_acc = float((y_eval_pred == y_eval_arr).mean())
                eval_f1 = float(f1_score(y_eval_arr, y_eval_pred, zero_division=0))
                eval_precision = float(precision_score(y_eval_arr, y_eval_pred, zero_division=0))
                eval_recall = float(recall_score(y_eval_arr, y_eval_pred, zero_division=0))
                metrics_at_t = {
                    'validation_accuracy': eval_acc,
                    'validation_f1': eval_f1,
                    'validation_precision': eval_precision,
                    'validation_recall': eval_recall,
                    'validation_auc': eval_auc,
                    'validation_brier': eval_brier,
                }
                passed_t, gate_t, reason_t = self.model_manager.evaluate_validation_gate(horizon, metrics_at_t)
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
            # AUC < 0.50 说明模型方向反了，强制 gate 失败（比随机预测更差）
            final_gate_passed = bool(best_t['validation_passed'])
            final_gate = best_t['validation_gate']
            final_reason = best_t['validation_reason']
            if eval_auc is not None and eval_auc < 0.50:
                final_gate_passed = False
                final_gate = 'failed'
                final_reason = f"AUC={eval_auc:.4f} < 0.50 (reversed model, worse than random)"
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
                'validation_gate': final_gate,
                'validation_passed': final_gate_passed,
                'validation_reason': final_reason,
            })

        if not candidate_results:
            print(f"  ❌ 未找到有效候选模型（校准/评估失败）")
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

        # 过拟合检测：train_acc 和 val_acc 都极高，说明模型记住了小验证集
        if gate_passed and train_acc >= 0.95 and val_acc >= 0.97:
            gate_passed = False
            gate_name = 'failed'
            gate_reason = f'overfitting: train_acc={train_acc:.4f}, val_acc={val_acc:.4f}'
        # 小验证集检测：验证样本太少，评估结果不可信
        if gate_passed and len(y_val_arr) < 40:
            gate_passed = False
            gate_name = 'failed'
            gate_reason = f'val_samples={len(y_val_arr)} < 40 (too small, evaluation unreliable)'

        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")
        print(f"  📊 验证F1(最优阈值): {best_f1:.2%} (threshold={best_threshold:.2f}, "
              f"precision={val_precision:.2%}, recall={val_recall:.2%})")
        print(f"  📊 概率校准: {cal_method} (samples={calibration_samples}) | "
              f"gate={gate_name} | passed={gate_passed}")
        if val_auc is not None:
            print(f"  📊 AUC={val_auc:.4f}, Brier={val_brier:.4f}")

        model_file = os.path.join(self.models_dir, f'{asset_type}_{horizon_key}_model.pkl')
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'val_f1': best_f1,
            'val_precision': val_precision,
            'val_recall': val_recall,
            'val_auc': val_auc,
            'val_brier': val_brier,
            'train_date': datetime.now().isoformat(),
            'period_days': horizon,
            'asset_type': asset_type,
            'decision_threshold': best_threshold,
            'validation_gate': gate_name,
            'validation_passed': gate_passed,
            'validation_reason': gate_reason,
            'calibration_method': cal_method,
            'calibrator': calibrator_obj,
            'calibration_samples': calibration_samples,
            'horizon_key': horizon_key,
            'metadata': {
                'validation_accuracy': val_acc,
                'validation_f1': best_f1,
                'validation_precision': val_precision,
                'validation_recall': val_recall,
                'validation_auc': val_auc,
                'validation_brier': val_brier,
                'decision_threshold': best_threshold,
                'period_days': horizon,
                'asset_type': asset_type,
                'validation_gate': gate_name,
                'validation_passed': gate_passed,
                'validation_reason': gate_reason,
                'calibration_method': cal_method,
                'calibration_samples': calibration_samples,
            },
        }

        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)

        # legacy 默认模型：仅 short_term 且 gate 通过时写入
        if horizon == 5 and gate_passed:
            legacy_file = os.path.join(self.models_dir, f'{asset_type}_model.pkl')
            with open(legacy_file, 'wb') as f:
                pickle.dump(model_data, f)
        elif horizon == 5 and not gate_passed:
            print(f"  ⚠️ {asset_type} 5日模型未通过validation gate，保留现有 {asset_type}_model.pkl 不覆盖")

        print(f"  ✅ 模型已保存: {model_file}")

        return model_data
    
    def run(self, selected_assets=None, horizons=None):
        """运行贵金属模型训练"""
        print("=" * 60)
        print("贵金属模型训练 - 独立训练脚本")
        print("=" * 60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if not self.load_data():
            print("\n❌ 数据加载失败")
            return False

        selected = set(selected_assets or ['gold', 'silver'])
        target_horizons = [int(item) for item in (horizons or [5, 20, 60]) if int(item) > 0]
        results = {}

        for asset_type in ['gold', 'silver']:
            if asset_type not in selected:
                continue
            data = self.gold_data if asset_type == 'gold' else self.silver_data
            if data is None:
                results[asset_type] = {}
                continue
            results[asset_type] = {}
            for horizon in target_horizons:
                results[asset_type][horizon] = self.train_model(asset_type, period_days=horizon)

        print("\n" + "=" * 60)
        print("贵金属训练完成汇总")
        print("=" * 60)

        for asset_type, horizon_results in results.items():
            for horizon, result in horizon_results.items():
                if result:
                    acc = result.get('val_accuracy', 0)
                    print(f"  ✅ {asset_type} {horizon}日: 验证准确率 {acc:.2%}")
                else:
                    print(f"  ❌ {asset_type} {horizon}日: 训练失败")

        print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        return all(bool(result) for horizon_results in results.values() for result in horizon_results.values()) if results else False


