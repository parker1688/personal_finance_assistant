#!/usr/bin/env python3
"""
ETF模型训练脚本 - 独立训练

用途: 针对ETF单独训练预测模型
数据源: 历史ETF价格 (优先本地CSV/数据库)
模型输出: data/models/etf_short_term_model.pkl, etf_medium_term_model.pkl, etf_long_term_model.pkl
         并兼容写回 etf_model.pkl 作为默认ETF模型
"""

import sys
import os
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.dummy import DummyClassifier

sys.path.append(str(Path(__file__).resolve().parent.parent))

from indicators.technical import TechnicalIndicator
from recommenders.etf_recommender import ETFRecommender

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestClassifier
    print('⚠️  XGBoost未安装，将使用RandomForest')


class ETFTrainer:
    """ETF模型训练器"""

    COMMODITY_ETF_CODES = {'GLD', 'IAU', 'GLDM', 'SGOL', 'SLV', 'SIVR', 'PSLV', '518880.SH', '518800.SH', '159934.SZ'}
    HORIZON_NAME_MAP = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}

    @staticmethod
    def _normalize_code(code: str) -> str:
        return str(code or '').upper().strip().replace('.SH', '').replace('.SZ', '')

    def __init__(self):
        self.ti = TechnicalIndicator()
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        self.data = None
        self.etf_pool = ETFRecommender().etf_pool
        raw_meta = {str(item['code']).upper().strip(): item for item in self.etf_pool}
        raw_meta.update({
            'GLD': {'code': 'GLD', 'name': 'SPDR Gold Trust', 'type': '商品', 'fee': 0.40},
            'IAU': {'code': 'IAU', 'name': 'iShares Gold Trust', 'type': '商品', 'fee': 0.25},
            'GLDM': {'code': 'GLDM', 'name': 'SPDR Gold MiniShares', 'type': '商品', 'fee': 0.10},
            'SGOL': {'code': 'SGOL', 'name': 'abrdn Physical Gold Shares', 'type': '商品', 'fee': 0.17},
            'SLV': {'code': 'SLV', 'name': 'iShares Silver Trust', 'type': '商品', 'fee': 0.50},
            'SIVR': {'code': 'SIVR', 'name': 'abrdn Physical Silver Shares', 'type': '商品', 'fee': 0.30},
            'PSLV': {'code': 'PSLV', 'name': 'Sprott Physical Silver Trust', 'type': '商品', 'fee': 0.45},
        })
        self.etf_meta = {}
        for key, value in raw_meta.items():
            self.etf_meta[str(key).upper().strip()] = value
            self.etf_meta[self._normalize_code(key)] = value
        self.allowed_etf_codes = set(self.etf_meta.keys()) | {self._normalize_code(item) for item in self.COMMODITY_ETF_CODES}

    def _normalize_frame(self, df: pd.DataFrame) -> pd.DataFrame:
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
        keep_cols = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
        frame = frame[[col for col in keep_cols if col in frame.columns]].copy()
        frame = frame.dropna(subset=['code', 'date', 'close'])
        frame['code'] = frame['code'].astype(str).str.upper().str.strip()
        return frame.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'])

    def _is_etf_like(self, code: str, name: str = '') -> bool:
        code = str(code or '').upper().strip()
        normalized = self._normalize_code(code)
        name = str(name or '').upper().strip()
        if not code:
            return False
        if code in self.allowed_etf_codes or normalized in self.allowed_etf_codes:
            return True
        if 'ETF' in name or 'TRUST' in name:
            return True
        numeric = code.replace('.SH', '').replace('.SZ', '')
        return numeric.isdigit() and len(numeric) >= 6 and numeric.startswith(('510', '511', '512', '513', '515', '516', '517', '518', '159', '588'))

    def load_data(self):
        """加载ETF数据"""
        print('\n' + '=' * 60)
        print('加载ETF数据')
        print('=' * 60)

        frames = []

        csv_candidates = [
            'data/historical_etf.csv',
            'data/historical_a_stock.csv',
            'data/fund_nav.csv',
            'data/historical_funds.csv',
        ]

        for file in csv_candidates:
            if not os.path.exists(file):
                continue
            try:
                df = pd.read_csv(file)
                if 'code' not in df.columns:
                    continue
                df['code'] = df['code'].astype(str).str.upper().str.strip()
                name_col = 'name' if 'name' in df.columns else ('asset' if 'asset' in df.columns else None)
                if file.endswith('historical_etf.csv'):
                    subset = df.copy()
                else:
                    subset = df[df.apply(lambda row: self._is_etf_like(row.get('code'), row.get(name_col, '')), axis=1)].copy()
                if not subset.empty:
                    frames.append(self._normalize_frame(subset))
                    print(f'   ✅ 已加载ETF数据文件: {file} ({len(subset):,} 条)')
            except Exception as e:
                print(f'   ⚠️  读取 {file} 失败: {e}')

        if not frames:
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
                    db_df = pd.DataFrame(rows, columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])
                    db_df['code'] = db_df['code'].astype(str).str.upper().str.strip()
                    db_df = db_df[db_df['code'].apply(lambda x: self._is_etf_like(x, ''))].copy()
                    if not db_df.empty:
                        frames.append(self._normalize_frame(db_df))
                        print(f'   ✅ 已从数据库加载ETF数据: {len(db_df):,} 条')
            except Exception as e:
                print(f'   ⚠️  从数据库读取ETF数据失败: {e}')

        if not frames:
            print('   ❌ 未找到可训练的ETF历史数据')
            print('   提示: 请先采集ETF行情或补齐本地历史文件')
            return False

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'])
        self.data = combined
        print(f'   ✅ ETF总样本: {len(combined):,} 条, {combined["code"].nunique()} 只ETF')
        print(f'   ✅ 时间范围: {combined["date"].min().date()} ~ {combined["date"].max().date()}')
        return True

    def _encode_etf_type(self, code: str) -> dict:
        meta = self.etf_meta.get(code, {})
        etf_type = meta.get('type', '')
        if not etf_type:
            if code in self.COMMODITY_ETF_CODES:
                etf_type = '商品'
            elif str(code).startswith(('510300', '510500', '510050', '159915', '588000')):
                etf_type = '宽基'
            else:
                etf_type = '行业'
        return {
            'is_broad_index': 1 if etf_type == '宽基' else 0,
            'is_sector_theme': 1 if etf_type == '行业' else 0,
            'is_commodity': 1 if etf_type == '商品' else 0,
            'expense_fee_bp': float(meta.get('fee', 0.2)),
        }

    def extract_features(self, df: pd.DataFrame, period_days=5):
        """提取ETF特征与标签"""
        horizon = int(period_days or 5)
        if df is None or len(df) < max(80, 70 + horizon):
            return None, None

        close = df['close'].astype(float).values
        high = df['high'].astype(float).values
        low = df['low'].astype(float).values
        volume = df['volume'].astype(float).replace(0, 1).values
        code = str(df['code'].iloc[0]).upper()
        type_features = self._encode_etf_type(code)

        X_list = []
        y_list = []
        step_size = 5 if horizon <= 5 else (8 if horizon <= 20 else 10)

        for i in range(60, len(close) - horizon, step_size):
            window_close = close[:i + 1]
            window_high = high[:i + 1]
            window_low = low[:i + 1]
            window_volume = volume[:i + 1]
            returns = np.diff(window_close) / np.clip(window_close[:-1], 1e-8, None)

            features = dict(type_features)
            features['rsi'] = float(self.ti.calculate_rsi(pd.Series(window_close)) or 50)
            macd = self.ti.calculate_macd(pd.Series(window_close))
            features['macd_hist'] = float(macd.get('hist', 0) or 0)

            ma5 = self.ti.calculate_ma(pd.Series(window_close), 5)
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20

            features['price_ma5_ratio'] = (window_close[-1] / ma5 - 1) if ma5 else 0
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 else 0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 else 0
            features['return_5d'] = (window_close[-1] - window_close[-6]) / window_close[-6] if len(window_close) >= 6 else 0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0
            features['volatility_10d'] = float(np.std(returns[-10:]) * np.sqrt(252)) if len(returns) >= 10 else 0
            features['volatility_20d'] = float(np.std(returns[-20:]) * np.sqrt(252)) if len(returns) >= 20 else 0

            avg_volume = float(np.mean(window_volume[-20:])) if len(window_volume) >= 20 else float(window_volume[-1])
            features['volume_ratio'] = float(window_volume[-1] / avg_volume) if avg_volume > 0 else 1.0

            recent_high = float(np.max(window_high[-20:])) if len(window_high) >= 20 else float(window_high[-1])
            recent_low = float(np.min(window_low[-20:])) if len(window_low) >= 20 else float(window_low[-1])
            features['breakout_20d'] = (window_close[-1] - recent_low) / (recent_high - recent_low) if recent_high > recent_low else 0.5
            features['drawdown_20d'] = (window_close[-1] / recent_high - 1) if recent_high > 0 else 0
            features['up_days_ratio_20d'] = float(np.mean(returns[-20:] > 0)) if len(returns) >= 20 else 0.5

            for key, value in list(features.items()):
                if pd.isna(value) or np.isinf(value):
                    features[key] = 0.0

            future_return = (close[i + horizon] - close[i]) / close[i]
            label_threshold = 0.003 if horizon <= 5 else (0.006 if horizon <= 20 else 0.012)
            label = 1 if future_return > label_threshold else 0

            X_list.append(list(features.values()))
            y_list.append(label)

        if not X_list:
            return None, None

        X = pd.DataFrame(X_list, columns=list(features.keys()))
        y = pd.Series(y_list)
        return X, y

    @classmethod
    def _horizon_key(cls, period_days):
        return cls.HORIZON_NAME_MAP.get(int(period_days or 5), f"{int(period_days or 5)}d")

    def train_model(self, period_days=5):
        """训练ETF模型"""
        horizon = int(period_days or 5)
        horizon_key = self._horizon_key(horizon)
        print(f"\n{'=' * 60}")
        print(f'训练ETF {horizon}日模型')
        print(f"{'=' * 60}")

        train_parts = []
        train_labels = []
        val_parts = []
        val_labels = []

        codes = sorted(
            code for code in self.data['code'].unique()
            if self._normalize_code(code) in self.allowed_etf_codes
        )
        if not codes:
            codes = sorted(self.data['code'].unique())
        print(f'  遍历 {len(codes)} 只ETF...')

        for idx, code in enumerate(codes, start=1):
            if idx % 20 == 0:
                print(f'    进度: {idx}/{len(codes)}')
            etf_df = self.data[self.data['code'] == code].copy().sort_values('date').reset_index(drop=True)
            if len(etf_df) < max(80, 70 + horizon):
                continue
            X, y = self.extract_features(etf_df, period_days=horizon)
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
            print('  ❌ 无有效ETF训练样本')
            return None

        X_train = pd.concat(train_parts, ignore_index=True)
        y_train = pd.concat(train_labels, ignore_index=True)
        X_val = pd.concat(val_parts, ignore_index=True)
        y_val = pd.concat(val_labels, ignore_index=True)
        X = pd.concat([X_train, X_val], ignore_index=True)
        y = pd.concat([y_train, y_val], ignore_index=True)

        print(f'  ✅ 总样本数: {len(X):,}')
        print(f'  ✅ 特征数: {len(X.columns)}')
        print(f'  ✅ 正样本比例: {float(y.mean()):.2%}')

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)

        print(f'  训练ETF {horizon}日模型中...')
        if int(pd.Series(y_train).nunique()) < 2:
            baseline_label = int(pd.Series(y_train).iloc[0]) if len(y_train) else 0
            print(f'  ⚠️ 当前周期样本标签单一，使用稳定基线模型: label={baseline_label}')
            model = DummyClassifier(strategy='constant', constant=baseline_label)
            model.fit(X_train_scaled, y_train)
        elif HAS_XGB:
            pos = max(float(y_train.mean()), 1e-6)
            neg = max(1.0 - pos, 1e-6)
            model = xgb.XGBClassifier(
                n_estimators=160 if horizon <= 20 else 220,
                max_depth=3 if horizon <= 20 else 4,
                learning_rate=0.05 if horizon <= 20 else 0.04,
                subsample=0.9,
                colsample_bytree=0.9,
                min_child_weight=4,
                gamma=0.1,
                reg_lambda=2.0,
                reg_alpha=0.2,
                scale_pos_weight=neg / pos,
                random_state=42,
                eval_metric='logloss',
            )
            model.fit(X_train_scaled, y_train, eval_set=[(X_val_scaled, y_val)], verbose=False)
        else:
            model = RandomForestClassifier(
                n_estimators=160,
                max_depth=8,
                min_samples_leaf=3,
                random_state=42,
                n_jobs=-1,
            )
            model.fit(X_train_scaled, y_train)

        train_acc = float(model.score(X_train_scaled, y_train))
        val_acc = float(model.score(X_val_scaled, y_val))

        print(f'  📊 训练准确率: {train_acc:.2%}')
        print(f'  📊 验证准确率: {val_acc:.2%}')

        model_file = os.path.join(self.models_dir, f'etf_{horizon_key}_model.pkl')
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': horizon,
            'asset_type': 'etf',
            'horizon_key': horizon_key,
        }

        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)

        if horizon == 20:
            with open(os.path.join(self.models_dir, 'etf_model.pkl'), 'wb') as f:
                pickle.dump(model_data, f)

        print(f'  ✅ 模型已保存: {model_file}')
        return model_data

    def run(self, horizons=None):
        print('=' * 60)
        print('ETF模型训练 - 独立训练脚本')
        print('=' * 60)
        print(f'开始时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        if not self.load_data():
            print('\n❌ 数据加载失败')
            return False

        target_horizons = [int(item) for item in (horizons or [5, 20, 60]) if int(item) > 0]
        results = []
        for horizon in target_horizons:
            results.append(self.train_model(period_days=horizon))

        print('\n' + '=' * 60)
        if all(bool(item) for item in results):
            print('✅ ETF多周期模型训练成功')
        else:
            print('❌ ETF模型训练存在失败周期')
        print(f'完成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('=' * 60)
        return all(bool(item) for item in results)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='ETF模型训练脚本')
    parser.add_argument('--periods', default='5,20,60', help='训练周期，支持 all 或逗号分隔，如 5,20,60')
    args = parser.parse_args()

    if str(args.periods).strip().lower() == 'all':
        horizons = [5, 20, 60]
    else:
        horizons = [int(item.strip()) for item in str(args.periods).split(',') if item.strip()]
        horizons = [item for item in horizons if item > 0]

    trainer = ETFTrainer()
    raise SystemExit(0 if trainer.run(horizons=horizons) else 1)
