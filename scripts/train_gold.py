#!/usr/bin/env python3
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

sys.path.append(str(Path(__file__).resolve().parent.parent))

from indicators.technical import TechnicalIndicator
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import GradientBoostingClassifier, ExtraTreesClassifier
from sklearn.dummy import DummyClassifier

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
        y_arr = np.asarray(y_true, dtype=int)
        p_arr = np.asarray(y_proba, dtype=float)
        best_threshold = float(min_threshold)
        best_acc = -1.0
        for threshold in np.arange(min_threshold, max_threshold + 1e-9, step):
            pred = (p_arr >= threshold).astype(int)
            acc = float((pred == y_arr).mean())
            if acc > best_acc:
                best_threshold = float(threshold)
                best_acc = acc
        return round(best_threshold, 2), best_acc

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
        """从贵金属数据提取特征"""
        if df is None or len(df) < max(60, 70 + int(forecast_horizon or 5)):
            return None, None
        
        code = str(df['code'].iloc[0]).upper() if 'code' in df.columns and len(df) > 0 else ''
        close = df['close'].values if 'close' in df.columns else df['price'].values
        high = df['high'].values if 'high' in df.columns else close
        low = df['low'].values if 'low' in df.columns else close
        volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))
        
        X_list = []
        y_list = []
        horizon = int(forecast_horizon or 5)
        base_step = 5 if horizon <= 5 else (8 if horizon <= 20 else 10)
        # 白银样本更易受高频噪声影响，适度降低重叠样本密度
        step_size = base_step if asset_type == 'gold' else max(4, base_step - 1)
        
        for i in range(60, len(close) - forecast_horizon, step_size):
            features = {}
            
            window_close = close[:i+1]
            window_high = high[:i+1]
            window_low = low[:i+1]
            window_volume = volume[:i+1]
            
            # 技术指标
            features['is_futures_proxy'] = 1 if code.endswith('=F') else 0
            features['is_fx_proxy'] = 1 if code.endswith('=X') else 0
            features['is_etf_proxy'] = 1 if not code.endswith(('=F', '=X')) else 0

            rsi = self.ti.calculate_rsi(pd.Series(window_close))
            features['rsi'] = rsi if not np.isnan(rsi) else 50
            
            macd = self.ti.calculate_macd(pd.Series(window_close))
            features['macd_hist'] = macd['hist'] if not np.isnan(macd['hist']) else 0
            
            # 移动平均
            ma5 = self.ti.calculate_ma(pd.Series(window_close), 5)
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20
            
            features['price_ma5_ratio'] = (window_close[-1] / ma5 - 1) if ma5 > 0 else 0
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 > 0 else 0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 > 0 else 0
            
            # 量价
            avg_volume = np.mean(window_volume[-20:]) if len(window_volume) >= 20 else window_volume[-1]
            features['volume_ratio'] = window_volume[-1] / avg_volume if avg_volume > 0 else 1
            
            # 波动率
            returns = np.diff(window_close) / window_close[:-1]
            features['volatility'] = np.std(returns[-20:]) * np.sqrt(252) if len(returns) >= 20 else 0.3
            features['volatility_5d'] = np.std(returns[-5:]) * np.sqrt(252) if len(returns) >= 5 else 0.3
            
            # 收益率
            features['return_5d'] = (window_close[-1] - window_close[-6]) / window_close[-6] if len(window_close) >= 6 else 0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0
            
            # 价格位置
            price_min = np.min(window_close[-60:]) if len(window_close) >= 60 else close[0]
            price_max = np.max(window_close[-60:]) if len(window_close) >= 60 else close[0]
            if price_max > price_min:
                features['price_position'] = (window_close[-1] - price_min) / (price_max - price_min)
            else:
                features['price_position'] = 0.5
            
            # 高低价比
            if len(window_high) >= 20 and len(window_low) >= 20:
                h20 = np.max(window_high[-20:])
                l20 = np.min(window_low[-20:])
                features['high_low_ratio'] = (window_close[-1] - l20) / (h20 - l20) if h20 > l20 else 0.5
            else:
                features['high_low_ratio'] = 0.5
            
            # 趋势强度 (简单版)
            if len(returns) >= 20:
                up_days = np.sum(returns[-20:] > 0)
                features['uptrend_strength'] = up_days / 20
            else:
                features['uptrend_strength'] = 0.5
            
            # 处理NaN
            for k in features:
                if pd.isna(features[k]) or np.isinf(features[k]):
                    features[k] = 0
            
            # 标签: 不同周期的未来涨跌方向
            if i + forecast_horizon < len(close):
                future_return = (close[i + forecast_horizon] - close[i]) / close[i]
                label_threshold = 0.0 if horizon <= 5 else (0.004 if horizon <= 20 else 0.008)
                label = 1 if future_return > label_threshold else 0
            else:
                label = 0
            
            X_list.append(list(features.values()))
            y_list.append(label)
        
        if len(X_list) == 0:
            return None, None
        
        X = pd.DataFrame(X_list, columns=list(features.keys()))
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
            model = ExtraTreesClassifier(
                n_estimators=220 if horizon <= 20 else 280,
                max_depth=4 if horizon <= 20 else 5,
                min_samples_leaf=5 if horizon <= 20 else 4,
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            )
        else:
            model = GradientBoostingClassifier(
                n_estimators=60 if horizon <= 20 else 90,
                learning_rate=0.04 if horizon <= 20 else 0.03,
                max_depth=2 if horizon <= 20 else 3,
                random_state=42
            )
        model.fit(X_train_scaled, y_train)

        decision_threshold = 0.5
        train_scores = self._predict_proba_or_score(model, X_train_scaled)
        val_scores = self._predict_proba_or_score(model, X_val_scaled)
        decision_threshold, val_acc = self._find_best_threshold(y_val, val_scores)
        train_acc = float(((train_scores >= decision_threshold).astype(int) == np.asarray(y_train, dtype=int)).mean())

        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")

        model_file = os.path.join(self.models_dir, f'{asset_type}_{horizon_key}_model.pkl')
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': horizon,
            'asset_type': asset_type,
            'decision_threshold': decision_threshold,
            'horizon_key': horizon_key,
        }

        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)

        if horizon == 5:
            legacy_file = os.path.join(self.models_dir, f'{asset_type}_model.pkl')
            with open(legacy_file, 'wb') as f:
                pickle.dump(model_data, f)

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


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='贵金属模型训练脚本')
    parser.add_argument('--asset', choices=['gold', 'silver', 'both'], default='both', help='选择训练黄金、白银或全部')
    parser.add_argument('--periods', default='5,20,60', help='训练周期，支持 all 或逗号分隔，如 5,20,60')
    args = parser.parse_args()

    selected_assets = ['gold', 'silver'] if args.asset == 'both' else [args.asset]
    if str(args.periods).strip().lower() == 'all':
        horizons = [5, 20, 60]
    else:
        horizons = [int(item.strip()) for item in str(args.periods).split(',') if item.strip()]
        horizons = [item for item in horizons if item > 0]

    trainer = GoldTrainer()
    raise SystemExit(0 if trainer.run(selected_assets=selected_assets, horizons=horizons) else 1)
