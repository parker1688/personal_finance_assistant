#!/usr/bin/env python3
"""
美股模型训练脚本 - 独立训练

用途: 针对美股单独训练预测模型  
数据源: historical_us_stock.csv + 相关指标数据
模型输出: data/models/us_stock_short_term_model.pkl 等

美股特点:
  - 完全市场化 (无涨跌停限制)
  - 24小时全球流动性最好
  - 美元计价 (汇率风险较小)
  - 包括纳斯达克(科技股)、纽交所(蓝筹)等
  - 受联储政策影响大
  - 波动率通常低于A股/港股

使用方式:
  python3 scripts/train_us_stock.py               # 完整训练
  python3 scripts/train_us_stock.py --period 5   # 仅训练5日模型
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from indicators.technical import TechnicalIndicator
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestClassifier
    print("⚠️  XGBoost未安装，将使用RandomForest")


class USStockTrainer:
    """美股模型训练器"""
    
    def __init__(self):
        self.ti = TechnicalIndicator()
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        self.data = None
    
    @staticmethod
    def _predict_proba_or_score(model, X):
        if hasattr(model, 'predict_proba'):
            return np.asarray(model.predict_proba(X)[:, 1], dtype=float)
        raw = np.asarray(model.predict(X), dtype=float).reshape(-1)
        return np.clip(raw, 0, 1)

    @staticmethod
    def _find_best_threshold(y_true, y_proba, min_threshold=0.35, max_threshold=0.65, step=0.01):
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

    def load_data(self):
        """加载美股数据"""
        print("\n" + "=" * 60)
        print("加载美股数据")
        print("=" * 60)
        
        print("\n[1/1] 加载美股行情数据...")
        if os.path.exists('data/historical_us_stock.csv'):
            df = pd.read_csv('data/historical_us_stock.csv')
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['code', 'date']).reset_index(drop=True)
            print(f"   ✅ 美股数据: {len(df):,} 条, {df['code'].nunique()} 只股票")
            print(f"   ✅ 时间范围: {df['date'].min().date()} ~ {df['date'].max().date()}")
            self.data = df
            return True
        else:
            print("   ❌ 文件不存在: data/historical_us_stock.csv")
            print("   提示: 请先运行 scripts/collect_historical_data.py --asset us")
            return False
    
    def extract_features(self, df, period_days=5):
        """从美股数据提取特征"""
        if df is None or len(df) < 60:
            return None, None
        
        close = df['close'].values
        high = df['high'].values
        low = df['low'].values
        volume = df['volume'].values
        
        X_list = []
        y_list = []
        step_size = 4 if period_days <= 5 else 5
        
        for i in range(60, len(close) - period_days, step_size):
            features = {}
            
            window_close = close[:i+1]
            window_high = high[:i+1]
            window_low = low[:i+1]
            window_volume = volume[:i+1]
            
            # 技术指标
            rsi = self.ti.calculate_rsi(pd.Series(window_close))
            features['rsi'] = rsi if not np.isnan(rsi) else 50
            
            macd = self.ti.calculate_macd(pd.Series(window_close))
            macd_hist = macd.get('hist', 0)
            macd_signal = macd.get('dea', 0)
            features['macd_hist'] = macd_hist if not np.isnan(macd_hist) else 0
            features['macd_signal'] = macd_signal if not np.isnan(macd_signal) else 0
            
            # 移动平均
            ma5 = self.ti.calculate_ma(pd.Series(window_close), 5)
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20
            ma200 = self.ti.calculate_ma(pd.Series(window_close), 200) if len(window_close) >= 200 else ma60
            
            features['price_ma5_ratio'] = (window_close[-1] / ma5 - 1) if ma5 > 0 else 0
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 > 0 else 0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 > 0 else 0
            features['price_ma200_ratio'] = (window_close[-1] / ma200 - 1) if ma200 > 0 else 0
            
            # 长期趋势 (美股重视长期趋势)
            if ma20 > ma200:
                features['long_term_trend'] = 1
            elif ma20 < ma200:
                features['long_term_trend'] = -1
            else:
                features['long_term_trend'] = 0
            
            # ATR改编 (美股波动率指标)
            if len(window_high) >= 20:
                tr_list = []
                for j in range(1, min(20, len(window_high))):
                    h_l = window_high[-(20-j)] - window_low[-(20-j)]
                    h_c = abs(window_high[-(20-j)] - window_close[-(21-j)])
                    l_c = abs(window_low[-(20-j)] - window_close[-(21-j)])
                    tr_list.append(max(h_l, h_c, l_c))
                features['atr'] = np.mean(tr_list) / window_close[-1] if window_close[-1] > 0 else 0
            else:
                features['atr'] = 0
            
            # 量价
            avg_volume = np.mean(window_volume[-20:]) if len(window_volume) >= 20 else window_volume[-1]
            features['volume_ratio'] = window_volume[-1] / avg_volume if avg_volume > 0 else 1
            
            # 成交金额
            if 'amount' in df.columns:
                avg_amount = np.mean(df['amount'][-20:].values if len(df['amount']) >= 20 else [df['amount'].iloc[-1]])
                features['amount_ratio'] = df['amount'].iloc[i] / avg_amount if avg_amount > 0 else 1
            else:
                features['amount_ratio'] = features['volume_ratio']
            
            # 波动率
            returns = np.diff(window_close) / window_close[:-1]
            features['volatility'] = np.std(returns[-20:]) * np.sqrt(252) if len(returns) >= 20 else 0.2
            features['volatility_5d'] = np.std(returns[-5:]) * np.sqrt(252) if len(returns) >= 5 else 0.2
            
            # 收益率
            features['return_5d'] = (window_close[-1] - window_close[-6]) / window_close[-6] if len(window_close) >= 6 else 0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0
            
            # 支撑阻力
            if len(window_close) >= 60:
                price_min_60 = np.min(window_close[-60:])
                price_max_60 = np.max(window_close[-60:])
                if price_max_60 > price_min_60:
                    features['price_position_60d'] = (window_close[-1] - price_min_60) / (price_max_60 - price_min_60)
                else:
                    features['price_position_60d'] = 0.5
            else:
                features['price_position_60d'] = 0.5
            
            # 处理NaN
            for k in features:
                if pd.isna(features[k]) or np.isinf(features[k]):
                    features[k] = 0
            
            # 标签: 未来period_days天涨跌方向
            if i + period_days < len(close):
                future_return = (close[i + period_days] - close[i]) / close[i]
                label = 1 if future_return > 0 else 0
            else:
                label = 0
            
            X_list.append(list(features.values()))
            y_list.append(label)
        
        if len(X_list) == 0:
            return None, None
        
        X = pd.DataFrame(X_list, columns=list(features.keys()))
        y = pd.Series(y_list)
        
        return X, y
    
    def train_model(self, period_days, model_key):
        """训练美股模型"""
        print(f"\n{'='*60}")
        print(f"训练美股 {model_key} 模型 ({period_days}日预测)")
        print(f"{'='*60}")
        
        train_parts = []
        train_labels = []
        val_parts = []
        val_labels = []
        
        codes = self.data['code'].unique()
        print(f"  遍历 {len(codes)} 只美股...")
        
        for idx, code in enumerate(codes):
            if (idx + 1) % 50 == 0:
                print(f"    进度: {idx+1}/{len(codes)}")
            
            stock_df = self.data[self.data['code'] == code].copy().sort_values('date').reset_index(drop=True)
            if len(stock_df) < 100:
                continue
            
            X, y = self.extract_features(stock_df, period_days)
            if X is None or len(X) < 20:
                continue

            split_idx = max(1, int(len(X) * 0.8))
            split_idx = min(split_idx, len(X) - 1)
            train_parts.append(X.iloc[:split_idx])
            train_labels.append(y.iloc[:split_idx])
            val_parts.append(X.iloc[split_idx:])
            val_labels.append(y.iloc[split_idx:])
        
        if not train_parts or not val_parts:
            print("  ❌ 无有效训练数据")
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
        
        # 标准化
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        # 训练模型
        print("  训练模型中...")
        if period_days <= 5:
            model = GradientBoostingClassifier(
                n_estimators=80,
                learning_rate=0.04,
                max_depth=2,
                random_state=42
            )
            model.fit(X_train_scaled, y_train)
        elif period_days <= 20:
            model = LogisticRegression(max_iter=3000, class_weight='balanced', C=0.3)
            model.fit(X_train_scaled, y_train)
        elif HAS_XGB:
            pos = max(float(y_train.mean()), 1e-6)
            neg = max(1.0 - pos, 1e-6)
            model = xgb.XGBClassifier(
                n_estimators=90,
                max_depth=3,
                learning_rate=0.05,
                subsample=0.75,
                colsample_bytree=0.75,
                min_child_weight=6,
                gamma=0.2,
                reg_lambda=3.0,
                reg_alpha=0.3,
                scale_pos_weight=neg / pos,
                random_state=42,
                eval_metric='logloss',
                use_label_encoder=False
            )
            model.fit(X_train_scaled, y_train, eval_set=[(X_val_scaled, y_val)], verbose=False)
        else:
            model = RandomForestClassifier(
                n_estimators=120,
                max_depth=7,
                min_samples_leaf=4,
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train_scaled, y_train)

        decision_threshold = 0.5
        train_scores = self._predict_proba_or_score(model, X_train_scaled)
        val_scores = self._predict_proba_or_score(model, X_val_scaled)
        decision_threshold, val_acc = self._find_best_threshold(y_val, val_scores)
        train_acc = float(((train_scores >= decision_threshold).astype(int) == np.asarray(y_train, dtype=int)).mean())
        
        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")
        
        # 保存模型
        model_file = os.path.join(self.models_dir, f'us_stock_{model_key}_model.pkl')  # 美股专用模型
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': period_days,
            'asset_type': 'us_stock',
            'decision_threshold': decision_threshold
        }
        
        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"  ✅ 模型已保存: {model_file}")
        
        return model_data
    
    def run(self, periods=None):
        """运行美股模型训练"""
        print("=" * 60)
        print("美股模型训练 - 独立训练脚本")
        print("=" * 60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.load_data():
            print("\n❌ 数据加载失败")
            return False
        
        period_map = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}
        selected_periods = [p for p in (periods or [5, 20, 60]) if p in period_map]
        results = {}
        
        for period_days in selected_periods:
            model_key = period_map[period_days]
            results[model_key] = self.train_model(period_days, model_key)
        
        print("\n" + "=" * 60)
        print("美股训练完成汇总")
        print("=" * 60)
        
        for name, result in results.items():
            if result:
                acc = result.get('val_accuracy', 0)
                print(f"  ✅ {name}: 验证准确率 {acc:.2%}")
            else:
                print(f"  ❌ {name}: 训练失败")
        
        print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        return all(bool(result) for result in results.values())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='美股模型训练脚本')
    parser.add_argument('--period', type=int, choices=[5, 20, 60], help='仅训练单个周期')
    parser.add_argument('--periods', type=str, default='', help='训练周期，逗号分隔，例如 5,20,60')
    args = parser.parse_args()

    selected_periods = []
    if args.periods:
        for item in args.periods.split(','):
            item = item.strip()
            if item:
                try:
                    value = int(item)
                except Exception:
                    continue
                if value in (5, 20, 60) and value not in selected_periods:
                    selected_periods.append(value)
    if args.period and args.period not in selected_periods:
        selected_periods.append(args.period)

    trainer = USStockTrainer()
    raise SystemExit(0 if trainer.run(periods=selected_periods or None) else 1)
