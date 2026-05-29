# -*- coding: utf-8 -*-
"""美股模型训练器 - 迁移自 scripts/train_us_stock.py"""

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


from indicators.technical import TechnicalIndicator
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score, brier_score_loss

from predictors.model_manager import ModelManager

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
        self.model_manager = ModelManager()
        self.data = None
    
    @staticmethod
    def _predict_proba_or_score(model, X):
        if hasattr(model, 'predict_proba'):
            return np.asarray(model.predict_proba(X)[:, 1], dtype=float)
        raw = np.asarray(model.predict(X), dtype=float).reshape(-1)
        return np.clip(raw, 0, 1)

    @staticmethod
    def _find_best_threshold(y_true, y_proba, min_threshold=0.35, max_threshold=0.65, step=0.01):
        """F1-optimal 阈值搜索（对齐 A股标准）。"""
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
                from sklearn.linear_model import LogisticRegression as _LR
                clf = _LR(solver='lbfgs', max_iter=500)
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
        step_size = 2 if period_days <= 5 else 3
        
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
                # 中性区过滤：排除绝对收益小于0.3%的噪声样本
                if abs(future_return) < 0.003:
                    continue
                label = 1 if future_return > 0 else 0
            else:
                continue
            
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
            # 美股 5 日信号可分性偏弱，优先使用更稳的线性基线，避免小幅过拟合被放大成伪信号。
            model = LogisticRegression(max_iter=4000, class_weight='balanced', C=0.12)
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

        train_acc = float(((self._predict_proba_or_score(model, X_train_scaled) >= 0.5).astype(int) ==
                            np.asarray(y_train, dtype=int)).mean())

        # ── 校准候选 + gate 评估（A股流程对齐）──────────────────────────
        y_val_arr = np.asarray(y_val, dtype=int)
        raw_eval_proba = self._predict_proba_or_score(model, X_val_scaled)

        n_val = len(y_val_arr)
        n_cal = n_val // 2
        can_calibrate = n_cal >= 80 and len(np.unique(y_val_arr)) >= 2

        if can_calibrate:
            raw_cal_proba = self._predict_proba_or_score(model, X_val_scaled[:n_cal])
            raw_eval_proba = self._predict_proba_or_score(model, X_val_scaled[n_cal:])
            y_cal = y_val_arr[:n_cal]
            y_eval_arr = y_val_arr[n_cal:]
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

            passed_rows, all_rows = [], []
            for threshold in np.arange(0.30, 0.65 + 1e-9, 0.01):
                y_pred = (eval_proba >= float(threshold)).astype(int)
                eval_acc = float((y_pred == y_eval_arr).mean())
                eval_f1 = float(f1_score(y_eval_arr, y_pred, zero_division=0))
                eval_prec = float(precision_score(y_eval_arr, y_pred, zero_division=0))
                eval_rec = float(recall_score(y_eval_arr, y_pred, zero_division=0))
                metrics_at_t = {
                    'validation_accuracy': eval_acc, 'validation_f1': eval_f1,
                    'validation_precision': eval_prec, 'validation_recall': eval_rec,
                    'validation_auc': eval_auc, 'validation_brier': eval_brier,
                }
                passed_t, gate_t, reason_t = self.model_manager.evaluate_validation_gate(period_days, metrics_at_t)
                row = {
                    'threshold': float(threshold), 'validation_accuracy': eval_acc,
                    'validation_f1': eval_f1, 'validation_precision': eval_prec,
                    'validation_recall': eval_rec, 'validation_gate': gate_t,
                    'validation_passed': bool(passed_t), 'validation_reason': reason_t,
                }
                all_rows.append(row)
                if passed_t:
                    passed_rows.append(row)

            best_t = max(
                passed_rows if passed_rows else all_rows,
                key=lambda r: (r['validation_f1'], r['validation_accuracy'], r['validation_precision']),
            )
            final_gate_passed = bool(best_t['validation_passed'])
            final_gate = best_t['validation_gate']
            final_reason = best_t['validation_reason']
            if eval_auc is not None and eval_auc < 0.45:
                final_gate_passed = False
                final_gate = 'failed'
                final_reason = f'AUC={eval_auc:.4f} < 0.45 (reversed model)'
            candidate_results.append({
                'calibration_method': method, 'calibrator': calibrator,
                'calibration_samples': int(n_cal) if method != 'none' else 0,
                'validation_accuracy': float(best_t['validation_accuracy']),
                'validation_f1': float(best_t['validation_f1']),
                'validation_precision': float(best_t['validation_precision']),
                'validation_recall': float(best_t['validation_recall']),
                'validation_auc': eval_auc, 'validation_brier': eval_brier,
                'decision_threshold': round(float(best_t['threshold']), 2),
                'validation_gate': final_gate, 'validation_passed': final_gate_passed,
                'validation_reason': final_reason,
            })

        if not candidate_results:
            print('  ❌ 未找到有效候选模型（校准/评估失败）')
            return None

        best = max(
            candidate_results,
            key=lambda r: (int(r['validation_passed']), r['validation_f1'],
                           r['validation_accuracy'], r['validation_precision']),
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

        # 过拟合检测
        if gate_passed and train_acc >= 0.95 and val_acc >= 0.97:
            gate_passed = False
            gate_name = 'failed'
            gate_reason = f'overfitting: train_acc={train_acc:.4f}, val_acc={val_acc:.4f}'
        # 小验证集检测
        if gate_passed and len(y_val_arr) < 40:
            gate_passed = False
            gate_name = 'failed'
            gate_reason = f'val_samples={len(y_val_arr)} < 40 (too small, evaluation unreliable)'

        print(f'  📊 训练准确率: {train_acc:.2%}')
        print(f'  📊 验证准确率: {val_acc:.2%}')
        print(f'  📊 验证F1(最优阈值): {best_f1:.2%} (threshold={best_threshold:.2f}, '
              f'precision={val_precision:.2%}, recall={val_recall:.2%})')
        print(f'  📊 概率校准: {cal_method} (samples={calibration_samples}) | '
              f'gate={gate_name} | passed={gate_passed}')
        if val_auc is not None:
            print(f'  📊 AUC={val_auc:.4f}, Brier={val_brier:.4f}')

        # 保存模型
        model_file = os.path.join(self.models_dir, f'us_stock_{model_key}_model.pkl')  # 美股专用模型
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
            'period_days': period_days,
            'asset_type': 'us_stock',
            'decision_threshold': best_threshold,
            'validation_gate': gate_name,
            'validation_passed': gate_passed,
            'validation_reason': gate_reason,
            'calibration_method': cal_method,
            'calibrator': calibrator_obj,
            'calibration_samples': calibration_samples,
            'metadata': {
                'validation_accuracy': val_acc,
                'validation_f1': best_f1,
                'validation_precision': val_precision,
                'validation_recall': val_recall,
                'validation_auc': val_auc,
                'validation_brier': val_brier,
                'decision_threshold': best_threshold,
                'period_days': period_days,
                'asset_type': 'us_stock',
                'validation_gate': gate_name,
                'validation_passed': gate_passed,
                'validation_reason': gate_reason,
                'calibration_method': cal_method,
                'calibration_samples': calibration_samples,
            },
        }
        
        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f'  ✅ 模型已保存: {model_file}')
        
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
                f1 = result.get('val_f1', 0)
                gate = result.get('validation_gate', 'unknown')
                passed = result.get('validation_passed', False)
                print(f"  {'✅' if passed else '⚠️'} {name}: 验证准确率 {acc:.2%} | F1={f1:.2%} | gate={gate} | passed={passed}")
            else:
                print(f"  ❌ {name}: 训练失败")
        
        print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        return all(bool(result) for result in results.values())


