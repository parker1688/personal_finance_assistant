#!/usr/bin/env python3
"""
A股完整训练脚本 - 整合全部8个数据源
数据源: 行情 + 资金流向 + 北向资金 + 融资融券 + 龙虎榜 + 财务指标 + 每日估值 + 新闻舆情 + 券商研报
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.technical import TechnicalIndicator
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# 尝试导入XGBoost
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("⚠️ XGBoost未安装，将使用RandomForest")

MODEL_DIR = 'data/models'
os.makedirs(MODEL_DIR, exist_ok=True)


class AShareFullTrainer:
    """A股完整训练器 - 整合全部数据源"""
    
    def __init__(self):
        self.ti = TechnicalIndicator()
        
        # 数据存储
        self.data = None           # 行情
        self.moneyflow = None      # 资金流向
        self.north_money = None    # 北向资金
        self.margin = None         # 融资融券
        self.top_list = None       # 龙虎榜
        self.financial = None      # 财务指标
        self.daily_basic = None    # 每日估值
        self.news = None           # 新闻舆情
        self.research = None       # 券商研报
    
    def load_all_data(self):
        """加载全部数据源"""
        print("\n" + "=" * 60)
        print("加载全部数据源")
        print("=" * 60)
        
        # 1. 行情数据
        print("\n[1/8] 加载行情数据...")
        df = pd.read_csv('data/historical_a_stock.csv')
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['code', 'date'])
        print(f"   ✅ 行情数据: {len(df):,} 条, {df['code'].nunique()} 只股票")
        self.data = df
        
        # 2. 资金流向
        print("\n[2/8] 加载资金流向...")
        if os.path.exists('data/moneyflow_all.csv'):
            mf = pd.read_csv('data/moneyflow_all.csv')
            mf['trade_date'] = pd.to_datetime(mf['trade_date'].astype(str), format='%Y%m%d')
            mf = mf.rename(columns={'ts_code': 'code', 'trade_date': 'date'})
            mf = mf[['code', 'date', 'net_mf_amount']]
            print(f"   ✅ 资金流向: {len(mf):,} 条, {mf['code'].nunique()} 只股票")
            self.moneyflow = mf
        else:
            print("   ⚠️ 资金流向文件不存在")
        
        # 3. 北向资金
        print("\n[3/8] 加载北向资金...")
        if os.path.exists('data/north_money_all.csv'):
            nm = pd.read_csv('data/north_money_all.csv')
            nm['trade_date'] = pd.to_datetime(nm['trade_date'].astype(str), format='%Y%m%d')
            nm = nm.rename(columns={'trade_date': 'date'})
            nm = nm[['date', 'north_money']]
            print(f"   ✅ 北向资金: {len(nm):,} 条")
            self.north_money = nm
        else:
            print("   ⚠️ 北向资金文件不存在")
        
        # 4. 融资融券
        print("\n[4/8] 加载融资融券...")
        if os.path.exists('data/margin_all.csv'):
            mg = pd.read_csv('data/margin_all.csv')
            mg['trade_date'] = pd.to_datetime(mg['trade_date'].astype(str), format='%Y%m%d')
            mg = mg.rename(columns={'trade_date': 'date'})
            mg_sum = mg.groupby('date').agg({
                'rzye': 'sum',
                'rzmre': 'sum',
                'rqye': 'sum'
            }).reset_index()
            print(f"   ✅ 融资融券: {len(mg_sum):,} 条")
            self.margin = mg_sum
        else:
            print("   ⚠️ 融资融券文件不存在")
        
        # 5. 龙虎榜
        print("\n[5/8] 加载龙虎榜...")
        if os.path.exists('data/top_list.csv'):
            tl = pd.read_csv('data/top_list.csv')
            if 'trade_date' in tl.columns:
                tl['trade_date'] = pd.to_datetime(tl['trade_date'].astype(str), format='%Y%m%d')
                tl = tl.rename(columns={'trade_date': 'date'})
            if 'ts_code' in tl.columns:
                tl = tl.rename(columns={'ts_code': 'code'})
            tl['has_top_list'] = 1
            tl = tl[['code', 'date', 'has_top_list']].drop_duplicates()
            print(f"   ✅ 龙虎榜: {len(tl):,} 条, {tl['code'].nunique()} 只股票")
            self.top_list = tl
        else:
            print("   ⚠️ 龙虎榜文件不存在")
        
        # 6. 财务指标
        print("\n[6/8] 加载财务指标...")
        if os.path.exists('data/financial_indicator.csv'):
            fi = pd.read_csv('data/financial_indicator.csv')
            if 'end_date' in fi.columns:
                fi['date'] = pd.to_datetime(fi['end_date'].astype(str))
            if 'ts_code' in fi.columns:
                fi = fi.rename(columns={'ts_code': 'code'})
            keep_cols = ['code', 'date', 'eps', 'roe']
            fi = fi[[c for c in keep_cols if c in fi.columns]]
            print(f"   ✅ 财务指标: {len(fi):,} 条, {fi['code'].nunique()} 只股票")
            self.financial = fi
        else:
            print("   ⚠️ 财务指标文件不存在")
        
        # 7. 每日估值
        print("\n[7/8] 加载每日估值...")
        if os.path.exists('data/daily_basic.csv'):
            db = pd.read_csv('data/daily_basic.csv')
            db['trade_date'] = pd.to_datetime(db['trade_date'].astype(str), format='%Y%m%d')
            db = db.rename(columns={'ts_code': 'code', 'trade_date': 'date'})
            keep_cols = ['code', 'date', 'pe', 'pe_ttm', 'pb']
            db = db[[c for c in keep_cols if c in db.columns]]
            print(f"   ✅ 每日估值: {len(db):,} 条, {db['code'].nunique()} 只股票")
            self.daily_basic = db
        else:
            print("   ⚠️ 每日估值文件不存在")
        
        # 8. 新闻舆情
        print("\n[8/8] 加载新闻舆情...")
        if os.path.exists('data/news_all.csv'):
            ns = pd.read_csv('data/news_all.csv')
            if 'datetime' in ns.columns:
                ns['date'] = pd.to_datetime(ns['datetime']).dt.date
                ns['date'] = pd.to_datetime(ns['date'])
            if 'sentiment' in ns.columns:
                ns = ns[['date', 'sentiment']]
                ns = ns.groupby('date')['sentiment'].mean().reset_index()
                print(f"   ✅ 新闻舆情: {len(ns):,} 条")
                self.news = ns
            else:
                print("   ⚠️ 新闻文件格式不匹配")
        else:
            print("   ⚠️ 新闻舆情文件不存在")
        
        # 9. 券商研报（可选）
        print("\n[9/9] 加载券商研报...")
        if os.path.exists('data/research_report.csv'):
            rr = pd.read_csv('data/research_report.csv')
            if 'trade_date' in rr.columns:
                rr['date'] = pd.to_datetime(rr['trade_date'].astype(str), format='%Y%m%d')
                rr = rr[['date']]
                rr['has_report'] = 1
                rr = rr.drop_duplicates()
                print(f"   ✅ 券商研报: {len(rr):,} 条")
                self.research = rr
            else:
                print("   ⚠️ 研报文件格式不匹配（无trade_date列）")
        else:
            print("   ⚠️ 券商研报文件不存在")
    
    def merge_features(self, code, stock_df):
        """合并单只股票的所有特征"""
        if stock_df is None or len(stock_df) < 60:
            return None
        
        df = stock_df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # 1. 资金流向
        if self.moneyflow is not None:
            mf = self.moneyflow[self.moneyflow['code'] == code].copy()
            if len(mf) > 0:
                df = df.merge(mf[['date', 'net_mf_amount']], on='date', how='left')
            else:
                df['net_mf_amount'] = 0
        else:
            df['net_mf_amount'] = 0
        
        # 2. 每日估值
        if self.daily_basic is not None:
            db = self.daily_basic[self.daily_basic['code'] == code].copy()
            if len(db) > 0:
                df = df.merge(db[['date', 'pe', 'pe_ttm', 'pb']], on='date', how='left')
            else:
                df['pe'] = 0
                df['pe_ttm'] = 0
                df['pb'] = 0
        else:
            df['pe'] = 0
            df['pe_ttm'] = 0
            df['pb'] = 0
        
        # 3. 龙虎榜
        if self.top_list is not None:
            tl = self.top_list[self.top_list['code'] == code].copy()
            if len(tl) > 0:
                df = df.merge(tl[['date', 'has_top_list']], on='date', how='left')
            else:
                df['has_top_list'] = 0
        else:
            df['has_top_list'] = 0
        
        # 4. 财务指标（季度数据，前向填充）
        if self.financial is not None:
            fi = self.financial[self.financial['code'] == code].copy()
            if len(fi) > 0:
                df = df.merge(fi[['date', 'eps', 'roe']], on='date', how='left')
                # 修复 fillna method 问题
                df['eps'] = df['eps'].ffill()
                df['roe'] = df['roe'].ffill()
                df['eps'] = df['eps'].fillna(0)
                df['roe'] = df['roe'].fillna(0)
            else:
                df['eps'] = 0
                df['roe'] = 0
        else:
            df['eps'] = 0
            df['roe'] = 0
        
        # 5. 北向资金（全市场）
        if self.north_money is not None:
            df = df.merge(self.north_money[['date', 'north_money']], on='date', how='left')
            df['north_money'] = df['north_money'].fillna(0)
        else:
            df['north_money'] = 0
        
        # 6. 融资融券（全市场）
        if self.margin is not None:
            df = df.merge(self.margin[['date', 'rzye', 'rzmre', 'rqye']], on='date', how='left')
            df['rzye'] = df['rzye'].fillna(0)
            df['rzmre'] = df['rzmre'].fillna(0)
            df['rqye'] = df['rqye'].fillna(0)
        else:
            df['rzye'] = 0
            df['rzmre'] = 0
            df['rqye'] = 0
        
        # 7. 新闻情感（全市场）
        if self.news is not None:
            df = df.merge(self.news[['date', 'sentiment']], on='date', how='left')
            df['sentiment'] = df['sentiment'].fillna(0)
        else:
            df['sentiment'] = 0
        
        # 8. 券商研报
        if self.research is not None:
            df = df.merge(self.research[['date', 'has_report']], on='date', how='left')
            df['has_report'] = df['has_report'].fillna(0)
        else:
            df['has_report'] = 0
        
        # 填充所有缺失值
        df = df.fillna(0)
        
        return df
    
    def extract_features(self, df, period_days=5):
        """从合并后的数据提取特征和标签"""
        if df is None or len(df) < 60:
            return None, None
        
        close = df['close'].values
        volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))
        
        X_list = []
        y_list = []
        
        for i in range(60, len(close) - period_days, 5):
            window_df = df.iloc[:i+1]
            window_close = close[:i+1]
            window_volume = volume[:i+1]
            
            features = {}
            
            # ===== 技术指标 =====
            rsi = self.ti.calculate_rsi(pd.Series(window_close))
            features['rsi'] = rsi if not np.isnan(rsi) else 50
            
            macd = self.ti.calculate_macd(pd.Series(window_close))
            features['macd_hist'] = macd['hist'] if not np.isnan(macd['hist']) else 0
            
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 != 0 else 0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 != 0 else 0
            
            avg_volume = np.mean(window_volume[-20:]) if len(window_volume) >= 20 else window_volume[-1]
            features['volume_ratio'] = window_volume[-1] / avg_volume if avg_volume != 0 else 1
            
            returns = np.diff(window_close) / window_close[:-1]
            features['volatility'] = np.std(returns[-20:]) * np.sqrt(252) if len(returns) >= 20 else 0.3
            
            features['return_5d'] = (window_close[-1] - window_close[-6]) / window_close[-6] if len(window_close) >= 6 else 0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0
            
            features['momentum_5d'] = features['return_5d']
            features['momentum_10d'] = features['return_10d']
            
            ma5 = self.ti.calculate_ma(pd.Series(window_close), 5)
            bullish_count = 0
            if window_close[-1] > ma5:
                bullish_count += 1
            if ma5 > ma20:
                bullish_count += 1
            if ma20 > ma60:
                bullish_count += 1
            features['ma_bullish_count'] = bullish_count
            
            vol_short = np.std(returns[-10:]) if len(returns) >= 10 else 0
            vol_long = np.std(returns[-30:]) if len(returns) >= 30 else vol_short
            features['volatility_trend'] = vol_short - vol_long
            
            if len(window_close) >= 60:
                price_min = np.min(window_close[-60:])
                price_max = np.max(window_close[-60:])
                if price_max != price_min:
                    features['price_position_60d'] = (window_close[-1] - price_min) / (price_max - price_min)
                else:
                    features['price_position_60d'] = 0.5
            else:
                features['price_position_60d'] = 0.5
            
            # ===== 资金流向 =====
            features['net_mf_amount'] = window_df['net_mf_amount'].iloc[-1] / 1e8
            
            # ===== 估值 =====
            features['pe'] = window_df['pe'].iloc[-1]
            features['pb'] = window_df['pb'].iloc[-1]
            
            # ===== 财务指标 =====
            features['eps'] = window_df['eps'].iloc[-1]
            features['roe'] = window_df['roe'].iloc[-1]
            
            # ===== 龙虎榜 =====
            features['has_top_list'] = window_df['has_top_list'].iloc[-1]
            
            # ===== 宏观 =====
            features['north_money'] = window_df['north_money'].iloc[-1] / 1e8
            features['rzye'] = window_df['rzye'].iloc[-1] / 1e8
            features['rzmre'] = window_df['rzmre'].iloc[-1] / 1e8
            
            # ===== 新闻情感 =====
            features['sentiment'] = window_df['sentiment'].iloc[-1]
            
            # ===== 券商研报 =====
            features['has_report'] = window_df['has_report'].iloc[-1]
            
            # 处理缺失值
            for k, v in features.items():
                if pd.isna(v) or np.isinf(v):
                    features[k] = 0
            
            # 标签
            if i + period_days < len(close):
                future_return = (close[i + period_days] - close[i]) / close[i]
                label = 1 if future_return > 0.02 else 0
            else:
                label = 0
            
            X_list.append(list(features.values()))
            y_list.append(label)
        
        if len(X_list) == 0:
            return None, None
        
        feature_columns = list(features.keys())
        X = pd.DataFrame(X_list, columns=feature_columns)
        y = pd.Series(y_list)
        
        return X, y
    
    def train_model(self, period_days, model_name):
        """训练指定周期的模型"""
        print(f"\n{'='*60}")
        print(f"训练 {model_name} ({period_days}日预测)")
        print(f"{'='*60}")
        
        all_X = []
        all_y = []
        
        codes = self.data['code'].unique()
        print(f"  遍历 {len(codes)} 只股票...")
        
        for idx, code in enumerate(codes):
            if (idx + 1) % 200 == 0:
                print(f"    进度: {idx+1}/{len(codes)}")
            
            stock_df = self.data[self.data['code'] == code].copy()
            if len(stock_df) < 100:
                continue
            
            merged_df = self.merge_features(code, stock_df)
            if merged_df is None:
                continue
            
            X, y = self.extract_features(merged_df, period_days)
            if X is not None and len(X) > 0:
                all_X.append(X)
                all_y.append(y)
        
        if not all_X:
            print("  ❌ 无有效训练数据")
            return None
        
        X = pd.concat(all_X, ignore_index=True)
        y = pd.concat(all_y, ignore_index=True)
        
        print(f"  ✅ 总样本数: {len(X):,}")
        print(f"  ✅ 特征数: {len(X.columns)}")
        print(f"  ✅ 特征列表: {list(X.columns)}")
        print(f"  ✅ 正样本比例: {y.sum()/len(y):.2%}")
        
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        print(f"  训练集: {len(X_train):,} 样本")
        print(f"  验证集: {len(X_val):,} 样本")
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        print("  训练模型...")
        
        if HAS_XGB:
            model = xgb.XGBClassifier(
                n_estimators=150,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.5,
                reg_alpha=0.5,
                random_state=42,
                eval_metric='logloss',
                use_label_encoder=False
            )
            model.fit(X_train_scaled, y_train, eval_set=[(X_val_scaled, y_val)], verbose=False)
        else:
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train_scaled, y_train)
        
        train_acc = model.score(X_train_scaled, y_train)
        val_acc = model.score(X_val_scaled, y_val)
        
        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")
        
        model_path = os.path.join(MODEL_DIR, f"{model_name}_model.pkl")
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': period_days,
            'asset_type': 'A'
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"  ✅ 模型已保存: {model_path}")
        
        return model_data
    
    def run(self):
        """运行完整训练"""
        print("=" * 60)
        print("A股完整训练 - 整合全部数据源")
        print("=" * 60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.load_all_data()
        
        if self.data is None:
            print("❌ 无行情数据，无法训练")
            return
        
        results = {}
        
        results['short_term'] = self.train_model(5, 'short_term')
        results['medium_term'] = self.train_model(20, 'medium_term')
        results['long_term'] = self.train_model(60, 'long_term')
        
        print("\n" + "=" * 60)
        print("训练结果汇总")
        print("=" * 60)
        
        for name, result in results.items():
            if result:
                acc = result.get('val_accuracy', 0)
                print(f"  ✅ {name}: 验证准确率 {acc:.2%}")
            else:
                print(f"  ❌ {name}: 训练失败")
        
        print("\n" + "=" * 60)
        print(f"训练完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)


if __name__ == '__main__':
    trainer = AShareFullTrainer()
    trainer.run()
