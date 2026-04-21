#!/usr/bin/env python3
"""
A股模型训练脚本 - 独立训练

用途: 针对A股单独训练短期(5日)、中期(20日)、长期(60日)预测模型
数据源: historical_a_stock.csv + 辅助数据 (资金、融资融券、估值等)
模型输出: data/models/short_term_model.pkl 等 (与scheduler兼容的通用格式)

使用方式:
  python3 scripts/train_a_stock.py               # 完整训练
  python3 scripts/train_a_stock.py --period 5   # 仅训练5日模型
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

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestClassifier
    print("⚠️  XGBoost未安装，将使用RandomForest")


class AStockTrainer:
    """A股模型训练器"""
    
    def __init__(self):
        self.ti = TechnicalIndicator()
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        
        # 数据存储
        self.data = None           # 行情
        self.moneyflow = None      # 资金流向
        self.north_money = None    # 北向资金
        self.margin = None         # 融资融券
        self.daily_basic = None    # 每日估值
        self.news = None           # 新闻情感
    
    def load_data(self):
        """加载A股训练数据"""
        print("\n" + "=" * 60)
        print("加载A股数据")
        print("=" * 60)
        
        # 行情数据
        print("\n[1/6] 加载行情数据...")
        price_file = 'data/historical_a_stock.csv'
        if os.path.exists(price_file):
            df = pd.read_csv(price_file)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['code', 'date']).reset_index(drop=True)
            print(f"   ✅ 行情数据: {len(df):,} 条, {df['code'].nunique()} 只股票")
            print(f"   ✅ 时间范围: {df['date'].min().date()} ~ {df['date'].max().date()}")
            self.data = df
        else:
            print(f"   ⚠️  文件不存在: {price_file}，尝试从数据库回退加载")
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
                    RawStockData.market,
                ).filter(RawStockData.market == 'A').all()
                session.close()
                if rows:
                    df = pd.DataFrame(rows, columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'market'])
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values(['code', 'date']).reset_index(drop=True)
                    print(f"   ✅ 已从数据库回退加载A股数据: {len(df):,} 条, {df['code'].nunique()} 只股票")
                    self.data = df
                else:
                    print("   ❌ 数据库中暂无可训练的A股历史数据")
                    return False
            except Exception as e:
                print(f"   ❌ 数据库回退加载失败: {e}")
                return False
        
        # 资金流向
        print("\n[2/6] 加载资金流向...")
        if os.path.exists('data/moneyflow_all.csv'):
            mf = pd.read_csv('data/moneyflow_all.csv')
            mf['trade_date'] = pd.to_datetime(mf['trade_date'].astype(str), format='%Y%m%d')
            mf = mf.rename(columns={'ts_code': 'code', 'trade_date': 'date'})
            mf = mf[['code', 'date', 'net_mf_amount']]
            print(f"   ✅ 资金流向: {len(mf):,} 条")
            self.moneyflow = mf
        else:
            print("   ⚠️  资金流向文件不存在")
        
        # 北向资金
        print("\n[3/6] 加载北向资金...")
        if os.path.exists('data/north_money_all.csv'):
            nm = pd.read_csv('data/north_money_all.csv')
            nm['trade_date'] = pd.to_datetime(nm['trade_date'].astype(str), format='%Y%m%d')
            nm = nm.rename(columns={'trade_date': 'date'})
            nm = nm[['date', 'north_money']]
            print(f"   ✅ 北向资金: {len(nm):,} 条")
            self.north_money = nm
        else:
            print("   ⚠️  北向资金文件不存在")
        
        # 融资融券
        print("\n[4/6] 加载融资融券...")
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
            print("   ⚠️  融资融券文件不存在")
        
        # 每日估值
        print("\n[5/6] 加载每日估值...")
        if os.path.exists('data/daily_basic.csv'):
            db = pd.read_csv('data/daily_basic.csv')
            db['trade_date'] = pd.to_datetime(db['trade_date'].astype(str), format='%Y%m%d')
            db = db.rename(columns={'trade_date': 'date', 'ts_code': 'code'})
            print(f"   ✅ 每日估值: {len(db):,} 条")
            self.daily_basic = db
        else:
            print("   ⚠️  每日估值文件不存在")
        
        # 新闻情感
        print("\n[6/6] 加载新闻舆情...")
        if os.path.exists('data/news_all.csv'):
            news = pd.read_csv('data/news_all.csv')
            date_col = None
            for candidate in ['pub_date', 'datetime', 'date']:
                if candidate in news.columns:
                    date_col = candidate
                    break
            if date_col and 'sentiment' in news.columns:
                news['date'] = pd.to_datetime(news[date_col]).dt.normalize()
                news_sentiment = news.groupby('date').agg({'sentiment': 'mean'}).reset_index()
                print(f"   ✅ 新闻舆情: {len(news_sentiment):,} 条")
                self.news = news_sentiment
            else:
                print("   ⚠️  新闻舆情字段不完整，跳过情绪特征")
        else:
            print("   ⚠️  新闻舆情文件不存在")
        
        return self.data is not None
    
    def merge_features(self, code, stock_df):
        """合并单只股票的特征数据"""
        if stock_df is None or len(stock_df) < 100:
            return None
        
        df = stock_df[['date', 'code', 'open', 'high', 'low', 'close', 'volume']].copy()
        
        # 资金流向
        if self.moneyflow is not None:
            mf = self.moneyflow[self.moneyflow['code'] == code][['date', 'net_mf_amount']]
            df = df.merge(mf, on='date', how='left')
        else:
            df['net_mf_amount'] = 0
        
        # 估值
        if self.daily_basic is not None and 'code' in self.daily_basic.columns:
            db = self.daily_basic[self.daily_basic['code'] == code][['date', 'pe', 'pb', 'ps']]
            df = df.merge(db, on='date', how='left')
        else:
            df['pe'] = 0
            df['pb'] = 0
            df['ps'] = 0
        
        # 北向资金、融资融券、新闻情感(全市场)
        if self.north_money is not None:
            df = df.merge(self.north_money, on='date', how='left')
        else:
            df['north_money'] = 0
        
        if self.margin is not None:
            df = df.merge(self.margin, on='date', how='left')
        else:
            df['rzye'] = 0
            df['rzmre'] = 0
            df['rqye'] = 0
        
        if self.news is not None:
            df = df.merge(self.news, on='date', how='left')
        else:
            df['sentiment'] = 0
        
        # 填充缺失值
        df = df.fillna(0)
        
        return df
    
    def extract_features(self, df, period_days=5):
        """从数据提取特征和标签"""
        if df is None or len(df) < 60:
            return None, None
        
        close = df['close'].values
        high = df['high'].values
        low = df['low'].values
        volume = df['volume'].values
        
        X_list = []
        y_list = []
        
        for i in range(60, len(close) - period_days, 5):
            features = {}
            
            # 技术指标
            window_close = close[:i+1]
            window_high = high[:i+1]
            window_low = low[:i+1]
            window_volume = volume[:i+1]
            
            # RSI
            rsi = self.ti.calculate_rsi(pd.Series(window_close))
            features['rsi'] = rsi if not np.isnan(rsi) else 50
            
            # MACD
            macd = self.ti.calculate_macd(pd.Series(window_close))
            features['macd_hist'] = macd['hist'] if not np.isnan(macd['hist']) else 0
            
            # MA
            ma5 = self.ti.calculate_ma(pd.Series(window_close), 5)
            ma20 = self.ti.calculate_ma(pd.Series(window_close), 20)
            ma60 = self.ti.calculate_ma(pd.Series(window_close), 60) if len(window_close) >= 60 else ma20
            
            features['price_ma5_ratio'] = (window_close[-1] / ma5 - 1) if ma5 != 0 else 0
            features['price_ma20_ratio'] = (window_close[-1] / ma20 - 1) if ma20 != 0 else 0
            features['price_ma60_ratio'] = (window_close[-1] / ma60 - 1) if ma60 != 0 else 0
            
            # 量价关系
            avg_volume = np.mean(window_volume[-20:]) if len(window_volume) >= 20 else window_volume[-1]
            features['volume_ratio'] = window_volume[-1] / avg_volume if avg_volume > 0 else 1
            
            # 波动率
            returns = np.diff(window_close) / window_close[:-1]
            features['volatility'] = np.std(returns[-20:]) * np.sqrt(252) if len(returns) >= 20 else 0.3
            
            # 收益率
            features['return_5d'] = (window_close[-1] - window_close[-6]) / window_close[-6] if len(window_close) >= 6 else 0
            features['return_10d'] = (window_close[-1] - window_close[-11]) / window_close[-11] if len(window_close) >= 11 else 0
            features['return_20d'] = (window_close[-1] - window_close[-21]) / window_close[-21] if len(window_close) >= 21 else 0
            
            # 价格位置
            if len(window_close) >= 60:
                price_min = np.min(window_close[-60:])
                price_max = np.max(window_close[-60:])
                features['price_position_60d'] = (window_close[-1] - price_min) / (price_max - price_min) if price_max > price_min else 0.5
            else:
                features['price_position_60d'] = 0.5
            
            # 布林带位置
            ma20_val = ma20
            std20 = np.std(window_close[-20:]) if len(window_close) >= 20 else 0
            if std20 > 0:
                features['bb_position'] = (window_close[-1] - (ma20_val - 2*std20)) / (4*std20)
            else:
                features['bb_position'] = 0.5
            
            # 基本面特征
            features['pe'] = df['pe'].iloc[i] or 0
            features['pb'] = df['pb'].iloc[i] or 0
            
            # 资金面特征
            features['net_mf_amount'] = df['net_mf_amount'].iloc[i] / 1e8 if df['net_mf_amount'].iloc[i] else 0
            features['north_money'] = df['north_money'].iloc[i] / 1e8 if df['north_money'].iloc[i] else 0
            features['rzye'] = df['rzye'].iloc[i] / 1e8 if df['rzye'].iloc[i] else 0
            
            # 市场情绪
            features['sentiment'] = df['sentiment'].iloc[i] if df['sentiment'].iloc[i] else 0
            
            # 处理NaN和Inf
            for k in features:
                if pd.isna(features[k]) or np.isinf(features[k]):
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
        
        X = pd.DataFrame(X_list, columns=list(features.keys()))
        y = pd.Series(y_list)
        
        return X, y
    
    def train_model(self, period_days, model_key):
        """训练指定周期的模型"""
        print(f"\n{'='*60}")
        print(f"训练A股 {model_key} 模型 ({period_days}日预测)")
        print(f"{'='*60}")
        
        all_X = []
        all_y = []
        
        codes = self.data['code'].unique()
        print(f"  遍历 {len(codes)} 只A股...")
        
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
        print(f"  ✅ 正样本比例: {y.sum()/len(y):.2%}")
        
        # 训练测试分割
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        print(f"  训练集: {len(X_train):,} 样本 | 验证集: {len(X_val):,} 样本")
        
        # 标准化
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        # 训练模型
        print("  训练模型中...")
        if HAS_XGB:
            model = xgb.XGBClassifier(
                n_estimators=150,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.5,
                random_state=42,
                eval_metric='logloss',
                use_label_encoder=False
            )
            model.fit(X_train_scaled, y_train, eval_set=[(X_val_scaled, y_val)], verbose=False)
        else:
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train_scaled, y_train)
        
        train_acc = model.score(X_train_scaled, y_train)
        val_acc = model.score(X_val_scaled, y_val)
        
        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")
        
        # 保存模型
        model_file = os.path.join(self.models_dir, f'{model_key}_model.pkl')
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': period_days,
            'asset_type': 'a_stock'
        }
        
        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"  ✅ 模型已保存: {model_file}")
        
        return model_data
    
    def run(self, periods=None):
        """运行A股模型训练"""
        print("=" * 60)
        print("A股模型训练 - 独立训练脚本")
        print("=" * 60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.load_data():
            print("❌ 数据加载失败")
            return False
        
        period_map = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}
        selected_periods = [p for p in (periods or [5, 20, 60]) if p in period_map]
        results = {}
        
        for period_days in selected_periods:
            model_key = period_map[period_days]
            results[model_key] = self.train_model(period_days, model_key)
        
        print("\n" + "=" * 60)
        print("A股训练完成汇总")
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
    parser = argparse.ArgumentParser(description='A股模型训练脚本')
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

    trainer = AStockTrainer()
    raise SystemExit(0 if trainer.run(periods=selected_periods or None) else 1)
