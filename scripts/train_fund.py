#!/usr/bin/env python3
"""
基金模型训练脚本 - 独立训练

用途: 针对基金单独训练预测模型
数据源: 基金净值数据 (如果有) + 基金属性信息
模型输出: data/models/fund_model.pkl

注: 基金通常使用长期持仓策略，不适合短期交易预测
    此脚本主要用于基金选择和投资组合优化
    基于基金的历史收益、波动率、评级等特征训练
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

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import RandomForestRegressor
    print("⚠️  XGBoost未安装，将使用RandomForest")


class FundTrainer:
    """基金模型训练器"""
    
    def __init__(self):
        self.ti = TechnicalIndicator()
        self.models_dir = 'data/models'
        os.makedirs(self.models_dir, exist_ok=True)
        self.data = None
    
    def load_data(self):
        """加载基金数据"""
        print("\n" + "=" * 60)
        print("加载基金数据")
        print("=" * 60)
        
        # 尝试加载基金净值数据
        fund_files = [
            'data/fund_nav.csv',
            'data/funds.csv',
            'data/historical_funds.csv'
        ]
        
        for file in fund_files:
            if os.path.exists(file):
                print(f"\n[1/1] 加载基金数据: {file}...")
                df = pd.read_csv(file)
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                elif 'trade_date' in df.columns:
                    df['date'] = pd.to_datetime(df['trade_date'])
                    df = df.drop('trade_date', axis=1)
                
                df = df.sort_values(['code', 'date']).reset_index(drop=True)
                print(f"   ✅ 基金数据: {len(df):,} 条, {df['code'].nunique()} 只基金")
                print(f"   ✅ 时间范围: {df['date'].min().date()} ~ {df['date'].max().date()}")
                self.data = df
                return True

        print("   ⚠️  未找到基金数据文件，尝试从数据库回退加载")
        try:
            from models import get_session, RawFundData
            session = get_session()
            rows = session.query(
                RawFundData.code,
                RawFundData.date,
                RawFundData.nav,
                RawFundData.accumulated_nav,
                RawFundData.daily_return,
            ).all()
            session.close()
            if rows:
                df = pd.DataFrame(rows, columns=['code', 'date', 'nav', 'accumulated_nav', 'daily_return'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(['code', 'date']).reset_index(drop=True)
                print(f"   ✅ 已从数据库回退加载基金数据: {len(df):,} 条, {df['code'].nunique()} 只基金")
                self.data = df
                return True
        except Exception as e:
            print(f"   ⚠️  数据库回退加载失败: {e}")

        print("   提示: 请先运行 collectors/fund_collector.py 采集基金数据")
        return False
    
    def extract_features(self, df):
        """从基金数据提取特征"""
        if df is None or len(df) < 20:
            return None, None
        
        nav_values = df['nav'].values if 'nav' in df.columns else df['close'].values
        
        X_list = []
        y_list = []
        
        for i in range(20, len(nav_values) - 20, 10):
            features = {}
            
            window_nav = nav_values[:i+1]
            
            # 收益率分析
            returns = np.diff(window_nav) / window_nav[:-1]
            
            features['return_5d'] = (nav_values[i] - nav_values[i-5]) / nav_values[i-5] if i >= 5 else 0
            features['return_10d'] = (nav_values[i] - nav_values[i-10]) / nav_values[i-10] if i >= 10 else 0
            features['return_30d'] = (nav_values[i] - nav_values[i-30]) / nav_values[i-30] if i >= 30 else 0
            
            # 波动率
            features['volatility_5d'] = np.std(returns[-5:]) * np.sqrt(252) if len(returns) >= 5 else 0
            features['volatility_10d'] = np.std(returns[-10:]) * np.sqrt(252) if len(returns) >= 10 else 0
            features['volatility_30d'] = np.std(returns[-30:]) * np.sqrt(252) if len(returns) >= 30 else 0
            
            # 夏普比率 (简化版)
            avg_return = np.mean(returns[-30:]) if len(returns) >= 30 else 0
            std_return = np.std(returns[-30:]) if len(returns) >= 30 else 1
            features['sharpe_ratio'] = (avg_return - 0.00005) / (std_return + 1e-6) if std_return > 0 else 0
            
            # 最大回撤
            cumulative = np.cumprod(1 + returns[-30:]) if len(returns) >= 30 else np.cumprod(1 + returns)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = (cumulative - running_max) / running_max
            features['max_drawdown'] = np.min(drawdown) if len(drawdown) > 0 else 0
            
            # 正收益天数占比
            positive_days = np.sum(returns[-20:] > 0) if len(returns) >= 20 else 0
            features['positive_days_ratio'] = positive_days / min(20, len(returns)) if len(returns) > 0 else 0
            
            # 均值回归
            ma10 = np.mean(window_nav[-10:]) if len(window_nav) >= 10 else window_nav[-1]
            features['nav_ma10_ratio'] = (window_nav[-1] / ma10 - 1) if ma10 > 0 else 0
            
            # 处理NaN
            for k in features:
                if pd.isna(features[k]) or np.isinf(features[k]):
                    features[k] = 0
            
            # 标签: 未来30天是否有正收益
            if i + 30 < len(nav_values):
                future_return = (nav_values[i + 30] - nav_values[i]) / nav_values[i]
                label = future_return  # 实际收益 (回归)
            else:
                label = 0
            
            X_list.append(list(features.values()))
            y_list.append(label)
        
        if len(X_list) == 0:
            return None, None
        
        X = pd.DataFrame(X_list, columns=list(features.keys()))
        y = pd.Series(y_list)
        
        return X, y
    
    def train_model(self):
        """训练基金选择模型"""
        print(f"\n{'='*60}")
        print("训练基金模型")
        print(f"{'='*60}")
        
        all_X = []
        all_y = []
        
        codes = self.data['code'].unique()
        print(f"  遍历 {len(codes)} 只基金...")
        
        for idx, code in enumerate(codes):
            if (idx + 1) % 100 == 0:
                print(f"    进度: {idx+1}/{len(codes)}")
            
            fund_df = self.data[self.data['code'] == code].copy()
            if len(fund_df) < 50:
                continue
            
            fund_df = fund_df.sort_values('date').reset_index(drop=True)
            
            X, y = self.extract_features(fund_df)
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
        print(f"  ✅ 目标变量范围: [{y.min():.4f}, {y.max():.4f}]")
        
        # 训练测试分割
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        print(f"  训练集: {len(X_train):,} 样本 | 验证集: {len(X_val):,} 样本")
        
        # 标准化
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        # 训练模型 (使用回归而不是分类)
        print("  训练模型中...")
        if HAS_XGB:
            model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42
            )
        else:
            model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        
        model.fit(X_train_scaled, y_train)
        
        train_score = model.score(X_train_scaled, y_train)
        val_score = model.score(X_val_scaled, y_val)
        
        print(f"  📊 训练 R²: {train_score:.4f}")
        print(f"  📊 验证 R²: {val_score:.4f}")
        
        # 保存模型
        model_file = os.path.join(self.models_dir, 'fund_model.pkl')
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X.columns),
            'train_score': train_score,
            'val_score': val_score,
            'train_date': datetime.now().isoformat(),
            'asset_type': 'fund'
        }
        
        with open(model_file, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"  ✅ 模型已保存: {model_file}")
        
        return model_data
    
    def run(self):
        """运行基金模型训练"""
        print("=" * 60)
        print("基金模型训练 - 独立训练脚本")
        print("=" * 60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.load_data():
            print("\n❌ 数据加载失败")
            return False
        
        result = self.train_model()
        
        print("\n" + "=" * 60)
        if result:
            print("✅ 基金模型训练成功")
        else:
            print("❌ 基金模型训练失败")
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        return bool(result)


if __name__ == '__main__':
    trainer = FundTrainer()
    raise SystemExit(0 if trainer.run() else 1)
