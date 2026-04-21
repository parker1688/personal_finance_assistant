"""
回测验证模块 - reviews/backtest_validator.py
验证未来信号建议的准确性 (take_profit和add建议的目标价达成率)
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).resolve().parent.parent))

from models import get_session, Holding, Recommendation, Prediction
from collectors.stock_collector import StockCollector
from utils import get_logger

logger = get_logger(__name__)


class BacktestValidator:
    """未来信号准确性回测验证器"""
    
    def __init__(self):
        self.session = get_session()
        self.collector = StockCollector()

    def _normalize_history_frame(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        """兼容 date/trade_date/DatetimeIndex/普通日期索引 四种历史数据格式。"""
        if df is None or len(df) == 0:
            return pd.DataFrame()

        history = df.copy()
        if 'close' not in history.columns:
            return pd.DataFrame()

        if isinstance(history.index, pd.DatetimeIndex):
            index_name = history.index.name or 'date'
            history = history.reset_index().rename(columns={index_name: 'date'})
        elif 'trade_date' in history.columns and 'date' not in history.columns:
            history = history.rename(columns={'trade_date': 'date'})
        elif 'date' not in history.columns and not isinstance(history.index, pd.RangeIndex):
            history = history.reset_index().rename(columns={history.reset_index().columns[0]: 'date'})

        if 'date' not in history.columns:
            return pd.DataFrame()

        history['date'] = pd.to_datetime(history['date'], errors='coerce')
        history['close'] = pd.to_numeric(history['close'], errors='coerce')
        history = history.dropna(subset=['date', 'close']).sort_values('date').reset_index(drop=True)
        return history

    def _resolve_current_price(self, holding, rec=None, history: Optional[pd.DataFrame] = None) -> float:
        """兼容当前模型字段，优先取推荐快照价格，再回退到历史收盘或成本价。"""
        for candidate in [
            getattr(holding, 'current_price_cny', None),
            getattr(holding, 'current_price', None),
            getattr(rec, 'current_price', None) if rec is not None else None,
            getattr(holding, 'cost_price', None),
        ]:
            try:
                if candidate is not None and float(candidate) > 0:
                    return float(candidate)
            except Exception:
                continue

        try:
            if history is not None and not history.empty:
                latest = pd.to_numeric(history['close'].iloc[-1], errors='coerce')
                if pd.notna(latest) and float(latest) > 0:
                    return float(latest)
        except Exception:
            pass

        return 0.0
    
    def validate_take_profit_signals(self, holding_id: int, days: int = 30) -> Dict[str, Any]:
        """
        验证止盈建议的准确性
        
        Args:
            holding_id: 持仓ID
            days: 回看天数（默认30天）
        
        Returns:
            {
                'holding_id': int,
                'code': str,
                'name': str,
                'signal_date': date,
                'signal_target_price': float,
                'signal_profit_rate': float,
                'actual_max_price': float,
                'days_to_target': int or None,  # 多少天后达到目标
                'target_hit': bool,              # 是否达到目标
                'max_profit_rate': float,        # 最高盈利率
                'status': 'hit' | 'building' | 'miss'
            }
        """
        try:
            holding = self.session.query(Holding).filter_by(id=holding_id).first()
            if not holding:
                return {'error': f'持仓ID {holding_id} 不存在'}
            
            # 获取历史价格数据
            df = self.collector.get_stock_data_from_db(holding.code)
            history = self._normalize_history_frame(df)
            if history is None or len(history) < 2:
                return {'error': f'{holding.code} 数据不足'}
            
            # 获取最新推荐（兼容当前 Recommendation.type 字段）
            rec = self.session.query(Recommendation).filter(
                Recommendation.code == holding.code,
                Recommendation.type == self._infer_rec_type(holding.asset_type, holding.code)
            ).order_by(Recommendation.created_at.desc()).first()
            
            if not rec:
                return {'error': f'{holding.code} 无推荐记录'}
            
            # 获取当前价格和目标价格
            current_price = self._resolve_current_price(holding, rec=rec, history=history)
            target_price = getattr(rec, 'target_high_20d', None) or getattr(rec, 'target_price_20d', None) or current_price * 1.05
            
            # 获取信号日期后的价格数据
            signal_date = rec.created_at.date() if rec.created_at else datetime.now().date()
            history_after_signal = history[history['date'] >= pd.Timestamp(signal_date)]
            
            if len(history_after_signal) < 1:
                return {
                    'holding_id': holding_id,
                    'code': holding.code,
                    'name': holding.name,
                    'signal_date': signal_date,
                    'signal_target_price': target_price,
                    'signal_profit_rate': (target_price - current_price) / current_price * 100 if current_price > 0 else 0,
                    'actual_max_price': current_price,
                    'days_to_target': None,
                    'target_hit': False,
                    'max_profit_rate': 0,
                    'status': 'building'
                }
            
            # 计算最高价和目标达成情况
            max_price = history_after_signal['close'].max()
            max_profit_rate = (max_price - current_price) / current_price * 100 if current_price > 0 else 0
            
            # 检查是否达到目标价
            target_hit = max_price >= target_price
            
            # 找出第一次达到目标价的日期
            days_to_target = None
            if target_hit:
                target_rows = history_after_signal[history_after_signal['close'] >= target_price]
                if len(target_rows) > 0:
                    target_date = pd.to_datetime(target_rows.iloc[0]['date']).date()
                    days_to_target = (target_date - signal_date).days
            
            # 判断状态
            if target_hit:
                status = 'hit'
            elif max_price >= current_price * 0.95:  # 至少有5%的上涨可能
                status = 'building'
            else:
                status = 'miss'
            
            return {
                'holding_id': holding_id,
                'code': holding.code,
                'name': holding.name,
                'signal_date': signal_date,
                'signal_target_price': target_price,
                'signal_profit_rate': (target_price - current_price) / current_price * 100 if current_price > 0 else 0,
                'actual_max_price': max_price,
                'days_to_target': days_to_target,
                'target_hit': target_hit,
                'max_profit_rate': max_profit_rate,
                'status': status
            }
            
        except Exception as e:
            logger.error(f"验证止盈信号失败 (ID:{holding_id}): {e}")
            return {'error': str(e)}
    
    def validate_add_signals(self, holding_id: int, days: int = 30) -> Dict[str, Any]:
        """
        验证加仓建议的准确性
        
        Args:
            holding_id: 持仓ID
            days: 回看天数（默认30天）
        
        Returns:
            {
                'holding_id': int,
                'code': str,
                'name': str,
                'signal_date': date,
                'signal_entry_price': float,
                'actual_best_price': float,  # 最低价（最佳建仓价）
                'entry_quality': float,      # 0-1 入场质量
                'price_move': float,         # 建议后价格变化 %
                'status': 'profitable' | 'breakeven' | 'loss' | 'too_early'
            }
        """
        try:
            holding = self.session.query(Holding).filter_by(id=holding_id).first()
            if not holding:
                return {'error': f'持仓ID {holding_id} 不存在'}
            
            # 获取历史价格数据
            df = self.collector.get_stock_data_from_db(holding.code)
            history = self._normalize_history_frame(df)
            if history is None or len(history) < 2:
                return {'error': f'{holding.code} 数据不足'}
            
            # 获取最新推荐
            rec = self.session.query(Recommendation).filter(
                Recommendation.code == holding.code
            ).order_by(Recommendation.created_at.desc()).first()
            
            if not rec:
                return {'error': f'{holding.code} 无推荐记录'}
            
            # 获取当前价格和建议价格
            current_price = self._resolve_current_price(holding, rec=rec, history=history)
            entry_price = getattr(rec, 'target_low_5d', None) or getattr(rec, 'target_price_5d', None) or current_price * 0.98
            
            # 获取信号日期后的价格数据
            signal_date = rec.created_at.date() if rec.created_at else datetime.now().date()
            history_after_signal = history[history['date'] >= pd.Timestamp(signal_date)]
            
            if len(history_after_signal) < 1:
                return {
                    'holding_id': holding_id,
                    'code': holding.code,
                    'name': holding.name,
                    'signal_date': signal_date,
                    'signal_entry_price': entry_price,
                    'actual_best_price': current_price,
                    'entry_quality': 0,
                    'price_move': 0,
                    'status': 'too_early'
                }
            
            # 计算最低价（最佳建仓机会）
            min_price = history_after_signal['close'].min()
            current = history_after_signal['close'].iloc[-1]
            
            # 入场质量：0-1 之间，越接近1越好
            price_range = history_after_signal['close'].max() - history_after_signal['close'].min()
            if price_range > 0:
                entry_quality = (current - min_price) / price_range
            else:
                entry_quality = 0.5
            
            # 价格变化
            price_move = (current - entry_price) / entry_price * 100 if entry_price > 0 else 0
            
            # 状态判断
            if price_move > 2:
                status = 'profitable'
            elif price_move > -2:
                status = 'breakeven'
            elif min_price < entry_price:
                status = 'loss'
            else:
                status = 'too_early'
            
            return {
                'holding_id': holding_id,
                'code': holding.code,
                'name': holding.name,
                'signal_date': signal_date,
                'signal_entry_price': entry_price,
                'actual_best_price': min_price,
                'entry_quality': entry_quality,
                'price_move': price_move,
                'status': status
            }
            
        except Exception as e:
            logger.error(f"验证加仓信号失败 (ID:{holding_id}): {e}")
            return {'error': str(e)}
    
    def generate_backtest_report(self, days_lookback: int = 30) -> Dict[str, Any]:
        """
        生成完整的回测验证报告
        
        Args:
            days_lookback: 回看天数（默认30天，检查过去30天的建议准确性）
        
        Returns:
            {
                'report_date': datetime,
                'analysis_period_days': int,
                'take_profit_analysis': {
                    'total_signals': int,
                    'targets_hit': int,
                    'hit_rate': float,           # %
                    'avg_profit_rate': float,    # %
                    'avg_days_to_target': float
                },
                'add_signals_analysis': {
                    'total_signals': int,
                    'profitable_signals': int,
                    'profitable_rate': float,    # %
                    'avg_price_move': float,     # %
                    'avg_entry_quality': float
                },
                'overall_accuracy': float,       # % （取平均）
                'recommendations': str          # AI建议
            }
        """
        try:
            # 查询所有持仓，并按代码去重，避免重复持仓把回测样本放大失真
            raw_holdings = self.session.query(Holding).all()
            unique_holdings = {}
            for holding in raw_holdings:
                key = str(getattr(holding, 'code', '') or '').strip().upper()
                if key and key not in unique_holdings:
                    unique_holdings[key] = holding
            holdings = list(unique_holdings.values())
            
            take_profit_results = []
            add_signal_results = []
            
            for holding in holdings:
                # 验证止盈建议
                tp_result = self.validate_take_profit_signals(holding.id, days_lookback)
                if 'error' not in tp_result:
                    take_profit_results.append(tp_result)
                
                # 验证加仓建议
                add_result = self.validate_add_signals(holding.id, days_lookback)
                if 'error' not in add_result:
                    add_signal_results.append(add_result)
            
            # 分析止盈信号
            tp_analysis = {
                'total_signals': len(take_profit_results),
                'targets_hit': sum(1 for r in take_profit_results if r.get('target_hit')),
                'avg_profit_rate': np.mean([r.get('signal_profit_rate', 0) for r in take_profit_results]) if take_profit_results else 0,
                'avg_days_to_target': np.mean([r.get('days_to_target', 0) for r in take_profit_results if r.get('days_to_target')]) if take_profit_results else 0,
            }
            tp_analysis['hit_rate'] = (tp_analysis['targets_hit'] / tp_analysis['total_signals'] * 100) if tp_analysis['total_signals'] > 0 else 0
            
            # 分析加仓信号
            add_analysis = {
                'total_signals': len(add_signal_results),
                'profitable_signals': sum(1 for r in add_signal_results if r.get('status') == 'profitable'),
                'avg_price_move': np.mean([r.get('price_move', 0) for r in add_signal_results]) if add_signal_results else 0,
                'avg_entry_quality': np.mean([r.get('entry_quality', 0) for r in add_signal_results]) if add_signal_results else 0,
            }
            add_analysis['profitable_rate'] = (add_analysis['profitable_signals'] / add_analysis['total_signals'] * 100) if add_analysis['total_signals'] > 0 else 0
            
            # 计算整体准确率
            total_action_samples = tp_analysis['total_signals'] + add_analysis['total_signals']
            has_action_samples = total_action_samples > 0
            overall_accuracy = ((tp_analysis['hit_rate'] + add_analysis['profitable_rate']) / 2) if has_action_samples else 0
            
            def _grade(pct, total_signals):
                if not total_signals:
                    return 'N/A'
                if pct >= 70:
                    return 'A'
                if pct >= 60:
                    return 'B'
                if pct >= 50:
                    return 'C'
                return 'D'

            action_quality_summary = {
                'take_profit_grade': _grade(tp_analysis['hit_rate'], tp_analysis['total_signals']),
                'add_signal_grade': _grade(add_analysis['profitable_rate'], add_analysis['total_signals']),
                'overall_grade': _grade(overall_accuracy, total_action_samples),
                'has_action_samples': has_action_samples,
                'sample_size': int(total_action_samples),
            }

            # 生成建议
            if not has_action_samples:
                recommendation = "⏳ 历史动作样本不足，系统正在等待更多到期预测完成验证"
            elif overall_accuracy >= 70:
                recommendation = "✅ 信号系统表现良好，通过率>70%，可以信任建议"
            elif overall_accuracy >= 50:
                recommendation = "⚠️ 信号系统表现一般，通过率≥50%，建议结合其他技术面判断"
            else:
                recommendation = "❌ 信号系统准确率<50%，建议不要盲目跟随建议"
            
            return {
                'report_date': datetime.now(),
                'analysis_period_days': days_lookback,
                'take_profit_analysis': tp_analysis,
                'add_signals_analysis': add_analysis,
                'overall_accuracy': overall_accuracy,
                'action_quality_summary': action_quality_summary,
                'recommendations': recommendation,
                'sample_take_profit_results': take_profit_results[:5],
                'sample_add_results': add_signal_results[:5]
            }
            
        except Exception as e:
            logger.error(f"生成回测报告失败: {e}", exc_info=True)
            return {
                'error': str(e),
                'report_date': datetime.now()
            }
    
    def _infer_rec_type(self, asset_type: str, code: str) -> str:
        """将持仓资产类型映射为推荐类型"""
        if asset_type == 'fund':
            return 'active_fund'
        if asset_type == 'etf':
            return 'etf'
        if asset_type == 'gold':
            return 'gold'
        if asset_type == 'silver':
            return 'silver'
        c = (code or '').upper()
        if c.endswith('.HK'):
            return 'hk_stock'
        if c.endswith('.SH') or c.endswith('.SZ'):
            return 'a_stock'
        return 'us_stock'

    def close(self):
        """关闭资源"""
        self.session.close()
