"""
预警监控模块 - alerts/monitor.py
监控持仓资产，触发预警
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).resolve().parent.parent))

from models import get_session, Holding, Warning as WarningModel
from alerts.rules import WarningRules
from alerts.notifier import Notifier
from indicators.technical import TechnicalIndicator
from indicators.money_flow import MoneyFlowIndicator
from collectors.stock_collector import StockCollector
from utils import get_logger

# 导入配置
try:
    from config import WARNING_DEDUP_HOURS, DAILY_DROP_THRESHOLD, DAILY_RISE_THRESHOLD
except ImportError:
    WARNING_DEDUP_HOURS = 24
    DAILY_DROP_THRESHOLD = 0.05
    DAILY_RISE_THRESHOLD = 0.07

logger = get_logger(__name__)


class WarningMonitor:
    """预警监控器"""
    
    def __init__(self):
        self.session = get_session()
        self.rules = WarningRules()
        self.notifier = Notifier()
        self.technical = TechnicalIndicator()
        self.money_flow = MoneyFlowIndicator()
        self.collector = StockCollector()
    
    def scan_holding(self, holding: Holding) -> List[Dict[str, Any]]:
        """扫描单个持仓的预警"""
        warnings = []
        
        try:
            df = self.collector.get_stock_data_from_db(holding.code)
            if df is None or len(df) < 60:
                logger.debug(f"{holding.code} 数据不足60天，跳过预警扫描")
                return warnings
            
            close = df['close']
            current_price = close.iloc[-1]
            
            # RSI 预警
            rsi = self.technical.calculate_rsi(close)
            warning = self.rules.check_rsi(rsi)
            if warning:
                warnings.append(warning)
            
            # 价格波动预警
            if len(close) >= 2:
                change_pct = (current_price - close.iloc[-2]) / close.iloc[-2] * 100
                warning = self.rules.check_price_change(change_pct)
                if warning:
                    warnings.append(warning)
            
            # MACD 预警
            if len(close) >= 26:
                macd = self.technical.calculate_macd(close)
                prev_macd = self.technical.calculate_macd(close.iloc[:-1])
                warning = self.rules.check_macd(
                    macd['dif'], macd['dea'], macd['hist'], prev_macd['hist']
                )
                if warning:
                    warnings.append(warning)
            
            # 均线破位预警
            ma20 = self.technical.calculate_ma(close, 20)
            if current_price < ma20:
                warning = self.rules.check_ma_break(current_price, ma20)
                if warning:
                    warnings.append(warning)
            
            # 资金流预警（如果数据可用）
            try:
                money_flow = self.money_flow.calculate_consecutive_flow(holding.code)
                if money_flow.get('is_warning', False):
                    warning = self.rules.check_money_flow(
                        money_flow.get('consecutive_days', 0),
                        money_flow.get('total_amount', 0)
                    )
                    if warning:
                        warnings.append(warning)
            except Exception as e:
                logger.debug(f"获取 {holding.code} 资金流数据失败: {e}")
            
        except Exception as e:
            logger.error(f"扫描 {holding.code} 预警失败: {e}")
        
        return warnings
    
    def _is_duplicate_warning(self, code: str, warning_type: str, 
                               dedup_hours: Optional[int] = None) -> bool:
        """
        检查是否已存在相同预警
        Args:
            code: 股票代码
            warning_type: 预警类型
            dedup_hours: 去重时间（小时），默认使用配置
        """
        if dedup_hours is None:
            dedup_hours = WARNING_DEDUP_HOURS
        
        cutoff_time = datetime.now() - timedelta(hours=dedup_hours)
        
        existing = self.session.query(WarningModel).filter(
            WarningModel.code == code,
            WarningModel.warning_type == warning_type,
            WarningModel.warning_time >= cutoff_time
        ).first()
        
        return existing is not None
    
    def scan_all_holdings(self) -> int:
        """扫描所有持仓的预警"""
        holdings = self.session.query(Holding).all()
        
        if not holdings:
            logger.info("无持仓数据，跳过预警扫描")
            return 0
        
        all_warnings = []
        
        for holding in holdings:
            warnings = self.scan_holding(holding)
            for w in warnings:
                # 检查是否已存在相同预警（使用配置的去重时间）
                if self._is_duplicate_warning(holding.code, w['type']):
                    logger.debug(f"跳过重复预警: {holding.code} - {w['type']}")
                    continue
                
                warning_record = WarningModel(
                    code=holding.code,
                    name=holding.name,
                    warning_time=datetime.now(),
                    warning_type=w['type'],
                    level=w['level'],
                    message=w['message'],
                    suggestion=w.get('suggestion', ''),
                    is_sent=False
                )
                self.session.add(warning_record)
                all_warnings.append(warning_record)
        
        self.session.commit()
        
        for warning in all_warnings:
            self.notifier.send_warning(warning)
        
        if all_warnings:
            logger.info(f"发现 {len(all_warnings)} 条新预警")
        else:
            logger.info("未发现新预警")
        
        return len(all_warnings)
    
    def get_current_warnings(self) -> List[Dict[str, Any]]:
        """获取当前预警（今日）"""
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        warnings = self.session.query(WarningModel).filter(
            WarningModel.warning_time >= today_start
        ).order_by(WarningModel.warning_time.desc()).all()
        
        result = []
        for w in warnings:
            result.append({
                'id': w.id,
                'code': w.code,
                'name': w.name,
                'type': w.warning_type,
                'level': w.level,
                'message': w.message,
                'suggestion': w.suggestion,
                'time': w.warning_time.strftime('%H:%M')
            })
        
        return result
    
    def get_warning_stats(self) -> Dict[str, int]:
        """获取预警统计"""
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        current_count = self.session.query(WarningModel).filter(
            WarningModel.warning_time >= today_start
        ).count()
        
        total_count = self.session.query(WarningModel).count()
        
        return {
            'current_count': current_count,
            'total_count': total_count
        }
    
    def clear_old_warnings(self, days: int = 30) -> int:
        """清理旧预警记录"""
        cutoff_time = datetime.now() - timedelta(days=days)
        
        deleted = self.session.query(WarningModel).filter(
            WarningModel.warning_time < cutoff_time
        ).delete()
        
        self.session.commit()
        logger.info(f"清理了 {deleted} 条 {days} 天前的预警记录")
        
        return deleted
    
    def close(self):
        """关闭资源"""
        self.session.close()


if __name__ == '__main__':
    monitor = WarningMonitor()
    count = monitor.scan_all_holdings()
    print(f"扫描完成，发现 {count} 条预警")
    
    stats = monitor.get_warning_stats()
    print(f"今日预警: {stats['current_count']} 条")
    print(f"总预警: {stats['total_count']} 条")
    
    monitor.close()