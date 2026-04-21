"""
预警规则模块 - warnings/rules.py
定义各类预警的触发规则
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger

logger = get_logger(__name__)


class WarningRules:
    """预警规则定义"""
    
    def __init__(self):
        # 默认阈值
        self.thresholds = {
            'rsi_overbought': 80,
            'rsi_oversold': 20,
            'macd_dead_cross': True,
            'ma_break_days': 20,
            'consecutive_outflow_days': 3,
            'daily_drop_threshold': 0.05,
            'daily_rise_threshold': 0.07,
            'pe_percentile_high': 90,
            'single_asset_ratio': 0.20,
            'sentiment_negative': -0.3
        }
    
    def update_thresholds(self, **kwargs):
        """更新阈值"""
        for key, value in kwargs.items():
            if key in self.thresholds:
                self.thresholds[key] = value
                logger.info(f"阈值已更新: {key} = {value}")
    
    def check_rsi(self, rsi):
        """
        检查RSI预警
        Returns:
            dict: 预警信息或None
        """
        if rsi >= self.thresholds['rsi_overbought']:
            return {
                'type': '技术超买',
                'level': 'high',
                'message': f'RSI={rsi:.1f}，处于超买区，注意回调风险',
                'suggestion': '注意回调风险，可考虑部分止盈'
            }
        elif rsi <= self.thresholds['rsi_oversold']:
            return {
                'type': '技术超卖',
                'level': 'low',
                'message': f'RSI={rsi:.1f}，处于超卖区，关注反弹机会',
                'suggestion': '关注企稳信号，暂不盲目抄底'
            }
        return None
    
    def check_macd(self, dif, dea, hist, prev_hist=None):
        """
        检查MACD预警
        Returns:
            dict: 预警信息或None
        """
        # 死叉判断
        if prev_hist is not None:
            if prev_hist > 0 and hist < 0:
                return {
                    'type': 'MACD死叉',
                    'level': 'high',
                    'message': 'MACD刚刚形成死叉，短期趋势转弱',
                    'suggestion': '考虑减仓或设止损'
                }
            elif prev_hist < 0 and hist > 0:
                return {
                    'type': 'MACD金叉',
                    'level': 'low',
                    'message': 'MACD刚刚形成金叉，短期趋势转强',
                    'suggestion': '关注突破机会'
                }
        return None
    
    def check_ma_break(self, current_price, ma):
        """
        检查均线破位预警
        Returns:
            dict: 预警信息或None
        """
        if current_price < ma:
            return {
                'type': '均线破位',
                'level': 'high',
                'message': f'收盘价跌破{self.thresholds["ma_break_days"]}日均线，中期趋势走弱',
                'suggestion': '建议减仓或设置止损线'
            }
        return None
    
    def check_money_flow(self, consecutive_days, total_outflow):
        """
        检查资金流预警
        Returns:
            dict: 预警信息或None
        """
        if consecutive_days >= self.thresholds['consecutive_outflow_days']:
            return {
                'type': '主力流出',
                'level': 'medium',
                'message': f'连续{consecutive_days}日主力净流出，累计{total_outflow/10000:.1f}亿',
                'suggestion': '关注资金流向变化，谨慎持有'
            }
        return None
    
    def check_price_change(self, change_pct):
        """
        检查价格波动预警
        Returns:
            dict: 预警信息或None
        """
        if change_pct <= -self.thresholds['daily_drop_threshold'] * 100:
            return {
                'type': '大幅下跌',
                'level': 'high',
                'message': f'今日跌{abs(change_pct):.1f}%，请注意',
                'suggestion': '关注是否有基本面变化，考虑止损'
            }
        elif change_pct >= self.thresholds['daily_rise_threshold'] * 100:
            return {
                'type': '大幅上涨',
                'level': 'low',
                'message': f'今日涨{change_pct:.1f}%，短期过热',
                'suggestion': '注意追高风险，可考虑部分止盈'
            }
        return None
    
    def check_valuation(self, pe_percentile):
        """
        检查估值预警
        Returns:
            dict: 预警信息或None
        """
        if pe_percentile >= self.thresholds['pe_percentile_high']:
            return {
                'type': '估值偏高',
                'level': 'medium',
                'message': f'PE处于历史{pe_percentile:.0f}%分位，估值偏高',
                'suggestion': '关注估值消化情况，谨慎追高'
            }
        return None
    
    def check_concentration(self, asset_ratio, asset_name):
        """
        检查集中度预警
        Returns:
            dict: 预警信息或None
        """
        if asset_ratio >= self.thresholds['single_asset_ratio']:
            return {
                'type': '集中度偏高',
                'level': 'medium',
                'message': f'{asset_name}占您总资产{asset_ratio*100:.1f}%，超过{self.thresholds["single_asset_ratio"]*100:.0f}%建议上限',
                'suggestion': '考虑部分减仓，分散配置'
            }
        return None
    
    def check_sentiment(self, sentiment_score):
        """
        检查新闻情绪预警
        Returns:
            dict: 预警信息或None
        """
        if sentiment_score <= self.thresholds['sentiment_negative']:
            return {
                'type': '负面新闻',
                'level': 'medium',
                'message': f'新闻情感得分{sentiment_score:.2f}，偏负面',
                'suggestion': '关注消息面变化，评估影响'
            }
        return None


# 测试代码
if __name__ == '__main__':
    rules = WarningRules()
    
    # 测试各规则
    print("RSI超买:", rules.check_rsi(85))
    print("RSI超卖:", rules.check_rsi(15))
    print("MACD死叉:", rules.check_macd(0, 0, -0.1, 0.1))
    print("大幅下跌:", rules.check_price_change(-6))
    print("估值偏高:", rules.check_valuation(95))