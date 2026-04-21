"""
基本面指标模块 - indicators/fundamental.py
"""

import pandas as pd
import numpy as np


class FundamentalIndicator:
    """基本面指标计算器"""
    
    def __init__(self):
        pass
    
    def calculate_pe_score(self, pe, industry_pe=None):
        """计算PE得分"""
        if pe is None or pe <= 0:
            return 3.0
        
        if industry_pe and industry_pe > 0:
            ratio = pe / industry_pe
            if ratio < 0.7:
                return 4.5
            elif ratio < 0.9:
                return 4.0
            elif ratio < 1.1:
                return 3.0
            elif ratio < 1.3:
                return 2.0
            else:
                return 1.0
        
        if pe < 10:
            return 4.5
        elif pe < 20:
            return 4.0
        elif pe < 30:
            return 3.0
        elif pe < 50:
            return 2.0
        else:
            return 1.0
    
    def calculate_pb_score(self, pb, industry_pb=None):
        """计算PB得分"""
        if pb is None or pb <= 0:
            return 3.0
        
        if industry_pb and industry_pb > 0:
            ratio = pb / industry_pb
            if ratio < 0.7:
                return 4.5
            elif ratio < 0.9:
                return 4.0
            elif ratio < 1.1:
                return 3.0
            elif ratio < 1.3:
                return 2.0
            else:
                return 1.0
        
        if pb < 1:
            return 4.5
        elif pb < 2:
            return 4.0
        elif pb < 3:
            return 3.0
        elif pb < 5:
            return 2.0
        else:
            return 1.0
    
    def calculate_roe_score(self, roe):
        """计算ROE得分"""
        if roe is None:
            return 3.0
        
        if roe > 20:
            return 5.0
        elif roe > 15:
            return 4.0
        elif roe > 10:
            return 3.0
        elif roe > 5:
            return 2.0
        else:
            return 1.0
    
    def calculate_growth_score(self, revenue_growth, profit_growth):
        """计算成长性得分"""
        score = 3.0
        
        if revenue_growth:
            if revenue_growth > 30:
                score += 1.0
            elif revenue_growth > 20:
                score += 0.5
            elif revenue_growth < 0:
                score -= 0.5
        
        if profit_growth:
            if profit_growth > 30:
                score += 1.0
            elif profit_growth > 20:
                score += 0.5
            elif profit_growth < 0:
                score -= 0.5
        
        return max(1.0, min(5.0, score))
    
    def get_valuation_level(self, pe):
        """兼容旧接口：根据PE返回估值水平。"""
        score = self.calculate_pe_score(pe)
        if pe is None or pe <= 0:
            level = 'unknown'
            level_text = '暂无估值'
        elif pe < 15:
            level = 'low'
            level_text = '估值偏低'
        elif pe < 30:
            level = 'reasonable'
            level_text = '估值合理'
        elif pe < 50:
            level = 'high'
            level_text = '估值偏高'
        else:
            level = 'very_high'
            level_text = '估值很高'
        return {
            'level': level,
            'level_text': level_text,
            'score': float(score),
        }

    def calculate_value_score(self, code=None, pe=None, pb=None, roe=None,
                              industry_pe=None, industry_name=None,
                              revenue_growth=None, profit_growth=None):
        """兼容旧接口：返回1-5区间的综合估值得分。"""
        return float(self.get_fundamental_score(
            pe=pe,
            pb=pb,
            roe=roe,
            revenue_growth=revenue_growth,
            profit_growth=profit_growth,
        ))

    def get_fundamental_score(self, pe=None, pb=None, roe=None, 
                               revenue_growth=None, profit_growth=None):
        """获取基本面综合得分"""
        scores = []
        
        if pe is not None:
            scores.append(self.calculate_pe_score(pe))
        if pb is not None:
            scores.append(self.calculate_pb_score(pb))
        if roe is not None:
            scores.append(self.calculate_roe_score(roe))
        
        if revenue_growth or profit_growth:
            scores.append(self.calculate_growth_score(revenue_growth, profit_growth))
        
        if not scores:
            return 3.0
        
        return sum(scores) / len(scores)


if __name__ == '__main__':
    fi = FundamentalIndicator()
    print(f"PE=15, 得分: {fi.calculate_pe_score(15)}")
    print(f"PB=1.5, 得分: {fi.calculate_pb_score(1.5)}")
    print(f"ROE=18%, 得分: {fi.calculate_roe_score(18)}")
