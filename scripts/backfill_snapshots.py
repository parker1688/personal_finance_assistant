#!/usr/bin/env python3
"""
回溯历史持仓快照数据
基于历史价格数据计算每日持仓市值
"""

import sys
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Holding, HoldingSnapshot
from api.holdings import get_current_price
from utils import get_logger

logger = get_logger(__name__)


def get_historical_price(code, asset_type, date):
    """获取指定日期的历史价格"""
    try:
        if asset_type == 'gold':
            ticker = yf.Ticker('GC=F')
            start = date - timedelta(days=5)
            end = date + timedelta(days=1)
            hist = ticker.history(start=start, end=end)
            if len(hist) > 0:
                # 找到最接近的日期
                hist.index = pd.to_datetime(hist.index).date
                if date in hist.index:
                    price_usd = hist.loc[date, 'Close']
                else:
                    # 取最近的
                    closest_date = min(hist.index, key=lambda x: abs((x - date).days))
                    price_usd = hist.loc[closest_date, 'Close']
                
                usd_to_cny = 7.25
                price_cny_per_gram = price_usd * usd_to_cny / 31.1035
                return price_cny_per_gram
        
        elif asset_type == 'fund':
            # 基金使用最近净值（简化处理）
            return get_current_price(code, asset_type)
        
        else:
            # 股票/ETF
            ticker = yf.Ticker(code)
            start = date - timedelta(days=5)
            end = date + timedelta(days=1)
            hist = ticker.history(start=start, end=end)
            if len(hist) > 0:
                hist.index = pd.to_datetime(hist.index).date
                if date in hist.index:
                    return hist.loc[date, 'Close']
                else:
                    closest_date = min(hist.index, key=lambda x: abs((x - date).days))
                    return hist.loc[closest_date, 'Close']
        
        return None
    except Exception as e:
        logger.error(f"获取历史价格失败 {code} {date}: {e}")
        return None


def backfill_snapshots(days=30):
    """回溯历史快照"""
    session = get_session()
    
    # 获取当前所有持仓
    holdings = session.query(Holding).all()
    if not holdings:
        logger.warning("无持仓数据")
        return
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    total_records = 0
    
    for single_date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1)):
        for h in holdings:
            # 检查是否已有快照
            existing = session.query(HoldingSnapshot).filter(
                HoldingSnapshot.snapshot_date == single_date,
                HoldingSnapshot.holding_id == h.id
            ).first()
            
            if existing:
                continue
            
            asset_type = h.asset_type if hasattr(h, 'asset_type') and h.asset_type else 'stock'
            
            # 获取历史价格
            price = get_historical_price(h.code, asset_type, single_date)
            if price is None:
                price = h.cost_price
            
            market_value = h.quantity * price
            
            snapshot = HoldingSnapshot(
                snapshot_date=single_date,
                holding_id=h.id,
                asset_type=asset_type,
                code=h.code,
                name=h.name,
                quantity=h.quantity,
                cost_price=h.cost_price,
                market_price=price,
                market_value=market_value
            )
            session.add(snapshot)
            total_records += 1
        
        if total_records % 100 == 0:
            session.commit()
            logger.info(f"已处理 {total_records} 条记录")
    
    session.commit()
    logger.info(f"✅ 回溯完成，共 {total_records} 条快照记录")
    session.close()


if __name__ == '__main__':
    backfill_snapshots(30)
