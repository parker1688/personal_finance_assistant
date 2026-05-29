"""
模拟交易员 - 持仓与价格管理 - trader/portfolio.py
负责从现有数据库表获取 T+1 开盘价 / 最新收盘价
"""

import sys
import os
from datetime import date, timedelta
import math

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, RawStockData, RawFundData
from utils import get_logger

logger = get_logger(__name__)

# 资产类型 -> 使用的表/价格字段映射
_STOCK_TYPES = {'a_stock', 'etf'}
_FUND_TYPES = {'active_fund'}
_PRECIOUS_TYPES = {'gold', 'silver'}

# 贵金属代码映射（使用 RawStockData 中存储的代码规范）
_PRECIOUS_CODE_MAP = {
    'gold': 'AU9999',   # 上海黄金交易所现货
    'silver': 'AG9999', # 上海黄金交易所白银
}


def _next_trading_day_open(session, code: str, asset_type: str, after_date: date) -> float | None:
    """
    获取 after_date 之后最近一个交易日的开盘价（T+1 成交价）。
    fund 类型无开盘价，返回 T+1 的 nav。
    """
    if asset_type in _FUND_TYPES:
        row = (
            session.query(RawFundData.nav, RawFundData.date)
            .filter(RawFundData.code == code, RawFundData.date > after_date)
            .order_by(RawFundData.date.asc())
            .first()
        )
        if row and row.nav:
            return float(row.nav)
        return None

    # A股 / ETF / 贵金属 均用 RawStockData
    row = (
        session.query(RawStockData.open, RawStockData.date)
        .filter(RawStockData.code == code, RawStockData.date > after_date)
        .order_by(RawStockData.date.asc())
        .first()
    )
    if row and row.open:
        return float(row.open)
    # 如果没有 open，兜底用 close
    row = (
        session.query(RawStockData.close, RawStockData.date)
        .filter(RawStockData.code == code, RawStockData.date > after_date)
        .order_by(RawStockData.date.asc())
        .first()
    )
    if row and row.close:
        return float(row.close)
    return None


def get_latest_price(session, code: str, asset_type: str, as_of: date | None = None) -> float | None:
    """
    获取指定日期（或最新）的收盘价，用于盈亏计算。
    as_of=None 表示取最新一条。
    """
    if asset_type in _FUND_TYPES:
        q = session.query(RawFundData.nav).filter(RawFundData.code == code)
        if as_of:
            q = q.filter(RawFundData.date <= as_of)
        row = q.order_by(RawFundData.date.desc()).first()
        if row and row.nav:
            return float(row.nav)
        return None

    q = session.query(RawStockData.close).filter(RawStockData.code == code)
    if as_of:
        q = q.filter(RawStockData.date <= as_of)
    row = q.order_by(RawStockData.date.desc()).first()
    if row and row.close:
        return float(row.close)
    return None


def get_execution_price(session, code: str, asset_type: str, signal_date: date) -> tuple[float | None, date | None]:
    """
    根据信号日期获取 T+1 成交价及实际成交日期。
    返回 (price, actual_trade_date) 或 (None, None)
    """
    if asset_type in _FUND_TYPES:
        row = (
            session.query(RawFundData.nav, RawFundData.date)
            .filter(RawFundData.code == code, RawFundData.date > signal_date)
            .order_by(RawFundData.date.asc())
            .first()
        )
        if row and row.nav:
            return float(row.nav), row.date
        return None, None

    row = (
        session.query(RawStockData.open, RawStockData.date)
        .filter(RawStockData.code == code, RawStockData.date > signal_date)
        .order_by(RawStockData.date.asc())
        .first()
    )
    if row:
        price = row.open if row.open else None
        if price is None:
            # 兜底取 close
            row2 = (
                session.query(RawStockData.close, RawStockData.date)
                .filter(RawStockData.code == code, RawStockData.date > signal_date)
                .order_by(RawStockData.date.asc())
                .first()
            )
            if row2 and row2.close:
                return float(row2.close), row2.date
        else:
            return float(price), row.date
    return None, None


def get_recent_return_series(session, code: str, asset_type: str, lookback: int = 30) -> list[tuple[date, float]]:
    """获取最近 lookback 个交易日的收益率序列，用于相关性计算。"""
    if asset_type in _FUND_TYPES:
        rows = (
            session.query(RawFundData.date, RawFundData.nav)
            .filter(RawFundData.code == code, RawFundData.nav.isnot(None))
            .order_by(RawFundData.date.desc())
            .limit(lookback + 1)
            .all()
        )
        rows = list(reversed(rows))
        values = [(r.date, float(r.nav)) for r in rows if r.nav is not None]
    else:
        rows = (
            session.query(RawStockData.date, RawStockData.close)
            .filter(RawStockData.code == code, RawStockData.close.isnot(None))
            .order_by(RawStockData.date.desc())
            .limit(lookback + 1)
            .all()
        )
        rows = list(reversed(rows))
        values = [(r.date, float(r.close)) for r in rows if r.close is not None]

    returns = []
    for idx in range(1, len(values)):
        prev_date, prev_price = values[idx - 1]
        curr_date, curr_price = values[idx]
        if prev_price:
            returns.append((curr_date, (curr_price - prev_price) / prev_price))
    return returns[-lookback:]


def calculate_return_correlation(series_a: list[tuple[date, float]], series_b: list[tuple[date, float]]) -> float | None:
    """按共同日期对齐后计算 Pearson 收益相关系数。"""
    if not series_a or not series_b:
        return None

    map_a = {d: v for d, v in series_a}
    map_b = {d: v for d, v in series_b}
    common_dates = sorted(set(map_a) & set(map_b))
    if len(common_dates) < 5:
        return None

    values_a = [map_a[d] for d in common_dates]
    values_b = [map_b[d] for d in common_dates]

    mean_a = sum(values_a) / len(values_a)
    mean_b = sum(values_b) / len(values_b)
    cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(values_a, values_b))
    var_a = sum((a - mean_a) ** 2 for a in values_a)
    var_b = sum((b - mean_b) ** 2 for b in values_b)
    if var_a <= 0 or var_b <= 0:
        return None

    corr = cov / math.sqrt(var_a * var_b)
    return max(-1.0, min(1.0, corr))
