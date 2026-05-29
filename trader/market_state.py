"""
模拟交易员 - 市场状态识别与动态策略 - trader/market_state.py

目标:
- 基于本地真实价格数据识别市场状态（bullish/neutral/volatile/bearish）
- 输出动态买入阈值、单笔仓位、现金保留比例，供交易员引擎使用
"""

import sys
import os
from statistics import pstdev

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import RawStockData


BENCHMARK_CODES = [
    '510300.SH',  # 沪深300ETF
    '159919.SZ',  # 沪深300ETF(深)
    '510500.SH',  # 中证500ETF
    '510050.SH',  # 上证50ETF
]


def _safe_return_pct(new_value, old_value):
    if old_value in (None, 0):
        return 0.0
    return (float(new_value) - float(old_value)) / float(old_value) * 100.0


def detect_market_state(session, as_of=None) -> dict:
    """检测A股市场状态并输出上下文指标。

    Args:
        as_of: 截止日期（回测时传入 signal_date，避免前视偏差）。
               为 None 时取最新数据（实盘模式）。
    """
    rows = []
    selected_code = None
    for code in BENCHMARK_CODES:
        q = (
            session.query(RawStockData.date, RawStockData.close)
            .filter(RawStockData.code == code, RawStockData.close.isnot(None))
        )
        if as_of is not None:
            q = q.filter(RawStockData.date <= as_of)
        rows = q.order_by(RawStockData.date.desc()).limit(30).all()
        if len(rows) >= 10:
            selected_code = code
            break

    if len(rows) < 10:
        return {
            'state': 'neutral',
            'benchmark_code': selected_code,
            'reason': '基准数据不足，回退到中性策略',
            'return_5d_pct': 0.0,
            'return_20d_pct': 0.0,
            'volatility_pct': 0.0,
        }

    rows = list(reversed(rows))
    closes = [float(r.close) for r in rows]
    last_close = closes[-1]
    close_5 = closes[-6] if len(closes) >= 6 else closes[0]
    close_20 = closes[-21] if len(closes) >= 21 else closes[0]

    ret_5 = _safe_return_pct(last_close, close_5)
    ret_20 = _safe_return_pct(last_close, close_20)

    daily_returns = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        curr = closes[idx]
        if prev:
            daily_returns.append((curr - prev) / prev * 100.0)
    vol = pstdev(daily_returns[-20:]) if len(daily_returns) >= 2 else 0.0

    if ret_20 >= 4.0 and ret_5 >= 1.0 and vol < 2.2:
        state = 'bullish'
        reason = '中期上行且短期延续，波动可控'
    elif ret_20 <= -4.0 or ret_5 <= -2.5:
        state = 'bearish'
        reason = '短中期回撤显著，进入防守模式'
    elif vol >= 2.2:
        state = 'volatile'
        reason = '波动率较高，优先控制风险'
    else:
        state = 'neutral'
        reason = '趋势与波动均处于中性区间'

    return {
        'state': state,
        'benchmark_code': selected_code,
        'reason': reason,
        'return_5d_pct': round(ret_5, 2),
        'return_20d_pct': round(ret_20, 2),
        'volatility_pct': round(vol, 2),
    }


def compute_adaptive_policy(session, cfg, as_of=None) -> dict:
    """根据市场状态生成动态策略参数。

    Args:
        as_of: 截止日期，透传给 detect_market_state 以避免前视偏差。
    """
    market = detect_market_state(session, as_of=as_of)

    buy_threshold = float(cfg.buy_score_threshold)
    max_position_pct = float(cfg.max_single_position_pct)
    min_cash_reserve_pct = float(cfg.min_cash_reserve_pct)

    if market['state'] == 'bullish':
        buy_threshold = max(0.45, buy_threshold - 0.05)
        max_position_pct = min(0.10, max_position_pct + 0.01)
        min_cash_reserve_pct = max(0.10, min_cash_reserve_pct - 0.05)
    elif market['state'] == 'bearish':
        buy_threshold = min(0.90, buy_threshold + 0.08)
        max_position_pct = max(0.02, max_position_pct - 0.02)
        min_cash_reserve_pct = min(0.50, min_cash_reserve_pct + 0.10)
    elif market['state'] == 'volatile':
        buy_threshold = min(0.90, buy_threshold + 0.04)
        max_position_pct = max(0.03, max_position_pct - 0.015)
        min_cash_reserve_pct = min(0.45, min_cash_reserve_pct + 0.05)

    # 熊市不建新仓，震荡市减半，防止反复止损侵蚀本金
    if market['state'] == 'bearish':
        max_new_position_count = 0       # 熊市：只管理现有持仓，不开新仓
    elif market['state'] == 'volatile':
        max_new_position_count = max(3, cfg.max_position_count // 2)  # 震荡：减半开仓
    else:
        max_new_position_count = cfg.max_position_count  # 牛市/中性：正常开仓

    return {
        'market': market,
        'buy_threshold': round(buy_threshold, 4),
        'max_single_position_pct': round(max_position_pct, 4),
        'min_cash_reserve_pct': round(min_cash_reserve_pct, 4),
        'max_new_position_count': max_new_position_count,
    }
