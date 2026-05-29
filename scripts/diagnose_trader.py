"""诊断模拟交易员：分析 pos=30 时额外买入了哪些低质量信号，以及推荐分布。"""
import sys, os
from datetime import date, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.optimize_trader_objectives import (
    _reset_trader_state, _set_trader_config, _run_backfill
)
from trader.profile_config import apply_threshold_adjustment
from models import get_session, SimulatedTrade, SimulatedDailyPnl, SimulatedPortfolio
from trader.engine import DEFAULT_TRADER_ID

START = date(2025, 11, 1)
END   = date(2026, 1, 31)
os.environ['TRADER_DISABLE_INSTANT_REVIEW'] = 'true'

def run_and_collect(th, sl, pos, cash, label):
    apply_threshold_adjustment(th, reason='diag')
    _set_trader_config(stop_loss_pct=sl, max_position_count=pos, min_cash_reserve_pct=cash)
    _reset_trader_state()
    _run_backfill(START, END)

    session = get_session()
    sells = session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID, action='sell').all()
    buys  = session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID, action='buy').all()
    pnls  = session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).order_by(SimulatedDailyPnl.pnl_date.desc()).first()

    total_ret = float(pnls.total_return) if pnls else 0
    max_dd    = float(pnls.max_drawdown) if pnls else 0
    n_sells   = len(sells)
    win       = sum(1 for t in sells if float(t.pnl or 0) > 0)
    total_pnl = sum(float(t.pnl or 0) for t in sells)

    # 按 signal_score 分桶
    score_buckets = {}
    for t in buys:
        score = float(t.signal_score or 0)
        bucket = round(score, 2)
        if bucket not in score_buckets:
            score_buckets[bucket] = {'count': 0, 'pnl': 0}
        score_buckets[bucket]['count'] += 1

    # 对应卖出 pnl
    for t in sells:
        score = float(t.signal_score or 0)
        bucket = round(score, 2)
        if bucket not in score_buckets:
            score_buckets[bucket] = {'count': 0, 'pnl': 0}
        score_buckets[bucket]['pnl'] += float(t.pnl or 0)

    # 亏损最多的标的
    code_pnl = {}
    for t in sells:
        code_pnl[t.code] = code_pnl.get(t.code, 0) + float(t.pnl or 0)
    worst = sorted(code_pnl.items(), key=lambda x: x[1])[:10]

    print(f"\n{'='*60}")
    print(f"[{label}] th={th} sl={sl*100:.0f}% pos={pos} cash={cash*100:.0f}%")
    print(f"  interval={total_ret:+.2f}%  max_dd={max_dd:.2f}%  sells={n_sells}  wr={win/n_sells*100:.1f}%  total_pnl={total_pnl:+.0f}")
    print(f"  买入分布 by score:")
    for s, v in sorted(score_buckets.items()):
        print(f"    score={s:.2f}  buys={v['count']}  pnl={v['pnl']:+.0f}")
    print(f"  亏损最多标的:")
    for code, pnl in worst:
        print(f"    {code}  pnl={pnl:+.0f}")

    session.close()
    return {'sells': sells, 'buys': buys, 'score_buckets': score_buckets}

# 对比 pos=15 vs pos=30
r15 = run_and_collect(0.56, 0.08, 15, 0.25, 'pos=15')
r30 = run_and_collect(0.56, 0.08, 30, 0.25, 'pos=30')

# 找出 pos=30 中 pos=15 没有的额外买入
codes15 = set(t.code for t in r15['buys'])
codes30 = set(t.code for t in r30['buys'])
extra = codes30 - codes15
print(f"\n\npos=30 额外买入的标的（pos=15没买的）: {len(extra)} 个")

session = get_session()
# 再跑一次pos=30获取完整trade数据
apply_threshold_adjustment(0.56, reason='diag_final')
_set_trader_config(stop_loss_pct=0.08, max_position_count=30, min_cash_reserve_pct=0.25)
_reset_trader_state()
_run_backfill(START, END)
sells30 = session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID, action='sell').all()
extra_sells = [t for t in sells30 if t.code in extra]
print(f"这些额外标的的卖出成绩:")
extra_code_pnl = {}
for t in extra_sells:
    extra_code_pnl[t.code] = extra_code_pnl.get(t.code, 0) + float(t.pnl or 0)
for code, pnl in sorted(extra_code_pnl.items(), key=lambda x: x[1])[:20]:
    score = next((float(t.signal_score or 0) for t in sells30 if t.code == code), 0)
    print(f"  {code}  pnl={pnl:+.0f}  score={score:.2f}")

session.close()
os.environ.pop('TRADER_DISABLE_INSTANT_REVIEW', None)
print("\n诊断完成")
