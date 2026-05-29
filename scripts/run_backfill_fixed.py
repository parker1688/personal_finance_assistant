"""
一键运行修复后的回测：强制重建推荐（skip_if_exists=False）+ 完整3个月回放
"""
import sys, json
sys.path.insert(0, 'D:/personal_finance_assistant')
from datetime import date, timedelta
from models import (
    get_session, SimulatedTrade, SimulatedDecisionLog,
    SimulatedPortfolio, SimulatedDailyPnl, SimulatedTraderConfig
)
from trader.engine import SimulatedTrader, DEFAULT_TRADER_ID
from scheduler import rebuild_recommendations_for_date
start_date = date(2025, 11, 1)
end_date   = date(2026, 1, 31)

print(f'=== Backfill {start_date} → {end_date} (force_regen=True) ===')

session = get_session()
session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
session.query(SimulatedPortfolio).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
if cfg:
    cfg.current_cash = cfg.initial_capital
session.commit()
session.close()
print('Reset: OK')

trader = SimulatedTrader()
cur = start_date
processed_days = 0
regen_inserted = 0

while cur <= end_date:
    # 强制重建（skip_if_exists=False），不依赖 Flask 缓存
    result = rebuild_recommendations_for_date(cur, skip_if_exists=False)
    regen_inserted += result.get('inserted', 0)
    trader.settle_pending_trades(cur)
    trader.run_daily(cur)
    processed_days += 1
    if processed_days % 15 == 0:
        print(f'  {cur} ({processed_days} days done, regen+={regen_inserted})')
    cur += timedelta(days=1)

trader.settle_pending_trades(end_date + timedelta(days=1))
print(f'\nDone: {processed_days} days, regen inserted={regen_inserted}')
