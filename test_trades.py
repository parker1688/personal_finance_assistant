import sys
sys.path.insert(0, 'D:/personal_finance_assistant')
from models import get_session, SimulatedTrade

s = get_session()
trades = s.query(SimulatedTrade).filter(SimulatedTrade.trade_date >= '2025-11-01').all()
print(f"Total trades: {len(trades)}")
buy = len([t for t in trades if t.action == 'buy'])
sell = len([t for t in trades if t.action == 'sell'])
print(f"BUY: {buy}, SELL: {sell}")

# Metrics
closed_trades = [t for t in trades if t.action == 'sell' and t.pnl is not None]
if closed_trades:
    pnl = sum([t.pnl for t in closed_trades])
    wins = len([t for t in closed_trades if t.pnl > 0])
    print(f"Closed: {len(closed_trades)}, PnL: ¥{pnl:.2f}, Wins: {wins}")
    if len(closed_trades) > 0:
        win_rate = 100.0 * wins / len(closed_trades)
        print(f"Win rate: {win_rate:.1f}%")

s.close()
