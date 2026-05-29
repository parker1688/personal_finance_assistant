from datetime import date, timedelta

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def _build_testing_session():
    from models import Base

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return TestingSession


def test_recent_reviews_include_closed_trade_immediate_review(monkeypatch):
    import api.reviews as reviews_api
    from models import SimulatedTrade

    TestingSession = _build_testing_session()
    monkeypatch.setattr(reviews_api, 'get_session', TestingSession)
    monkeypatch.setattr(reviews_api, '_ensure_due_reviews_current', lambda force=False: 0)

    seed = TestingSession()
    try:
        signal_date = date.today() - timedelta(days=2)
        buy_date = date.today() - timedelta(days=1)
        sell_date = date.today()

        seed.add(SimulatedTrade(
            trader_id='default',
            trade_date=buy_date,
            signal_date=signal_date,
            code='000001.SZ',
            name='平安银行',
            asset_type='a_stock',
            action='buy',
            shares=100.0,
            price=10.0,
            amount=1000.0,
            trigger='signal',
            signal_score=75.0,
        ))
        seed.add(SimulatedTrade(
            trader_id='default',
            trade_date=sell_date,
            signal_date=buy_date,
            code='000001.SZ',
            name='平安银行',
            asset_type='a_stock',
            action='sell',
            shares=100.0,
            price=10.8,
            amount=1080.0,
            trigger='take_profit',
            pnl=80.0,
            pnl_pct=8.0,
        ))
        seed.commit()
    finally:
        seed.close()

    app = Flask(__name__)
    reviews_api.register_reviews_routes(app)
    client = app.test_client()

    response = client.get('/api/reviews/recent?limit=10')

    assert response.status_code == 200
    payload = response.get_json()['data']
    assert any(item.get('status') == 'trade_reviewed' for item in payload)
    trade_row = next(item for item in payload if item.get('status') == 'trade_reviewed')
    assert trade_row['code'] == '000001.SZ'
    assert '即时成交复盘' in (trade_row.get('error_analysis') or '')
