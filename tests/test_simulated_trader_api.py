from datetime import date, datetime, timedelta

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def _build_testing_session():
    from models import Base

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return TestingSession


def test_simulated_trader_status_requires_admin_key_when_configured(monkeypatch):
    import api.simulated_trader as trader_api

    monkeypatch.setenv('ADMIN_API_KEY', 'secret-key')
    monkeypatch.setenv('ALLOW_LOCAL_ADMIN_BYPASS', 'false')

    app = Flask(__name__)
    trader_api.register_simulated_trader_routes(app)
    client = app.test_client()

    response = client.get('/api/simulated-trader/status')

    assert response.status_code == 403
    assert response.get_json()['code'] == 403


def test_simulated_trader_status_returns_archive_status_with_admin_key(monkeypatch):
    import api.simulated_trader as trader_api
    import trader.maintenance as trader_maintenance
    from models import SimulatedTraderConfig, SimulatedDecisionLog

    TestingSession = _build_testing_session()
    monkeypatch.setenv('ADMIN_API_KEY', 'secret-key')
    monkeypatch.setenv('ALLOW_LOCAL_ADMIN_BYPASS', 'false')
    monkeypatch.setattr(trader_api, 'get_session', TestingSession)
    monkeypatch.setattr(trader_maintenance, 'get_session', TestingSession)

    seed = TestingSession()
    try:
        seed.add(SimulatedTraderConfig(trader_id='default', initial_capital=1000000.0, current_cash=1000000.0))
        seed.add(SimulatedDecisionLog(
            trader_id='default',
            signal_date=date.today(),
            code='SYSTEM',
            name='测试',
            asset_type='system',
            decision_type='hold',
            final_action='hold',
        ))
        seed.commit()
    finally:
        seed.close()

    app = Flask(__name__)
    trader_api.register_simulated_trader_routes(app)
    client = app.test_client()

    response = client.get('/api/simulated-trader/status', headers={'X-Admin-Key': 'secret-key'})

    assert response.status_code == 200
    payload = response.get_json()['data']
    assert payload['initialized'] is True
    assert payload['archive_status']['total_logs'] == 1


def test_simulated_trader_archive_logs_dry_run_and_health(monkeypatch):
    import api.simulated_trader as trader_api
    import trader.maintenance as trader_maintenance
    from models import SimulatedTraderConfig, SimulatedDecisionLog

    TestingSession = _build_testing_session()
    monkeypatch.setenv('ADMIN_API_KEY', 'secret-key')
    monkeypatch.setenv('ALLOW_LOCAL_ADMIN_BYPASS', 'false')
    monkeypatch.setenv('SIMULATED_TRADER_LOG_RETENTION_DAYS', '30')
    monkeypatch.setattr(trader_api, 'get_session', TestingSession)
    monkeypatch.setattr(trader_maintenance, 'get_session', TestingSession)
    monkeypatch.setattr(
        trader_api,
        'compute_validation_report',
        lambda trader_id='default': {
            'loss_trade_analysis': {'total_loss_trades': 0},
        },
    )

    seed = TestingSession()
    try:
        seed.add(SimulatedTraderConfig(trader_id='default', initial_capital=1000000.0, current_cash=950000.0))
        seed.add(SimulatedDecisionLog(
            trader_id='default',
            signal_date=date.today() - timedelta(days=45),
            code='000001.SZ',
            name='旧日志',
            asset_type='a_stock',
            decision_type='reject',
            final_action='hold',
        ))
        seed.add(SimulatedDecisionLog(
            trader_id='default',
            signal_date=date.today(),
            code='SYSTEM',
            name='新日志',
            asset_type='system',
            decision_type='hold',
            final_action='hold',
        ))
        seed.commit()
    finally:
        seed.close()

    app = Flask(__name__)
    trader_api.register_simulated_trader_routes(app)
    client = app.test_client()
    headers = {'X-Admin-Key': 'secret-key'}

    archive_resp = client.post('/api/simulated-trader/archive-logs', json={'dry_run': True}, headers=headers)
    assert archive_resp.status_code == 200
    archive_payload = archive_resp.get_json()['data']
    assert archive_payload['deletable_logs'] == 1
    assert archive_payload['archived_logs'] == 0

    health_resp = client.get('/api/simulated-trader/health', headers=headers)
    assert health_resp.status_code == 200
    health_payload = health_resp.get_json()['data']
    assert health_payload['metrics']['decision_log_deletable'] == 1
    assert health_payload['overall_status'] == 'warning'