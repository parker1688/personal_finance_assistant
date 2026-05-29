"""模拟交易员维护工具：日志归档与运行健康摘要。"""

from __future__ import annotations

import os
from datetime import date, timedelta

from models import get_session, SimulatedDecisionLog

DEFAULT_TRADER_ID = 'default'


def get_decision_log_retention_days() -> int:
    return max(7, int(os.environ.get('SIMULATED_TRADER_LOG_RETENTION_DAYS', '90')))


def get_decision_log_archive_status(session, trader_id: str = DEFAULT_TRADER_ID) -> dict:
    retention_days = get_decision_log_retention_days()
    cutoff_date = date.today() - timedelta(days=retention_days)

    query = session.query(SimulatedDecisionLog).filter_by(trader_id=trader_id)
    total_logs = query.count()
    oldest = query.order_by(SimulatedDecisionLog.signal_date.asc()).first()
    deletable_count = query.filter(SimulatedDecisionLog.signal_date < cutoff_date).count()

    return {
        'retention_days': retention_days,
        'cutoff_date': cutoff_date.isoformat(),
        'total_logs': total_logs,
        'deletable_logs': deletable_count,
        'oldest_signal_date': oldest.signal_date.isoformat() if oldest and oldest.signal_date else None,
    }


def archive_old_decision_logs(trader_id: str = DEFAULT_TRADER_ID, retention_days: int | None = None, dry_run: bool = False) -> dict:
    session = get_session()
    try:
        effective_days = max(7, int(retention_days or get_decision_log_retention_days()))
        cutoff_date = date.today() - timedelta(days=effective_days)
        query = session.query(SimulatedDecisionLog).filter_by(trader_id=trader_id)
        deletable_query = query.filter(SimulatedDecisionLog.signal_date < cutoff_date)
        deletable_count = deletable_query.count()

        if not dry_run and deletable_count > 0:
            deletable_query.delete(synchronize_session=False)
            session.commit()

        remaining_count = query.count() if not dry_run else query.count()
        return {
            'trader_id': trader_id,
            'retention_days': effective_days,
            'cutoff_date': cutoff_date.isoformat(),
            'dry_run': dry_run,
            'archived_logs': 0 if dry_run else deletable_count,
            'deletable_logs': deletable_count,
            'remaining_logs': remaining_count,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()