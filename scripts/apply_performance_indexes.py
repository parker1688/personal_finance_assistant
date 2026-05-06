"""Apply performance-focused composite indexes for SQLite.

Usage:
    python scripts/apply_performance_indexes.py
"""

import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config import DATABASE_URL  # noqa: E402


def _resolve_sqlite_path(database_url: str) -> Path:
    url = str(database_url or "").strip()
    if not url.startswith("sqlite"):
        raise RuntimeError(f"Only sqlite is supported by this script: {url}")

    if "///" in url:
        raw = url.split("///", 1)[1]
    else:
        raw = url.split(":", 1)[-1]

    raw = raw.strip()
    if not raw:
        raise RuntimeError("Invalid sqlite url, missing db path")

    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def apply_indexes(conn: sqlite3.Connection) -> None:
    statements = [
        # warnings
        "CREATE INDEX IF NOT EXISTS idx_warning_code_type_time ON warnings(code, warning_type, warning_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_warning_type_time ON warnings(warning_type, warning_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_warning_time_level ON warnings(warning_time, level)",
        # predictions
        "CREATE INDEX IF NOT EXISTS idx_prediction_review_state_expiry ON predictions(is_direction_correct, expiry_date)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_period_review_expiry ON predictions(period_days, is_direction_correct, expiry_date)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_code_period_date_created ON predictions(code, period_days, date DESC, created_at DESC)",
        # reviews
        "CREATE INDEX IF NOT EXISTS idx_review_reviewed_at ON reviews(reviewed_at DESC)",
        # recommendations
        "CREATE INDEX IF NOT EXISTS idx_rec_code_type_date_rank_id ON recommendations(code, type, date DESC, rank, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_rec_type_date_score ON recommendations(type, date, total_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_rec_type_date_code ON recommendations(type, date, code)",
        # model_versions
        "CREATE INDEX IF NOT EXISTS idx_model_period_created_id ON model_versions(period_days, created_at DESC, id DESC)",
        # holdings
        "CREATE INDEX IF NOT EXISTS idx_holding_asset_code ON holdings(asset_type, code)",
    ]

    cur = conn.cursor()
    try:
        for sql in statements:
            cur.execute(sql)
        conn.commit()
    finally:
        cur.close()


def main() -> int:
    db_path = _resolve_sqlite_path(DATABASE_URL)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        apply_indexes(conn)
    finally:
        conn.close()

    print(f"Applied performance indexes on: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
