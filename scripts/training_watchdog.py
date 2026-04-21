#!/usr/bin/env python3
"""后台监控训练状态；若训练异常停止则自动重新启动。"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / 'logs'
WATCHDOG_LOG = LOG_DIR / 'training_watchdog.log'
TRAINING_LOG = LOG_DIR / 'model_training.log'
API_BASE = os.environ.get('TRAINING_API_BASE', 'http://localhost:8080').rstrip('/')
STATUS_URL = f'{API_BASE}/api/model/status'
TRAIN_URL = f'{API_BASE}/api/model/train'
POLL_SECONDS = int(os.environ.get('TRAINING_WATCHDOG_INTERVAL', '60'))
STALE_MINUTES = int(os.environ.get('TRAINING_WATCHDOG_STALE_MINUTES', '20'))


def log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(line, flush=True)
    with open(WATCHDOG_LOG, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def request_json(url: str, method: str = 'GET'):
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=20) as resp:
        charset = resp.headers.get_content_charset() or 'utf-8'
        return json.loads(resp.read().decode(charset, errors='ignore'))


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def log_is_stale() -> bool:
    if not TRAINING_LOG.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(TRAINING_LOG.stat().st_mtime)
        return (datetime.now() - mtime) > timedelta(minutes=STALE_MINUTES)
    except Exception:
        return False


def should_restart(progress: dict) -> tuple[bool, str]:
    status = str(progress.get('status') or 'idle')
    updated_at = parse_dt(progress.get('updated_at'))
    progress_pct = float(progress.get('progress_percent') or 0.0)

    if status == 'failed':
        return True, '状态已标记为 failed'

    if status in {'running', 'starting'}:
        if updated_at and (datetime.now() - updated_at) > timedelta(minutes=STALE_MINUTES) and log_is_stale():
            return True, '训练状态与日志均长期未更新'
        return False, ''

    if status == 'completed' and progress_pct >= 100:
        return False, ''

    return False, ''


def main() -> int:
    last_signature = None
    log(f'训练监控已启动，轮询间隔={POLL_SECONDS}s，接口={API_BASE}')

    while True:
        try:
            payload = request_json(STATUS_URL)
            data = payload.get('data') or {}
            progress = data.get('training_progress') or {}
            status = str(progress.get('status') or 'idle')
            current_asset = progress.get('current_asset') or '—'
            current_step = int(progress.get('current_step') or 0)
            total_steps = int(progress.get('total_steps') or 0)
            progress_percent = float(progress.get('progress_percent') or 0.0)
            signature = (status, current_asset, current_step, total_steps, progress_percent)

            if signature != last_signature:
                log(f'训练状态={status} | 当前资产={current_asset} | 步骤={current_step}/{total_steps} | 进度={progress_percent:.1f}%')
                last_signature = signature

            restart, reason = should_restart(progress)
            if restart:
                result = request_json(TRAIN_URL, method='POST')
                msg = result.get('message') or '已触发重新训练'
                log(f'检测到异常停止，已自动重启训练：{reason} | {msg}')
                last_signature = None

        except KeyboardInterrupt:
            log('训练监控已手动停止')
            return 0
        except Exception as exc:
            log(f'监控检查失败：{exc}')

        time.sleep(max(15, POLL_SECONDS))


if __name__ == '__main__':
    raise SystemExit(main())
