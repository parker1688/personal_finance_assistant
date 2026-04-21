"""
管理接口鉴权辅助 - api/auth.py

为高风险管理接口提供统一的管理员权限校验。
默认策略：
1. 若配置了 ADMIN_API_KEY，则所有受保护接口都必须提供正确密钥
2. 若未配置 ADMIN_API_KEY，则只允许本机回环地址访问受保护接口
"""

from __future__ import annotations

import ipaddress
import os
import secrets
from datetime import datetime
from functools import wraps
from typing import Callable, Optional

from flask import jsonify, request

from utils import get_logger

logger = get_logger(__name__)


def log_admin_audit(action: str, outcome: str, detail: str = '', *, level: str = 'INFO') -> None:
    """将管理员操作审计同时写入日志系统与数据库日志表。"""
    client_ip = _get_client_ip() or 'unknown'
    message = f"AUDIT action={action} outcome={outcome} ip={client_ip} path={request.path}"
    if detail:
        message = f"{message} detail={detail}"

    log_method = getattr(logger, str(level or 'INFO').lower(), logger.info)
    log_method(message)

    try:
        from models import get_session, Log
        session = get_session()
        try:
            session.add(Log(
                log_time=datetime.now(),
                level=str(level or 'INFO').upper()[:10],
                module='audit.auth',
                message=message[:2000],
                stack_trace=None,
            ))
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    except Exception:
        pass


def _normalize_ip(value: Optional[str]) -> str:
    return str(value or '').split(',')[0].strip().strip('[]')


def _is_loopback(value: Optional[str]) -> bool:
    candidate = _normalize_ip(value)
    if not candidate:
        return False
    if candidate.lower() == 'localhost':
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def _get_client_ip() -> str:
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        return _normalize_ip(forwarded)
    return _normalize_ip(request.remote_addr)


def is_local_request() -> bool:
    host = str(request.host or '').split(':')[0].strip()
    return _is_loopback(_get_client_ip()) or _is_loopback(host)


def get_admin_api_key() -> str:
    return str(os.environ.get('ADMIN_API_KEY') or '').strip()


def _allow_local_bypass() -> bool:
    return str(os.environ.get('ALLOW_LOCAL_ADMIN_BYPASS', 'true')).lower() == 'true'


def _extract_provided_key() -> str:
    direct = request.headers.get('X-Admin-Key') or request.headers.get('X-API-Key')
    if direct:
        return str(direct).strip()

    auth_header = str(request.headers.get('Authorization') or '').strip()
    if auth_header.lower().startswith('bearer '):
        return auth_header[7:].strip()
    return ''


def _forbidden_response(message: str):
    return jsonify({
        'code': 403,
        'status': 'forbidden',
        'message': message,
    }), 403


def require_admin_access(view_func: Optional[Callable] = None, *, action: str = 'admin_operation'):
    """保护高风险管理接口。"""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            configured_key = get_admin_api_key()
            provided_key = _extract_provided_key()
            client_ip = _get_client_ip() or 'unknown'

            if configured_key:
                if provided_key and secrets.compare_digest(provided_key, configured_key):
                    log_admin_audit(action, 'granted', 'admin_key', level='INFO')
                    return func(*args, **kwargs)

                log_admin_audit(action, 'denied', 'missing_or_invalid_key', level='WARNING')
                return _forbidden_response('该操作需要管理员凭证')

            if _allow_local_bypass() and is_local_request():
                log_admin_audit(action, 'granted', 'local_bypass', level='INFO')
                return func(*args, **kwargs)

            log_admin_audit(action, 'denied', 'no_key_configured', level='WARNING')
            return _forbidden_response('该操作需要管理员凭证，请配置 ADMIN_API_KEY 或从本机访问')

        return wrapper

    if view_func is not None:
        return decorator(view_func)
    return decorator
