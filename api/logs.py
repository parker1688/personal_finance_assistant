"""
日志API - api/logs.py
提供系统日志查询接口
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from flask import jsonify, request, send_file

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Log
from utils import get_logger
from api.auth import require_admin_access, log_admin_audit

logger = get_logger(__name__)
TRAINING_PROGRESS_FILE = Path(__file__).resolve().parent.parent / 'data' / 'models' / 'training_progress.json'
TRAINING_LOG_FILE = Path(__file__).resolve().parent.parent / 'logs' / 'model_training.log'


def _parse_dt(value, fallback=None):
    if not value:
        return fallback or datetime.now()
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except Exception:
        return fallback or datetime.now()


def _is_training_log_noise(line: str) -> bool:
    text = str(line or '').strip()
    if not text:
        return True
    noise_markers = [
        'use_label_encoder',
        'bst.update(',
        'site-packages/xgboost',
        '/Users/runner/work/xgboost',
        'UserWarning:',
        'Parameters: {',
    ]
    if any(marker in text for marker in noise_markers):
        return True
    if text in {'============================================================', '========================================================================'}:
        return True
    return False


def _is_meaningful_training_log(line: str) -> bool:
    text = str(line or '').strip()
    if _is_training_log_noise(text):
        return False
    keep_markers = [
        '开始', '完成', '训练', '加载', '进度', '总样本', '特征数', '正样本比例',
        '训练集', '验证集', '训练准确率', '验证准确率', '训练 R²', '验证 R²',
        '模型已保存', '失败', '跳过', '成功'
    ]
    return any(marker in text for marker in keep_markers)


def _load_runtime_training_logs(keyword='', level='', source='all', progress_file=None, log_file=None):
    """读取训练进度与训练文件日志，补充到日志页。"""
    if source not in {'all', 'training'}:
        return []

    progress_path = Path(progress_file) if progress_file else TRAINING_PROGRESS_FILE
    training_log_path = Path(log_file) if log_file else TRAINING_LOG_FILE

    items = []
    keyword_text = str(keyword or '').strip().lower()
    requested_level = str(level or '').strip().upper()

    if progress_path.exists():
        try:
            payload = json.loads(progress_path.read_text(encoding='utf-8'))
            if isinstance(payload, dict):
                status = str(payload.get('status') or 'idle')
                log_level = 'ERROR' if status == 'failed' else ('INFO' if status in {'running', 'starting', 'completed'} else 'DEBUG')
                message = (
                    f"训练状态={status}；{payload.get('message') or '暂无说明'}；"
                    f"当前资产={payload.get('current_asset') or '—'}；"
                    f"步骤={payload.get('current_step') or 0}/{payload.get('total_steps') or 0}；"
                    f"进度={payload.get('progress_percent') or 0}%"
                )
                items.append({
                    'id': 'training-progress',
                    'log_time': _parse_dt(payload.get('updated_at'), datetime.now()),
                    'level': log_level,
                    'module': 'training',
                    'message': message,
                    'stack_trace': None,
                })
        except Exception:
            pass

    if training_log_path.exists() and training_log_path.is_file():
        try:
            mtime = datetime.fromtimestamp(training_log_path.stat().st_mtime)
            with open(training_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = [line.strip() for line in f.readlines() if _is_meaningful_training_log(line)]
            for idx, line in enumerate(reversed(lines[-200:]), start=1):
                items.append({
                    'id': f'training-file-{idx}',
                    'log_time': mtime,
                    'level': 'INFO',
                    'module': 'training.file',
                    'message': line,
                    'stack_trace': None,
                })
        except Exception:
            pass

    filtered = []
    for item in items:
        if requested_level and str(item.get('level') or '').upper() != requested_level:
            continue
        if keyword_text and keyword_text not in str(item.get('message') or '').lower():
            continue
        filtered.append(item)
    return filtered


def register_logs_routes(app):
    """注册日志相关路由"""
    
    @app.route('/api/logs', methods=['GET'])
    @require_admin_access(action='logs.read')
    def get_logs():
        """获取系统日志"""
        try:
            page = int(request.args.get('page', 1))
            size = int(request.args.get('size', 50))
            level = request.args.get('level', '')
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            keyword = request.args.get('keyword', '')
            source = request.args.get('source', 'all')

            items = []
            session = None
            if source in {'all', 'system'}:
                session = get_session()
                query = session.query(Log)

                if level:
                    query = query.filter(Log.level == level)
                if start_date:
                    query = query.filter(Log.log_time >= start_date)
                if end_date:
                    query = query.filter(Log.log_time <= end_date + ' 23:59:59')
                if keyword:
                    query = query.filter(Log.message.contains(keyword))

                db_logs = query.order_by(Log.log_time.desc()).limit(500).all()
                for log in db_logs:
                    items.append({
                        'id': log.id,
                        'log_time': log.log_time,
                        'level': log.level,
                        'module': log.module,
                        'message': log.message,
                        'stack_trace': log.stack_trace
                    })

            items.extend(_load_runtime_training_logs(keyword=keyword, level=level, source=source))
            items.sort(key=lambda x: _parse_dt(x.get('log_time'), datetime.min), reverse=True)

            total = len(items)
            offset = (page - 1) * size
            paged_items = items[offset:offset + size]
            logs_list = [{
                'id': item.get('id'),
                'log_time': _parse_dt(item.get('log_time')).strftime('%Y-%m-%d %H:%M:%S') if item.get('log_time') else '',
                'level': item.get('level'),
                'module': item.get('module'),
                'message': item.get('message'),
                'stack_trace': item.get('stack_trace')
            } for item in paged_items]

            if session is not None:
                session.close()

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'total': total,
                    'page': page,
                    'size': size,
                    'items': logs_list
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取日志失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/logs', methods=['DELETE'])
    @require_admin_access(action='logs.clear')
    def clear_logs():
        """清空日志"""
        try:
            before_date = request.args.get('before_date', '')
            
            session = get_session()
            
            if before_date:
                deleted = session.query(Log).filter(Log.log_time <= before_date).delete()
            else:
                deleted = session.query(Log).delete()
            
            session.commit()
            session.close()
            
            log_admin_audit('logs.clear', 'success', f"deleted_count={deleted}")
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {'deleted_count': deleted},
                'message': f'已删除 {deleted} 条日志',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"清空日志失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/logs/export', methods=['GET'])
    @require_admin_access(action='logs.export')
    def export_logs():
        """导出日志"""
        try:
            level = request.args.get('level', '')
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            keyword = request.args.get('keyword', '')
            
            session = get_session()
            query = session.query(Log)
            
            if level:
                query = query.filter(Log.level == level)
            if start_date:
                query = query.filter(Log.log_time >= start_date)
            if end_date:
                query = query.filter(Log.log_time <= end_date + ' 23:59:59')
            if keyword:
                query = query.filter(Log.message.contains(keyword))
            
            logs = query.order_by(Log.log_time.desc()).limit(10000).all()
            
            import csv
            import io
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['时间', '级别', '模块', '内容'])
            
            for log in logs:
                writer.writerow([
                    log.log_time.strftime('%Y-%m-%d %H:%M:%S') if log.log_time else '',
                    log.level,
                    log.module,
                    log.message
                ])
            
            session.close()
            
            output.seek(0)
            from flask import send_file
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8-sig')),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'logs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
            
        except Exception as e:
            logger.error(f"导出日志失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500