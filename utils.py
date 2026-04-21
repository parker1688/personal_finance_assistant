"""
通用工具函数模块 - utils.py
"""

import os
import sys
import logging
import json
import time
import hashlib
import traceback
from datetime import datetime, timedelta, date
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Tuple

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入配置（使用安全导入，避免循环依赖）
try:
    from config import (
        LOGS_DIR, DATA_DIR, CACHE_DIR, TUSHARE_TOKEN,
        LOCAL_CACHE_TTL, PRICE_CACHE_TTL, MAX_RETRIES, REQUEST_DELAY,
        LOG_LEVEL, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT, CONSOLE_LOG_LEVEL
    )
except ImportError:
    # 默认配置（当config.py尚未加载时使用）
    BASE_DIR = Path(__file__).resolve().parent
    LOGS_DIR = BASE_DIR / 'logs'
    DATA_DIR = BASE_DIR / 'data'
    CACHE_DIR = DATA_DIR / 'cache'
    TUSHARE_TOKEN = ''
    LOCAL_CACHE_TTL = 300
    PRICE_CACHE_TTL = 3600
    MAX_RETRIES = 3
    REQUEST_DELAY = 0.3
    LOG_LEVEL = 'INFO'
    LOG_FILE = LOGS_DIR / 'app.log'
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    CONSOLE_LOG_LEVEL = 'INFO'

# 确保目录存在
for dir_path in [LOGS_DIR, DATA_DIR, CACHE_DIR]:
    dir_path = Path(dir_path) if isinstance(dir_path, str) else dir_path
    dir_path.mkdir(parents=True, exist_ok=True)


# ==================== 日志配置 ====================

class _DBLogHandler(logging.Handler):
    """将日志异步写入数据库 Log 表的 Handler（带缓冲，避免每条日志都开事务）"""

    # 类级缓冲区，所有 logger 共享
    _buffer: list = []
    _MAX_BUFFER = 20   # 累积 N 条后批量写入
    _last_flush: float = 0.0
    _FLUSH_INTERVAL = 30  # 最长 30 秒强制 flush 一次

    def emit(self, record: logging.LogRecord):
        try:
            stack_trace = None
            if record.exc_info:
                stack_trace = ''.join(traceback.format_exception(*record.exc_info))[:4000]

            entry = {
                'log_time': datetime.fromtimestamp(record.created),
                'level': record.levelname,
                'module': record.name.split('.')[-1][:50],
                'message': self.format(record)[:2000],
                'stack_trace': stack_trace,
            }
            _DBLogHandler._buffer.append(entry)

            now = time.time()
            if (len(_DBLogHandler._buffer) >= _DBLogHandler._MAX_BUFFER or
                    now - _DBLogHandler._last_flush >= _DBLogHandler._FLUSH_INTERVAL):
                self._flush()
        except Exception:
            self.handleError(record)

    @classmethod
    def _flush(cls):
        if not cls._buffer:
            return
        batch = cls._buffer[:]
        cls._buffer.clear()
        cls._last_flush = time.time()
        try:
            # 延迟导入，避免循环依赖
            from models import get_session, Log
            session = get_session()
            try:
                for entry in batch:
                    session.add(Log(
                        log_time=entry['log_time'],
                        level=entry['level'],
                        module=entry['module'],
                        message=entry['message'],
                        stack_trace=entry['stack_trace'],
                    ))
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()
        except Exception:
            pass  # DB 不可用时静默降级，不影响正常日志


# 全局单例 DB Handler（所有 logger 共用同一个实例，避免重复写入）
_db_log_handler: Optional['_DBLogHandler'] = None


def _get_db_handler() -> '_DBLogHandler':
    global _db_log_handler
    if _db_log_handler is None:
        _db_log_handler = _DBLogHandler()
        _db_log_handler.setLevel(logging.INFO)
        # 使用简洁格式写入 DB（不含时间前缀，DB 字段已有时间）
        _db_log_handler.setFormatter(logging.Formatter('%(message)s'))
    return _db_log_handler


def get_logger(name: str) -> logging.Logger:
    """获取日志记录器"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        # 确保日志目录存在
        log_file = Path(LOG_FILE) if isinstance(LOG_FILE, str) else LOG_FILE
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, CONSOLE_LOG_LEVEL.upper(), logging.INFO))
        
        # 格式化器
        formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.addHandler(_get_db_handler())
        logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    
    return logger


# ==================== 日期工具 ====================

def get_today() -> date:
    """获取今天的日期"""
    return date.today()


def get_date_str(dt: Optional[datetime] = None, fmt: str = '%Y-%m-%d') -> str:
    """格式化日期字符串"""
    if dt is None:
        dt = datetime.now()
    return dt.strftime(fmt)


def parse_date(date_str: str, fmt: str = '%Y-%m-%d') -> date:
    """解析日期字符串"""
    return datetime.strptime(date_str, fmt).date()


def parse_datetime(dt_str: str, fmt: str = '%Y-%m-%d %H:%M:%S') -> datetime:
    """解析日期时间字符串"""
    return datetime.strptime(dt_str, fmt)


def get_trading_dates(start_date: Union[str, date], end_date: Union[str, date]) -> List[date]:
    """获取交易日列表（周一到周五）"""
    if isinstance(start_date, str):
        start_date = parse_date(start_date)
    if isinstance(end_date, str):
        end_date = parse_date(end_date)
    
    date_list = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # 周一到周五
            date_list.append(current)
        current += timedelta(days=1)
    
    return date_list


def get_previous_trading_day(dt: Optional[date] = None) -> date:
    """获取上一个交易日"""
    if dt is None:
        dt = get_today()
    
    current = dt - timedelta(days=1)
    while current.weekday() >= 5:  # 周六周日
        current -= timedelta(days=1)
    
    return current


def get_next_trading_day(dt: Optional[date] = None) -> date:
    """获取下一个交易日"""
    if dt is None:
        dt = get_today()
    
    current = dt + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    
    return current


def ensure_dir(path: Union[str, Path]) -> None:
    """确保目录存在"""
    path = Path(path) if isinstance(path, str) else path
    path.mkdir(parents=True, exist_ok=True)


# ==================== 重试装饰器 ====================

def retry(max_attempts: Optional[int] = None, delay: Optional[float] = None, 
          backoff: float = 2, exceptions: Tuple = (Exception,)):
    """
    重试装饰器
    Args:
        max_attempts: 最大重试次数，默认使用配置中的 MAX_RETRIES
        delay: 重试间隔（秒），默认使用配置中的 REQUEST_DELAY
        backoff: 退避倍数
        exceptions: 需要重试的异常类型
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _max_attempts = max_attempts if max_attempts is not None else MAX_RETRIES
            _delay = delay if delay is not None else REQUEST_DELAY
            
            current_delay = _delay
            for attempt in range(_max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == _max_attempts - 1:
                        raise
                    logger = get_logger(func.__module__)
                    logger.warning(
                        f"{func.__name__} 失败 (尝试 {attempt+1}/{_max_attempts}): {e}, "
                        f"{current_delay:.1f}秒后重试"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator


# ==================== 缓存管理 ====================

class SimpleCache:
    """简单内存缓存类"""
    
    def __init__(self, ttl: Optional[int] = None):
        """
        Args:
            ttl: 缓存有效期（秒），默认使用配置中的 LOCAL_CACHE_TTL
        """
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.ttl = ttl if ttl is not None else LOCAL_CACHE_TTL
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        """设置缓存"""
        self.cache[key] = (value, time.time())
    
    def clear(self) -> None:
        """清空缓存"""
        self.cache.clear()
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        if key in self.cache:
            del self.cache[key]
    
    def has(self, key: str) -> bool:
        """检查缓存是否存在且未过期"""
        if key in self.cache:
            _, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return True
            else:
                del self.cache[key]
        return False
    
    def get_or_set(self, key: str, factory, *args, **kwargs) -> Any:
        """获取缓存，如果不存在则调用factory创建"""
        value = self.get(key)
        if value is None:
            value = factory(*args, **kwargs)
            self.set(key, value)
        return value


class FileCache:
    """文件缓存类"""
    
    def __init__(self, cache_dir: Optional[Union[str, Path]] = None, ttl: Optional[int] = None):
        """
        Args:
            cache_dir: 缓存目录，默认使用配置中的 CACHE_DIR
            ttl: 缓存有效期（秒），默认使用配置中的 PRICE_CACHE_TTL
        """
        self.cache_dir = Path(cache_dir) if cache_dir else CACHE_DIR
        self.ttl = ttl if ttl is not None else PRICE_CACHE_TTL
        ensure_dir(self.cache_dir)
    
    def _get_cache_path(self, key: str) -> Path:
        """获取缓存文件路径"""
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.json"
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        cache_path = self._get_cache_path(key)
        if cache_path.exists():
            mtime = cache_path.stat().st_mtime
            if time.time() - mtime < self.ttl:
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass
        return None
    
    def set(self, key: str, value: Any) -> None:
        """设置缓存"""
        cache_path = self._get_cache_path(key)
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(value, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger = get_logger(__name__)
            logger.error(f"保存缓存失败 {key}: {e}")
    
    def clear(self) -> None:
        """清空缓存"""
        for file_path in self.cache_dir.glob("*.json"):
            try:
                file_path.unlink()
            except OSError:
                pass
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        cache_path = self._get_cache_path(key)
        if cache_path.exists():
            try:
                cache_path.unlink()
            except OSError:
                pass
    
    def clear_old(self, max_age_seconds: Optional[int] = None) -> int:
        """清理过期缓存"""
        max_age = max_age_seconds if max_age_seconds is not None else self.ttl
        now = time.time()
        count = 0
        
        for file_path in self.cache_dir.glob("*.json"):
            if now - file_path.stat().st_mtime > max_age:
                try:
                    file_path.unlink()
                    count += 1
                except OSError:
                    pass
        
        return count


# ==================== Tushare 连接 ====================

_tushare_pro = None


def get_tushare_pro():
    """获取Tushare连接（单例）"""
    global _tushare_pro
    
    if _tushare_pro is None:
        try:
            import tushare as ts
            
            if TUSHARE_TOKEN:
                ts.set_token(TUSHARE_TOKEN)
                _tushare_pro = ts.pro_api()
                logger = get_logger(__name__)
                logger.info("Tushare连接成功")
            else:
                logger = get_logger(__name__)
                logger.warning("TUSHARE_TOKEN未设置")
                return None
        except ImportError:
            logger = get_logger(__name__)
            logger.warning("tushare未安装")
            return None
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Tushare初始化失败: {e}")
            return None
    
    return _tushare_pro


# ==================== 进度管理 ====================

class ProgressManager:
    """进度管理器基类"""
    
    def __init__(self, progress_file: Union[str, Path]):
        """
        Args:
            progress_file: 进度文件路径
        """
        self.progress_file = Path(progress_file)
        self.progress = self._load()
    
    def _load(self) -> Dict:
        """加载进度"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return self._get_default_progress()
    
    def _get_default_progress(self) -> Dict:
        """获取默认进度结构"""
        return {
            'completed': [],
            'failed': [],
            'skipped': [],
            'last_update': None,
            'total_records': 0
        }
    
    def save(self) -> None:
        """保存进度"""
        self.progress['last_update'] = datetime.now().isoformat()
        ensure_dir(self.progress_file.parent)
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)
    
    def is_completed(self, item_id: str) -> bool:
        """检查是否已完成"""
        return item_id in self.progress.get('completed', [])
    
    def mark_completed(self, item_id: str) -> None:
        """标记完成"""
        if item_id not in self.progress.get('completed', []):
            self.progress.setdefault('completed', []).append(item_id)
            self.save()
    
    def mark_failed(self, item_id: str, reason: Optional[str] = None) -> None:
        """标记失败"""
        self.progress.setdefault('failed', []).append({
            'id': item_id,
            'reason': reason,
            'time': datetime.now().isoformat()
        })
        self.save()
    
    def mark_skipped(self, item_id: str, reason: Optional[str] = None) -> None:
        """标记跳过"""
        self.progress.setdefault('skipped', []).append({
            'id': item_id,
            'reason': reason,
            'time': datetime.now().isoformat()
        })
        self.save()
    
    def add_records(self, count: int) -> None:
        """增加记录数"""
        self.progress['total_records'] = self.progress.get('total_records', 0) + count
        self.save()
    
    def reset(self) -> None:
        """重置进度"""
        self.progress = self._get_default_progress()
        self.save()
    
    def get_summary(self) -> Dict:
        """获取进度摘要"""
        return {
            'completed': len(self.progress.get('completed', [])),
            'failed': len(self.progress.get('failed', [])),
            'skipped': len(self.progress.get('skipped', [])),
            'total_records': self.progress.get('total_records', 0),
            'last_update': self.progress.get('last_update')
        }


# ==================== 价格缓存服务 ====================

# 全局价格缓存实例
_price_file_cache = None
_price_memory_cache = None


def get_price_cache() -> FileCache:
    """获取价格文件缓存（单例）"""
    global _price_file_cache
    if _price_file_cache is None:
        _price_file_cache = FileCache(ttl=PRICE_CACHE_TTL)
    return _price_file_cache


def get_memory_cache() -> SimpleCache:
    """获取内存缓存（单例）"""
    global _price_memory_cache
    if _price_memory_cache is None:
        _price_memory_cache = SimpleCache()
    return _price_memory_cache


def get_cached_price(code: str, target_date: Union[str, date]) -> Optional[float]:
    """获取缓存的价格"""
    if isinstance(target_date, date):
        target_date = target_date.isoformat()
    
    cache_key = f"price_{code}_{target_date}"
    
    # 先查内存缓存
    memory_cache = get_memory_cache()
    price = memory_cache.get(cache_key)
    if price is not None:
        return price
    
    # 再查文件缓存
    file_cache = get_price_cache()
    price = file_cache.get(cache_key)
    if price is not None:
        memory_cache.set(cache_key, price)
        return price
    
    return None


def set_cached_price(code: str, target_date: Union[str, date], price: float) -> None:
    """设置缓存的价格"""
    if isinstance(target_date, date):
        target_date = target_date.isoformat()
    
    cache_key = f"price_{code}_{target_date}"
    
    memory_cache = get_memory_cache()
    memory_cache.set(cache_key, price)
    
    file_cache = get_price_cache()
    file_cache.set(cache_key, price)


def clear_price_cache() -> None:
    """清空价格缓存"""
    memory_cache = get_memory_cache()
    memory_cache.clear()
    
    file_cache = get_price_cache()
    file_cache.clear()


# ==================== 股票代码工具 ====================

def normalize_code(code: str, market: str = 'A') -> str:
    """
    标准化股票代码
    Args:
        code: 原始代码
        market: 市场（A/H/US）
    Returns:
        str: 标准化后的代码
    """
    code = str(code).strip().upper()
    
    if market == 'A':
        if '.' not in code:
            if code.startswith('6'):
                code = f"{code}.SH"
            elif code.startswith(('0', '3')):
                code = f"{code}.SZ"
    elif market == 'H':
        if '.' not in code:
            code = f"{code}.HK"
    elif market == 'US':
        code = code.split('.')[0]
    
    return code


def get_market_from_code(code: str) -> str:
    """根据代码判断市场"""
    code = code.upper()
    if code.endswith(('.SH', '.SZ')):
        return 'A'
    elif code.endswith('.HK'):
        return 'H'
    else:
        return 'US'


def get_asset_type_from_code(code: str) -> str:
    """根据代码判断资产类型"""
    code = str(code or '').upper().strip()
    base_code = code.split('.')[0]

    if code in ('GC=F', 'GLD', 'IAU', '518880.SH', 'XAUUSD'):
        return 'gold'
    elif code in ('SI=F', 'SLV', 'SIVR', 'PSLV', 'XAGUSD'):
        return 'silver'
    elif code.endswith('.OF') and base_code.isdigit() and len(base_code) == 6:
        return 'fund'
    elif base_code.isdigit() and len(base_code) == 6 and base_code.startswith(('51', '15', '16', '18', '56', '58')):
        return 'etf'
    elif code.endswith(('.SH', '.SZ', '.BJ', '.HK')) or (code.isalpha() and len(code) <= 5):
        return 'stock'
    elif base_code.isdigit() and len(base_code) == 6:
        return 'fund'
    else:
        return 'stock'


# ==================== 数据验证 ====================

def validate_price_data(df) -> bool:
    """验证价格数据是否有效"""
    if df is None or len(df) == 0:
        return False
    
    # 检查必需列
    required_cols = ['open', 'high', 'low', 'close']
    for col in required_cols:
        if col not in df.columns:
            return False
    
    # 检查是否有NaN
    if df['close'].isna().all():
        return False
    
    return True


def filter_invalid_prices(df):
    """过滤无效价格数据"""
    if df is None:
        return None
    
    df = df.copy()
    
    # 删除close为NaN的行
    df = df.dropna(subset=['close'])
    
    # 删除价格为0的行
    df = df[df['close'] > 0]
    
    # 删除异常价格（过高或过低，超过5倍标准差）
    if len(df) > 0:
        mean_price = df['close'].mean()
        std_price = df['close'].std()
        if std_price > 0:
            df = df[abs(df['close'] - mean_price) <= 5 * std_price]
    
    return df


# ==================== 批量处理工具 ====================

def chunked(iterable: List, chunk_size: int):
    """将可迭代对象分块"""
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


def batch_insert(session, model, items: List, chunk_size: Optional[int] = None) -> int:
    """
    批量插入
    Args:
        session: 数据库会话
        model: SQLAlchemy模型类
        items: 要插入的对象列表
        chunk_size: 每批大小，默认使用配置中的 BATCH_SIZE
    Returns:
        int: 插入的记录数
    """
    from config import BATCH_SIZE as DEFAULT_BATCH_SIZE
    
    if not items:
        return 0
    
    _chunk_size = chunk_size if chunk_size is not None else DEFAULT_BATCH_SIZE
    chunks = list(chunked(items, _chunk_size))
    total = 0
    
    for chunk in chunks:
        session.add_all(chunk)
        session.commit()
        total += len(chunk)
    
    return total


# ==================== 数据格式化 ====================

def format_currency(value: float, currency: str = 'CNY') -> str:
    """格式化货币"""
    if currency == 'CNY':
        return f"¥{value:,.2f}"
    elif currency == 'USD':
        return f"${value:,.2f}"
    elif currency == 'HKD':
        return f"HK${value:,.2f}"
    else:
        return f"{value:,.2f}"


def format_percentage(value: float, decimal: int = 2) -> str:
    """格式化百分比"""
    return f"{value:.{decimal}f}%"


def format_number(value: float, decimal: int = 2) -> str:
    """格式化数字（带千分位）"""
    return f"{value:,.{decimal}f}"


# ==================== 时间测量装饰器 ====================

def timer(func):
    """测量函数执行时间的装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger = get_logger(func.__module__)
        logger.debug(f"{func.__name__} 执行时间: {elapsed:.2f}秒")
        return result
    return wrapper


if __name__ == '__main__':
    print("=" * 50)
    print("工具模块测试")
    print("=" * 50)
    
    # 测试日期工具
    print(f"今天: {get_today()}")
    print(f"上一个交易日: {get_previous_trading_day()}")
    
    # 测试缓存
    cache = SimpleCache(ttl=10)
    cache.set("test", "value")
    print(f"缓存读取: {cache.get('test')}")
    
    # 测试进度管理
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        progress_file = f.name
    
    pm = ProgressManager(progress_file)
    pm.mark_completed("test_code")
    print(f"进度摘要: {pm.get_summary()}")
    
    # 清理
    os.unlink(progress_file)
    
    print("\n✅ 工具模块测试完成")