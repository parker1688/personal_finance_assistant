"""
API调用优化工具 - api_utils.py
提供重试、超时、错误处理的公共功能
"""

import time
import requests
from functools import wraps
from typing import Optional, Callable, Any, List, Type
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)


class APIError(Exception):
    """API调用异常基类"""
    pass


class APITimeoutError(APIError):
    """API超时异常"""
    pass


class APIConnectionError(APIError):
    """API连接异常"""
    pass


class APIRateLimitError(APIError):
    """API速率限制异常"""
    pass


def setup_session_with_retries(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Optional[List[int]] = None,
    timeout: int = 10
) -> requests.Session:
    """
    设置带重试机制的requests Session
    
    Args:
        total_retries: 总重试次数
        backoff_factor: 退避因子 (第n次重试等待: backoff_factor * (2 ** (n-1)))
        status_forcelist: 需要重试的HTTP状态码
        timeout: 请求超时时间（秒）
    
    Returns:
        requests.Session: 配置好的会话
    
    Example:
        session = setup_session_with_retries(total_retries=3)
        response = session.get('https://api.example.com/data', timeout=10)
    """
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]
    
    session = requests.Session()
    
    retry_strategy = Retry(
        total=total_retries,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
        allowed_methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD']
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    # 设置默认超时
    session.timeout = timeout
    
    return session


def retry_with_exponential_backoff(
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    exceptions: Optional[tuple] = None,
    catch_all: bool = False
):
    """
    重试装饰器 - 使用指数退避算法
    
    Args:
        max_retries: 最大重试次数
        backoff_factor: 退避因子
        exceptions: 要捕获的异常类型元组
        catch_all: 是否捕获所有异常
    
    Example:
        @retry_with_exponential_backoff(max_retries=3)
        def fetch_data(url):
            response = requests.get(url, timeout=10)
            return response.json()
        
        data = fetch_data('https://api.example.com')
    """
    if exceptions is None:
        exceptions = (requests.RequestException, APIError)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor * (2 ** attempt)
                        logger.warning(
                            f"第{attempt + 1}次尝试失败 ({type(e).__name__})，"
                            f"将在{wait_time:.2f}秒后重试... "
                            f"({attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} 在{max_retries}次尝试后失败: {e}")
                        
                except Exception as e:
                    if catch_all:
                        last_exception = e
                        if attempt < max_retries - 1:
                            wait_time = backoff_factor * (2 ** attempt)
                            logger.warning(f"未预期的错误，将在{wait_time:.2f}秒后重试...")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"未预期的错误导致失败: {e}", exc_info=True)
                    else:
                        raise
            
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


def handle_api_response(
    response: requests.Response,
    raise_for_status: bool = True,
    json_response: bool = True
) -> Any:
    """
    处理API响应
    
    Args:
        response: requests响应对象
        raise_for_status: 是否在HTTP错误时抛出异常
        json_response: 是否解析为JSON
    
    Returns:
        解析后的响应数据
    
    Raises:
        APIStatusError: HTTP错误状态
        APIRateLimitError: 速率限制错误
    """
    # 检查速率限制
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After', 60)
        raise APIRateLimitError(
            f"API速率限制 - 请等待 {retry_after} 秒后重试"
        )
    
    # 处理HTTP错误
    if response.status_code >= 400:
        if raise_for_status:
            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(error_msg)
                raise APIError(error_msg) from e
        return None
    
    # 解析JSON
    if json_response:
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {response.text[:200]}")
            raise APIError(f"JSON解析失败: {str(e)}") from e
    
    return response


@retry_with_exponential_backoff(max_retries=3)
def fetch_with_timeout(
    url: str,
    method: str = 'GET',
    timeout: int = 10,
    **kwargs
) -> requests.Response:
    """
    带超时和重试的API请求
    
    Args:
        url: 请求URL
        method: HTTP方法
        timeout: 超时时间（秒）
        **kwargs: 其他requests参数
    
    Returns:
        requests.Response
    
    Raises:
        APITimeoutError: 请求超时
        APIConnectionError: 连接错误
        APIError: 其他API错误
    """
    try:
        response = requests.request(
            method=method,
            url=url,
            timeout=timeout,
            **kwargs
        )
        return response
        
    except requests.Timeout as e:
        raise APITimeoutError(f"请求超时 ({timeout}s): {url}") from e
    except requests.ConnectionError as e:
        raise APIConnectionError(f"连接错误: {url}") from e
    except requests.RequestException as e:
        raise APIError(f"请求失败: {str(e)}") from e


# 使用示例
if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 示例1: 使用带重试的会话
    session = setup_session_with_retries()
    try:
        response = session.get('https://api.github.com', timeout=10)
        data = handle_api_response(response)
        print(f"成功获取数据: {type(data)}")
    except APIError as e:
        print(f"API错误: {e}")
    
    # 示例2: 使用装饰器
    @retry_with_exponential_backoff(max_retries=3)
    def get_user_data(user_id):
        response = fetch_with_timeout(
            f'https://api.example.com/users/{user_id}',
            timeout=10
        )
        return handle_api_response(response, json_response=True)
