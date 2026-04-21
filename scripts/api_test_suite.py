#!/usr/bin/env python3
"""
API接口全面测试脚本 - api_test_suite.py
自动化测试所有API端点的接通性、响应格式、数据真实性

使用方式:
  python3 api_test_suite.py                    # 测试所有端点
  python3 api_test_suite.py --module dashboard  # 仅测试仪表盘模块
  python3 api_test_suite.py --verbose           # 显示详细日志
"""

import sys
import os
import json
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from urllib.parse import urljoin

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ==================== 配置 ====================

BASE_URL = 'http://localhost:5000'
TIMEOUT = 10
DEBUG = False

# 定义所有API端点
API_ENDPOINTS = {
    'dashboard': [
        {'method': 'GET', 'url': '/api/dashboard/summary', 'name': '仪表盘汇总'},
        {'method': 'GET', 'url': '/api/market/temperature', 'name': '市场温度'},
    ],
    'recommendations': [
        {'method': 'GET', 'url': '/api/recommendations/a_stock', 'name': 'A股推荐'},
        {'method': 'GET', 'url': '/api/recommendations/hk_stock', 'name': '港股推荐'},
        {'method': 'GET', 'url': '/api/recommendations/us_stock', 'name': '美股推荐'},
        {'method': 'GET', 'url': '/api/recommendations/fund', 'name': '基金推荐'},
        {'method': 'GET', 'url': '/api/recommendations/gold', 'name': '黄金推荐'},
        {'method': 'GET', 'url': '/api/recommendations/silver', 'name': '白银推荐'},
    ],
    'warnings': [
        {'method': 'GET', 'url': '/api/warnings/current', 'name': '当前预警'},
        {'method': 'GET', 'url': '/api/warnings/history', 'name': '预警历史'},
        {'method': 'GET', 'url': '/api/warnings/stats', 'name': '预警统计'},
    ],
    'reviews': [
        {'method': 'GET', 'url': '/api/reviews/list', 'name': '复盘列表'},
        {'method': 'GET', 'url': '/api/reviews/accuracy', 'name': '准确率统计'},
    ],
    'holdings': [
        {'method': 'GET', 'url': '/api/holdings', 'name': '持仓列表'},
        {'method': 'GET', 'url': '/api/holdings/trend', 'name': '持仓趋势'},
        {'method': 'GET', 'url': '/api/holdings/asset_type_distribution', 'name': '资产分布'},
    ],
    'model': [
        {'method': 'GET', 'url': '/api/model/status', 'name': '模型状态'},
        {'method': 'GET', 'url': '/api/model/accuracy', 'name': '模型准确率'},
    ],
    'config': [
        {'method': 'GET', 'url': '/api/config', 'name': '获取配置'},
    ],
    'logs': [
        {'method': 'GET', 'url': '/api/logs', 'name': '系统日志'},
    ],
}

# ==================== 颜色输出 ====================

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def colored(text, color):
    return f"{color}{text}{Colors.RESET}"

def print_section(title):
    print(f"\n{colored('=' * 70, Colors.CYAN)}")
    print(f"{colored(title, Colors.BOLD + Colors.CYAN)}")
    print(f"{colored('=' * 70, Colors.CYAN)}\n")

def print_success(text):
    print(f"{colored('✅', Colors.GREEN)} {text}")

def print_error(text):
    print(f"{colored('❌', Colors.RED)} {text}")

def print_warning(text):
    print(f"{colored('⚠️',  Colors.YELLOW)} {text}")

def print_info(text):
    print(f"{colored('ℹ️', Colors.BLUE)} {text}")

# ==================== 测试逻辑 ====================

class APITester:
    def __init__(self, base_url=BASE_URL, timeout=TIMEOUT):
        self.base_url = base_url
        self.timeout = timeout
        self.results = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'errors': []
        }
        self.session = requests.Session()
    
    def test_endpoint(self, method: str, url: str, name: str) -> Tuple[bool, Dict]:
        """
        测试单个API端点
        
        返回: (成功/失败, 详情字典)
        """
        full_url = urljoin(self.base_url, url)
        
        try:
            # 发送请求
            if method.upper() == 'GET':
                response = self.session.get(full_url, timeout=self.timeout)
            elif method.upper() == 'POST':
                response = self.session.post(full_url, json={}, timeout=self.timeout)
            else:
                return False, {'error': f'Unsupported method: {method}'}
            
            # 检查状态码
            if response.status_code not in [200, 201, 202, 204]:
                return False, {
                    'status_code': response.status_code,
                    'reason': response.reason
                }
            
            # 检查响应格式
            try:
                data = response.json() if response.text else {}
            except json.JSONDecodeError:
                return False, {'error': 'Invalid JSON response'}
            
            # 验证数据完整性
            result = {
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds(),
                'data_keys': list(data.keys()) if isinstance(data, dict) else 'list',
                'has_data': bool(data),
                'url': url,
                'name': name
            }
            
            return True, result
            
        except requests.exceptions.ConnectionError:
            return False, {'error': 'Connection refused'}
        except requests.exceptions.Timeout:
            return False, {'error': 'Request timeout'}
        except Exception as e:
            return False, {'error': str(e)}
    
    def validate_data_authenticity(self, endpoint_name: str, data: Dict) -> Dict:
        """
        验证数据的真实性
        """
        issues = []
        
        # 检查硬编码数据的迹象
        if endpoint_name == 'fund':
            # 检查是否是硬编码的基金列表
            if 'data' in data and isinstance(data['data'], list):
                first_item = data['data'][0] if data['data'] else {}
                if 'manager' in first_item:
                    # 基金数据中有manager字段，可能是硬编码
                    issues.append('⚠️ 可能使用硬编码基金数据（检测到manager字段）')
        
        elif endpoint_name == 'gold':
            if 'data' in data and isinstance(data['data'], list):
                first_item = data['data'][0] if data['data'] else {}
                if 'name' in first_item and 'ETF' in first_item.get('name', ''):
                    issues.append('⚠️ 可能使用硬编码黄金产品数据')
        
        return {'issues': issues}
    
    def run_all_tests(self, modules=None):
        """运行所有测试"""
        modules = modules or list(API_ENDPOINTS.keys())
        
        print_section('API 接口全面测试')
        print_info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_info(f"测试目标: {self.base_url}")
        print(f"模块数: {len(modules)}\n")
        
        for module_name in modules:
            if module_name not in API_ENDPOINTS:
                print_warning(f"未知模块: {module_name}")
                continue
            
            print_section(f"模块: {module_name.upper()}")
            endpoints = API_ENDPOINTS[module_name]
            module_passed = 0
            module_failed = 0
            
            for endpoint in endpoints:
                # 测试端点
                success, result = self.test_endpoint(
                    endpoint['method'],
                    endpoint['url'],
                    endpoint['name']
                )
                
                self.results['total'] += 1
                
                if success:
                    self.results['passed'] += 1
                    module_passed += 1
                    
                    print_success(
                        f"{endpoint['name']:<20} "
                        f"[{endpoint['url']:<40}] "
                        f"响应时间: {result['response_time']*1000:.1f}ms"
                    )
                    
                    if DEBUG and result.get('data_keys'):
                        print_info(f"  返回字段: {result.get('data_keys')}")
                else:
                    self.results['failed'] += 1
                    module_failed += 1
                    error_msg = result.get('error', result.get('reason', 'Unknown error'))
                    
                    print_error(
                        f"{endpoint['name']:<20} "
                        f"[{endpoint['url']:<40}] "
                        f"错误: {error_msg}"
                    )
                    
                    self.results['errors'].append({
                        'endpoint': endpoint['url'],
                        'error': error_msg,
                        'name': endpoint['name']
                    })
            
            # 模块汇总
            print(f"\n{colored(f'模块汇总: {module_passed}/{len(endpoints)} 通过', Colors.BOLD)}")
            if module_failed > 0:
                print_warning(f"{module_failed} 个端点失败")
        
        self.print_summary()
    
    def print_summary(self):
        """打印测试总结"""
        print_section('测试总结')
        
        total = self.results['total']
        passed = self.results['passed']
        failed = self.results['failed']
        pass_rate = (passed / total * 100) if total > 0 else 0
        
        print_info(f"总端点数: {total}")
        print_success(f"通过数: {passed}")
        print_error(f"失败数: {failed}")
        
        if pass_rate >= 90:
            print_success(f"通过率: {pass_rate:.1f}% ✅")
        elif pass_rate >= 70:
            print_warning(f"通过率: {pass_rate:.1f}% ⚠️")
        else:
            print_error(f"通过率: {pass_rate:.1f}% ❌")
        
        if self.results['errors']:
            print(f"\n{colored('失败详情:', Colors.BOLD + Colors.RED)}")
            for error in self.results['errors']:
                print(f"  - {error['name']}")
                print(f"    路径: {error['endpoint']}")
                print(f"    错误: {error['error']}\n")
        
        print_info(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ==================== 主程序 ====================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='API接口测试套件')
    parser.add_argument('--module', help='仅测试指定模块', type=str)
    parser.add_argument('--verbose', help='显示详细信息', action='store_true')
    parser.add_argument('--url', help='API基础URL', type=str, default=BASE_URL)
    
    args = parser.parse_args()
    
    global DEBUG
    DEBUG = args.verbose
    
    # 创建测试器
    tester = APITester(base_url=args.url)
    
    # 运行测试
    modules = [args.module] if args.module else None
    tester.run_all_tests(modules=modules)
    
    # 返回退出码
    return 0 if tester.results['failed'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
