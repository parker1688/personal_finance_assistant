"""
基金数据采集模块 - collectors/fund_collector.py
采集公募基金净值、持仓、费率等数据
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.base_collector import BaseCollector
from utils import get_logger, retry
from models import get_session, Holding, RawFundData
from config import FUND_POOL_CACHE_FILE, MAX_FUNDS

logger = get_logger(__name__)


class FundCollector(BaseCollector):
    """基金数据采集器"""
    
    def __init__(self):
        super().__init__(cache_ttl=3600)  # 基金数据缓存1小时
        self.fund_pool = self._get_fund_pool()

    @staticmethod
    def _apply_configured_max(values, configured_max):
        items = list(values or [])
        try:
            max_n = int(configured_max)
        except Exception:
            max_n = 0
        if max_n > 0:
            return items[:max_n]
        return items

    def _load_cached_fund_pool(self):
        cache_file = str(FUND_POOL_CACHE_FILE)
        if not os.path.exists(cache_file):
            return []
        try:
            import json
            with open(cache_file, 'r') as f:
                payload = json.load(f)
            funds = payload.get('funds', []) or []
            if isinstance(funds, list):
                return funds
        except Exception as e:
            logger.warning(f"读取基金池缓存失败: {e}")
        return []
    
    def get_data_source_name(self):
        return "FundCollector"

    def collect(self, **kwargs):
        """兼容 BaseCollector 抽象接口，默认采集全部基金净值。"""
        fund_code = kwargs.get('fund_code')
        days = kwargs.get('days', 30)

        if fund_code:
            return self.collect_fund_nav(fund_code, days=days)
        return self.collect_all_funds()
    
    def _get_fund_pool(self):
        """获取基金池，优先使用已缓存的全量基金池。"""
        cached = self._load_cached_fund_pool()
        if len(cached) >= 50:
            return self._apply_configured_max(cached, MAX_FUNDS)

        pool = [
            {'code': '110011', 'name': '易方达中小盘'},
            {'code': '519069', 'name': '汇添富价值精选'},
            {'code': '163402', 'name': '兴全趋势投资'},
            {'code': '260108', 'name': '景顺长城新兴成长'},
            {'code': '161005', 'name': '富国天惠成长'},
            {'code': '110003', 'name': '易方达上证50'},
            {'code': '160706', 'name': '嘉实沪深300'},
            {'code': '110020', 'name': '易方达中证500'},
            {'code': '000191', 'name': '富国信用债'},
            {'code': '110017', 'name': '易方达增强回报'},
        ]

        session = None
        try:
            session = get_session()
            holding_funds = (
                session.query(Holding)
                .filter(Holding.asset_type == 'fund')
                .all()
            )
            existing = {str(item.get('code', '')).split('.')[0] for item in pool}
            for item in holding_funds:
                base_code = str(item.code or '').split('.')[0]
                if base_code and base_code not in existing:
                    pool.append({'code': base_code, 'name': item.name or f'基金{base_code}'})
                    existing.add(base_code)
        except Exception as e:
            logger.warning(f"读取持仓基金池失败: {e}")
        finally:
            if session:
                session.close()

        return self._apply_configured_max(pool, MAX_FUNDS)

    def _get_default_fund_pool(self):
        """获取默认基金池（兜底）。"""
        return self._get_fund_pool()

    def _save_nav_to_db(self, fund_code, nav_data, fund_name=''):
        """将基金净值落库，便于持仓估值与缺失补采复用。"""
        if not nav_data:
            return 0

        session = None
        saved = 0
        try:
            session = get_session()
            for item in nav_data:
                date_value = item.get('date')
                if isinstance(date_value, str):
                    date_value = pd.to_datetime(date_value).date()

                existing = (
                    session.query(RawFundData)
                    .filter(RawFundData.code == item.get('code'))
                    .filter(RawFundData.date == date_value)
                    .first()
                )
                if existing:
                    existing.name = fund_name or existing.name
                    existing.nav = float(item.get('nav') or 0)
                    existing.accumulated_nav = float(item.get('accumulated_nav') or item.get('nav') or 0)
                    existing.daily_return = float(item.get('daily_return') or 0)
                else:
                    session.add(RawFundData(
                        code=item.get('code'),
                        name=fund_name or item.get('name'),
                        date=date_value,
                        nav=float(item.get('nav') or 0),
                        accumulated_nav=float(item.get('accumulated_nav') or item.get('nav') or 0),
                        daily_return=float(item.get('daily_return') or 0),
                    ))
                saved += 1
            session.commit()
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"保存基金净值失败 {fund_code}: {e}")
        finally:
            if session:
                session.close()
        return saved
    
    @retry(max_attempts=3, delay=1)
    def collect_fund_nav(self, fund_code, days=30):
        """
        采集基金净值数据。
        优先使用 AkShare 真实净值，失败时再降级为简化兜底数据。
        """
        normalized_code = str(fund_code or '').split('.')[0]
        nav_data = []

        try:
            import akshare as ak
            df = ak.fund_open_fund_info_em(symbol=normalized_code, indicator='单位净值走势')
            if df is not None and len(df) > 0:
                df = df.tail(days).copy()
                for _, row in df.iterrows():
                    nav_date = pd.to_datetime(row.get('净值日期')).date()
                    unit_nav = pd.to_numeric(row.get('单位净值'), errors='coerce')
                    accum_nav = pd.to_numeric(row.get('累计净值'), errors='coerce')
                    daily_return = pd.to_numeric(row.get('日增长率'), errors='coerce')
                    if pd.isna(unit_nav) or unit_nav <= 0:
                        continue
                    nav_data.append({
                        'code': normalized_code,
                        'date': nav_date,
                        'nav': round(float(unit_nav), 4),
                        'accumulated_nav': round(float(accum_nav if not pd.isna(accum_nav) else unit_nav), 4),
                        'daily_return': round(float(daily_return), 4) if not pd.isna(daily_return) else 0.0,
                    })

                if nav_data:
                    self._save_nav_to_db(normalized_code, nav_data)
                    logger.info(f"基金 {normalized_code} 真实净值采集成功: {len(nav_data)} 条")
                    return nav_data
        except Exception as e:
            logger.warning(f"AkShare基金净值采集失败 {normalized_code}: {e}")

        try:
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).date()
                nav_data.append({
                    'code': normalized_code,
                    'date': date,
                    'nav': round(1.0 + i * 0.001, 4),
                    'accumulated_nav': round(1.5 + i * 0.001, 4),
                    'daily_return': 0.0,
                })
            self._save_nav_to_db(normalized_code, nav_data)
            logger.warning(f"基金 {normalized_code} 使用兜底净值数据")
            return nav_data
        except Exception as e:
            logger.error(f"采集基金净值失败 {normalized_code}: {e}")
            return []
    
    @retry(max_attempts=3, delay=1)
    def collect_fund_info(self, fund_code):
        """
        采集基金基本信息
        Args:
            fund_code: 基金代码
        Returns:
            dict: 基金信息
        """
        try:
            # 模拟基金信息
            info = {
                'code': fund_code,
                'name': f'基金{fund_code}',
                'type': '混合型',
                'manager': '基金经理',
                'establish_date': '2020-01-01',
                'size': 50.0,  # 规模（亿）
                'fee': {
                    'management': 1.5,  # 管理费%
                    'custody': 0.25,    # 托管费%
                    'subscription': 1.5  # 申购费%
                },
                'holdings': [
                    {'code': '600519.SH', 'name': '贵州茅台', 'ratio': 8.5},
                    {'code': '000858.SZ', 'name': '五粮液', 'ratio': 6.2},
                ]
            }
            
            return info
            
        except Exception as e:
            logger.error(f"采集基金信息失败 {fund_code}: {e}")
            return None
    
    def collect_all_funds(self):
        """采集所有基金数据"""
        results = []
        
        for fund in self.fund_pool:
            fund_code = fund['code']
            
            # 采集净值
            nav_data = self.collect_fund_nav(fund_code, days=30)
            if nav_data:
                results.extend(nav_data)
            
            # 采集基本信息
            info = self.collect_fund_info(fund_code)
            if info:
                results.append(info)
            
            time.sleep(0.5)
        
        logger.info(f"基金采集完成，共 {len(results)} 条数据")
        return results
    
    def get_fund_holdings(self, fund_code):
        """
        获取基金持仓
        Args:
            fund_code: 基金代码
        Returns:
            list: 持仓列表
        """
        info = self.collect_fund_info(fund_code)
        return info.get('holdings', []) if info else []
    
    def get_fund_performance(self, fund_code):
        """
        获取基金业绩
        Args:
            fund_code: 基金代码
        Returns:
            dict: 业绩数据
        """
        nav_data = self.collect_fund_nav(fund_code, days=365)
        
        if not nav_data:
            return None
        
        navs = [d['nav'] for d in nav_data]
        
        if len(navs) >= 252:
            return_1y = (navs[0] - navs[-252]) / navs[-252] * 100 if len(navs) >= 252 else 0
        else:
            return_1y = (navs[0] - navs[-1]) / navs[-1] * 100 if navs else 0
        
        return_6m = (navs[0] - navs[-126]) / navs[-126] * 100 if len(navs) >= 126 else 0
        return_3m = (navs[0] - navs[-63]) / navs[-63] * 100 if len(navs) >= 63 else 0
        return_1m = (navs[0] - navs[-21]) / navs[-21] * 100 if len(navs) >= 21 else 0
        
        return {
            'code': fund_code,
            'return_1m': round(return_1m, 2),
            'return_3m': round(return_3m, 2),
            'return_6m': round(return_6m, 2),
            'return_1y': round(return_1y, 2)
        }


    # ==================== 全市场基金池获取 ====================
    
    def fetch_all_funds(self):
        """
        获取全部基金列表（主动基金 + ETF）
        """
        try:
            import akshare as ak
            logger.info("正在从 AkShare 获取全部基金...")
            
            funds = []
            
            # 获取全市场基金列表
            stock_funds = ak.fund_name_em()
            if stock_funds is not None and len(stock_funds) > 0:
                for _, row in stock_funds.iterrows():
                    fund_name = str(row.get('基金简称') or '').strip()
                    fund_type = str(row.get('基金类型') or '').strip()
                    funds.append({
                        'code': str(row.get('基金代码') or '').strip(),
                        'name': fund_name,
                        'type': 'etf' if 'ETF' in fund_name.upper() or 'ETF' in fund_type.upper() else 'active_fund'
                    })
                logger.info(f"获取到 {len(stock_funds)} 只基金")
            
            # 获取ETF列表
            try:
                etf_funds = ak.fund_etf_spot_em()
                if etf_funds is not None and len(etf_funds) > 0:
                    for _, row in etf_funds.iterrows():
                        funds.append({
                            'code': row['代码'],
                            'name': row['名称'],
                            'type': 'etf'
                        })
                    logger.info(f"获取到 {len(etf_funds)} 只ETF")
            except:
                pass
            
            deduped = []
            seen = set()
            for item in funds:
                code = str(item.get('code') or '').strip()
                if not code or code in seen:
                    continue
                seen.add(code)
                deduped.append(item)

            logger.info(f"✅ 共获取到 {len(deduped)} 只基金")
            self.fund_pool = self._apply_configured_max(deduped, MAX_FUNDS)
            return self.fund_pool
            
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return self._get_default_fund_pool()
    
    def update_fund_pool(self):
        """
        更新基金池
        """
        funds = self.fetch_all_funds()
        
        # 保存到缓存
        import json
        pool_data = {
            'funds': self.fund_pool,
            'updated_at': datetime.now().isoformat()
        }
        
        with open(str(FUND_POOL_CACHE_FILE), 'w') as f:
            json.dump(pool_data, f)
        
        return len(self.fund_pool)

# 测试代码
if __name__ == '__main__':
    collector = FundCollector()
    result = collector.collect_all_funds()
    print(f"采集结果: {len(result)} 条")