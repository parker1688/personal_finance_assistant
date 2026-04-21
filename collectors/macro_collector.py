"""
宏观数据采集模块 - collectors/macro_collector.py
采集宏观经济指标数据
"""

import time
import requests
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.base_collector import BaseCollector
from utils import get_logger, retry

logger = get_logger(__name__)


class MacroCollector(BaseCollector):
    """宏观数据采集器"""
    
    def __init__(self):
        super().__init__(cache_ttl=86400)  # 宏观数据缓存24小时
    
    def get_data_source_name(self):
        return "MacroCollector"
    
    def collect(self, **kwargs):
        """
        采集宏观经济数据 (实现 abstract 方法)
        
        Returns:
            dict: 宏观数据汇总
        """
        return self.get_all_macro_data()

    def _get_tushare_pro(self):
        """兼容不同版本 TuShare 的连接方式。"""
        import tushare as ts

        try:
            return ts.pro_api()
        except Exception:
            return ts.pro_connect()
    
    def get_china_10y_yield(self):
        """
        获取中国10年期国债收益率
        Returns:
            float: 收益率（%）
        """
        try:
            import pandas as pd

            pro = self._get_tushare_pro()
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=45)).strftime('%Y%m%d')

            # 1001.CB: 中债国债收益率曲线
            yc_df = pro.yc_cb(
                ts_code='1001.CB',
                start_date=start_date,
                end_date=end_date,
                fields='trade_date,curve_term,yield'
            )
            if yc_df is not None and not yc_df.empty:
                yc_df = yc_df.copy()
                yc_df['curve_term'] = pd.to_numeric(yc_df['curve_term'], errors='coerce')
                yc_df['yield'] = pd.to_numeric(yc_df['yield'], errors='coerce')
                yc_df = yc_df.dropna(subset=['curve_term', 'yield'])
                if not yc_df.empty:
                    latest_day = str(yc_df['trade_date'].max())
                    latest_df = yc_df[yc_df['trade_date'] == latest_day].copy()
                    if not latest_df.empty:
                        latest_df['term_diff'] = (latest_df['curve_term'] - 10.0).abs()
                        best = latest_df.sort_values('term_diff').iloc[0]
                        y = float(best['yield'])
                        if 0 < y < 15:
                            return y

            # 回退: 使用Shibor 1Y作为利率代理
            shibor = self.get_shibor()
            y1 = shibor.get('1y') if isinstance(shibor, dict) else None
            if y1 is not None and y1 > 0:
                return float(y1)

        except Exception as e:
            logger.error(f"获取国债收益率失败: {e}")

        return 2.85
    
    def get_shibor(self):
        """
        获取Shibor利率
        Returns:
            dict: 各期限利率
        """
        try:
            import pandas as pd

            pro = self._get_tushare_pro()
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            df = pro.shibor(start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return {}

            df = df.sort_values('date', ascending=False)
            row = df.iloc[0]
            return {
                'overnight': float(pd.to_numeric(pd.Series([row.get('on')]), errors='coerce').iloc[0]),
                '1w': float(pd.to_numeric(pd.Series([row.get('1w')]), errors='coerce').iloc[0]),
                '2w': float(pd.to_numeric(pd.Series([row.get('2w')]), errors='coerce').iloc[0]),
                '1m': float(pd.to_numeric(pd.Series([row.get('1m')]), errors='coerce').iloc[0]),
                '3m': float(pd.to_numeric(pd.Series([row.get('3m')]), errors='coerce').iloc[0]),
                '6m': float(pd.to_numeric(pd.Series([row.get('6m')]), errors='coerce').iloc[0]),
                '9m': float(pd.to_numeric(pd.Series([row.get('9m')]), errors='coerce').iloc[0]),
                '1y': float(pd.to_numeric(pd.Series([row.get('1y')]), errors='coerce').iloc[0]),
                'date': str(row.get('date', '')),
                'data_source': 'TuShare shibor',
            }
        except Exception as e:
            logger.error(f"获取Shibor失败: {e}")
            return {}
    
    def get_cpi(self):
        """
        获取真实CPI数据 (同比%)
        
        数据源: TuShare / FRED (US)
        更新频率: 每月 (国家统计局)
        改进: 之前是0.2%的硬编码，现在使用真实API
        """
        import tushare as ts
        import pandas as pd
        
        try:
            pro = self._get_tushare_pro()
            
            # TuShare 字段在不同版本可能不同，优先全量拉取后做列适配。
            cpi_data = pro.cn_cpi()
            
            if cpi_data.empty:
                logger.warning("CPI数据为空")
                return self._default_cpi()
            
            month_col = 'month' if 'month' in cpi_data.columns else ('MONTH' if 'MONTH' in cpi_data.columns else None)
            if month_col is None:
                logger.warning("CPI缺少月份字段")
                return self._default_cpi()

            cpi_col = None
            for cand in ['cpi', 'nt_yoy', 'town_yoy', 'cnt_yoy']:
                if cand in cpi_data.columns:
                    cpi_col = cand
                    break
            if cpi_col is None:
                logger.warning("CPI缺少数值字段")
                return self._default_cpi()

            raw_month = cpi_data[month_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:6]
            cpi_data['month'] = pd.to_datetime(raw_month, format='%Y%m', errors='coerce')
            cpi_data['cpi_value'] = pd.to_numeric(cpi_data[cpi_col], errors='coerce')
            cpi_data = cpi_data.dropna(subset=['month', 'cpi_value'])
            if cpi_data.empty:
                logger.warning("CPI有效数据为空")
                return self._default_cpi()
            cpi_data = cpi_data.sort_values('month')
            
            latest = cpi_data.iloc[-1]
            
            return {
                'value': float(latest['cpi_value']),
                'month': latest['month'].strftime('%Y-%m-%d'),
                'update_date': latest['month'].strftime('%Y-%m-%d'),
                'history_12m': cpi_data[['month', 'cpi_value']].tail(12).to_dict('records'),
                'trend': self._analyze_cpi_trend(cpi_data),
                'timestamp': datetime.now().isoformat(),
                'data_source': 'TuShare/NBS'
            }
            
        except Exception as e:
            logger.error(f"获取CPI失败: {e}")
            return self._default_cpi()
    
    def _analyze_cpi_trend(self, cpi_data):
        """分析CPI趋势"""
        if len(cpi_data) < 2:
            return 'unknown'
        
        latest = float(cpi_data.iloc[-1]['cpi_value'])
        prev = float(cpi_data.iloc[-2]['cpi_value'])
        
        if latest > prev + 0.5:
            return 'rising'
        elif latest < prev - 0.5:
            return 'falling'
        else:
            return 'stable'
    
    def _default_cpi(self):
        """CPI默认降级方案"""
        return {
            'value': None,
            'status': 'unavailable',
            'source': 'fallback',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_pmi(self):
        """
        获取真实PMI数据 (制造业/非制造业)
        
        数据源: TuShare / NBS
        更新频率: 每月
        改进: 之前是50.2的硬编码，现在使用真实API
        """
        import tushare as ts
        import pandas as pd
        
        try:
            pro = self._get_tushare_pro()
            
            # 获取中国制造业PMI
            pmi_data = pro.cn_pmi()
            
            if pmi_data.empty:
                logger.warning("PMI数据为空")
                return self._default_pmi()
            
            date_col = None
            for cand in ['release_date', 'month', 'MONTH', 'trade_date']:
                if cand in pmi_data.columns:
                    date_col = cand
                    break
            if date_col is None:
                logger.warning("PMI缺少日期字段")
                return self._default_pmi()

            mfg_col = None
            for cand in ['manufacturing_pmi', 'PMI010000', 'PMI010100', 'pmi']:
                if cand in pmi_data.columns:
                    mfg_col = cand
                    break
            if mfg_col is None:
                logger.warning("PMI缺少制造业字段")
                return self._default_pmi()

            svcs_col = None
            for cand in ['non_manufacturing_pmi', 'PMI020000', 'PMI020100']:
                if cand in pmi_data.columns:
                    svcs_col = cand
                    break

            raw_date = pmi_data[date_col].astype(str).str.replace(r'[^0-9]', '', regex=True)
            pmi_data['release_date'] = pd.to_datetime(raw_date.str[:8], format='%Y%m%d', errors='coerce')
            mask_missing = pmi_data['release_date'].isna()
            if mask_missing.any():
                pmi_data.loc[mask_missing, 'release_date'] = pd.to_datetime(raw_date[mask_missing].str[:6], format='%Y%m', errors='coerce')

            pmi_data['manufacturing_pmi'] = pd.to_numeric(pmi_data[mfg_col], errors='coerce')
            if svcs_col is not None:
                pmi_data['non_manufacturing_pmi'] = pd.to_numeric(pmi_data[svcs_col], errors='coerce')
            else:
                pmi_data['non_manufacturing_pmi'] = pmi_data['manufacturing_pmi']

            pmi_data = pmi_data.dropna(subset=['release_date', 'manufacturing_pmi'])
            if pmi_data.empty:
                logger.warning("PMI有效数据为空")
                return self._default_pmi()

            pmi_data = pmi_data.sort_values('release_date')
            
            latest = pmi_data.iloc[-1]
            
            mfg_pmi = float(latest['manufacturing_pmi'])
            svcs_pmi = float(latest['non_manufacturing_pmi'])
            
            return {
                'manufacturing_pmi': mfg_pmi,
                'non_manufacturing_pmi': svcs_pmi,
                'composite_pmi': (mfg_pmi + svcs_pmi) / 2,
                'release_date': latest['release_date'].strftime('%Y-%m-%d'),
                'status': self._interpret_pmi(mfg_pmi),
                'history': pmi_data[['release_date', 'manufacturing_pmi', 'non_manufacturing_pmi']].tail(12).to_dict('records'),
                'timestamp': datetime.now().isoformat(),
                'data_source': 'TuShare/NBS'
            }
            
        except Exception as e:
            logger.error(f"获取PMI失败: {e}")
            return self._default_pmi()
    
    def _interpret_pmi(self, pmi_value):
        """解释PMI数值"""
        if pmi_value > 52:
            return 'strong_expansion'  # 强劲扩张
        elif pmi_value > 50:
            return 'mild_expansion'    # 温和扩张
        elif pmi_value > 48:
            return 'mild_contraction'  # 温和收缩
        else:
            return 'strong_contraction' # 强烈收缩
    
    def _default_pmi(self):
        """PMI默认降级方案"""
        return {
            'manufacturing_pmi': None,
            'status': 'unavailable',
            'source': 'fallback',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_exchange_rate(self, pair='USDCNY', quote=None):
        """
        获取真实汇率数据
        
        参数:
            pair: 货币对或基准货币 (例: 'USDCNY' 或 'USD')
            quote: 报价货币 (例: 'CNY')，若提供则组合为 pair+quote
        
        返回:
            dict: 当前汇率和信息
            
        改进: 之前是硬编码的汇率 (7.25), 现在使用真实API
        """
        import tushare as ts
        
        try:
            pro = self._get_tushare_pro()
            
            # 兼容两种调用：get_exchange_rate('USDCNY') / get_exchange_rate('USD', 'CNY')
            ts_code = f"{pair}{quote}" if quote else str(pair)
            
            # 获取最新汇率
            fx_data = pro.fx_daily(ts_code=ts_code)
            
            if fx_data.empty:
                logger.debug("汇率数据为空，使用默认回退值")
                return self._default_fx_rate()
            
            latest = fx_data.iloc[0]
            
            return {
                'pair': ts_code,
                'rate': float(latest['close']),
                'open': float(latest['open']),
                'high': float(latest['high']),
                'low': float(latest['low']),
                'change': float(latest['change']),
                'date': latest['trade_date'],
                'timestamp': datetime.now().isoformat(),
                'data_source': 'TuShare'
            }
            
        except Exception as e:
            logger.error(f"获取汇率失败: {e}")
            return self._default_fx_rate()
    
    def _default_fx_rate(self):
        """汇率默认降级方案"""
        return {
            'rate': None,
            'status': 'unavailable',
            'source': 'fallback',
            'timestamp': datetime.now().isoformat()
        }

    def get_gdp(self):
        """
        获取真实GDP增速数据

        数据源: TuShare / NBS
        更新频率: 每季度一次
        """
        import pandas as pd

        try:
            pro = self._get_tushare_pro()

            # 获取最近季度GDP数据
            gdp_data = pro.cn_gdp(fields='quarter,gdp_yoy')

            if gdp_data is None or gdp_data.empty:
                logger.warning("GDP数据为空")
                return self._default_gdp()

            # 按季度排序，获取最新
            gdp_data = gdp_data.sort_values('quarter', ascending=False)
            latest = gdp_data.iloc[0]

            return {
                'value': float(latest['gdp_yoy']),  # 同比增速 (%)
                'quarter': latest['quarter'],
                'timestamp': datetime.now().isoformat(),
                'data_source': 'TuShare/NBS'
            }

        except Exception as e:
            logger.error(f"获取GDP失败: {e}")
            return self._default_gdp()

    def _default_gdp(self):
        """GDP默认降级方案"""
        return {
            'value': None,
            'status': 'unavailable',
            'source': 'fallback',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_all_macro_data(self):
        """
        获取所有宏观数据
        Returns:
            dict: 宏观数据汇总
        """
        return {
            'bond_yield': self.get_china_10y_yield(),
            'shibor': self.get_shibor(),
            'cpi': self.get_cpi(),
            'pmi': self.get_pmi(),
            'gdp': self.get_gdp(),
            'exchange_rate': {
                'usd_cny': self.get_exchange_rate('USD', 'CNY'),
                'hkd_cny': self.get_exchange_rate('HKD', 'CNY')
            },
            'updated_at': datetime.now().isoformat()
        }

    def _get_data_dir(self):
        return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

    def _upsert_csv_row(self, filename, row, key_col):
        import pandas as pd

        data_dir = self._get_data_dir()
        os.makedirs(data_dir, exist_ok=True)
        path = os.path.join(data_dir, filename)
        new_df = pd.DataFrame([row])

        if os.path.exists(path):
            try:
                existing = pd.read_csv(path)
                merged = pd.concat([existing, new_df], ignore_index=True)
            except Exception:
                merged = new_df
        else:
            merged = new_df

        if key_col in merged.columns:
            merged = merged.dropna(subset=[key_col])
            merged[key_col] = merged[key_col].astype(str)
            merged = merged.drop_duplicates(subset=[key_col], keep='last')
            parsed_key = pd.to_datetime(merged[key_col], errors='coerce')
            if parsed_key.notna().any():
                merged = merged.assign(_sort_key=parsed_key).sort_values('_sort_key', ascending=False).drop(columns=['_sort_key'])
            else:
                merged = merged.sort_values(key_col, ascending=False)

        merged.to_csv(path, index=False)
        return int(len(merged))

    def _refresh_cross_asset_daily_csv(self):
        import pandas as pd
        import numpy as np
        import yfinance as yf

        def _series(symbol, period='1y'):
            try:
                hist = yf.Ticker(symbol).history(period=period)
                if hist is None or hist.empty or 'Close' not in hist.columns:
                    return pd.Series(dtype=float)
                s = hist['Close'].copy()
                s.index = pd.to_datetime(s.index, errors='coerce').tz_localize(None)
                s = s.dropna()
                s.name = symbol
                return s
            except Exception:
                return pd.Series(dtype=float)

        vix = _series('^VIX')
        dxy = _series('DX-Y.NYB')
        if dxy.empty:
            dxy = _series('UUP')
        tnx = _series('^TNX')
        gold = _series('GC=F')
        oil = _series('CL=F')
        if oil.empty:
            oil = _series('BZ=F')

        base = pd.DataFrame(index=vix.index.union(dxy.index).union(tnx.index).union(gold.index).union(oil.index))
        base = base.sort_index()
        base['vix'] = pd.to_numeric(vix.reindex(base.index), errors='coerce')
        base['dxy'] = pd.to_numeric(dxy.reindex(base.index), errors='coerce')
        base['tnx'] = pd.to_numeric(tnx.reindex(base.index), errors='coerce')
        base['gold'] = pd.to_numeric(gold.reindex(base.index), errors='coerce')
        base['oil'] = pd.to_numeric(oil.reindex(base.index), errors='coerce')
        base[['vix', 'dxy', 'tnx', 'gold', 'oil']] = base[['vix', 'dxy', 'tnx', 'gold', 'oil']].ffill()
        base = base.dropna(subset=['vix', 'dxy', 'tnx', 'gold', 'oil'], how='all')

        if base.empty:
            return 0

        base['gold_oil_ratio'] = np.where(base['oil'].abs() > 1e-8, base['gold'] / base['oil'], 0.0)
        base['risk_off_proxy'] = np.clip((base['vix'].fillna(20.0) - 20.0) / 15.0, -1.0, 1.0)
        dollar_part = np.clip((base['dxy'].fillna(103.0) - 103.0) / 8.0, -1.0, 1.0)
        rate_part = np.clip((base['tnx'].fillna(4.5) - 4.5) / 2.0, -1.0, 1.0)
        base['dollar_proxy'] = np.clip(0.7 * dollar_part + 0.3 * rate_part, -1.0, 1.0)

        out = base.reset_index()
        if 'trade_date' not in out.columns:
            first_col = out.columns[0]
            out = out.rename(columns={first_col: 'trade_date'})
        out['trade_date'] = pd.to_datetime(out['trade_date'], errors='coerce')
        out = out.dropna(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
        out['trade_date'] = out['trade_date'].dt.strftime('%Y-%m-%d')
        out = out[['trade_date', 'vix', 'dxy', 'tnx', 'gold_oil_ratio', 'risk_off_proxy', 'dollar_proxy']]
        out.to_csv(os.path.join(self._get_data_dir(), 'cross_asset_daily.csv'), index=False)
        return int(len(out))

    def export_macro_feature_csvs(self):
        macro = self.get_all_macro_data() or {}
        counts = {}

        cpi = macro.get('cpi') or {}
        cpi_month = str(cpi.get('month') or cpi.get('update_date') or '')
        if cpi_month:
            counts['macro_cpi.csv'] = self._upsert_csv_row('macro_cpi.csv', {
                'month': cpi_month,
                'cpi': cpi.get('value'),
                'update_date': cpi.get('update_date') or cpi_month,
                'data_source': cpi.get('data_source') or cpi.get('source') or 'MacroCollector',
            }, 'month')

        pmi = macro.get('pmi') or {}
        pmi_month = str(pmi.get('release_date') or pmi.get('month') or '')
        if pmi_month:
            counts['macro_pmi.csv'] = self._upsert_csv_row('macro_pmi.csv', {
                'month': pmi_month,
                'manufacturing_pmi': pmi.get('manufacturing_pmi'),
                'non_manufacturing_pmi': pmi.get('non_manufacturing_pmi'),
                'composite_pmi': pmi.get('composite_pmi'),
                'data_source': pmi.get('data_source') or pmi.get('source') or 'MacroCollector',
            }, 'month')

        shibor = macro.get('shibor') or {}
        shibor_date = str(shibor.get('date') or '')
        if shibor_date:
            counts['macro_shibor.csv'] = self._upsert_csv_row('macro_shibor.csv', {
                'date': shibor_date,
                'on': shibor.get('overnight'),
                '1w': shibor.get('1w'),
                '2w': shibor.get('2w'),
                '1m': shibor.get('1m'),
                '3m': shibor.get('3m'),
                '6m': shibor.get('6m'),
                '9m': shibor.get('9m'),
                '1y': shibor.get('1y'),
            }, 'date')

        counts['cross_asset_daily.csv'] = self._refresh_cross_asset_daily_csv()
        return {'updated_files': list(counts.keys()), 'row_counts': counts}
        
    # ==================== 宏观经济数据 ====================
    
    def get_historical_macro(self, start_date, end_date):
        """
        获取历史宏观经济数据 (改进版: 使用真实API而非随机数)
        
        Returns:
            DataFrame: 每日宏观指标
        """
        import pandas as pd
        import tushare as ts
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        macro_data = []
        
        # 获取历史CPI和PMI数据
        try:
            pro = self._get_tushare_pro()
            
            # 获取月度CPI数据
            cpi_monthly = pro.cn_cpi(fields='month,cpi')
            cpi_dict = {}
            if cpi_monthly is not None and not cpi_monthly.empty:
                for _, row in cpi_monthly.iterrows():
                    month_key = str(row['month'])[:7]  # YYYY-MM
                    cpi_dict[month_key] = float(row['cpi'])
            
            # 获取月度PMI数据
            pmi_monthly = pro.cn_pmi(fields='month,manufacture')
            pmi_dict = {}
            if pmi_monthly is not None and not pmi_monthly.empty:
                for _, row in pmi_monthly.iterrows():
                    month_key = str(row['month'])[:7]
                    pmi_dict[month_key] = float(row['manufacture'])
        except Exception as e:
            logger.warning(f"获取历史宏观数据失败: {e}")
            cpi_dict = {}
            pmi_dict = {}
        
        for date in dates:
            month_key = date.strftime('%Y-%m')
            
            macro_data.append({
                'date': date,
                'cpi': cpi_dict.get(month_key),  # 使用真实数据，无数据则为None
                'pmi': pmi_dict.get(month_key),  # 使用真实数据，无数据则为None
                'rate_10y': self.get_china_10y_yield()
            })
        
        df = pd.DataFrame(macro_data)
        df.set_index('date', inplace=True)
        return df
    
    def _get_cpi_for_month(self, month):
        """
        获取指定月份CPI (改进版: 使用真实API)
        
        Args:
            month: YYYY-MM 格式的月份字符串
        """
        import tushare as ts
        
        try:
            pro = self._get_tushare_pro()
            cpi_data = pro.cn_cpi(fields='month,cpi')
            
            if cpi_data is not None and not cpi_data.empty:
                target_month = cpi_data[cpi_data['month'].astype(str).str.startswith(month)]
                if not target_month.empty:
                    return float(target_month.iloc[0]['cpi'])
        except Exception as e:
            logger.debug(f"获取CPI for {month} 失败: {e}")
        
        return None  # 不使用随机数，返回None表示无数据
    
    def _get_pmi_for_month(self, month):
        """
        获取指定月份PMI (改进版: 使用真实API)
        
        Args:
            month: YYYY-MM 格式的月份字符串
        """
        import tushare as ts
        
        try:
            pro = self._get_tushare_pro()
            pmi_data = pro.cn_pmi(fields='month,manufacture')
            
            if pmi_data is not None and not pmi_data.empty:
                target_month = pmi_data[pmi_data['month'].astype(str).str.startswith(month)]
                if not target_month.empty:
                    return float(target_month.iloc[0]['manufacture'])
        except Exception as e:
            logger.debug(f"获取PMI for {month} 失败: {e}")
        
        return None  # 不使用随机数，返回None表示无数据


# 测试代码
if __name__ == '__main__':
    collector = MacroCollector()
    data = collector.get_all_macro_data()
    print("宏观数据:")
    for key, value in data.items():
        print(f"  {key}: {value}")