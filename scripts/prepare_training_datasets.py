#!/usr/bin/env python3
"""
训练数据准备脚本

用途：
1. 检查并补齐基金 / ETF / 黄金 / 白银训练所需数据
2. 必要时调用现有采集器补采到数据库
3. 导出训练脚本需要的标准 CSV 文件
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils import get_logger
from models import get_session, RawStockData, RawFundData

logger = get_logger(__name__)
DATA_DIR = PROJECT_ROOT / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

ETF_CODES = {
    '510300.SH', '510500.SH', '510050.SH', '159915.SZ', '588000.SH',
    '512880.SH', '512690.SH', '515030.SH', '512010.SH', '518880.SH',
}
GOLD_CODES = {'GC=F', 'XAUUSD=X', 'GLD', 'IAU', 'GLDM', 'SGOL', '518880.SH', '518800.SH', '159934.SZ'}
SILVER_CODES = {'SI=F', 'XAGUSD=X', 'SLV', 'SIVR', 'PSLV'}


def _dedupe_asset_pool(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in items or []:
        code = str((item or {}).get('code') or '').strip()
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append({
            'code': code,
            'name': str((item or {}).get('name') or code).strip(),
            'type': str((item or {}).get('type') or '').strip() or 'active_fund',
        })
    return deduped


def build_dataset_plan() -> List[Dict[str, Any]]:
    return [
        {
            'dataset': 'fund_nav',
            'output_file': 'data/fund_nav.csv',
            'description': '基金净值训练样本',
        },
        {
            'dataset': 'etf_history',
            'output_file': 'data/historical_etf.csv',
            'description': 'ETF历史行情训练样本',
        },
        {
            'dataset': 'precious_metals',
            'output_file': 'data/precious_metals.csv',
            'description': '贵金属历史行情汇总',
        },
        {
            'dataset': 'gold_prices',
            'output_file': 'data/gold_prices.csv',
            'description': '黄金训练样本',
        },
        {
            'dataset': 'silver_prices',
            'output_file': 'data/silver_prices.csv',
            'description': '白银训练样本',
        },
        {
            'dataset': 'a_stock_history',
            'output_file': 'data/historical_a_stock.csv',
            'description': 'A股历史行情导出',
        },
    ]


def _run_missing_collectors() -> None:
    """仅在缺失时补采 ETF / 贵金属 / 基金训练数据。"""
    session = get_session()
    try:
        stock_rows = session.query(RawStockData.code).all()
        stock_codes = {str(row[0]).strip() for row in stock_rows if row and row[0]}
        fund_count = session.query(RawFundData.id).count()
    finally:
        session.close()

    missing_etf = not bool(stock_codes & ETF_CODES)
    missing_metal = not bool(stock_codes & (GOLD_CODES | SILVER_CODES))
    missing_fund = fund_count < 500

    expanded_fund_pool: List[Dict[str, Any]] = []
    try:
        if missing_etf or missing_fund:
            from collectors.fund_collector import FundCollector
            fund_collector = FundCollector()
            expanded_fund_pool = _dedupe_asset_pool(fund_collector.fetch_all_funds() or fund_collector.fund_pool or [])
        else:
            fund_collector = None
    except Exception as e:
        fund_collector = None
        logger.warning(f'加载全量基金池失败，将回退到现有默认池: {e}')

    try:
        if missing_etf or missing_metal:
            from collectors.stock_collector import StockCollector
            collector = StockCollector()

            if missing_etf:
                etf_funds: List[Dict[str, Any]] = []
                try:
                    from recommenders.etf_recommender import ETFRecommender
                    etf_funds.extend([
                        {'code': str(item.get('code') or '').strip(), 'name': item.get('name'), 'type': 'etf'}
                        for item in (ETFRecommender().etf_pool or [])
                    ])
                except Exception:
                    pass

                etf_funds.extend([
                    item for item in expanded_fund_pool
                    if 'ETF' in str(item.get('name') or '').upper() or str(item.get('type') or '').lower() == 'etf'
                ])
                etf_funds = _dedupe_asset_pool(etf_funds)

                if not etf_funds:
                    etf_funds = _dedupe_asset_pool([
                        {'code': '510300.SH', 'name': '沪深300ETF', 'type': 'etf'},
                        {'code': '510500.SH', 'name': '中证500ETF', 'type': 'etf'},
                        {'code': '510050.SH', 'name': '上证50ETF', 'type': 'etf'},
                        {'code': '159915.SZ', 'name': '创业板ETF', 'type': 'etf'},
                        {'code': '588000.SH', 'name': '科创50ETF', 'type': 'etf'},
                        {'code': '512880.SH', 'name': '证券ETF', 'type': 'etf'},
                        {'code': '512690.SH', 'name': '酒ETF', 'type': 'etf'},
                        {'code': '515030.SH', 'name': '新能源车ETF', 'type': 'etf'},
                        {'code': '512010.SH', 'name': '医药ETF', 'type': 'etf'},
                        {'code': '518880.SH', 'name': '黄金ETF', 'type': 'etf'},
                    ])

                logger.info(f'开始补采 ETF 历史样本，共 {len(etf_funds)} 只...')
                collector.collect_funds_batch(funds=etf_funds, years=3, limit=None)
            else:
                logger.info('ETF 历史样本已存在，跳过补采')

            if missing_metal:
                logger.info('开始补采 黄金/白银 历史样本...')
                collector.collect_precious_metals(years=3)
            else:
                logger.info('黄金/白银历史样本已存在，跳过补采')
    except Exception as e:
        logger.warning(f'补采 ETF/贵金属数据失败: {e}')

    try:
        if missing_fund:
            from collectors.fund_collector import FundCollector
            if fund_collector is None:
                fund_collector = FundCollector()
                expanded_fund_pool = _dedupe_asset_pool(fund_collector.fetch_all_funds() or fund_collector.fund_pool or [])

            target_funds = [
                item for item in expanded_fund_pool
                if str(item.get('type') or '').lower() != 'etf'
            ] or _dedupe_asset_pool(fund_collector.fund_pool or [])

            logger.info(f'开始补采基金净值样本，共 {len(target_funds)} 只...')
            for fund in target_funds:
                fund_collector.collect_fund_nav(fund.get('code'), days=365 * 3)
        else:
            logger.info('基金净值样本已充足，跳过补采')
    except Exception as e:
        logger.warning(f'补采基金净值失败: {e}')


def _query_raw_stock_df(session, market: Optional[str] = None) -> pd.DataFrame:
    query = session.query(
        RawStockData.code,
        RawStockData.name,
        RawStockData.date,
        RawStockData.open,
        RawStockData.high,
        RawStockData.low,
        RawStockData.close,
        RawStockData.volume,
        RawStockData.market,
    )
    if market:
        query = query.filter(RawStockData.market == market)
    rows = query.all()
    if not rows:
        return pd.DataFrame(columns=['code', 'name', 'date', 'open', 'high', 'low', 'close', 'volume', 'market'])
    df = pd.DataFrame(rows, columns=['code', 'name', 'date', 'open', 'high', 'low', 'close', 'volume', 'market'])
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'])


def _query_fund_df(session) -> pd.DataFrame:
    rows = session.query(
        RawFundData.code,
        RawFundData.name,
        RawFundData.date,
        RawFundData.nav,
        RawFundData.accumulated_nav,
        RawFundData.daily_return,
    ).all()
    if not rows:
        return pd.DataFrame(columns=['code', 'name', 'date', 'nav', 'accumulated_nav', 'daily_return'])
    df = pd.DataFrame(rows, columns=['code', 'name', 'date', 'nav', 'accumulated_nav', 'daily_return'])
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'])


def _export_csv(df: pd.DataFrame, relative_path: str) -> int:
    output_path = PROJECT_ROOT / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f'已导出 {relative_path}: {len(df):,} 条')
    return int(len(df))


def _pick_primary_code(df: pd.DataFrame, preferred_codes: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    codes = {str(code).upper() for code in df['code'].dropna().astype(str)}
    for candidate in preferred_codes:
        if candidate.upper() in codes:
            return df[df['code'].astype(str).str.upper() == candidate.upper()].copy()
    first_code = str(df['code'].dropna().astype(str).iloc[0]).upper()
    return df[df['code'].astype(str).str.upper() == first_code].copy()


def prepare_training_datasets(collect_missing: bool = True) -> Dict[str, int]:
    if collect_missing:
        _run_missing_collectors()

    session = get_session()
    try:
        stock_df = _query_raw_stock_df(session)
        a_df = stock_df[stock_df['market'].astype(str).str.upper() == 'A'].copy() if not stock_df.empty else pd.DataFrame()
        fund_df = _query_fund_df(session)

        etf_df = stock_df[
            stock_df['code'].astype(str).isin(ETF_CODES)
            | stock_df['name'].astype(str).str.contains('ETF', case=False, na=False)
        ].copy() if not stock_df.empty else pd.DataFrame()

        metal_df = stock_df[
            stock_df['code'].astype(str).isin(GOLD_CODES | SILVER_CODES)
        ].copy() if not stock_df.empty else pd.DataFrame()
        gold_all_df = metal_df[metal_df['code'].astype(str).isin(GOLD_CODES)].copy() if not metal_df.empty else pd.DataFrame()
        silver_all_df = metal_df[metal_df['code'].astype(str).isin(SILVER_CODES)].copy() if not metal_df.empty else pd.DataFrame()
        gold_df = _pick_primary_code(gold_all_df, ['GC=F', 'XAUUSD=X', 'GLD', 'IAU', '518880.SH'])
        silver_df = _pick_primary_code(silver_all_df, ['SI=F', 'XAGUSD=X', 'SLV', 'SIVR'])

        results = {
            'a_stock_history': _export_csv(a_df, 'data/historical_a_stock.csv') if not a_df.empty else 0,
            'fund_nav': _export_csv(fund_df, 'data/fund_nav.csv') if not fund_df.empty else 0,
            'etf_history': _export_csv(etf_df, 'data/historical_etf.csv') if not etf_df.empty else 0,
            'precious_metals': _export_csv(metal_df, 'data/precious_metals.csv') if not metal_df.empty else 0,
            'gold_prices': _export_csv(gold_df, 'data/gold_prices.csv') if not gold_df.empty else 0,
            'silver_prices': _export_csv(silver_df, 'data/silver_prices.csv') if not silver_df.empty else 0,
        }
        return results
    finally:
        session.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='准备训练所需数据集')
    parser.add_argument('--no-collect', action='store_true', help='仅导出已有数据库数据，不触发补采')
    args = parser.parse_args(argv)

    print('=' * 72)
    print('训练数据准备')
    print('=' * 72)
    print(f'开始时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    results = prepare_training_datasets(collect_missing=not bool(args.no_collect))
    for key, value in results.items():
        print(f'  {key}: {value:,} 条')

    success = any(value > 0 for value in results.values())
    print('=' * 72)
    print('✅ 数据准备完成' if success else '⚠️ 未生成有效数据，请检查采集源')
    print('=' * 72)
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
