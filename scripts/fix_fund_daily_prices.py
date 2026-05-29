# -*- coding: utf-8 -*-
"""
修复 daily_prices 表中 active_fund 价格混用单位净值/累计净值的脏数据。

问题：api/backfill.py 历史版本写 close=unit_nav，而 backfill_recommendation_history.py
     写 close=accum_nav，导致同一只基金在不同日期 close 数值尺度完全不同。

修复逻辑：
1. 从 raw_fund_data 表中取 accumulated_nav（累计净值）
2. 用 accumulated_nav 更新 daily_prices.close/open/high/low（若 accumulated_nav 缺失则用 unit_nav）
3. 只处理 market='FUND' 的行
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import RawFundData, DailyPrice

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       'data', 'database', 'finance.db')
engine  = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)


def fix_fund_prices():
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()

        # Step1: 建临时表存放 (code去.后缀, date, 正确价格)
        print("Step1: 建临时表...")
        cur.execute("DROP TABLE IF EXISTS _fund_price_fix")
        cur.execute("""
            CREATE TEMP TABLE _fund_price_fix AS
            SELECT
                SUBSTR(code, 1, INSTR(code||'.', '.')-1) AS base_code,
                date,
                -- 累计净值优先；但若 accum_nav/unit_nav > 10（数据异常），回退用 unit_nav
                CASE
                    WHEN accumulated_nav > 0 AND nav > 0 AND accumulated_nav / nav <= 10
                        THEN accumulated_nav
                    ELSE nav
                END AS correct_price
            FROM raw_fund_data
            -- 必须有有效的 unit_nav（nav>0），排除 nav=0 导致 accum_nav 被错误使用的行
            WHERE nav > 0
        """)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM _fund_price_fix")
        n = cur.fetchone()[0]
        print(f"  临时表行数: {n}")

        # Step2: 批量更新 daily_prices，只更新偏差 > 5% 的行
        print("Step2: 批量更新 daily_prices ...")
        cur.execute("""
            UPDATE daily_prices
            SET close = f.correct_price,
                open  = f.correct_price,
                high  = f.correct_price,
                low   = f.correct_price
            FROM _fund_price_fix f
            WHERE daily_prices.code   = f.base_code
              AND daily_prices.date   = f.date
              AND daily_prices.market = 'FUND'
              AND ABS(daily_prices.close - f.correct_price) / f.correct_price > 0.05
              AND f.correct_price > 0
              AND f.correct_price < 20
              AND f.correct_price / daily_prices.close <= 5
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  更新行数: {updated}")

        # 验证 000043
        cur.execute("""
            SELECT date, close FROM daily_prices
            WHERE code='000043' AND market='FUND'
            ORDER BY date LIMIT 10
        """)
        print()
        print("  验证 000043 修复后价格（前10行）:")
        for row in cur.fetchall():
            print(f"    {row[0]}  close={row[1]:.4f}")

        # 额外验证：是否还有价格突变（单日跌幅 > 50%）
        cur.execute("""
            SELECT a.code, a.date, a.close, b.close AS prev_close,
                   ABS(a.close - b.close) / b.close AS chg
            FROM daily_prices a
            JOIN daily_prices b
              ON a.code = b.code AND a.market = b.market
             AND b.date = (
                 SELECT MAX(date) FROM daily_prices
                 WHERE code=a.code AND market='FUND' AND date < a.date
             )
            WHERE a.market = 'FUND'
              AND ABS(a.close - b.close) / b.close > 0.5
            ORDER BY chg DESC
            LIMIT 10
        """)
        anomalies = cur.fetchall()
        if anomalies:
            print()
            print(f"  [!] 仍有 {len(anomalies)} 处价格突变 (>50%):")
            for row in anomalies:
                print(f"    {row[0]} {row[1]} close={row[2]:.4f} prev={row[3]:.4f} chg={row[4]*100:.1f}%")
        else:
            print()
            print("  [OK] 无价格突变异常")

    finally:
        conn.close()


if __name__ == '__main__':
    fix_fund_prices()
