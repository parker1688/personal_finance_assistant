"""
新闻采集模块 - collectors/news_collector.py
只使用Tushare真实数据，不生成模拟数据
集成数据验证框架 - 修复版
"""

import os
import sys
import time
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入数据验证器
try:
    from data.validators import DataValidator, ValidationResult
    HAS_VALIDATOR = True
except ImportError:
    HAS_VALIDATOR = False

# 导入日志
from utils import get_logger
logger = get_logger(__name__)

try:
    import tushare as ts
    from config import TUSHARE_TOKEN, NEWSAPI_KEY, NEWSAPI_ENABLED
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    print("✅ Tushare连接成功")
except Exception as e:
    print(f"❌ Tushare连接失败: {e}")
    pro = None

PROGRESS_DIR = 'data/collect_progress'
os.makedirs(PROGRESS_DIR, exist_ok=True)

OUTPUT_FILE = 'data/news_all.csv'


class NewsCollector:
    """新闻采集器（优先Tushare，必要时补充NewsAPI）"""
    
    def __init__(self):
        self.pro = pro
        self.newsapi_key = NEWSAPI_KEY
        self.newsapi_enabled = bool(NEWSAPI_ENABLED and NEWSAPI_KEY)
        # 初始化数据验证器
        self.validator = None
        if HAS_VALIDATOR:
            try:
                self.validator = DataValidator(
                    rules={
                        'ts_code': {'type': 'string', 'required': False},
                        'pub_date': {'type': 'datetime', 'required': False},
                        'datetime': {'type': 'datetime', 'required': False},
                        'title': {'type': 'string', 'min_length': 1},
                        'content': {'type': 'string'},
                        'sentiment': {'type': 'float', 'min': -1, 'max': 1}
                    }
                )
                logger.info("✅ DataValidator 已初始化")
            except Exception as e:
                logger.warning(f"⚠️ DataValidator 初始化失败: {e}")
        else:
            logger.warning("⚠️ DataValidator 不可用")
    
    def _get_progress_file(self):
        return os.path.join(PROGRESS_DIR, 'news_progress.json')
    
    def _load_progress(self):
        progress_file = self._get_progress_file()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'last_page': 0, 'total_records': 0, 'last_update': None}
    
    def _save_progress(self, progress):
        progress['last_update'] = datetime.now().isoformat()
        with open(self._get_progress_file(), 'w') as f:
            json.dump(progress, f, indent=2)
    
    def simple_sentiment(self, text):
        """简单情感分析（标题+正文启发式），尽量让新闻信号在训练中可用。"""
        if not text:
            return 0.0
        txt = str(text).strip()
        if not txt:
            return 0.0

        positive = [
            '涨', '升', '利好', '增长', '上涨', '突破', '新高', '增持', '买入', '推荐', '反弹', '回暖',
            '走强', '改善', '超预期', '提振', '受益', '乐观', '扩张', '回升', '稳健', '向好'
        ]
        negative = [
            '跌', '降', '利空', '下滑', '下跌', '跌破', '新低', '减持', '卖出', '回避', '暴跌', '风险',
            '承压', '走弱', '亏损', '下修', '恶化', '收缩', '悲观', '波动', '紧张', '冲击'
        ]
        pos = sum(txt.count(w) for w in positive)
        neg = sum(txt.count(w) for w in negative)
        if pos + neg == 0:
            return 0.0
        score = (pos - neg) / (pos + neg)
        return float(max(-1.0, min(1.0, score)))

    def simple_sentiment_analysis(self, text):
        """兼容旧接口：返回[-1, 1]情感得分。"""
        return float(self.simple_sentiment(text))

    def _fetch_newsapi_articles(self, keyword=None, days=1, page_size=50):
        """必要时补充 NewsAPI 新闻，用于舆情与海外资产新闻覆盖。"""
        if not self.newsapi_enabled:
            return []
        try:
            q = str(keyword or 'stock OR market OR economy').strip()
            from_dt = (datetime.utcnow() - timedelta(days=max(int(days), 1))).isoformat(timespec='seconds') + 'Z'
            response = requests.get(
                'https://newsapi.org/v2/everything',
                params={
                    'q': q,
                    'from': from_dt,
                    'language': 'zh,en',
                    'sortBy': 'publishedAt',
                    'pageSize': max(1, min(int(page_size), 100)),
                    'apiKey': self.newsapi_key,
                },
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json() or {}
            articles = payload.get('articles', []) or []
            results = []
            for item in articles:
                title = str(item.get('title') or '').strip()
                content = str(item.get('description') or item.get('content') or '').strip()
                text = f"{title} {content}".strip()
                if not text:
                    continue
                results.append({
                    'title': title,
                    'content': content,
                    'datetime': item.get('publishedAt') or datetime.utcnow().isoformat() + 'Z',
                    'source': 'NewsAPI',
                    'sentiment': self.simple_sentiment(text),
                })
            return results
        except Exception as e:
            logger.warning(f"NewsAPI 获取失败: {e}")
            return []

    def fetch_news_from_api(self, keyword=None, days=1):
        """兼容旧接口：优先读取本地真实新闻缓存，必要时补充 NewsAPI。"""
        try:
            frames = []
            if os.path.exists(OUTPUT_FILE):
                df = pd.read_csv(OUTPUT_FILE)
                if df is not None and not df.empty:
                    if 'datetime' in df.columns:
                        parsed = pd.to_datetime(df['datetime'], errors='coerce')
                        cutoff = pd.Timestamp.now() - pd.Timedelta(days=max(int(days), 1))
                        filtered = df.loc[parsed >= cutoff]
                        if not filtered.empty:
                            df = filtered

                    if keyword:
                        kw = str(keyword).strip()
                        if kw:
                            mask = pd.Series(False, index=df.index)
                            for col in ['title', 'content']:
                                if col in df.columns:
                                    mask = mask | df[col].fillna('').astype(str).str.contains(kw, case=False, na=False)
                            if mask.any():
                                df = df.loc[mask]
                    frames.append(df)

            newsapi_items = self._fetch_newsapi_articles(keyword=keyword, days=days)
            if newsapi_items:
                frames.append(pd.DataFrame(newsapi_items))

            if not frames:
                return []

            merged = pd.concat(frames, ignore_index=True, sort=False)
            merged = self._deduplicate_news_df(merged)
            return merged.head(100).to_dict('records') if merged is not None else []
        except Exception as e:
            logger.warning(f"兼容新闻接口读取失败: {e}")
            return []

    def _deduplicate_news_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """稳健去重：优先标题，其次使用时间+内容，避免 title 为空导致误去重。"""
        if df is None or df.empty:
            return df

        out = df.copy()
        has_title = 'title' in out.columns and out['title'].fillna('').astype(str).str.strip().ne('').any()
        if has_title:
            out['_dedup_key'] = out['title'].fillna('').astype(str).str.strip()
        elif 'datetime' in out.columns and 'content' in out.columns:
            out['_dedup_key'] = (
                out['datetime'].astype(str).fillna('') + '|' +
                out['content'].fillna('').astype(str).str.replace(r'\s+', ' ', regex=True).str.slice(0, 120)
            )
        elif 'content' in out.columns:
            out['_dedup_key'] = out['content'].fillna('').astype(str).str.replace(r'\s+', ' ', regex=True).str.slice(0, 120)
        else:
            return out.drop_duplicates(keep='first')

        out = out[out['_dedup_key'].astype(str).str.strip() != '']
        out = out.drop_duplicates(subset=['_dedup_key'], keep='last')
        return out.drop(columns=['_dedup_key'], errors='ignore')
    
    def validate_and_clean_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """验证和清理采集的数据"""
        if self.validator is None:
            logger.warning("⚠️ 验证器不可用，跳过数据验证")
            return df
        
        try:
            valid_rows = []
            invalid_count = 0
            
            for idx, row in df.iterrows():
                record = row.to_dict()
                result = self.validator.validate(record)
                
                if result['valid']:
                    valid_rows.append(record)
                else:
                    invalid_count += 1
                    logger.debug(f"❌ 第{idx}行数据验证失败: {result['errors']}")
            
            if invalid_count > 0:
                logger.warning(f"⚠️ 发现{invalid_count}条无效数据，已清理")
            
            if valid_rows:
                cleaned_df = pd.DataFrame(valid_rows)
                logger.info(f"✅ 数据验证完成: {len(valid_rows)}/{len(df)} 有效")
                return cleaned_df
            else:
                logger.error("❌ 所有数据都验证失败")
                return None
                
        except Exception as e:
            logger.error(f"❌ 数据验证异常: {e}")
            return df
    
    def collect_historical(self, days=365, resume=True, start_date=None, end_date=None, max_pages=100):
        """
        采集历史新闻（使用分页方式）
        Args:
            days: 采集天数（用于过滤日期范围）
            resume: 是否断点续传
        """
        print("\n" + "=" * 60)
        print("新闻舆情采集（Tushare真实数据 - 分页方式）")
        print(f"采集天数范围: {days} 天")
        print("=" * 60)
        
        if self.pro is None:
            print("❌ Tushare未连接，无法采集新闻")
            return None
        
        # 计算日期范围用于过滤（优先使用显式区间）
        if start_date is not None and end_date is not None:
            start_raw = str(start_date)
            end_raw = str(end_date)
            start_dt = pd.to_datetime(start_raw)
            end_dt = pd.to_datetime(end_raw)

            # 仅给到日期时，按整日范围处理，避免只匹配到00:00:00
            if ':' not in start_raw:
                start_dt = start_dt.normalize()
            if ':' not in end_raw:
                end_dt = end_dt.normalize() + timedelta(days=1) - timedelta(seconds=1)

            # 区间采集必须从首页重拉，避免断点页号跨区间导致漏采
            resume = False
        else:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)

        start_str = start_dt.strftime('%Y-%m-%d')
        end_str = end_dt.strftime('%Y-%m-%d')
        print(f"过滤日期范围: {start_str} ~ {end_str}")
        
        # 加载进度
        progress = self._load_progress() if resume else {'last_page': 0, 'total_records': 0}
        last_page = progress.get('last_page', 0)
        
        # 加载已有数据
        existing_df = None
        existing_dates = set()
        if os.path.exists(OUTPUT_FILE):
            existing_df = pd.read_csv(OUTPUT_FILE)
            print(f"📁 已有数据: {len(existing_df)} 条")
            if 'datetime' in existing_df.columns:
                existing_dates = set(pd.to_datetime(existing_df['datetime']).dt.date)
            elif 'pub_date' in existing_df.columns:
                existing_dates = set(pd.to_datetime(existing_df['pub_date']).dt.date)
        
        new_batches = []
        new_count = 0
        page = last_page + 1
        max_pages = max(1, int(max_pages))
        
        explicit_range = start_date is not None and end_date is not None
        consecutive_out_of_range = 0

        while page <= max_pages:
            print(f"正在采集第 {page} 页...", end=' ', flush=True)
            
            try:
                # 分页获取新闻（不指定日期）
                raw_df = self.pro.news(limit=1000, page=page)
                
                if raw_df is None or len(raw_df) == 0:
                    print("无更多数据")
                    break

                page_raw_count = len(raw_df)
                df = raw_df.copy()
                
                # 处理日期列
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                elif 'pub_date' in df.columns:
                    df['datetime'] = pd.to_datetime(df['pub_date'])

                # 区间模式下快速终止: 连续多页均明显不在目标区间，提前结束
                if explicit_range and 'datetime' in df.columns:
                    valid_dt = df['datetime'].dropna()
                    if not valid_dt.empty:
                        page_min = valid_dt.min().date()
                        page_max = valid_dt.max().date()

                        if page_max < start_dt.date():
                            print("已越过目标区间（更早日期），结束采集")
                            break

                        if page_min > end_dt.date():
                            consecutive_out_of_range += 1
                            if consecutive_out_of_range >= 20:
                                print("连续多页均晚于目标区间，判定源端无该区间数据，结束采集")
                                break
                        else:
                            consecutive_out_of_range = 0
                
                # 过滤时间范围（支持到小时级）
                if 'datetime' in df.columns:
                    df = df[(df['datetime'] >= start_dt) & (df['datetime'] <= end_dt)]
                
                if len(df) == 0:
                    print(f"⚠️ 本页数据不在日期范围内，继续下一页")
                    page += 1
                    time.sleep(0.5)
                    continue
                
                # 稳健去重（避免标题为空时误把不同新闻折叠掉）
                df = self._deduplicate_news_df(df)
                
                # 添加情感分析：优先使用 标题+正文 组合，避免正文为空时信号失效
                if 'content' in df.columns and 'title' in df.columns:
                    df['sentiment_text'] = df['title'].fillna('').astype(str) + ' ' + df['content'].fillna('').astype(str)
                    df['sentiment'] = df['sentiment_text'].apply(self.simple_sentiment)
                    df = df.drop(columns=['sentiment_text'], errors='ignore')
                elif 'content' in df.columns:
                    df['sentiment'] = df['content'].apply(self.simple_sentiment)
                elif 'title' in df.columns:
                    df['sentiment'] = df['title'].apply(self.simple_sentiment)
                else:
                    df['sentiment'] = 0.0
                
                # 先按页校验，坏页直接跳过，避免阻断整次采集
                validated_page = self.validate_and_clean_data(df)
                if validated_page is None or validated_page.empty:
                    print("⚠️ 本页数据验证失败，已跳过")
                    page += 1
                    time.sleep(0.5)
                    continue

                new_batches.append(validated_page)
                new_count += len(validated_page)
                print(f"✅ {len(validated_page)}条")
                
                # 更新进度
                progress['last_page'] = page
                progress['total_records'] = progress.get('total_records', 0) + len(df)
                self._save_progress(progress)
                
                # 每10页保存一次
                if page % 10 == 0:
                    temp_parts = []
                    if existing_df is not None and not existing_df.empty:
                        temp_parts.append(existing_df)
                    if new_batches:
                        temp_parts.extend(new_batches)
                    if temp_parts:
                        temp_df = pd.concat(temp_parts, ignore_index=True)
                        temp_df = self._deduplicate_news_df(temp_df)
                        temp_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
                        print(f"   💾 已保存，累计 {len(temp_df)} 条")
                
                page += 1
                time.sleep(0.5)
                
                # 必须基于源页原始数量判断是否最后一页；不能用过滤后的 df，
                # 否则会因时间范围过滤而过早停止，导致历史新闻覆盖不足。
                if page_raw_count < 1000:
                    print("已采集完最后一页")
                    break
                    
            except Exception as e:
                print(f"❌ 采集失败: {e}")
                break
        
        # 最终保存
        final_parts = []
        if existing_df is not None and not existing_df.empty:
            final_parts.append(existing_df)
        if new_batches:
            final_parts.extend(new_batches)

        if final_parts:
            result = pd.concat(final_parts, ignore_index=True)
            result = self._deduplicate_news_df(result)

            result.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
            print(f"\n✅ 新闻采集完成")
            print(f"   新增: {new_count} 条")
            print(f"   累计: {len(result)} 条")
            print(f"   保存至: {OUTPUT_FILE}")
            return result

        print("❌ 无新闻数据")
        return None

    def collect_recent_hours(self, hours=6, max_pages=20):
        """采集最近N小时新闻（实时增量场景）"""
        safe_hours = max(1, min(int(hours), 72))
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=safe_hours)
        return self.collect_historical(
            start_date=start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_date=end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            resume=False,
            max_pages=max_pages,
        )
    
    def collect_latest(self, days=30):
        """采集最近N天新闻"""
        return self.collect_historical(days=days)


if __name__ == '__main__':
    collector = NewsCollector()
    collector.collect_latest(days=30)