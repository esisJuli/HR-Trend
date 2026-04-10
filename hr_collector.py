"""
hr_collector.py
HR 인사노무 트렌드 데이터 수집 스크립트

실행: python hr_collector.py
결과: output/data/YYYY-MM-DD.json
"""

import os
import json
import time
import logging
import feedparser
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from googleapiclient.discovery import build

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
NEWS_SOURCES_FILE = BASE_DIR / "news_sources.json"
MEDIA_CHANNELS_FILE = BASE_DIR / "media_channels.json"
OUTPUT_DIR = BASE_DIR / "output" / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_media_filter():
    """언론사/뉴스 채널 필터 로드"""
    data = json.loads(MEDIA_CHANNELS_FILE.read_text(encoding="utf-8"))
    channel_ids = {ch["id"] for ch in data["channels"]}
    name_keywords = data["channel_name_keywords"]
    return channel_ids, name_keywords


def is_media_channel(channel_id: str, channel_name: str, media_ids: set, name_keywords: list) -> bool:
    """언론사/뉴스 채널인지 판별"""
    if channel_id in media_ids:
        return True
    return any(kw.lower() in channel_name.lower() for kw in name_keywords)

# HR 검색 키워드
HR_KEYWORDS = [
    # 노무/법률 기본
    "인사노무", "노무사", "노동법", "근로기준법", "노동관계법",
    # 근로 조건
    "최저임금", "연장근로", "주52시간", "휴일근로", "연차휴가", "퇴직금",
    # 고용/계약
    "해고", "권고사직", "계약직", "프리랜서 노동", "비정규직",
    # HR 실무
    "HR 트렌드", "인사관리", "채용 트렌드", "성과평가", "조직문화",
    # 사회보험
    "4대보험", "산재보험", "고용보험",
]


# ─────────────────────────────────────────
# 1. YouTube 키워드 검색으로 인기 영상 수집
# ─────────────────────────────────────────

def get_youtube_client():
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("YouTube API 키가 설정되지 않았습니다.")
    return build("youtube", "v3", developerKey=api_key)


def parse_duration_seconds(duration: str) -> int:
    """ISO 8601 duration(PT1M30S) → 초 단위 변환"""
    import re
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s


def collect_youtube_data() -> dict:
    """HR 키워드로 유튜브 전체 검색 — 언론사/뉴스 채널만 필터 후 일반영상 TOP5 / 쇼츠 TOP5 반환"""
    youtube = get_youtube_client()
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    all_videos = {}
    media_ids, name_keywords = load_media_filter()

    for keyword in HR_KEYWORDS:
        log.info(f"유튜브 검색 중: '{keyword}'")
        try:
            search_resp = youtube.search().list(
                part="id,snippet",
                q=keyword,
                publishedAfter=since,
                order="viewCount",
                type="video",
                regionCode="KR",
                relevanceLanguage="ko",
                maxResults=10,
            ).execute()

            video_ids = [item["id"]["videoId"] for item in search_resp.get("items", [])]
            if not video_ids:
                continue

            # contentDetails 추가로 영상 길이 수집
            stats_resp = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids),
            ).execute()

            for item in stats_resp.get("items", []):
                vid_id = item["id"]
                if vid_id in all_videos:
                    continue
                channel_id = item["snippet"]["channelId"]
                channel_name = item["snippet"]["channelTitle"]

                # 언론사/뉴스 채널 아니면 제외
                if not is_media_channel(channel_id, channel_name, media_ids, name_keywords):
                    log.info(f"    제외 (비언론사): {channel_name} — {item['snippet']['title'][:30]}")
                    continue

                stats = item.get("statistics", {})
                duration_str = item.get("contentDetails", {}).get("duration", "PT0S")
                duration_sec = parse_duration_seconds(duration_str)
                is_shorts = duration_sec <= 60

                all_videos[vid_id] = {
                    "id": vid_id,
                    "title": item["snippet"]["title"],
                    "channel_name": channel_name,
                    "published_at": item["snippet"]["publishedAt"],
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "duration_sec": duration_sec,
                    "is_shorts": is_shorts,
                    "search_keyword": keyword,
                }

            time.sleep(0.3)

        except Exception as e:
            log.warning(f"유튜브 검색 실패 ({keyword}): {e}")

    # 조회수 + 좋아요 가중치 점수 계산
    for v in all_videos.values():
        v["score"] = v["view_count"] + v["like_count"] * 10

    # 일반영상 / 쇼츠 분리 후 각 TOP 5
    regular = sorted(
        [v for v in all_videos.values() if not v["is_shorts"]],
        key=lambda v: v["score"], reverse=True
    )[:5]
    shorts = sorted(
        [v for v in all_videos.values() if v["is_shorts"]],
        key=lambda v: v["score"], reverse=True
    )[:5]

    log.info(f"유튜브 수집 완료 — 일반영상: {len(regular)}개, 쇼츠: {len(shorts)}개")
    return {"regular": regular, "shorts": shorts}


# ─────────────────────────────────────────
# 2. HR 뉴스 RSS 수집
# ─────────────────────────────────────────

def get_week_range() -> tuple:
    """이번 주 일요일 00:00 ~ 현재 시각 범위 반환"""
    now = datetime.now(timezone.utc)
    # 파이썬 weekday: 월=0 ... 토=5, 일=6
    # 일요일로부터 며칠 지났는지: 일=0, 월=1, 화=2, 수=3, 목=4, 금=5, 토=6
    days_since_sunday = (now.weekday() + 1) % 7
    week_start = (now - timedelta(days=days_since_sunday)).replace(hour=0, minute=0, second=0, microsecond=0)
    return week_start, now


def collect_news_rss() -> list:
    log.info("HR 뉴스 RSS 수집 중...")
    sources = json.loads(NEWS_SOURCES_FILE.read_text(encoding="utf-8"))["rss_feeds"]
    all_articles = []
    week_start, week_end = get_week_range()
    log.info(f"  수집 기간: {week_start.strftime('%Y-%m-%d')} ~ {week_end.strftime('%Y-%m-%d')}")

    for source in sources:
        try:
            log.info(f"  뉴스 수집: {source['name']}")
            feed = feedparser.parse(source["url"])
            articles = []

            for entry in feed.entries[:50]:
                published = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

                # 이번 주 범위 필터
                if published and not (week_start <= published < week_end):
                    continue

                articles.append({
                    "title": entry.get("title", ""),
                    "url": entry.get("link", ""),
                    "summary": entry.get("summary", "")[:200],
                    "published_at": published.isoformat() if published else "",
                    "source": source["name"],
                    "category": source["category"],
                })

            all_articles.extend(articles)
            log.info(f"  → {len(articles)}개 기사 수집")

        except Exception as e:
            log.warning(f"RSS 수집 실패 ({source['name']}): {e}")

    # 최신순 정렬 후 최대 10개
    all_articles.sort(key=lambda x: x["published_at"], reverse=True)
    return all_articles[:10]


# ─────────────────────────────────────────
# 3. 네이버 뉴스 검색
# ─────────────────────────────────────────

NAVER_KEYWORDS = [
    "인사노무", "노동법", "근로기준법", "최저임금", "주52시간",
    "퇴직금", "해고", "연차휴가", "4대보험", "노무사",
]

# 허용 언론사 도메인
TARGET_SOURCES = ["labortoday.co.kr", "worklaw.co.kr", "lawtimes.co.kr"]

SOURCE_NAME_MAP = {
    "labortoday.co.kr": "매일노동뉴스",
    "worklaw.co.kr": "노동법률",
    "lawtimes.co.kr": "법률신문",
}

def get_source_name(url: str) -> str:
    for domain, name in SOURCE_NAME_MAP.items():
        if domain in url:
            return name
    return ""

def collect_naver_keyword_trend() -> list:
    """키워드별 이번 주 기사 수 집계 — 뉴스 빈도 트렌드"""
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    if not client_id or not client_secret:
        return []

    log.info("네이버 뉴스 키워드 빈도 수집 중...")
    week_start, week_end = get_week_range()
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }

    from email.utils import parsedate_to_datetime
    keyword_counts = []

    for keyword in NAVER_KEYWORDS:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                headers=headers,
                params={"query": keyword, "display": 100, "sort": "date"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            total = data.get("total", 0)
            keyword_counts.append({"keyword": keyword, "count": total})
            time.sleep(0.1)

        except Exception as e:
            log.warning(f"키워드 빈도 수집 실패 ({keyword}): {e}")

    keyword_counts.sort(key=lambda x: x["count"], reverse=True)
    log.info(f"키워드 빈도 수집 완료: {len(keyword_counts)}개 키워드")
    return keyword_counts


def collect_naver_news() -> list:
    """네이버 뉴스 검색 API — 매일노동뉴스/노동법률/법률신문 기사만 필터"""
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    if not client_id or not client_secret:
        log.warning("네이버 API 키가 없습니다.")
        return []

    log.info("네이버 뉴스 검색 중 (매일노동뉴스/노동법률/법률신문)...")
    week_start, week_end = get_week_range()
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }

    import re
    from email.utils import parsedate_to_datetime

    all_articles = {}
    for keyword in NAVER_KEYWORDS:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                headers=headers,
                params={"query": keyword, "display": 100, "sort": "date"},
                timeout=10,
            )
            if resp.status_code != 200:
                log.warning(f"네이버 API 오류 ({keyword}): {resp.status_code}")
                continue

            items = resp.json().get("items", [])
            for item in items:
                original_url = item.get("originallink", "")
                naver_url = item.get("link", "")

                # 허용 언론사 도메인 필터
                matched = any(domain in original_url for domain in TARGET_SOURCES)
                if not matched:
                    continue

                url = original_url or naver_url
                if url in all_articles:
                    continue

                # 날짜 파싱
                pub_str = item.get("pubDate", "")
                try:
                    published = parsedate_to_datetime(pub_str).astimezone(timezone.utc)
                except Exception:
                    published = None

                # 이번 주 범위 필터
                if published and not (week_start <= published <= week_end):
                    continue

                title = re.sub(r"<[^>]+>", "", item.get("title", ""))
                description = re.sub(r"<[^>]+>", "", item.get("description", ""))
                source_name = get_source_name(original_url)

                all_articles[url] = {
                    "title": title,
                    "url": url,
                    "summary": description[:200],
                    "published_at": published.isoformat() if published else "",
                    "source": source_name,
                    "category": keyword,
                }

            time.sleep(0.1)

        except Exception as e:
            log.warning(f"네이버 뉴스 수집 실패 ({keyword}): {e}")

    articles = sorted(all_articles.values(), key=lambda x: x["published_at"], reverse=True)
    log.info(f"네이버 뉴스 수집 완료: {len(articles)}개 (매일노동뉴스/노동법률/법률신문)")
    return articles


# ─────────────────────────────────────────
# 4. 키워드 분석
# ─────────────────────────────────────────

def extract_keywords(youtube_dict: dict, news_data: list) -> list:
    from collections import Counter
    import re

    stopwords = {
        "이", "그", "저", "을", "를", "이", "가", "은", "는", "에", "의",
        "로", "으로", "와", "과", "도", "만", "에서", "하다", "있다", "되다",
        "하는", "있는", "되는", "위한", "위해", "관련", "대한", "통해", "위해서",
        "the", "and", "for", "with",
    }

    word_counts = Counter()

    all_videos = youtube_dict.get("regular", []) + youtube_dict.get("shorts", [])
    for video in all_videos:
        words = re.findall(r"[가-힣]{2,}|[a-zA-Z]{3,}", video["title"])
        for w in words:
            if w not in stopwords:
                word_counts[w] += 1

    for article in news_data:
        words = re.findall(r"[가-힣]{2,}|[a-zA-Z]{3,}", article["title"])
        for w in words:
            if w not in stopwords:
                word_counts[w] += 1

    return [
        {"keyword": kw, "count": cnt}
        for kw, cnt in word_counts.most_common(20)
    ]


# ─────────────────────────────────────────
# 4. 메인 실행
# ─────────────────────────────────────────

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"=== HR 인사노무 트렌드 수집 시작: {today} ===")

    youtube_data = collect_youtube_data()
    news_data = collect_naver_news()[:10]
    keyword_trend = collect_naver_keyword_trend()

    result = {
        "collected_at": datetime.now().isoformat(),
        "report_date": today,
        "youtube_regular": youtube_data["regular"],
        "youtube_shorts": youtube_data["shorts"],
        "news_data": news_data,
        "keyword_trend": keyword_trend,
        "summary": {
            "total_regular": len(youtube_data["regular"]),
            "total_shorts": len(youtube_data["shorts"]),
            "total_news_articles": len(news_data),
            "search_keywords": HR_KEYWORDS,
        },
    }

    output_path = OUTPUT_DIR / f"{today}.json"
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"=== 수집 완료: {output_path} ===")
    print(f"\n수집 완료!")
    print(f"저장 위치: {output_path}")
    print(f"일반영상: {result['summary']['total_regular']}개")
    print(f"쇼츠: {result['summary']['total_shorts']}개")
    print(f"수집 뉴스: {result['summary']['total_news_articles']}개")

    return result


if __name__ == "__main__":
    main()
