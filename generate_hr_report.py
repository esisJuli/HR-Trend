"""
generate_hr_report.py
수집된 JSON 데이터를 HTML 리포트로 생성

사용법:
  python generate_hr_report.py           # 오늘 날짜 데이터
  python generate_hr_report.py 2026-04-10  # 특정 날짜
"""

import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import json as _json

BASE_DIR = Path(__file__).parent
OUTPUT_DATA_DIR = BASE_DIR / "output" / "data"
OUTPUT_REPORT_DIR = BASE_DIR / "output" / "reports"
TEMPLATE_FILE = BASE_DIR / "hr_report_template.html"
OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)


WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]

def format_number(n: int) -> str:
    return f"{n:,}"

def get_week_label(report_date_str: str) -> str:
    """생성 일자 기준 이번 주 일요일~생성일 범위 한국어 표기"""
    today = datetime.strptime(report_date_str, "%Y-%m-%d")
    days_since_sunday = (today.weekday() + 1) % 7
    week_start = today - timedelta(days=days_since_sunday)
    start_str = f"{week_start.year}. {week_start.month}. {week_start.day}.({WEEKDAY_KO[week_start.weekday()]})"
    end_str = f"{today.year}. {today.month}. {today.day}.({WEEKDAY_KO[today.weekday()]})"
    return f"{start_str} ~ {end_str}"


def load_data(date_str: str = None) -> dict:
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    data_file = OUTPUT_DATA_DIR / f"{date_str}.json"
    if not data_file.exists():
        raise FileNotFoundError(
            f"데이터 파일이 없습니다: {data_file}\n"
            f"먼저 python hr_collector.py 를 실행해주세요."
        )
    return json.loads(data_file.read_text(encoding="utf-8"))


def build_search_keywords_html(data: dict) -> str:
    keywords = data.get("summary", {}).get("search_keywords", [])
    return " ".join(f'<span class="kw-tag hot">{kw}</span>' for kw in keywords)


def build_pie_chart_data(data: dict) -> str:
    trends = data.get("keyword_trend", [])
    if not trends:
        return _json.dumps({"labels": [], "values": [], "total": 0})
    labels = [t["keyword"] for t in trends]
    values = [t["count"] for t in trends]
    total = sum(values)
    return _json.dumps({"labels": labels, "values": values, "total": total}, ensure_ascii=False)


def build_keyword_trend_html(data: dict) -> str:
    trends = data.get("keyword_trend", [])
    if not trends:
        return "<p style='color:#aaa'>데이터 없음</p>"
    max_count = max((t["count"] for t in trends), default=1)
    rows = []
    for t in trends:
        kw = t["keyword"]
        count = t["count"]
        bar_width = int((count / max(max_count, 1)) * 220)
        rows.append(f"""
        <div class="trend-row">
          <span class="trend-label">{kw}</span>
          <div class="trend-bar-wrap">
            <div class="trend-bar" style="width:{bar_width}px;"></div>
          </div>
          <span class="trend-count">{f"{count//1000}k" if count >= 1000 else f"{count}건"}</span>
        </div>""")
    return "\n".join(rows)


def make_video_card(v: dict, rank: int) -> str:
    rank_classes = {1: "top1", 2: "top2", 3: "top3"}
    rank_cls = rank_classes.get(rank, "")
    pub = v.get("published_at", "")[:10]
    duration = v.get("duration_sec", 0)
    duration_str = f"{duration}초" if duration <= 60 else f"{duration//60}분 {duration%60}초"
    return f"""<div class="video-card">
          <div class="video-rank {rank_cls}">{rank}</div>
          <div class="video-info">
            <div class="video-channel">{v['channel_name']}</div>
            <div class="video-title"><a href="{v['url']}" target="_blank">{v['title']}</a></div>
            <div class="video-stats">
              <span>👁 {format_number(v['view_count'])}</span>
              <span>👍 {format_number(v['like_count'])}</span>
              <span>⏱ {duration_str}</span>
              <span>📅 {pub}</span>
            </div>
          </div>
        </div>"""


def build_video_grid_html(regular: list, shorts: list) -> str:
    """일반/쇼츠를 한 그리드에 행 단위로 나란히 배치 — 높이 자동 동일"""
    max_len = max(len(regular), len(shorts), 1)
    rows = []
    for i in range(max_len):
        left = make_video_card(regular[i], i + 1) if i < len(regular) else "<div class='video-card empty'></div>"
        right = make_video_card(shorts[i], i + 1) if i < len(shorts) else "<div class='video-card empty'></div>"
        rows.append(f"{left}\n{right}")
    return "\n".join(rows)


def build_news_html(data: dict) -> str:
    articles = data.get("news_data", [])[:10]
    if not articles:
        return "<p style='color:#aaa; text-align:center; padding:20px;'>수집된 뉴스가 없습니다</p>"

    html_parts = []
    for article in articles:
        pub = article.get("published_at", "")[:10]
        html_parts.append(f"""
        <a href="{article['url']}" target="_blank" style="text-decoration:none; color:inherit; display:block;">
          <div class="news-card">
            <div class="news-meta">
              <span class="news-category">{article.get('category', '')}</span>
              <span class="news-source">{article.get('source', '')}</span>
              <span class="news-date">{pub}</span>
            </div>
            <div class="news-title">{article['title']}</div>
            <div class="news-summary">{article.get('summary', '')}</div>
          </div>
        </a>""")
    return "\n".join(html_parts)


def render_html(data: dict) -> str:
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    summary = data.get("summary", {})
    report_date = data.get("report_date", datetime.now().strftime("%Y-%m-%d"))

    replacements = {
        "{{ report_date }}": report_date,
        "{{ week_label }}": get_week_label(report_date),
        "{{ total_regular }}": str(summary.get("total_regular", 0)),
        "{{ total_shorts }}": str(summary.get("total_shorts", 0)),
        "{{ total_news }}": str(summary.get("total_news_articles", 0)),
        "{{ search_keywords_html }}": build_search_keywords_html(data),
        "{{ keyword_trend_html }}": build_keyword_trend_html(data),
        "{{ pie_chart_data }}": build_pie_chart_data(data),
        "{{ video_grid_html }}": build_video_grid_html(data.get("youtube_regular", []), data.get("youtube_shorts", [])),
        "{{ news_html }}": build_news_html(data),
        "{{ generated_at }}": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    html = template
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    return html


def main():
    parser = argparse.ArgumentParser(description="HR 인사노무 트렌드 HTML 리포트 생성")
    parser.add_argument("date", nargs="?", default=None)
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    print(f"HR 리포트 생성 중: {date_str}")

    data = load_data(date_str)
    html = render_html(data)

    html_path = OUTPUT_REPORT_DIR / f"hr-report-{date_str}.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"완료! → {html_path}")
    return str(html_path)


if __name__ == "__main__":
    main()
