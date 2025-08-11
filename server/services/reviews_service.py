from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, date
import xml.etree.ElementTree as ET
import requests
from typing import Any, Dict, List, Optional, Set, Tuple
import urllib.request
import urllib.parse
import json

import pandas as pd
from google_play_scraper import Sort, reviews, app as gp_app


def count_korean_chars(text: str) -> int:
    return len(re.findall(r"[가-힣]", text or ""))


def count_meaningful_chars_all(text: str) -> int:
    """Count alpha-numeric characters across languages (space/punct excluded)."""
    if not text:
        return 0
    return sum(1 for ch in text if ch.isalnum())


def get_rating_weight(score: int) -> float:
    return {1: 1.0, 2: 0.8, 3: 0.5, 4: 0.2, 5: 0.1}.get(score, 0.0)


def calc_percentile_display(rank: int, total: int) -> float:
    return round((total - rank + 1) / total * 100, 2) if total else 0.0


def compute_threshold_dt(days: int, from_date: Optional[date]) -> Optional[datetime]:
    if from_date is not None:
        return datetime.combine(from_date, datetime.min.time())
    if days and days > 0:
        return datetime.now() - timedelta(days=days)
    return None


def fetch_reviews_iteratively(
    app_id: str,
    desired_count: int,
    threshold_dt: Optional[datetime],
    rating_exact: Optional[int],
    rating_max: Optional[int],
) -> List[Dict[str, Any]]:
    all_reviews: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    token: Optional[Tuple] = None

    # 안전 상한: 필터로 많이 탈락할 수 있어 넉넉히 가져옴
    max_batches = 30  # 최대 30회 호출(대략 6000개 수준)
    batch_size = 200

    for _ in range(max_batches):
        result, token = reviews(
            app_id,
            lang="ko",
            country="kr",
            sort=Sort.NEWEST,
            count=batch_size,
            continuation_token=token,
        )

        if not result:
            break

        for r in result:
            review_id = r.get("reviewId") or r.get("review_id") or ""
            if review_id in seen_ids:
                continue
            seen_ids.add(review_id)

            text = r.get("content", "")
            score = int(r.get("score", 0) or 0)
            thumbs_up = int(r.get("thumbsUpCount", 0) or 0)
            at: datetime = r.get("at")

            # 필터
            if score not in [1, 2, 3, 4, 5]:
                continue
            if rating_exact is not None and score != rating_exact:
                continue
            if rating_exact is None and rating_max is not None and score > max(1, min(5, rating_max)):
                continue
            if not isinstance(at, datetime):
                continue
            if threshold_dt is not None and at < threshold_dt:
                # NEWEST 정렬이므로 임계 이전이면 더 이어갈 가치가 없음
                token = None
                break
            # iOS는 한국어 외 리뷰가 섞여 있어, 의미있는 문자(영숫자) 15자 기준으로 필터
            if count_meaningful_chars_all(text) < 15:
                continue

            weight = get_rating_weight(score)
            priority_score = round(weight * (1 + math.log2(1 + thumbs_up)), 2)

            all_reviews.append(
                {
                    "출처": "Google Play",
                    "날짜_dt": at,
                    "날짜": at.strftime("%Y-%m-%d"),
                    "닉네임": r.get("userName"),
                    "내용": text,
                    "심각도 점수": priority_score,
                    "평점": score,
                    "좋아요": thumbs_up,
                }
            )

        if token is None or len(all_reviews) >= desired_count:
            break

    return all_reviews


def build_reviews_payload(
    app_id: str,
    count: int = 250,
    days: int = 365,
    from_date: Optional[date] = None,
    rating_exact: Optional[int] = None,
    rating_max: Optional[int] = 3,
) -> Dict[str, Any]:
    app_info = gp_app(app_id, lang="ko", country="kr")
    app_name: str = app_info.get("title", "").split("-")[0].strip()

    threshold_dt = compute_threshold_dt(days=days, from_date=from_date)

    raw_rows = fetch_reviews_iteratively(
        app_id=app_id,
        desired_count=max(count, 500),  # 넉넉히 확보 후 절단
        threshold_dt=threshold_dt,
        rating_exact=rating_exact,
        rating_max=rating_max,
    )

    # 서비스명 할당 및 상위 count로 절단
    for row in raw_rows:
        row["서비스명"] = app_name

    rows = raw_rows[:count]

    if rows:
        df = pd.DataFrame(rows)
        # 요청: 심각도 점수 우선 정렬(동점 시 최신 순)
        df = df.sort_values(by=["심각도 점수", "날짜_dt"], ascending=[False, False]).reset_index(drop=True)
        df["순위"] = df["심각도 점수"].rank(method="dense", ascending=False).astype(int)
        total = len(df)
        df["백분위"] = df["순위"].apply(lambda r: calc_percentile_display(int(r), total))
        df = df[[
            "출처",
            "서비스명",
            "날짜",
            "닉네임",
            "내용",
            "심각도 점수",
            "순위",
            "백분위",
            "평점",
            "좋아요",
        ]]
        rows = df.to_dict(orient="records")

    payload: Dict[str, Any] = {
        "meta": {
            "service_name": app_name,
            "app_id": app_id,
            "total": len(rows),
            "generated_at": datetime.now().isoformat(),
            "developer": app_info.get("developer"),
            "icon": app_info.get("icon"),
            "score": app_info.get("score"),
        },
        "rows": rows,
    }
    return payload


def _safe_get(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def fetch_reviews_ios(
    app_id: str,
    desired_count: int,
    threshold_dt: Optional[datetime],
    rating_exact: Optional[int],
    rating_max: Optional[int],
) -> List[Dict[str, Any]]:
    """Apple RSS(XML) 직접 파싱.
    1) 권장 단일 엔드포인트: /rss/customerreviews/id=.../sortBy=mostRecent/xml (KR→US)
    2) 보완: 필요시 page=.. JSON/RSS 루프 시도(레거시)
    """
    collected: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    countries = ["kr", "us"]
    max_pages = 10
    ns = {"atom": "http://www.w3.org/2005/Atom", "im": "http://itunes.apple.com/rss"}

    # 1) 단일 XML 피드 우선 시도 (페이지 매개변수 없이 최신순)
    headers = {"User-Agent": "Mozilla/5.0"}
    for country in countries:
        url = f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortBy=mostRecent/xml"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
        except Exception:
            root = None

        if root is not None:
            entries = root.findall("atom:entry", ns)
            if entries and len(entries) > 1:
                for e in entries[1:]:
                    review_id = e.findtext("atom:id", default="", namespaces=ns) or ""
                    if not review_id or review_id in seen_ids:
                        continue
                    seen_ids.add(review_id)

                    text = e.findtext("atom:content", default="", namespaces=ns) or ""
                    author = e.findtext("atom:author/atom:name", default="", namespaces=ns) or ""
                    rating_str = e.findtext("im:rating", default="0", namespaces=ns) or "0"
                    try:
                        score = int(rating_str)
                    except Exception:
                        score = 0
                    updated_str = e.findtext("atom:updated", default="", namespaces=ns) or ""
                    at: Optional[datetime] = None
                    if updated_str:
                        try:
                            ts = updated_str.replace("Z", "+0000")
                            at = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
                        except Exception:
                            try:
                                at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                            except Exception:
                                at = None

                    if score not in [1, 2, 3, 4, 5]:
                        continue
                    if rating_exact is not None and score != rating_exact:
                        continue
                    if rating_exact is None and rating_max is not None and score > max(1, min(5, rating_max)):
                        continue
                    if not isinstance(at, datetime):
                        continue
                    if threshold_dt is not None and at < threshold_dt:
                        # 최신순이므로 더 볼 필요 없음
                        return collected
                    if count_meaningful_chars_all(text) < 10:
                        continue

                    weight = get_rating_weight(score)
                    priority_score = round(weight * (1 + math.log2(1 + 0)), 2)
                    collected.append(
                        {
                            "OS": "iOS",
                            "출처": "iOS",
                            "날짜_dt": at,
                            "날짜": at.strftime("%Y-%m-%d"),
                            "닉네임": author,
                            "내용": text,
                            "심각도 점수": priority_score,
                            "평점": score,
                            "좋아요": 0,
                        }
                    )
                    if len(collected) >= desired_count:
                        return collected[:desired_count]

    # 2) 보완: 페이지 루프(레거시) 시도
    for country in countries:
        for page in range(1, max_pages + 1):
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent"
            try:
                with urllib.request.urlopen(url, timeout=15) as resp:
                    xml_bytes = resp.read()
                root = ET.fromstring(xml_bytes)
            except Exception:
                continue

            entries = root.findall("atom:entry", ns)
            if not entries or len(entries) < 2:
                continue

            for e in entries[1:]:  # 첫 entry는 앱 메타
                rid = e.findtext("atom:id", default="", namespaces=ns)
                if not rid or rid in seen_ids:
                    continue
                seen_ids.add(rid)

                text = e.findtext("atom:content", default="", namespaces=ns) or ""
                author = e.findtext("atom:author/atom:name", default="", namespaces=ns) or ""
                rating_str = e.findtext("im:rating", default="0", namespaces=ns) or "0"
                try:
                    score = int(rating_str)
                except Exception:
                    score = 0
                updated_str = e.findtext("atom:updated", default="", namespaces=ns) or ""

                at: Optional[datetime] = None
                if updated_str:
                    try:
                        ts = updated_str.replace("Z", "+0000")
                        at = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        try:
                            at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                        except Exception:
                            at = None

                # 필터 조건
                if score not in [1, 2, 3, 4, 5]:
                    continue
                if rating_exact is not None and score != rating_exact:
                    continue
                if rating_exact is None and rating_max is not None and score > max(1, min(5, rating_max)):
                    continue
                if not isinstance(at, datetime):
                    continue
                if threshold_dt is not None and at < threshold_dt:
                    return collected
                if count_meaningful_chars_all(text) < 10:
                    continue

                weight = get_rating_weight(score)
                priority_score = round(weight * (1 + math.log2(1 + 0)), 2)

                collected.append(
                    {
                        "OS": "iOS",
                        "출처": "iOS",
                        "날짜_dt": at,
                        "날짜": at.strftime("%Y-%m-%d"),
                        "닉네임": author,
                        "내용": text,
                        "심각도 점수": priority_score,
                        "평점": score,
                        "좋아요": 0,
                    }
                )
                if len(collected) >= desired_count:
                    break
            if len(collected) >= desired_count:
                break
        if len(collected) >= desired_count:
            break

    return collected[:desired_count]
    
    # NOTE: 위에서 return 되지 않았다면 JSON 피드 보완 수집 시도
    # 형식: https://itunes.apple.com/{country}/rss/customerreviews/page={n}/id={app_id}/sortby=mostrecent/json
    # 일부 환경에서 XML이 비어있고 JSON에 값이 존재하는 사례 대응
    for country in countries:
        for page in range(1, max_pages + 1):
            url = (
                f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
            )
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                if r.status_code != 200:
                    continue
                data = r.json()
            except Exception:
                continue

            entries = _safe_get(data, ["feed", "entry"], [])
            if not isinstance(entries, list) or len(entries) < 2:
                continue

            for e in entries[1:]:
                rid = _safe_get(e, ["id", "label"], "")
                if not rid or rid in seen_ids:
                    continue
                seen_ids.add(rid)

                text = _safe_get(e, ["content", "label"], "") or ""
                author = _safe_get(e, ["author", "name", "label"], "") or ""
                rating_str = _safe_get(e, ["im:rating", "label"], "0") or "0"
                try:
                    score = int(rating_str)
                except Exception:
                    score = 0
                updated_str = _safe_get(e, ["updated", "label"], "") or ""
                at = None
                if updated_str:
                    try:
                        ts = updated_str.replace("Z", "+0000")
                        at = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        try:
                            at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                        except Exception:
                            at = None

                if score not in [1, 2, 3, 4, 5]:
                    continue
                if rating_exact is not None and score != rating_exact:
                    continue
                if rating_exact is None and rating_max is not None and score > max(1, min(5, rating_max)):
                    continue
                if not isinstance(at, datetime):
                    continue
                if threshold_dt is not None and at < threshold_dt:
                    return collected
                if count_meaningful_chars_all(text) < 10:
                    continue

                weight = get_rating_weight(score)
                priority_score = round(weight * (1 + math.log2(1 + 0)), 2)
                collected.append(
                    {
                        "OS": "iOS",
                        "출처": "iOS",
                        "날짜_dt": at,
                        "날짜": at.strftime("%Y-%m-%d"),
                        "닉네임": author,
                        "내용": text,
                        "심각도 점수": priority_score,
                        "평점": score,
                        "좋아요": 0,
                    }
                )
                if len(collected) >= desired_count:
                    return collected[:desired_count]

    return collected[:desired_count]


def build_reviews_multi_payload(
    selected_apps: List[Dict[str, Any]],
    count_per_app: int,
    days: int,
    from_date: Optional[date],
    rating_exact: Optional[int],
    rating_max: Optional[int],
) -> Dict[str, Any]:
    threshold_dt = compute_threshold_dt(days=days, from_date=from_date)
    combined_rows: List[Dict[str, Any]] = []

    for app in selected_apps[:10]:
        app_id = app.get("appId") or app.get("id")
        os_name = app.get("os", "android")
        service_name = app.get("appName") or app.get("title") or ""

        if os_name == "ios":
            ios_rows = fetch_reviews_ios(
                app_id=app_id,
                desired_count=count_per_app,
                threshold_dt=threshold_dt,
                rating_exact=rating_exact,
                rating_max=rating_max,
            )
            for r in ios_rows:
                r["서비스명"] = service_name
                r["OS"] = "iOS"
            combined_rows.extend(ios_rows)
        else:
            # android
            raw = fetch_reviews_iteratively(
                app_id=app_id,
                desired_count=count_per_app,
                threshold_dt=threshold_dt,
                rating_exact=rating_exact,
                rating_max=rating_max,
            )
            for r in raw:
                r["서비스명"] = service_name
                r["OS"] = "Android"
            combined_rows.extend(raw)

    if combined_rows:
        df = pd.DataFrame(combined_rows)

        # 안전 제한: 앱(서비스명)별 최대 count_per_app 개수로 제한
        # 우선 기간(날짜_dt) 기준 최신순 정렬 후 Head 적용 → 평점/기간 필터는 수집 단계에서 이미 적용됨
        if "서비스명" in df.columns:
            if "날짜_dt" in df.columns:
                df = df.sort_values(by=["서비스명", "날짜_dt"], ascending=[True, False])
            else:
                df = df.sort_values(by=["서비스명"])  # 날짜 정보가 없으면 그룹 정렬만
            df = df.groupby("서비스명", group_keys=False).head(count_per_app)

        # 최종 표시 정렬: 심각도 점수 우선, 동점 시 최신순
        if "날짜_dt" in df.columns:
            df = df.sort_values(by=["심각도 점수", "날짜_dt"], ascending=[False, False]).reset_index(drop=True)
        else:
            df = df.sort_values(by=["심각도 점수"], ascending=[False]).reset_index(drop=True)

        df["순위"] = df["심각도 점수"].rank(method="dense", ascending=False).astype(int)
        total = len(df)
        df["백분위"] = df["순위"].apply(lambda r: calc_percentile_display(int(r), total))
        present_cols = [c for c in [
            "OS", "서비스명", "순위", "내용", "심각도 점수", "백분위", "평점", "좋아요", "닉네임", "날짜"
        ] if c in df.columns]
        df = df[present_cols]
        combined_rows = df.to_dict(orient="records")

    payload: Dict[str, Any] = {
        "meta": {
            "total": len(combined_rows),
            "generated_at": datetime.now().isoformat(),
        },
        "rows": combined_rows,
    }
    return payload


