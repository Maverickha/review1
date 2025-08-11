from __future__ import annotations

import io
import math
import os
import re
import time
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List

import pandas as pd
from flask import Flask, jsonify, request, send_from_directory, Response
from server.config import settings
from google_play_scraper import Sort, reviews, app as gp_app, search as gp_search
from server.services.reviews_service import (
    build_reviews_payload as build_reviews_payload_service,
    build_reviews_multi_payload,
)
from urllib.parse import quote


def create_app() -> Flask:
    # 기본 로깅 설정(운영/디버깅 가독성 향상)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    flask_app = Flask(
        __name__,
        static_folder="static",
        static_url_path="",
    )

    # 선택적 rate limiting (미설치 환경에서도 동작하도록)
    limiter = None
    try:  # noqa: SIM105
        from flask_limiter import Limiter
        from flask_limiter.util import get_remote_address

        limiter = Limiter(get_remote_address, app=flask_app, default_limits=["100 per hour"])
    except Exception:  # pragma: no cover - 선택 의존성
        logger.info("flask-limiter 미설치: rate limiting 비활성화")

    # 초간단 TTL 캐시(검색 응답 캐싱)
    _search_cache: Dict[str, Dict[str, Any]] = {}
    _SEARCH_TTL_SECONDS = settings.cache_ttl_seconds

    @flask_app.get("/config.js")
    def public_config() -> Response:
        js = f"window.__APP_CONFIG__={{GA_ID:'{settings.ga_id}'}};"
        return Response(js, mimetype="application/javascript")

    @flask_app.get("/")
    def index() -> Any:
        return send_from_directory(flask_app.static_folder, "index.html")

    @flask_app.get("/api/health")
    def health() -> Any:
        return {"status": "ok", "timestamp": datetime.now().isoformat()}

    @flask_app.get("/api/search")
    def search_apps() -> Any:
        query = request.args.get("q", "").strip()
        os_name = request.args.get("os", "android").strip().lower()
        if not query:
            return jsonify({"items": []})

        # URL 기반 App ID 직접 추출 지원
        def parse_app_url(q: str) -> tuple[str | None, str | None]:
            try:
                import re as _re
                lower = q.lower()
                # App Store URL 예: https://apps.apple.com/kr/app/토스/id839333328
                if "apps.apple.com" in lower:
                    m = _re.search(r"/id(\d+)", lower)
                    if m:
                        return "ios", m.group(1)
                # Google Play URL 예: https://play.google.com/store/apps/details?id=com.kakao.talk
                if "play.google.com" in lower:
                    m = _re.search(r"[?&]id=([a-zA-Z0-9_\.]+)", q)
                    if m:
                        return "android", m.group(1)
            except Exception:
                pass
            return None, None

        try:
            parsed_os, parsed_id = parse_app_url(query)

            # 길이 제한: 일반 검색어만 제한. URL이나 명시적 ID는 허용
            if parsed_id is None and len(query) > 100:
                return jsonify({"error": "검색어가 너무 깁니다."}), 400

            # 캐시 키
            cache_key = f"{os_name}__{query}"
            cached = _search_cache.get(cache_key)
            now = time.time()
            if cached and cached.get("exp", 0) > now:
                return jsonify({"items": cached["data"]})

            items: List[Dict[str, Any]] = []

            # URL에서 직접 추출된 경우: 단건 조회로 메타 구성
            if parsed_id:
                effective_os = parsed_os or os_name
                if effective_os == "ios":
                    import urllib.request, urllib.parse, json
                    lookup_url = (
                        "https://itunes.apple.com/lookup?" + urllib.parse.urlencode({
                            "id": parsed_id,
                            "country": "KR",
                        })
                    )
                    try:
                        with urllib.request.urlopen(lookup_url, timeout=15) as resp:
                            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                        r = (data.get("results") or [{}])[0]
                        items.append({
                            "appId": str(r.get("trackId") or parsed_id),
                            "title": r.get("trackName") or "",
                            "developer": r.get("sellerName") or r.get("artistName") or "",
                            "score": r.get("averageUserRating"),
                            "icon": r.get("artworkUrl100") or r.get("artworkUrl60") or "",
                            "os": "ios",
                        })
                    except Exception:
                        # 메타 조회 실패 시 ID만 반환
                        items.append({
                            "appId": str(parsed_id),
                            "title": "",
                            "developer": "",
                            "score": None,
                            "icon": "",
                            "os": "ios",
                        })
                else:  # android
                    try:
                        app_meta = gp_app(parsed_id, lang="ko", country="kr")
                        items.append({
                            "appId": parsed_id,
                            "title": app_meta.get("title"),
                            "developer": app_meta.get("developer"),
                            "score": app_meta.get("score"),
                            "icon": app_meta.get("icon"),
                            "os": "android",
                        })
                    except Exception:
                        items.append({
                            "appId": parsed_id,
                            "title": "",
                            "developer": "",
                            "score": None,
                            "icon": "",
                            "os": "android",
                        })
            else:
                # 일반 검색 흐름
                if os_name == "ios":
                    # iTunes Search API (limited fields)
                    import urllib.request, urllib.parse, json
                    url = (
                        "https://itunes.apple.com/search?" + urllib.parse.urlencode({
                            "term": query,
                            "country": "KR",
                            "entity": "software",
                            "limit": 20,
                        })
                    )
                    with urllib.request.urlopen(url, timeout=15) as resp:
                        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                    for r in data.get("results", []):
                        items.append({
                            "appId": str(r.get("trackId")),
                            "title": r.get("trackName"),
                            "developer": r.get("sellerName") or r.get("artistName"),
                            "score": r.get("averageUserRating"),
                            "icon": r.get("artworkUrl100") or r.get("artworkUrl60"),
                            "os": "ios",
                        })
                else:
                    results = gp_search(
                        query,
                        lang="ko",
                        country="kr",
                        n_hits=20,
                    )
                    for r in results:
                        items.append(
                            {
                                "appId": r.get("appId"),
                                "title": r.get("title"),
                                "developer": r.get("developer"),
                                "score": r.get("score"),
                                "icon": r.get("icon"),
                                "os": "android",
                            }
                        )

            # 캐시에 적재
            _search_cache[cache_key] = {"data": items, "exp": now + _SEARCH_TTL_SECONDS}
            return jsonify({"items": items})
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @flask_app.get("/api/reviews")
    def fetch_reviews() -> Any:
        app_id = request.args.get("appId", "").strip()
        if not app_id:
            return jsonify({"error": "Missing appId"}), 400

        try:
            count = int(request.args.get("count", "250"))
        except ValueError:
            count = 250

        try:
            # 기간: days(정수, 0이면 전체) 또는 fromDate(YYYYMMDD)
            from_date_str = request.args.get("fromDate")
            if from_date_str:
                try:
                    from_date = datetime.strptime(from_date_str, "%Y%m%d").date()
                except ValueError:
                    from_date = None
            else:
                from_date = None

            try:
                days = int(request.args.get("days", "365"))
            except ValueError:
                days = 365

            # 평점: ratingExact 또는 ratingMax
            rating_exact = request.args.get("ratingExact")
            rating_max = request.args.get("ratingMax")
            try:
                rating_exact_val = int(rating_exact) if rating_exact is not None else None
            except ValueError:
                rating_exact_val = None
            try:
                rating_max_val = int(rating_max) if rating_max is not None else None
            except ValueError:
                rating_max_val = None

            payload = build_reviews_payload_service(
                app_id=app_id,
                count=count,
                days=days,
                from_date=from_date,
                rating_exact=rating_exact_val,
                rating_max=rating_max_val,
            )
            return jsonify(payload)
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @flask_app.get("/api/export/csv")
    def export_csv() -> Response:
        app_id = request.args.get("appId", "").strip()
        if not app_id:
            return Response("Missing appId", status=400)

        try:
            count_param = request.args.get("count", "250")
            try:
                count = int(count_param)
            except ValueError:
                count = 250

            # 동일한 필터 적용
            from_date_str = request.args.get("fromDate")
            if from_date_str:
                try:
                    from_date = datetime.strptime(from_date_str, "%Y%m%d").date()
                except ValueError:
                    from_date = None
            else:
                from_date = None

            try:
                days = int(request.args.get("days", "365"))
            except ValueError:
                days = 365

            rating_exact = request.args.get("ratingExact")
            rating_max = request.args.get("ratingMax")
            try:
                rating_exact_val = int(rating_exact) if rating_exact is not None else None
            except ValueError:
                rating_exact_val = None
            try:
                rating_max_val = int(rating_max) if rating_max is not None else None
            except ValueError:
                rating_max_val = None

            payload = build_reviews_payload_service(
                app_id=app_id,
                count=count,
                days=days,
                from_date=from_date,
                rating_exact=rating_exact_val,
                rating_max=rating_max_val,
            )
            df = pd.DataFrame(payload["rows"]) if payload["rows"] else pd.DataFrame()

            # Preserve column order
            columns = [
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
            ]
            if not df.empty:
                df = df[columns]

            csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            mem = io.BytesIO(csv_bytes)
            today_str = datetime.now().strftime("%Y%m%d")
            filename_utf8 = f"{payload['meta']['service_name']}_리뷰_{today_str}.csv"
            filename_quoted = quote(filename_utf8)
            headers = {
                # ASCII fallback + RFC5987 filename*
                "Content-Disposition": f"attachment; filename=export.csv; filename*=UTF-8''{filename_quoted}",
                "Content-Type": "text/csv; charset=utf-8",
            }
            return Response(mem.getvalue(), headers=headers)
        except Exception as exc:  # noqa: BLE001
            return Response(str(exc), status=500)

    @flask_app.post("/api/reviews/multi")
    def fetch_reviews_multi() -> Any:
        try:
            data = request.get_json(force=True) or {}
            selected_apps = data.get("selectedApps", [])
            if not isinstance(selected_apps, list) or not selected_apps:
                return jsonify({"error": "선택된 앱이 없습니다."}), 400
            if len(selected_apps) > 10:
                return jsonify({"error": "최대 10개의 앱만 선택할 수 있습니다."}), 400

            def _parse_int(val: Any, default: int, min_v: int | None = None, max_v: int | None = None) -> int:
                try:
                    num = int(val)
                except Exception:
                    return default
                if min_v is not None and num < min_v:
                    num = min_v
                if max_v is not None and num > max_v:
                    num = max_v
                return num

            count_per_app = _parse_int(data.get("countPerApp", 250), 250, 1, 1000)
            days = _parse_int(data.get("days", 365), 365, 1, None)
            from_date_str = data.get("fromDate")
            from_date = None
            if isinstance(from_date_str, str) and len(from_date_str) == 8:
                try:
                    from_date = datetime.strptime(from_date_str, "%Y%m%d").date()
                except ValueError:
                    from_date = None
            rating_exact = data.get("ratingExact")
            rating_max = data.get("ratingMax")
            try:
                rating_exact_val = int(rating_exact) if rating_exact is not None else None
            except Exception:
                rating_exact_val = None
            try:
                rating_max_val = int(rating_max) if rating_max is not None else None
            except Exception:
                rating_max_val = None

            payload = build_reviews_multi_payload(
                selected_apps=selected_apps,
                count_per_app=count_per_app,
                days=days,
                from_date=from_date,
                rating_exact=rating_exact_val,
                rating_max=rating_max_val,
            )
            return jsonify(payload)
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    return flask_app


"""
Legacy in-file build_reviews_payload removed. Using services module.
"""


app = create_app()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=settings.debug)


