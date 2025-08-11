## 리뷰 수집 · 우선순위 정렬 (1페이지 웹)

- **검색**: 구글 플레이스토어 기반으로 앱 검색 → `appId` 선택
- **수집/정렬**: 최근 리뷰 수집 → 가중치 기반 우선순위 계산 → 정렬
- **내보내기**: CSV 다운로드, 표 복사 지원
- **추적**: GA4 컴포넌트 단위 이벤트 로깅 포함

### 요구 사항
- Python 3.10+

### 설치
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 실행
```bash
python app.py
```
- 브라우저: `http://localhost:5000`
- GA ID(`G-XXXXXXX`)를 `static/index.html`에서 실제값으로 교체하세요

### API
- `GET /api/search?q=키워드` → 앱 검색
- `GET /api/reviews?appId=앱ID&count=250` → 리뷰 수집/가공 응답
- `GET /api/export/csv?appId=앱ID&count=250` → CSV 다운로드

### 산식/필터
- 가중치: 평점 1→1.0, 2→0.8, 3→0.5, 4→0.2, 5→0.1
- 우선순위: `weight * (1 + log2(1 + thumbs_up))`
- 필터: 최근 1년, 한글 15자 이상
