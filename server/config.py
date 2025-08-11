from __future__ import annotations

import os
from dataclasses import dataclass


try:  # optional: load .env if present
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()  # pragma: no cover
except Exception:  # pragma: no cover
    pass


@dataclass(frozen=True)
class Settings:
    ga_id: str = os.getenv("GA_ID", "")
    cache_ttl_seconds: int = int(os.getenv("SEARCH_CACHE_TTL", "1800") or "1800")
    debug: bool = os.getenv("FLASK_DEBUG", "true").lower() in {"1", "true", "yes"}


settings = Settings()


