from __future__ import annotations

# Vercel serverless entrypoint for Flask (WSGI)
# Exposes `app` for @vercel/python to detect.

from app import app as flask_app  # type: ignore

# Vercel expects a module-level `app` or `application`.
app = flask_app


