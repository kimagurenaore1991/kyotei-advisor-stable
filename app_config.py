from __future__ import annotations

import os
from pathlib import Path
from datetime import timezone, timedelta

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
LOCK_FILE = BASE_DIR / "scraping.lock"
JST = timezone(timedelta(hours=9))
DB_NAME = os.environ.get("DATABASE_PATH", str(BASE_DIR / "kyotei.db"))
REQUEST_TIMEOUT = 15
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
