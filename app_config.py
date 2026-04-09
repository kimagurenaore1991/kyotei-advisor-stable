from __future__ import annotations

import os
from pathlib import Path
from datetime import timezone, timedelta

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
LOCK_FILE = BASE_DIR / "scraping.lock"
LAST_SCRAPE_FILE = BASE_DIR / ".last_full_scrape"
JST = timezone(timedelta(hours=9))
DB_NAME = os.environ.get("DATABASE_PATH", str(BASE_DIR / "kyotei.db"))
REQUEST_TIMEOUT = 15
# Supabase configuration
USE_SUPABASE = os.environ.get("USE_SUPABASE", "True").lower() == "true"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://rngzcwztmshadaevaxqz.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJuZ3pjd3p0bXNoYWRhZXZheHF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQzNjE1MDMsImV4cCI6MjA4OTkzNzUwM30.YY8Q_7h_UwKQNOlpCPNTqMjL8iW8ZuxW70yuy7dHUk4")
# Note: Service role key is not usually needed for the client, but keeping it as an environment option if needed.
# Stripe configuration
STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "sk_test_placeholder")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_placeholder")
STRIPE_PRICE_ID_MONTHLY = os.environ.get("STRIPE_PRICE_ID_MONTHLY", "price_1...")
STRIPE_PRICE_ID_90DAY = os.environ.get("STRIPE_PRICE_ID_90DAY", "price_2...")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
