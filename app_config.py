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
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://rngzcwztmshadaevaxqz.supabase.co")
SUPABASE_KEY = "sb_publishable_6huF1R37Wdmbr6sGESBs_Q_6ky2OXqz"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# --- Payment Configuration ---
PAYMENT_EMAIL = "kimagurenaore@gmail.com"
PAYMENT_PASS = "konoyarou0209"
PAYMENT_IMAP_SERVER = "imap.gmail.com"

# Payment Destination Info
SMBC_INFO = {
    "bank_name": "三井住友銀行",
    "branch_name": "オリーブＤＩＬＬ支店",
    "account_type": "普通",
    "account_number": "2361008"
}
PAYPAY_LINK = "https://qr.paypay.ne.jp/p2p01_0qVbjhYqfPcDjczN"

