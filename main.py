from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict
from pydantic import BaseModel
import random
import os
import json
import math
import itertools
import concurrent.futures
import hashlib
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Body, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from live_scraper import (
    fetch_racer_profile, fetch_live_odds, fetch_match_result,
    fetch_exhibition_data, fetch_all_odds
)
import scraper
from scraper import (
    scrape_index, scrape_race_syusso, update_exhibition, update_result, 
    update_all_active_races, get_racer_results_stats
)
import time
import asyncio

from app_config import JST, LOCK_FILE, STATIC_DIR, LAST_SCRAPE_FILE
from database import get_db_connection, init_db, cleanup_old_data, sync_from_supabase

app = FastAPI(title="Kyotei Advisor MVP")
init_db()

# 選手プロフィールのメモリキャッシュ (toban -> {data, timestamp})
RACER_STATS_CACHE = {}
RACER_CACHE_TTL = 3600 # 1時間

# 的中・回収率（daily_hits）のメモリキャッシュ
# { "date_settings_hash": { "data": results, "ts": timestamp } }
DAILY_HITS_CACHE = {}
DAILY_HITS_CACHE_TTL = 300 # 5分 (結果が更新される可能性があるため短め)

# --- Authentication & Access Control ---

from supabase_client import get_supabase_client

async def get_current_user(authorization: str = Header(None)) -> Dict:
    """
    SupabaseのJWTを検証し、ユーザー情報を取得する（アクセス制限は行わない）。
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )
    
    token = authorization.split(" ")[1]
    supabase = get_supabase_client()
    
    try:
        user_res = supabase.auth.get_user(token)
        if not user_res or not user_res.user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token"
            )
        
        user_id = user_res.user.id
        profile_res = supabase.table("profiles").select("*").eq("id", user_id).execute()
        
        if not profile_res.data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User profile not initialized"
            )
        
        profile = profile_res.data[0]
        is_premium_active = profile.get("is_premium", False)
        premium_until_str = profile.get("premium_until")
        now_utc = datetime.now(timezone.utc)
        
        if premium_until_str:
            try:
                ts_until = premium_until_str.replace("Z", "+00:00")
                premium_until = datetime.fromisoformat(ts_until)
                if now_utc < premium_until:
                    is_premium_active = True
            except:
                premium_until = None
        else:
            premium_until = None
            
        trial_started_at_str = profile.get("trial_started_at")
        if trial_started_at_str:
            try:
                ts_trial = trial_started_at_str.replace("Z", "+00:00")
                trial_started_at = datetime.fromisoformat(ts_trial)
            except:
                trial_started_at = datetime.now(timezone.utc)
        else:
            trial_started_at = datetime.now(timezone.utc)

        trial_end = trial_started_at + timedelta(days=3)
        is_in_trial = now_utc < trial_end
        
        return {
            "user_id": user_id,
            "is_premium": is_premium_active,
            "is_in_trial": is_in_trial,
            "trial_end": trial_end,
            "premium_until": premium_until_str,
            "remaining_trial": str(trial_end - now_utc) if is_in_trial else "Expired"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AUTH ERROR] {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Auth error: {str(e)}"
        )

async def require_access(user: Dict = Depends(get_current_user)) -> Dict:
    """
    プレミアム機能へのアクセス制限を行う依存関係。
    """
    if not user["is_premium"] and not user["is_in_trial"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Trial period expired. Subscription required to view details."
        )
    return user


# --- Payment Webhook (Email Detect & Manual Registration) ---

class PaymentRegistration(BaseModel):
    payment_name: str

@app.post("/api/user/payment_registration")
async def register_payment_name(
    reg: PaymentRegistration, 
    user: Dict = Depends(get_current_user),
    authorization: str = Header(None)
):
    """ユーザーの振込名義を登録する"""
    user_id = user["user_id"]
    supabase = get_supabase_client()
    
    try:
        res = supabase.table("profiles").update({
            "payment_name": reg.payment_name
        }).eq("id", user_id).execute()
        
        if not res.data:
            raise HTTPException(status_code=500, detail="Failed to update profile")
            
        return {"status": "ok", "payment_name": reg.payment_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/webhooks/payment")
async def payment_webhook(data: dict):
    """
    PayPayや銀行振込完了通知を受け取るエンドポイント（プレースホルダ）。
    """
    print(f"[WEBHOOK] Received payment notification: {data}")
    return {"status": "ok"}

@app.on_event("startup")
async def startup_event():
    # 起動時のデータ収集戦略の決定
    now_jst = datetime.now(JST)
    today_iso = now_jst.strftime('%Y-%m-%d')
    
    last_scrape_date = ""
    if LAST_SCRAPE_FILE.exists():
        try:
            last_scrape_date = LAST_SCRAPE_FILE.read_text(encoding="utf-8").strip()
        except:
            pass
    
    loop = asyncio.get_event_loop()
    
    # 1. データ同期・取得（初回: 公式から全取得、2回目以降: Supabaseから同期）
    try:
        if last_scrape_date != today_iso:
            print(f"[STARTUP] {today_iso} の初回起動です。Supabaseから今日のデータを先行取得し、公式からもバックグラウンドで更新します...")
            # 1. 今日のデータをSupabaseから最優先で取得 (UI即時表示のため)
            from database import sync_specific_date_from_supabase
            await loop.run_in_executor(None, sync_specific_date_from_supabase, today_iso)

            # 2. 残りの同期と公式スクレイピングはバックグラウンドで実行
            asyncio.create_task(asyncio.to_thread(sync_from_supabase, 1))
            asyncio.create_task(asyncio.to_thread(scraper.scrape_today, now_jst))
            
            try:
                LAST_SCRAPE_FILE.write_text(today_iso, encoding="utf-8")
            except: pass
        else:
            print(f"[STARTUP] 本日2回目以降の起動です。バックグラウンドで同期・補填を行います...")
            # 今日・明日のデータをSupabaseから先行取得
            from database import sync_specific_date_from_supabase
            asyncio.create_task(asyncio.to_thread(sync_specific_date_from_supabase, today_iso))
            
            asyncio.create_task(asyncio.to_thread(sync_from_supabase, 1))
            asyncio.create_task(asyncio.to_thread(scraper.scrape_missing_today, now_jst))
            
        # 2. まだDBが空の場合（初回起動失敗時など）のフォールバック
        asyncio.create_task(initial_fetch_if_empty())
        
    except Exception as e:
        print(f"[STARTUP ERROR] Data initialization failed: {e}")

    # ロックファイルのクリーンアップ
    if LOCK_FILE.exists():
        try:
            LOCK_FILE.unlink()
        except:
            pass
    
    # SSEコールバックをスクレイパーに登録
    def _sync_sse_push(event_type: str, data: dict):
        try:
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(sse_push(event_type, data), loop)
        except Exception as e:
            print(f"[SSE BRIDGE ERROR] {e}")
    
    scraper.sse_broadcast_callback = _sync_sse_push
    
    # バックグラウンドワーカーの起動
    asyncio.create_task(background_worker())
    
    # 支払監視ワーカーの起動 (Email Monitor)
    try:
        from payment_monitor import payment_monitor_loop
        asyncio.create_task(payment_monitor_loop())
    except ImportError:
        print("[MONITOR] payment_monitor.py not found. Skipping...")
    except Exception as e:
        print(f"[MONITOR ERROR] Failed to start: {e}")

async def initial_fetch_if_empty():
    """起動時にデータが不足している場合にのみウェブから取得する"""
    await asyncio.sleep(10) # 起動直後の負荷分散
    loop = asyncio.get_event_loop()
    now_jst = datetime.now(JST)
    
    for days in [-1, 0, 1]:
        target_dt = now_jst + timedelta(days=days)
        target_iso = target_dt.strftime('%Y-%m-%d')
        
        conn = get_db_connection()
        try:
            # 開催場があるかチェック
            exists = conn.execute("SELECT 1 FROM races WHERE race_date = ? LIMIT 1", (target_iso,)).fetchone()
            if not exists:
                if days == 1 and now_jst.hour < 18: continue # 翌日分は18時以降
                print(f"[SYSTEM] Data missing for {target_iso}. Triggering initial fetch...")
                await loop.run_in_executor(None, scraper.scrape_today, target_dt)
        except Exception as e:
            print(f"[SYSTEM ERROR] Initial fetch ({target_iso}): {e}")
        finally:
            conn.close()

async def background_worker():
    """定期的に全レース場のデータを自動更新し、日次サイクルを管理する"""
    print("[SYSTEM] Background worker started.")
    await asyncio.sleep(30)
    
    last_yesterday_check = 0
    last_tomorrow_check = 0
    
    while True:
        try:
            loop = asyncio.get_event_loop()
            now_jst = datetime.now(JST)
            today_iso = now_jst.strftime('%Y-%m-%d')
            
            # 1. 当日の更新 (1分おき)
            await loop.run_in_executor(None, update_all_active_races)
            
            # 2. 翌日のチェック (18時以降、10分おき)
            if now_jst.hour >= 18 and (time.time() - last_tomorrow_check > 600):
                tomorrow_dt = now_jst + timedelta(days=1)
                await loop.run_in_executor(None, update_all_active_races, tomorrow_dt)
                last_tomorrow_check = time.time()
                
            # 3. 昨日の整合性チェック (1時間おき)
            if time.time() - last_yesterday_check > 3600:
                yesterday_dt = now_jst - timedelta(days=1)
                # 昨日のレースが全て終了しているか、変更がないかサイレント更新
                # update_all_active_races は終了フラグをチェックして未了分のみ取得するが、
                # 整合性チェックとして呼び出す
                await loop.run_in_executor(None, update_all_active_races, yesterday_dt)
                last_yesterday_check = time.time()

            # 4. 古いデータのクリーンアップ（2日以上前のデータを削除）
            two_days_ago_dt = now_jst - timedelta(days=2)
            two_days_ago_iso = two_days_ago_dt.strftime('%Y-%m-%d')
            await loop.run_in_executor(None, cleanup_old_data, two_days_ago_iso)
                
        except Exception as e:
            print(f"[WORKER ERROR] {e}")
        
        await asyncio.sleep(60)

def get_today_str() -> str:
    now_jst = datetime.now(JST)
    return now_jst.strftime('%Y-%m-%d')

allowed_origins = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "*").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

from fastapi.responses import RedirectResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
        return response

app.add_middleware(NoCacheStaticMiddleware)

@app.get("/")
def read_root():
    return RedirectResponse(url="/static/index.html")


# ─────────────────────────── Server-Sent Events (SSE) ───────────────────────
# Simple broadcast: any number of clients subscribe via GET /api/events
# Backend pushes JSON messages when races finish or update.

import asyncio
from fastapi.responses import StreamingResponse

_sse_clients: list[asyncio.Queue] = []

async def sse_push(event_type: str, data: dict):
    """Broadcast a JSON event to all connected SSE clients."""
    import json
    msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    dead = []
    for q in _sse_clients:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try: _sse_clients.remove(q)
        except ValueError: pass

@app.get("/api/events")
async def sse_endpoint():
    """SSE stream endpoint. The frontend connects once and receives push events."""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=50)
    _sse_clients.append(q)

    async def generator():
        try:
            yield ": connected\n\n"  # initial keep-alive comment
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"  # prevent proxy timeout
        except asyncio.CancelledError:
            pass
        finally:
            try: _sse_clients.remove(q)
            except ValueError: pass

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )



# ─────────────────────────── Pydantic Models ────────────────────────────────

class CustomWeights(BaseModel):
    win_rate: float = 1.0
    motor: float = 1.0
    exhibition: float = 1.0
    st: float = 1.0
    course: float = 1.0
    wind: float = 0.0

class PlayerOverride(BaseModel):
    boat_number: int
    entry_course: Optional[int] = None
    exhibition_time: Optional[float] = None
    start_timing: Optional[float] = None
    tilt: Optional[float] = None
    is_absent: Optional[bool] = None
    parts_exchange: Optional[str] = None
    weight_adjustment: Optional[float] = None
    propeller: Optional[str] = None

class ExhibitionUpdate(BaseModel):
    boat_number: int
    exhibition_time: float
    start_timing: float
    entry_course: int
    tilt: Optional[float] = None
    is_absent: bool = False
    parts_exchange: Optional[str] = ""
    weight_adjustment: Optional[float] = 0.0
    propeller: Optional[str] = ""

class PredictSettings(BaseModel):
    max_items: int = 8
    bet_type: str = "3連単"
    fixed_1st: int = 0
    hit_type: str = "both"  # "buy", "ai", "custom", "both"
    ai_prediction_mode: int = 0  # 0:本命, 1:中穴, 2:大穴
    custom_prediction_mode: int = 0

class PredictRequest(BaseModel):
    weights: CustomWeights
    settings: PredictSettings = PredictSettings()
    overrides: Optional[List[PlayerOverride]] = None
    recalculate_ai: bool = False
    ignore_exhibition: bool = False


# ─────────────────────────── Places ─────────────────────────────────────────

places_dict_order = [
    "桐生", "戸田", "江戸川", "平和島", "多摩川", "浜名湖",
    "蒲郡", "常滑", "津", "三国", "びわこ", "住之江",
    "尼崎", "鳴門", "丸亀", "児島", "宮島", "徳山",
    "下関", "若松", "芦屋", "福岡", "唐津", "大村"
]

_active_places_cache_time = 0.0
_active_places_cache_jcds = []

# 選手の戦績キャッシュは上部で定義済み


@app.get("/api/racer_stats/{toban}")
def api_racer_stats(toban: str, jcd: Optional[str] = Query(None), date: Optional[str] = Query(None)):
    now = time.time()
    # Unique cache key per toban, jcd and date to prevent incorrect cross-venue cache hits
    cache_key = f"{toban}_{jcd}_{date}"
    
    if cache_key in RACER_STATS_CACHE:
        entry = RACER_STATS_CACHE[cache_key]
        if now - entry.get("timestamp", 0) < 3600:
            return entry.get("data")
    
    try:
        raw_stats = get_racer_results_stats(toban, jcd, date)
        
        # Format for frontend
        formatted_results = []
        
        for item in raw_stats.get("current_series", []):
            formatted_results.append({
                "date": "今節",
                "venue": "",
                "race_no": item.get("race_no"),
                "course": item.get("course_st", "").split("(")[0],
                "st": item.get("course_st", "").split("(")[1].replace(")", "") if "(" in item.get("course_st", "") else "",
                "rank": item.get("rank")
            })
            
        for series in raw_stats.get("back3", []):
            title = series.get("series_title", "")
            ranks = series.get("ranks", [])
            for i, rank in enumerate(ranks):
                formatted_results.append({
                    "date": "前節" if i == 0 else "",
                    "venue": title if i == 0 else "",
                    "race_no": "-",
                    "course": "-",
                    "st": "-",
                    "rank": rank
                })

        data = {
            "profile": {
                "toban": toban,
                "name": raw_stats.get("profile", {}).get("name"),
                "class": raw_stats.get("profile", {}).get("class"),
                "branch": raw_stats.get("profile", {}).get("branch"),
                "birthplace": raw_stats.get("profile", {}).get("hometown")
            },
            "seasonal_stats": {
                "global_win_rate": raw_stats.get("seasonal", {}).get("win_rate"),
                "local_win_rate": raw_stats.get("seasonal", {}).get("win_rate"),
                "global_quinella": raw_stats.get("seasonal", {}).get("quinella_rate", "").replace("%", ""),
                "local_quinella": "0.00"
            },
            "recent_results": formatted_results
        }
        
        RACER_STATS_CACHE[cache_key] = {"timestamp": now, "data": data}
        return data
    except Exception as e:
        print(f"Error in api_racer_stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch racer statistics")
def _grade_priority(title: str) -> int:
    """Grade priority: higher = more important. Used to pick best title per meet."""
    if not title:
        return 0
    t = title.upper()
    # SG
    if any(kw in t for kw in ['SG', 'ＳＧ', 'グランプリ', 'ダービー', 'メモリアル', 'チャンピオンシップ', 'オールスター', 'ピーターリング', 'クラシック']):
        return 5
    # G1
    if any(g in t for g in ['G1', 'GⅠ', 'G１', 'GI', 'Ｇ１', 'ＧⅠ', 'ＧＩ']) or any(kw in title for kw in ['龍王', '王者']):
        return 4
    # G2
    if any(g in t for g in ['G2', 'GⅡ', 'G２', 'GII', 'Ｇ２', 'ＧⅡ', 'ＧＩＩ']):
        return 3
    # G3
    if any(g in t for g in ['G3', 'GⅢ', 'G３', 'GIII', 'Ｇ３', 'ＧⅢ', 'ＧＩＩＩ']):
        return 2
    # Lady / Venus
    if any(k in title for k in ['ヴィーナス', 'レディース', '女子', 'クイーン']):
        return 1
    return 0

@app.get("/api/places")
async def get_places(date: Optional[str] = Query(None), background_tasks: BackgroundTasks = None):
    now = time.time()
    today_str = date if date else get_today_str()
    loop = asyncio.get_running_loop()

    conn = get_db_connection()
    try:
        # Fetch all races for today to determine best grade across all race numbers
        all_race_rows = conn.execute(
            'SELECT place_code, place_name, race_title, race_number, day_label FROM races WHERE race_date = ?',
            (today_str,)
        ).fetchall()
        
        if not all_race_rows:
            # データがない場合はSupabaseから取得を試みる (Supabase-first)
            print(f"[API] No data for {today_str}. Attempting initial sync...")
            from database import sync_specific_date_from_supabase
            
            # 最初の1回は同期的に実行して、ユーザーに「空」を返すのを極力避ける（最大3秒待機）
            await loop.run_in_executor(None, sync_specific_date_from_supabase, today_str)
            
            # 同期後に再度確認（少し待機）
            for _ in range(3):
                all_race_rows = conn.execute(
                    'SELECT place_code, place_name, race_title, race_number, day_label FROM races WHERE race_date = ?',
                    (today_str,)
                ).fetchall()
                if all_race_rows:
                    break
                await asyncio.sleep(1.0)
            
            if not all_race_rows:
                # それでも無い場合はバックグラウンドで公式からスクレイピングを開始
                if background_tasks:
                    background_tasks.add_task(scraper.scrape_today, today_str)
                all_race_rows = []

        db_place_codes = {row['place_code'] for row in all_race_rows}
    finally:
        conn.close()


    # Build grade_map: per place_code, keep the title with highest grade priority
    # and track max race_number separately
    grade_map_by_code: dict = {}
    for row in all_race_rows:
        pc = row['place_code']
        rn = row['race_number'] or 0
        rt = row['race_title'] or ''
        pname = row['place_name']
        dl = row['day_label'] or ''
        if pc not in grade_map_by_code:
            grade_map_by_code[pc] = (rt, rn, pname, dl)
        else:
            existing_title, existing_max, _, existing_dl = grade_map_by_code[pc]
            best_title = rt if _grade_priority(rt) > _grade_priority(existing_title) else existing_title
            best_max = max(existing_max, rn)
            best_dl = dl if dl else existing_dl
            grade_map_by_code[pc] = (best_title, best_max, pname, best_dl)

    is_today = (today_str == get_today_str())

    # 背景：閲覧者が増えても耐えられるよう、APIリクエスト時の自動スクレイピングは廃止。
    # 代わりにバックグラウンドワーカーが常に最新状態を保つ。
    
    # (既存のキャッシュ更新ロジックはデバッグ用に残すが、実質的にはDBからのみ取得)
    if is_today:
        active_place_codes = set(db_place_codes) | set(_active_places_cache_jcds)
    else:
        active_place_codes = set(db_place_codes)

    result = []
    for place in places_dict_order:
        place_code = next((c for c, n in scraper.places_dict.items() if n == place), None)
        is_active = place_code in active_place_codes if place_code else False
        race_title = ''
        max_race = 0
        day_label = ''
        if place_code and place_code in grade_map_by_code:
            race_title, max_race, _, day_label = grade_map_by_code[place_code]

        result.append({
            "place": place,
            "place_code": place_code or '',
            "is_active": is_active,
            "grade": race_title,
            "day_label": day_label,
            "max_race": max_race,
        })
    return result


@app.get("/api/places/{place_name}/races")
def get_races(place_name: str, date: Optional[str] = Query(None)):
    today_str = date if date else get_today_str()
    conn = get_db_connection()
    races = conn.execute(
        'SELECT id, race_date, place_code, place_name, race_number, race_title, is_finished, is_exhibition_done, scheduled_time '
        'FROM races WHERE place_name = ? AND race_date = ? GROUP BY race_number ORDER BY race_number',
        (place_name, today_str)
    ).fetchall()
    conn.close()
    if not races:
        return []
    return [dict(r) for r in races]


@app.get("/api/status")
def get_status():
    return {"is_scraping": LOCK_FILE.exists()}


@app.get("/api/racers/{toban}")
def get_racer_detail(toban: str, place_code: Optional[str] = Query(None)):
    """選手の詳細情報（プロフィール・コース別成績・会場別過去着順）を取得する"""
    import live_scraper
    import database
    
    now = time.time()
    
    # 1. 基本プロフィールとコース別成績（キャッシュ利用）
    stats = None
    if toban in RACER_STATS_CACHE:
        cache_data = RACER_STATS_CACHE[toban]
        if now - cache_data['ts'] < RACER_CACHE_TTL:
            stats = cache_data['data']
    
    if not stats:
        stats = live_scraper.fetch_racer_profile(toban)
        if stats:
            RACER_STATS_CACHE[toban] = {'data': stats, 'ts': now}
            # SQLite / Supabase 同期
            _sync_racer_profile_to_db(toban, stats)
    
    # 2. 過去着順の取得
    # DBから取得（entries 統合済み版）
    past_results = database.get_racer_results(toban, place_code)
    
    # 今日出走しているかチェック
    today_iso = datetime.now(JST).strftime('%Y-%m-%d')
    conn = database.get_db_connection()
    is_racing_today = conn.execute(
        "SELECT COUNT(*) FROM entries INNER JOIN races ON entries.race_id = races.id WHERE entries.racer_id = ? AND races.race_date = ?",
        (toban, today_iso)
    ).fetchone()[0] > 0
    conn.close()
    
    # データが極端に少ない、または今日出走中で最新データが必要な場合のみスクレイピング
    # 既にDBに30件以上（または今節複数件）あるなら、頻繁なスクレイピングは避ける
    needs_update = not past_results
    if not needs_update and is_racing_today:
        # 今日の日付のデータがまだ無い、かつ最後に更新してから1時間以上経過している場合
        last_update_ts = 0
        if past_results:
            # 暫定的に最初のデータのupdated_atを見る（あれば）
            pass # 既存スキーマにupdated_atがない場合は一旦常に1回は取るか、時間で制御
        
        # 簡易的に：30件未満なら1回は取る
        if len(past_results) < 20:
            needs_update = True

    if needs_update:
        # 重い処理なので必要な時だけ
        new_results = live_scraper.fetch_racer_past_results(toban)
        if new_results:
            database.save_racer_results(toban, new_results)
            past_results = database.get_racer_results(toban, place_code)
            
    return {
        "toban": toban,
        "name": stats.get("name", "") if stats else "",
        "course_stats": stats.get("course_stats", []) if stats else [],
        "past_results": past_results
    }


@app.post("/api/scrape/today")
def trigger_scrape_today(background_tasks: BackgroundTasks, date: Optional[str] = Query(None)):
    if LOCK_FILE.exists():
        raise HTTPException(status_code=400, detail="既にデータ取得処理が実行中です。")
    
    target_dt = None
    if date:
        try:
            target_dt = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=JST)
        except Exception:
            raise HTTPException(status_code=400, detail="日付形式が正しくありません (YYYY-MM-DD)。")

    print(f"[INFO] Triggering scrape for date: {date or 'today'}")
    background_tasks.add_task(scrape_today, target_dt)
    return {"status": "started", "message": f"{date or '今日'}のデータ取得を開始しました。"}


# ─────────────────────────── Scenario Engine (全艇対応) ─────────────────────

def calculate_scenarios(scored_players):
    """
    2〜6号艇それぞれの「捲り」「差し」「捲り差し」確率を計算する。
    """
    scenarios = []

    for player in scored_players:
        if player.get("is_absent"):
            continue
        course = player["calc_course"]

        boat_no = player["boat_number"]
        racer_name = player.get("racer_name", "")
        st = player["calc_st"]
        motor = player.get("motor_2_quinella") if player.get("motor_2_quinella") is not None else 30.0
        ex_time = player["calc_ex"]
        tilt = player.get("tilt") if player.get("tilt") is not None else 0.0

        inner_players = [p for p in scored_players if p["calc_course"] < course]
        if not inner_players:
            continue

        avg_inner_st = sum(p["calc_st"] for p in inner_players) / len(inner_players)
        st_diff = avg_inner_st - st

        # 捲り確率
        makuri_base = 8 + (st_diff * 350) + ((motor - 30) * 1.2)
        makuri_base -= (course - 2) * 1.5
        makuri_base += max(0, (6.80 - ex_time) * 25)
        makuri_base += tilt * 3.0
        makuri_prob = max(3.0, min(82.0, makuri_base))

        # 差し確率
        if len(inner_players) >= 2:
            st_spread = (
                max(p["calc_st"] for p in inner_players)
                - min(p["calc_st"] for p in inner_players)
            )
        else:
            st_spread = 0.02

        sashi_base = 6 + (st_diff * 180) + (st_spread * 280) + ((motor - 30) * 0.9)
        sashi_base += (course - 2) * 1.2
        sashi_base -= tilt * 1.5
        sashi_base += max(0, (6.80 - ex_time) * 15)
        sashi_prob = max(3.0, min(78.0, sashi_base))

        # 捲り差し確率
        makuri_sashi_prob = max(3.0, min(75.0, (makuri_prob * 0.45 + sashi_prob * 0.55) * 0.92))

        best_prob = max(makuri_prob, sashi_prob, makuri_sashi_prob)
        if best_prob < 18:
            continue

        others = [p["boat_number"] for p in scored_players if p["boat_number"] != boat_no]
        focus_2nd = others[0] if others else "-"
        focus_3rd = others[1] if len(others) > 1 else "-"

        label_parts = []
        if makuri_prob >= 22:
            label_parts.append(f"捲り {makuri_prob:.0f}%")
        if sashi_prob >= 20:
            label_parts.append(f"差し {sashi_prob:.0f}%")
        if makuri_sashi_prob >= 20 and not label_parts:
            label_parts.append(f"捲り差し {makuri_sashi_prob:.0f}%")

        if not label_parts:
            continue

        scenarios.append({
            "boat_number": boat_no,
            "racer_name": racer_name,
            "course": course,
            "makuri_prob": round(makuri_prob, 1),
            "sashi_prob": round(sashi_prob, 1),
            "makuri_sashi_prob": round(makuri_sashi_prob, 1),
            "best_prob": round(best_prob, 1),
            "scenario_label": " | ".join(label_parts),
            "focus": f"{boat_no}-{focus_2nd}-{focus_3rd}",
            "alert": best_prob >= 38,
        })

    scenarios.sort(key=lambda x: x["best_prob"], reverse=True)
    return scenarios


# AI解析結果のサーバー側キャッシュ
# { race_id: { "hash": "data_hash", "results": { ...AI成果物... } } }
AI_RESULT_CACHE = {}

def compute_settings_hash(weights: CustomWeights, settings: PredictSettings) -> str:
    """予測設定（重み、モード、券種等）をハッシュ化する"""
    s_dict = {
        "w": weights.dict(),
        "m": settings.max_items,
        "bt": settings.bet_type,
        "f1": settings.fixed_1st,
        "ht": settings.hit_type,
        "am": settings.ai_prediction_mode,
        "cm": settings.custom_prediction_mode
    }
    s_json = json.dumps(s_dict, sort_keys=True)
    return hashlib.mdsafe_hex_digest(s_json.encode()).hexdigest() if hasattr(hashlib, 'mdsafe_hex_digest') else hashlib.md5(s_json.encode()).hexdigest()

def compute_players_hash(race_data: dict, players_data: List[dict], weights: CustomWeights = None, settings: PredictSettings = None) -> str:
    """AIの計算に影響する項目のみを抽出してハッシュ化する"""
    # 会場や風、波など、レース全体の条件をベースに含める
    relevant_context = {
        "place": race_data.get("place_name"),
        "weather": race_data.get("weather"),
        "wd": race_data.get("wind_direction"),
        "ws": race_data.get("wind_speed"),
        "wv": race_data.get("wave_height")
    }
    
    relevant_players = []
    # 艇番、展示タイム、ST、進入、チルト、欠場、および「選手の能力値」をハッシュに含める
    for p in sorted(players_data, key=lambda x: x.get('boat_number', 0)):
        relevant_players.append({
            "b": p.get("boat_number"),
            "e": p.get("exhibition_time"),
            "s": p.get("start_timing"),
            "c": p.get("entry_course"),
            "t": p.get("tilt"),
            "a": p.get("is_absent"),
            "pe": p.get("parts_exchange"),
            "wa": p.get("weight_adjustment"),
            "pr": p.get("propeller"),
            "rid": p.get("racer_id"),
            "gwr": p.get("global_win_rate"),
            "lwr": p.get("local_win_rate")
        })
    
    # ユーザー設定（重みや買い目種別、軸固定など）もハッシュに含めることで、設定変更時に再計算させる
    w_data = weights.dict() if weights and hasattr(weights, 'dict') else weights
    s_data = settings.dict() if settings and hasattr(settings, 'dict') else settings

    full_data = {"ctx": relevant_context, "pls": relevant_players, "w": w_data, "s": s_data}
    return hashlib.md5(json.dumps(full_data, sort_keys=True).encode()).hexdigest()

def calculate_predictions(race_data, players_data, weights: CustomWeights, settings: PredictSettings = None, ai_cache: dict = None):
    if settings is None:
        settings = PredictSettings()

    # シード値を固定 (race_id) して結果のジッターを防ぐ
    race_id = race_data.get("id", 0)
    rng = random.Random(race_id)

    scored_players = []
    active_players_for_sim = []

    # 伸び足・出足判定用の事前ランキング計算
    active_for_rank = [p for p in players_data if not p.get("is_absent")]
    ex_times = [(p["boat_number"], p.get("exhibition_time") if p.get("exhibition_time") is not None else 6.80) for p in active_for_rank]
    ex_times.sort(key=lambda x: x[1])
    ex_rank_map = {b: i+1 for i, (b, t) in enumerate(ex_times)}
    
    g_wins = [(p["boat_number"], p.get("global_win_rate") if p.get("global_win_rate") is not None else 4.0) for p in active_for_rank]
    g_wins.sort(key=lambda x: x[1], reverse=True)
    win_rank_map = {b: i+1 for i, (b, w) in enumerate(g_wins)}

    for row in players_data:
        p = dict(row)
        
        # 伸び足・出足のタグ情報（フロント用）
        p["leg_type"] = ""
        p["leg_type_color"] = ""

        if p.get("is_absent"):
            p["rule_score"] = -1.0
            p["rule_mark"] = "欠"
            p["ai_score"] = -1.0
            p["ai_mark"] = "欠場"
            p["calc_course"] = p.get("entry_course") or p["boat_number"]
            p["calc_ex"] = 6.80
            p["calc_st"] = 0.15
            scored_players.append(p)
            continue
        
        # ── 以下、欠場でない艇の計算 ──
        active_players_for_sim.append(p)

        ex_time = p.get("exhibition_time") if p.get("exhibition_time") is not None else 6.80
        # ★ ST=0.0 は有効値なので None のみデフォルト補填
        st_time = p.get("start_timing")
        if st_time is None:
            st_time = 0.15
        course = p.get("entry_course") if p.get("entry_course") is not None else p["boat_number"]
        g_win = p.get("global_win_rate") if p.get("global_win_rate") is not None else 4.0
        m_rate = p.get("motor_2_quinella") if p.get("motor_2_quinella") is not None else 30.0
        tilt = p.get("tilt") if p.get("tilt") is not None else 0.0

        tilt_adj = tilt * 2.5
        course_score = max(0, 7 - course) * 3.0 * weights.course
        win_score = g_win * 10.0 * weights.win_rate
        motor_score = m_rate * 0.5 * weights.motor
        exhibition_score = max(0, (7.0 - ex_time) * 50) * weights.exhibition
        st_score = max(0, (0.3 - st_time) * 100) * weights.st
        
        # ── 風の影響計算 ──
        wind_adj = 0.0
        # ユーザー要望: 展示情報が入っていない場合は風を考慮しない
        # STが0.15以外、または展示タイムが6.80以外を「展示あり」と簡易判定（あるいは None チェック）
        has_exhibition = (p.get("start_timing") is not None) or (p.get("exhibition_time") is not None)

        if has_exhibition and weights.wind > 0:
            w_speed = float(race_data.get("wind_speed") or 0)
            w_dir = race_data.get("wind_direction") or ""
            
            if "追い風" in w_dir:
                if w_speed <= 4:
                    if course == 1: wind_adj = 2.5
                    elif course == 2: wind_adj = 1.0
                else:
                    if course == 1: wind_adj = -4.0
                    elif course == 2: wind_adj = 5.0
                    elif course in [3, 4]: wind_adj = 3.0
            elif "向かい風" in w_dir:
                if w_speed <= 4:
                    if course in [3, 4]: wind_adj = 3.0
                    if course == 1: wind_adj = -0.5
                else:
                    if course in [3, 4]: wind_adj = 10.0
                    if course in [5, 6]: wind_adj = 6.0
                    if course == 1: wind_adj = -5.0
            elif "右横風" in w_dir or "右斜め" in w_dir:
                if course == 2: wind_adj = 3.0
                if course == 1: wind_adj = -1.5
            elif "左横風" in w_dir or "左斜め" in w_dir:
                if course in [3, 4, 5]: wind_adj = 2.0
                if course == 1: wind_adj = -2.0
            elif w_dir:
                if course == 1: wind_adj = -1.0
        
        # ── 部品交換・重量などの影響計算 ──
        parts_exchange = p.get("parts_exchange") or ""
        weight_adj = p.get("weight_adjustment") or 0.0
        propeller = p.get("propeller") or ""
        
        parts_adj = 0.0
        parts_tags = []
        if "リング" in parts_exchange:
            parts_adj += 3.0
            parts_tags.append("リング交換")
        if "ピストン" in parts_exchange:
            parts_adj += 2.0
            parts_tags.append("ピストン交換")
        if "シリンダ" in parts_exchange:
            parts_adj += 4.0
            parts_tags.append("シリンダ交換")
        if "キャリア" in parts_exchange:
            parts_adj += 2.0
        if "ギヤ" in parts_exchange:
            parts_adj += 2.0
        if "新" in propeller:
            parts_adj += 2.5
            parts_tags.append("新プロペラ")
            
        p["parts_tag_str"] = " ".join(parts_tags)
        
        # ── 伸び足・出足の判定 ──
        ex_rank = ex_rank_map.get(p["boat_number"], 6)
        win_rank = win_rank_map.get(p["boat_number"], 6)
        
        if ex_rank <= 2 and win_rank >= 4:
            p["leg_type"] = "伸び足特化"
            p["leg_type_color"] = "text-amber-500 font-bold"
        elif ex_rank >= 4 and win_rank <= 2:
            p["leg_type"] = "出足型警戒"
            p["leg_type_color"] = "text-indigo-400 font-bold"
        elif ex_rank <= 2 and win_rank <= 2:
            p["leg_type"] = "超抜クラス"
            p["leg_type_color"] = "text-red-500 font-bold"

        rule_score = course_score + win_score + motor_score + exhibition_score + st_score + tilt_adj + (wind_adj * weights.wind) + parts_adj

        # AIの計算 (キャッシュがあれば優先的に使用)
        race_id = race_data.get("id", 0)
        p_hash = compute_players_hash(race_data, players_data, weights, settings)
        
        # 外部から渡されたai_cache、あるいはサーバー側のグローバルキャッシュを確認
        effective_ai_cache = ai_cache
        if not effective_ai_cache and race_id in AI_RESULT_CACHE:
            if AI_RESULT_CACHE[race_id]["hash"] == p_hash:
                effective_ai_cache = AI_RESULT_CACHE[race_id]["results"]

        if effective_ai_cache and "ai_player_data" in effective_ai_cache:
            p_cache = effective_ai_cache["ai_player_data"].get(str(p["boat_number"]), {})
            ai_base = p_cache.get("ai_base", 40.0)
            ai_score = p_cache.get("ai_score", 40.0)
            p["ai_mark"] = p_cache.get("ai_mark", "")
        else:
            # 選手の勝率計算: 当地勝率が0の場合は全国勝率を使用、両方0の場合はデフォルト3.5
            l_win = p.get("local_win_rate") or 0.0
            g_win_raw = p.get("global_win_rate") or 0.0
            effective_win = l_win if l_win > 0 else (g_win_raw if g_win_raw > 0 else 3.5)
            
            ai_base = (effective_win * 13 * weights.win_rate) + (m_rate * 0.75 * weights.motor)
            # 1コース偏重をわずかに緩和 (45 -> 38) し、実力差を反映しやすくする
            course_bonus = {1: 38, 2: 24, 3: 15, 4: 10, 5: 5, 6: 0}.get(course, 0)
            ai_base += (course_bonus * weights.course)

            # ── 会場特性による補正 ──
            place_name = race_data.get("place_name", "")
            if course == 1:
                # インが極めて強い会場
                if place_name in ["徳山", "大村", "下関"]:
                    ai_base += 6.0
                # インが比較的強い会場
                elif place_name in ["芦屋", "尼崎", "常滑", "若松"]:
                    ai_base += 3.0
                # インが弱く荒れやすい会場
                elif place_name in ["戸田", "江戸川", "平和島"]:
                    ai_base -= 6.0
                # インがやや弱め
                elif place_name in ["鳴門", "多摩川"]:
                    ai_base -= 2.5

            # 微小なジッターを加えてベーススコア自体に揺らぎを持たせる
            ai_base += rng.gauss(0, 1.5)

            # ── 風の影響をAI基礎点にも反映 ──
            if has_exhibition and weights.wind > 0:
                ai_base += (wind_adj * weights.wind * 2.0)
                
            # ── 部品交換・伸び足等のAI評価付加 ──
            if p["leg_type"] == "伸び足特化":
                ai_base += 2.0
            elif p["leg_type"] == "出足型警戒":
                ai_base += 2.0
            elif p["leg_type"] == "超抜クラス":
                ai_base += 4.0
                
            if "リング" in parts_exchange:
                ai_base += 4.0
            if "キャリア" in parts_exchange or "シリンダ" in parts_exchange:
                ai_base += 4.0
            if "新" in propeller:
                ai_base += 2.5

            if course >= 4 and st_time < 0.12:
                ai_base += (15 * weights.st)
            if tilt >= 1.0 and course >= 3:
                ai_base += (tilt * 4 * weights.motor) # モータ/チルト関連
            
            ai_base += max(0, (7.0 - ex_time) * 30 * weights.exhibition)
            ai_score = round(ai_base + rng.uniform(-2, 2), 2)
            p["ai_mark"] = "" # Will fill later

        p["rule_score"] = round(rule_score, 2)
        p["ai_base"] = ai_base
        p["ai_score"] = ai_score
        p["calc_st"] = st_time
        p["calc_course"] = course
        p["calc_ex"] = ex_time
        p["tilt"] = tilt
        scored_players.append(p)

    if not scored_players:
        empty = {"active": False, "probability": 0, "text": "", "focus": ""}
        return [], {"rule_focus": [], "ai_focus": [], "scenario": empty, "scenarios": []}

    marks = ["◎", "〇", "▲", "△", "×", ""]

    scored_players.sort(key=lambda x: x["rule_score"], reverse=True)
    for i, p in enumerate(scored_players):
        p["rule_mark"] = marks[i] if i < len(marks) else ""

    if not ai_cache or "ai_player_data" not in ai_cache:
        scored_players.sort(key=lambda x: x["ai_score"], reverse=True)
        for i, p in enumerate(scored_players):
            p["ai_mark"] = marks[i] if i < len(marks) else ""

    scored_players.sort(key=lambda x: x["boat_number"])

    # シナリオ、モンテカルロ等のAI関連情報の取得
    race_id = race_data.get("id", 0)
    p_hash = compute_players_hash(race_data, players_data)
    
    effective_ai_cache = ai_cache
    if not effective_ai_cache and race_id in AI_RESULT_CACHE:
        if AI_RESULT_CACHE[race_id]["hash"] == p_hash:
            effective_ai_cache = AI_RESULT_CACHE[race_id]["results"]

    if effective_ai_cache:
        scenarios = effective_ai_cache.get("scenarios", [])
        ai_win_probs = effective_ai_cache.get("ai_win_probs", {})
        # パターンカウントからの生成
        ai_pattern_counts = {}
        for pat_str, count in effective_ai_cache.get("ai_pattern_counts_list", []):
            try:
                tup = tuple(int(x) for x in pat_str.split(','))
                ai_pattern_counts[tup] = count
            except: pass
        NUM_SIMS = effective_ai_cache.get("num_sims", 10000)
    else:
        scenarios = calculate_scenarios(scored_players)
        
        # 欠場艇を除外してシミュレーション
        active_for_sim = [p for p in scored_players if not p.get("is_absent")]
        NUM_SIMS = 10000
        ai_wins = {p["boat_number"]: 0 for p in scored_players}
        ai_pattern_counts = {}

        # 抽出による高速化
        sim_boats = [p["boat_number"] for p in active_for_sim]
        sim_bases = [p["ai_base"] for p in active_for_sim]
        num_active = len(active_for_sim)
        gauss = rng.gauss

        for _ in range(NUM_SIMS):
            # dict lookupやループ内の属性アクセスを回避して高速化
            sim_scores = [(sim_boats[i], sim_bases[i] + gauss(0, 25.0)) for i in range(num_active)]
            sim_scores.sort(key=lambda x: x[1], reverse=True)
            
            # 走っている艇数に応じて安全に取得
            top_boats = [s[0] for s in sim_scores]
            top1 = top_boats[0] if len(top_boats) > 0 else 0
            top2 = top_boats[1] if len(top_boats) > 1 else 0
            top3 = top_boats[2] if len(top_boats) > 2 else 0

            if top1:
                ai_wins[top1] += 1
            
            if top1 and top2 and top3:
                trio = (top1, top2, top3)
                ai_pattern_counts[trio] = ai_pattern_counts.get(trio, 0) + 1

        ai_win_probs = {str(b): round((count / NUM_SIMS)*100, 1) for b, count in ai_wins.items()}

        # 計算結果をサーバーキャッシュに保存（以降の設定変更時に再利用）
        AI_RESULT_CACHE[race_id] = {
            "hash": p_hash,
            "results": {
                "ai_player_data": {str(p["boat_number"]): {"ai_base": p["ai_base"], "ai_score": p["ai_score"], "ai_mark": p["ai_mark"]} for p in scored_players},
                "scenarios": scenarios,
                "ai_win_probs": ai_win_probs,
                "ai_pattern_counts_list": [[",".join(map(str, k)), v] for k, v in ai_pattern_counts.items()],
                "num_sims": NUM_SIMS
            }
        }

    legacy_scenario = {"active": False, "probability": 0, "text": "", "focus": ""}
    if scenarios:
        top = scenarios[0]
        legacy_scenario = {
            "active": True,
            "probability": top["best_prob"],
            "text": (
                f"【展開注目】{top['boat_number']}号艇（{top['racer_name']}）"
                f" {top['scenario_label']}の可能性あり"
            ),
            "focus": top["focus"],
        }

    def get_probabilities(score_key):
        max_score = max(p[score_key] for p in scored_players)
        exps = [math.exp((p[score_key] - max_score) / 50.0) for p in scored_players]
        s = sum(exps)
        return {p["boat_number"]: e / s for p, e in zip(scored_players, exps)}

    rule_probs = get_probabilities("rule_score")

    def generate_rule_combinations(boat_probs, bet_type, max_items, fixed_head=0):
        boats = list(boat_probs.keys())
        results = []
        if bet_type == "3連単":
            for c in itertools.permutations(boats, 3):
                if fixed_head and c[0] != fixed_head: continue
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                p3 = boat_probs[c[2]] / (1 - p1 - boat_probs[c[1]] + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}-{c[2]}", "prob": p1*p2*p3})
        elif bet_type == "3連複":
            for c in itertools.combinations(boats, 3):
                if fixed_head and fixed_head not in c: continue
                total = 0.0
                for perm in itertools.permutations(c, 3):
                    p1 = boat_probs[perm[0]]; p2 = boat_probs[perm[1]] / (1 - p1 + 1e-9)
                    p3 = boat_probs[perm[2]] / (1 - p1 - boat_probs[perm[1]] + 1e-9)
                    total += p1*p2*p3
                results.append({"pattern": f"{c[0]}={c[1]}={c[2]}", "prob": total})
        elif bet_type == "2連単":
            for c in itertools.permutations(boats, 2):
                if fixed_head and c[0] != fixed_head: continue
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}", "prob": p1*p2})
        elif bet_type == "2連複":
            for c in itertools.combinations(boats, 2):
                if fixed_head and fixed_head not in c: continue
                total = sum(boat_probs[p[0]] * boat_probs[p[1]] / (1 - boat_probs[p[0]] + 1e-9)
                            for p in itertools.permutations(c, 2))
                results.append({"pattern": f"{c[0]}={c[1]}", "prob": total})
        elif bet_type == "単勝":
            for b in boats:
                if fixed_head and b != fixed_head: continue
                results.append({"pattern": str(b), "prob": boat_probs[b]})
        else:
            for c in itertools.permutations(boats, 3):
                if fixed_head and c[0] != fixed_head: continue
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                p3 = boat_probs[c[2]] / (1 - p1 - boat_probs[c[1]] + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}-{c[2]}", "prob": p1*p2*p3})
        results.sort(key=lambda x: x["prob"], reverse=True)
        
        # モード（本命・中穴・大穴）に応じたスライス
        # 0: 1-8位, 1: 9-16位, 2: 17-24位 (max_items=8の場合)
        mode = settings.custom_prediction_mode
        start_idx = mode * settings.max_items
        end_idx = start_idx + settings.max_items
        
        sliced_results = results[start_idx : end_idx]
        
        return [{"pattern": r["pattern"], "prob": round(r["prob"]*100, 1)} for r in sliced_results]

    def generate_ai_combinations(pattern_counts, bet_type, max_items, total_sims, fixed_head=0):
        # AIの結果から確率を計算
        prob_map = {}
        boats = [p["boat_number"] for p in scored_players]

        # 0回でも確実に8点を出すため、ベースとして全ての対象組み合わせを0回で初期化
        def prepopulate():
            if bet_type == "3連単":
                for c in itertools.permutations(boats, 3):
                    if fixed_head and c[0] != fixed_head: continue
                    prob_map[f"{c[0]}-{c[1]}-{c[2]}"] = 0
            elif bet_type == "3連複":
                for c in itertools.combinations(boats, 3):
                    if fixed_head and fixed_head not in c: continue
                    prob_map[f"{c[0]}={c[1]}={c[2]}"] = 0
            elif bet_type == "2連単":
                for c in itertools.permutations(boats, 2):
                    if fixed_head and c[0] != fixed_head: continue
                    prob_map[f"{c[0]}-{c[1]}"] = 0
            elif bet_type == "2連複":
                for c in itertools.combinations(boats, 2):
                    if fixed_head and fixed_head not in c: continue
                    prob_map[f"{c[0]}={c[1]}"] = 0
            elif bet_type == "単勝":
                for b in boats:
                    if fixed_head and b != fixed_head: continue
                    prob_map[str(b)] = 0
            else:
                for c in itertools.permutations(boats, 3):
                    if fixed_head and c[0] != fixed_head: continue
                    prob_map[f"{c[0]}-{c[1]}-{c[2]}"] = 0

        prepopulate()

        for (b1, b2, b3), count in pattern_counts.items():
            if bet_type == "3連単":
                if fixed_head and b1 != fixed_head: continue
                pat = f"{b1}-{b2}-{b3}"
                prob_map[pat] = prob_map.get(pat, 0) + count
            elif bet_type == "3連複":
                c = tuple(sorted([b1, b2, b3]))
                if fixed_head and fixed_head not in c: continue
                pat = f"{c[0]}={c[1]}={c[2]}"
                prob_map[pat] = prob_map.get(pat, 0) + count
            elif bet_type == "2連単":
                if fixed_head and b1 != fixed_head: continue
                pat = f"{b1}-{b2}"
                prob_map[pat] = prob_map.get(pat, 0) + count
            elif bet_type == "2連複":
                c = tuple(sorted([b1, b2]))
                if fixed_head and fixed_head not in c: continue
                pat = f"{c[0]}={c[1]}"
                prob_map[pat] = prob_map.get(pat, 0) + count
            elif bet_type == "単勝":
                if fixed_head and b1 != fixed_head: continue
                pat = str(b1)
                prob_map[pat] = prob_map.get(pat, 0) + count
            else:
                if fixed_head and b1 != fixed_head: continue
                pat = f"{b1}-{b2}-{b3}"
                prob_map[pat] = prob_map.get(pat, 0) + count
        
        results = [{"pattern": pat, "prob": round((cnt / total_sims) * 100, 1)} for pat, cnt in prob_map.items()]
        results.sort(key=lambda x: x["prob"], reverse=True)
        
        # モードに応じたランク帯のスライス (AI用)
        mode = settings.ai_prediction_mode
        start_idx = mode * settings.max_items
        end_idx = start_idx + settings.max_items
        
        return results[start_idx : end_idx]

    predictions = {
        "rule_focus": generate_rule_combinations(rule_probs, settings.bet_type, settings.max_items, 0),
        "ai_focus": generate_ai_combinations(ai_pattern_counts, settings.bet_type, settings.max_items, NUM_SIMS, settings.fixed_1st),
        "ai_default_focus": generate_ai_combinations(ai_pattern_counts, "3連単", 8, NUM_SIMS, 0),
        "ai_win_probs": ai_win_probs,
        "scenario": legacy_scenario,
        "scenarios": scenarios,
        "ai_pattern_counts_list": [[",".join(map(str, k)), v] for k, v in ai_pattern_counts.items()],
        "num_sims": NUM_SIMS
    }
    return scored_players, predictions


def _sync_save_result(race_id, result):
    """取得した結果をDBに保存するヘルパー"""
    conn = get_db_connection()
    try:
        ranking_str = result.get("ranking_str", "")
        result_json = json.dumps(result)
        conn.execute('UPDATE races SET is_finished = 1, ranking_str = ?, result_json = ? WHERE id = ?', 
                     (ranking_str, result_json, race_id))
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] _sync_save_result: {e}")
    finally:
        conn.close()


def _sync_save_odds(race_id, odds_cache):
    """取得したオッズキャッシュをDBに保存するヘルパー"""
    conn = get_db_connection()
    try:
        conn.execute('UPDATE races SET odds_json = ? WHERE id = ?', (json.dumps(odds_cache), race_id))
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] _sync_save_odds: {e}")
    finally:
        conn.close()


# ─────────────────────────── Race APIs ──────────────────────────────────────

# 選手統計情報用のインメモリキャッシュは上部で定義済み


@app.get("/api/races/{race_id}/options")
def api_get_race_live_data(race_id: int, bet_type: str = Query(default="3t")):
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date, is_finished, result_json, odds_json, scheduled_time, '
        'weather, wind_direction, wind_speed, wave_height FROM races WHERE id = ?', (race_id,)
    ).fetchone()

    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    
    date_str = race["race_date"].replace("-", "")
    
    # すでに終了して結果が保存されていればそれを優先的に取得するが、オッズ取得も並行して試行する
    result = None
    if race["is_finished"] and race["result_json"]:
        try:
            result = json.loads(race["result_json"])
        except:
            pass

    # 結果が未保存、または終了していても結果詳細がない場合は再取得
    if not result:
        # 同期的に取得を試みる (ユーザーがこのレースを見ているため優先)
        result = fetch_match_result(race["place_code"], race["race_number"], date_str)
        if result and result.get("finished"):
            _sync_save_result(race_id, result)
        elif not result:
            result = {"finished": False, "error": "No result available yet"}

    # オッズ取得 (bet_type 対応。終了していても最終オッズとして取得を試みる)
    # 日本語の券種名が来た場合の補正
    type_map = { '3連単': '3t', '3連複': '3f', '2連単': '2t', '2連複': '2f', '単勝': '1t', '複勝': '1f' }
    bt_code = type_map.get(bet_type, bet_type.lower())

    odds = None
    odds_cache = {}
    if race["odds_json"]:
        try:
            odds_cache = json.loads(race["odds_json"])
            if bt_code in odds_cache:
                odds = odds_cache[bt_code]
        except Exception as e:
            print(f"Error parsing odds_cache for race {race_id}: {e}")
    
    if not odds:
        print(f"Fetching odds from web for race {race_id} ({bt_code})...")
        odds = fetch_all_odds(race["place_code"], race["race_number"], date_str, bt_code)
        # 終了している場合は永続化
        if race["is_finished"] and odds and "error" not in odds:
            odds_cache[bt_code] = odds
            _sync_save_odds(race_id, odds_cache)
    
    return {
        "result": result, 
        "odds": odds, 
        "debug_bt": bt_code,
        "weather": race["weather"],
        "wind_direction": race["wind_direction"],
        "wind_speed": race["wind_speed"],
        "wave_height": race["wave_height"]
    }



def is_hit(pattern: str, result_text: str) -> bool:
    if not result_text or result_text == '--':
        return False
    r = result_text.split('-')
    if len(r) < 3:
        return False
    p = pattern
    if '-' in p and len(p.split('-')) == 3:
        # 3連単
        return r[0] == p.split('-')[0] and r[1] == p.split('-')[1] and r[2] == p.split('-')[2]
    elif '=' in p:
        pts = p.split('=')
        if len(pts) == 3:
            return pts[0] in r[0:3] and pts[1] in r[0:3] and pts[2] in r[0:3]
        elif len(pts) == 2:
            return pts[0] in r[0:2] and pts[1] in r[0:2]
    elif '-' in p and len(p.split('-')) == 2:
        # 2連単
        return r[0] == p.split('-')[0] and r[1] == p.split('-')[1]
    else:
        # 単勝
        return r[0] == p
    return False

@app.post("/api/daily_hits")
def get_daily_hits(req: PredictRequest, date: str = Query(...)):
    now = time.time()
    s_hash = compute_settings_hash(req.weights, req.settings)
    cache_key = f"{date}_{s_hash}"
    
    # キャッシュ確認
    if cache_key in DAILY_HITS_CACHE:
        entry = DAILY_HITS_CACHE[cache_key]
        if now - entry["ts"] < DAILY_HITS_CACHE_TTL:
            return entry["data"]

    conn = get_db_connection()
    try:
        races = conn.execute(
            'SELECT * FROM races WHERE race_date = ? AND is_finished = 1 AND ranking_str IS NOT NULL AND ranking_str != ""',
            (date,)
        ).fetchall()
        if not races:
            return {}
        
        race_ids = [r['id'] for r in races]
        if not race_ids:
            return {}
        placeholders = ','.join('?' for _ in race_ids)
        entries = conn.execute(
            f'SELECT * FROM entries WHERE race_id IN ({placeholders}) ORDER BY race_id, boat_number',
            race_ids
        ).fetchall()
    finally:
        conn.close()

    entries_by_race = {}
    for e in entries:
        entries_by_race.setdefault(e['race_id'], []).append(dict(e))
        
    hits = {}
    for race in races:
        rid = race['id']
        players = entries_by_race.get(rid, [])
        if not players:
            hits[rid] = False
            continue
            
        rdict = dict(race)
        _, preds = calculate_predictions(rdict, players, req.weights, req.settings)
        
        ranking = rdict.get("ranking_str", "")
        hit = False
        ht = req.settings.hit_type
        hit_count = 0
        if ht == "custom":
            hit_count = 1 if any(is_hit(p["pattern"], ranking) for p in preds["rule_focus"]) else 0
        elif ht == "ai":
            hit_count = 1 if any(is_hit(p["pattern"], ranking) for p in preds["ai_focus"]) else 0
        else: # "both" or "buy"
            # AIとカスタムを個別に的中判定し、的中したセット数をカウント（両方の場合は2倍の払戻）
            hit_custom = 1 if any(is_hit(p["pattern"], ranking) for p in preds["rule_focus"]) else 0
            hit_ai = 1 if any(is_hit(p["pattern"], ranking) for p in preds["ai_focus"]) else 0
            hit_count = hit_custom + hit_ai
            
        hit = (hit_count > 0)
        
        # 払戻金の抽出
        payout = 0
        if hit and rdict.get("result_json"):
            try:
                res_obj = json.loads(rdict["result_json"])
                payouts = res_obj.get("payouts", [])
                target_type = req.settings.bet_type # 例: "3連単"
                for p in payouts:
                    if p.get("type") == target_type:
                        p_str = p.get("payout", "0").replace(",", "").replace("円", "")
                        p_int = int(p_str)
                        # 的中したセット数分だけ払戻金を計上
                        payout = p_int * hit_count
                        break
            except:
                pass

        hits[rid] = {
            "hit": hit,
            "payout": payout,
            "ranking": ranking,
            "place": rdict.get("place_name")
        }
        
    DAILY_HITS_CACHE[cache_key] = {"data": hits, "ts": now}
    return hits

@app.get("/api/races/{race_id}/odds")
def api_get_race_odds(race_id: int, bet_type: str = Query(default="3t"), user_status: dict = Depends(require_access)):
    """指定賭式オッズ取得。bet_type: 3t/3f/2t/2f/1t"""
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date, is_finished, odds_json FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    date_str = race["race_date"].replace("-", "")
    
    # DBキャッシュ確認
    odds = None
    odds_cache = {}
    if race["odds_json"]:
        try:
            odds_cache = json.loads(race["odds_json"])
            if bet_type in odds_cache:
                odds = odds_cache[bet_type]
        except:
            pass
            
    if not odds:
        odds = fetch_all_odds(race["place_code"], race["race_number"], date_str, bet_type)
        if race["is_finished"] and odds and "error" not in odds:
            odds_cache[bet_type] = odds
            conn = get_db_connection()
            try:
                conn.execute('UPDATE races SET odds_json = ? WHERE id = ?', (json.dumps(odds_cache), race_id))
                conn.commit()
            finally:
                conn.close()
                
    return odds


@app.get("/api/races/{race_id}/weather")
def api_get_race_weather(race_id: int, user_status: dict = Depends(require_access)):
    """展示情報ページから最新の気象・チルト情報を取得"""
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    date_str = race["race_date"].replace("-", "")
    data = fetch_exhibition_data(race["place_code"], race["race_number"], date_str)
    weather_info = data.get("weather_info", {})
    if weather_info:
        conn = get_db_connection()
        try:
            conn.execute(
                'UPDATE races SET weather=?, wind_direction=?, wind_speed=?, wave_height=? WHERE id=?',
                (weather_info.get("weather"), weather_info.get("wind_direction"),
                 weather_info.get("wind_speed"), weather_info.get("wave_height"), race_id)
            )
            conn.commit()
        finally:
            conn.close()
    tilt_info = _get_tilt_info(race_id)
    return {"weather_info": weather_info, "tilt_info": tilt_info}


def _get_tilt_info(race_id: int) -> dict:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            'SELECT boat_number, tilt FROM entries WHERE race_id = ? ORDER BY boat_number',
            (race_id,)
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    result = {}
    for row in rows:
        try:
            bn = row["boat_number"]
            tilt = row["tilt"] if row["tilt"] is not None else 0.0
        except (TypeError, KeyError):
            continue
        result[str(bn)] = tilt
    return result


@app.get("/api/races/{race_id}")
def get_race_detail(race_id: int, user_status: dict = Depends(require_access)):
    return get_custom_predict(race_id, PredictRequest(weights=CustomWeights(), settings=PredictSettings()))


@app.post("/api/races/{race_id}/predict")
def get_custom_predict(race_id: int, req: PredictRequest, user_status: dict = Depends(require_access)):
    conn = get_db_connection()
    try:
        race = conn.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
        if not race:
            raise HTTPException(status_code=404, detail="Race not found")
        players = conn.execute(
            'SELECT * FROM entries WHERE race_id = ? ORDER BY boat_number', (race_id,)
        ).fetchall()

        race_dict = dict(race)
        
        # レース終了済みにも関わらず結果が不完全（"--" や 3着分揃っていない等）な場合は再取得を試行
        ranking_str = race_dict.get("ranking_str") or ""
        is_incomplete = not ranking_str or ranking_str == "--" or ranking_str.count("-") < 2
        
        # 保存済みの結果データを精査し、選手名に金額や不要な文字列が混じっている場合も再取得対象にする
        if not is_incomplete and race_dict.get("result_json"):
            try:
                import json
                res_data = json.loads(race_dict["result_json"])
                rank_list = res_data.get("ranking", [])
                for r in rank_list:
                    nm = str(r.get("name", ""))
                    # 金額記号や「単勝」「複勝」などの文字列が含まれていれば異常と判定
                    if "¥" in nm or "￥" in nm or "円" in nm or "単勝" in nm or "複勝" in nm:
                        is_incomplete = True
                        break
                # また、着順リストが異常に多い（10行以上など）場合も異常
                if len(rank_list) > 9:
                    is_incomplete = True
            except:
                pass
        
        if race_dict.get("is_finished") and is_incomplete:
            print(f"[REPAIR] Incomplete result for finished race {race_id} ('{ranking_str}'). Updating...")
            try:
                dt_obj = datetime.strptime(race_dict["race_date"], '%Y-%m-%d').replace(tzinfo=JST)
                scraper.update_result(race_dict["place_code"], race_dict["race_number"], dt_obj)
                # 最新の状態を再取得
                race = conn.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
                race_dict = dict(race)
            except Exception as e:
                print(f"[REPAIR ERROR] Failed to repair race {race_id}: {e}")
        
        # 優先読み込み: 出走表データ（entries）がない場合は即時取得
        if not players:
            print(f"[PRIORITY] Missing entries for race {race_id}. Scraping now...")
            scraper.scrape_race_syusso(race_dict["place_code"], race_dict["race_number"], 
                                      datetime.strptime(race_dict["race_date"], '%Y-%m-%d').replace(tzinfo=JST))
            # 再取得
            players = conn.execute(
                'SELECT * FROM entries WHERE race_id = ? ORDER BY boat_number', (race_id,)
            ).fetchall()
        
        race_dict = dict(race)
        
        # Overrides or Defaults 適用
        scored_players_raw = [dict(p) for p in players]
        
        if req.ignore_exhibition:
            for p in scored_players_raw:
                p["entry_course"] = p["boat_number"]
                p["exhibition_time"] = None 
                p["start_timing"] = None
                p["tilt"] = 0.0
                # 欠場情報もリマインドに基づきリセット
                p["is_absent"] = 0
        if req.overrides:
            for ov in req.overrides:
                for p in scored_players_raw:
                    if p["boat_number"] == ov.boat_number:
                        if ov.entry_course is not None: p["entry_course"] = ov.entry_course
                        if ov.exhibition_time is not None: p["exhibition_time"] = ov.exhibition_time
                        if ov.start_timing is not None: p["start_timing"] = ov.start_timing
                        if ov.tilt is not None: p["tilt"] = ov.tilt
                        if ov.is_absent is not None: p["is_absent"] = 1 if ov.is_absent else 0
                        # 常にこれらを計算用に使用するようにフラグをセット
                        p["calc_course"] = p["entry_course"]
                        p["calc_ex"] = p["exhibition_time"]
                        p["calc_st"] = p["start_timing"]

        # 現時点の選手データ・レース条件 + 重み設定からハッシュを計算
        scored_players_raw = [dict(p) for p in players]
        p_hash = compute_players_hash(race_dict, scored_players_raw, req.weights, req.settings)

        ai_cache = None
        # overrides があっても recalculate_ai が False なら、
        # もしDBにキャッシュがあり、かつハッシュが一致すればそれを利用する（表示用）
        if race_dict.get("ai_predictions_json") and not req.recalculate_ai:
            try:
                temp_cache = json.loads(race_dict["ai_predictions_json"])
                # ハッシュチェック: データが古い、または会場・選手構成が変わった場合は無効化
                stored_hash = temp_cache.get("hash")
                
                # 展示データが新たに更新された場合も無効化
                ex_done_now = race_dict.get("is_exhibition_done")
                ex_done_stored = temp_cache.get("is_exhibition_done")

                if stored_hash != p_hash or (ex_done_now and not ex_done_stored):
                    ai_cache = None  # ハッシュ不一致または展示後に再計算
                else:
                    ai_cache = temp_cache
            except Exception as e:
                print(f"[CACHE DEBUG] Cache parse error: {e}")
                pass

        scored_players, predictions = calculate_predictions(race_dict, scored_players_raw, req.weights, req.settings, ai_cache=ai_cache)

        # 新しく計算した場合（キャッシュがなかった、または無効だった場合）はDBに保存
        if not ai_cache:
            new_ai_cache = {
                "hash": p_hash,
                "is_exhibition_done": race_dict.get("is_exhibition_done"),
                "ai_player_data": {
                    str(p["boat_number"]): {
                        "ai_base": p["ai_base"],
                        "ai_score": p["ai_score"],
                        "ai_mark": p["ai_mark"]
                    } for p in scored_players
                },
                "ai_win_probs": predictions["ai_win_probs"],
                "ai_pattern_counts_list": predictions.get("ai_pattern_counts_list", []),
                "num_sims": predictions.get("num_sims", 10000),
                "scenarios": predictions.get("scenarios", [])
            }
            try:
                conn.execute(
                    'UPDATE races SET ai_predictions_json = ? WHERE id = ?',
                    (json.dumps(new_ai_cache), race_id)
                )
                conn.commit()
            except Exception as e:
                print(f"[CACHE ERROR] Failed to save AI cache: {e}")

        return {
            "race": race_dict,
            "players": scored_players,
            "predictions": predictions,
            "current_weights": req.weights.dict(),
            "current_settings": req.settings.dict(),
        }
    finally:
        conn.close()


@app.get("/api/races/{race_id}/exhibition/scrape")
def scrape_exhibition(race_id: int, user_status: dict = Depends(require_access)):
    """公式サイトから展示情報・気象を自動取得しDBに保存"""
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    date_str = race["race_date"].replace("-", "")
    data = fetch_exhibition_data(race["place_code"], race["race_number"], date_str)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    exhibition = data.get("exhibition", {})
    if not exhibition:
        raise HTTPException(status_code=404, detail="展示データが見つかりませんでした")

    conn = get_db_connection()
    try:
        for boat_number, info in exhibition.items():
            conn.execute(
                'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=?, tilt=?, is_absent=? '
                'WHERE race_id=? AND boat_number=?',
                (info["exhibition_time"], info["start_timing"], info["entry_course"],
                 info.get("tilt", 0.0), 1 if info.get("is_absent") else 0,
                 race_id, int(boat_number))
            )
        weather_info = data.get("weather_info", {})
        if weather_info:
            conn.execute(
                'UPDATE races SET weather=?, wind_direction=?, wind_speed=?, wave_height=?, '
                'is_exhibition_done=1 WHERE id=?',
                (weather_info.get("weather"), weather_info.get("wind_direction"),
                 weather_info.get("wind_speed"), weather_info.get("wave_height"), race_id)
            )
        # 展示データ更新時はAIキャッシュを無効化して再計算させる
        conn.execute('UPDATE races SET ai_predictions_json = NULL WHERE id = ?', (race_id,))
        conn.commit()
    finally:
        conn.close()

    # インメモリキャッシュもクリア
    if race_id in AI_RESULT_CACHE:
        del AI_RESULT_CACHE[race_id]

    race_data = get_race_detail(race_id)
    race_data["scraped_exhibition"] = exhibition
    race_data["scraped_weather"] = data.get("weather_info", {})
    race_data["tilt_info"] = _get_tilt_info(race_id)
    return race_data


@app.post("/api/races/{race_id}/exhibition")
def update_exhibition(race_id: int, updates: List[ExhibitionUpdate], user_status: dict = Depends(require_access)):
    """展示情報を手動更新（チルト含む）し再計算"""
    conn = get_db_connection()
    try:
        for update in updates:
            try:
                conn.execute(
                    'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=?, tilt=?, is_absent=? '
                    'WHERE race_id=? AND boat_number=?',
                    (update.exhibition_time, update.start_timing, update.entry_course,
                     update.tilt if update.tilt is not None else 0.0,
                     1 if update.is_absent else 0,
                     race_id, update.boat_number)
                )
            except Exception:
                conn.execute(
                    'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=?, is_absent=? '
                    'WHERE race_id=? AND boat_number=?',
                    (update.exhibition_time, update.start_timing, update.entry_course,
                     1 if update.is_absent else 0,
                     race_id, update.boat_number)
                )
        conn.commit()
    finally:
        conn.close()
    return get_race_detail(race_id)



@app.get("/api/search/racers")
def search_racers(q: str, user_status: dict = Depends(get_current_user)):
    """選手名または登番で選手を検索し、出場予定レースを返す"""
    conn = get_db_connection()
    try:
        # 1. 選手を検索
        if q.isdigit():
            # 登番で検索
            racer_rows = conn.execute(
                "SELECT DISTINCT racer_id as toban, racer_name as name FROM entries WHERE racer_id = ? "
                "UNION "
                "SELECT DISTINCT racer_id as toban, '' as name FROM racer_results WHERE racer_id = ? LIMIT 10",
                (q, q)
            ).fetchall()
        else:
            # 名前（カタカナまたは漢字）で検索
            racer_rows = conn.execute(
                "SELECT DISTINCT racer_id as toban, racer_name as name FROM entries WHERE racer_name LIKE ? LIMIT 20",
                (f"%{q}%",)
            ).fetchall()
        
        results = []
        now_jst = datetime.now(JST)
        today_iso = now_jst.strftime('%Y-%m-%d')
        tomorrow_iso = (now_jst + timedelta(days=1)).strftime('%Y-%m-%d')
        
        for r in racer_rows:
            toban = r["toban"]
            name = r["name"]
            
            if not toban: continue

            # お気に入り状態の確認
            is_fav = conn.execute("SELECT 1 FROM favorite_racers WHERE toban = ?", (toban,)).fetchone() is not None

            # 名前の補完（entriesにない場合racer_results等から探す）
            if not name:
                name_row = conn.execute("SELECT racer_name FROM entries WHERE racer_id = ? AND racer_name != '' LIMIT 1", (toban,)).fetchone()
                if name_row:
                    name = name_row["racer_name"]
                else:
                    name = "不明"
            
            # 出場予定レースを検索
            scheduled_rows = conn.execute(
                "SELECT r.id, r.race_date, r.place_name, r.race_number, e.boat_number "
                "FROM entries e JOIN races r ON e.race_id = r.id "
                "WHERE e.racer_id = ? AND r.race_date IN (?, ?) "
                "ORDER BY r.race_date, r.race_number",
                (toban, today_iso, tomorrow_iso)
            ).fetchall()
            
            scheduled_races = []
            for sr in scheduled_rows:
                scheduled_races.append({
                    "race_id": sr["id"],
                    "date": sr["race_date"],
                    "place": sr["place_name"],
                    "race_no": sr["race_number"],
                    "boat_no": sr["boat_number"]
                })
            
            # 同一選手が複数行出るのを防ぐ（UNIONでもnameが違うと別行になるため）
            if not any(x["toban"] == toban for x in results):
                results.append({
                    "toban": toban,
                    "name": name,
                    "is_favorite": is_fav,
                    "scheduled_races": scheduled_races
                })
            
        return results
    finally:
        conn.close()

@app.get("/api/search/high_expectation")
def search_high_expectation(user_status: dict = Depends(require_access)):
    """AI予想に基づき、1号艇の勝率が高いなどの『期待値の高いレース』を抽出する"""
    conn = get_db_connection()
    try:
        today_iso = datetime.now(JST).strftime('%Y-%m-%d')
        # 本日の全レースをスキャン
        rows = conn.execute(
            "SELECT id, place_name, race_number, ai_predictions_json, is_finished "
            "FROM races WHERE race_date = ?",
            (today_iso,)
        ).fetchall()
        
        picked = []
        for r in rows:
            ai_data_str = r["ai_predictions_json"]
            if not ai_data_str:
                continue
            
            try:
                ai_data = json.loads(ai_data_str)
                win_probs = ai_data.get("ai_win_probs", {})
                
                # 1号艇の勝率が 70% 以上のものをピックアップ
                prob_1 = win_probs.get("1", 0)
                if prob_1 >= 70:
                    picked.append({
                        "id": r["id"],
                        "place": r["place_name"],
                        "race_no": r["race_number"],
                        "reason": f"1号勝率 {prob_1:.0f}%",
                        "prob": prob_1,
                        "is_finished": bool(r["is_finished"])
                    })
            except:
                continue
        
        # 勝率順にソートして上限件数を返す
        picked.sort(key=lambda x: x["prob"], reverse=True)
        return picked[:15]
    finally:
        conn.close()

@app.get("/api/favorites")
def get_favorites(user_status: dict = Depends(get_current_user)):
    """お気に入り選手の一覧と近日出場予定を返す"""
    conn = get_db_connection()
    try:
        fav_rows = conn.execute("SELECT toban, name FROM favorite_racers ORDER BY created_at DESC").fetchall()
        
        results = []
        now_jst = datetime.now(JST)
        today_iso = now_jst.strftime('%Y-%m-%d')
        tomorrow_iso = (now_jst + timedelta(days=1)).strftime('%Y-%m-%d')
        
        for f in fav_rows:
            toban = f["toban"]
            name = f["name"]
            
            # 出場予定レースを検索
            scheduled_rows = conn.execute(
                "SELECT r.id, r.race_date, r.place_name, r.race_number, e.boat_number "
                "FROM entries e JOIN races r ON e.race_id = r.id "
                "WHERE e.racer_id = ? AND r.race_date IN (?, ?) "
                "ORDER BY r.race_date, r.race_number",
                (toban, today_iso, tomorrow_iso)
            ).fetchall()
            
            scheduled_races = []
            for sr in scheduled_rows:
                scheduled_races.append({
                    "race_id": sr["id"],
                    "date": sr["race_date"],
                    "place": sr["place_name"],
                    "race_no": sr["race_number"],
                    "boat_no": sr["boat_number"]
                })
            
            results.append({
                "toban": toban,
                "name": name,
                "scheduled_races": scheduled_races
            })
            
        return results
    finally:
        conn.close()

@app.post("/api/favorites/toggle")
def toggle_favorite(toban: str = Body(...), name: str = Body(...), active: bool = Body(...), user_status: dict = Depends(get_current_user)):
    """お気に入り登録・解除。解除時は Supabase からも削除を試みる。"""
    conn = get_db_connection()
    try:
        now_str = datetime.now(JST).isoformat()
        if active:
            conn.execute(
                "INSERT OR REPLACE INTO favorite_racers (toban, name, created_at) VALUES (?, ?, ?)",
                (toban, name, now_str)
            )
            from supabase_client import upsert_favorites
            upsert_favorites([{"toban": toban, "name": name, "created_at": now_str}])
        else:
            conn.execute("DELETE FROM favorite_racers WHERE toban = ?", (toban,))
            # Supabase削除 (オプション：完全に同期させたい場合)
            try:
                from supabase_client import get_supabase_client
                get_supabase_client().table("favorite_racers").delete().eq("toban", toban).execute()
            except: pass
            
        conn.commit()
        return {"success": True, "toban": toban, "active": active}
    finally:
        conn.close()

def _sync_racer_profile_to_db(toban: str, data: dict):
    """選手プロフィール情報を SQLite / Supabase に保存・同期する"""
    if not data or "error" in data: return
    
    conn = get_db_connection()
    try:
        now_str = datetime.now(JST).isoformat()
        name = data.get("name", "")
        stats_json = json.dumps(data.get("course_stats", []))
        
        conn.execute(
            "INSERT OR REPLACE INTO racer_profiles (toban, name, course_stats_json, updated_at) VALUES (?, ?, ?, ?)",
            (toban, name, stats_json, now_str)
        )
        conn.commit()
        
        # Supabase同期
        from supabase_client import upsert_racer_profiles
        upsert_racer_profiles([{
            "toban": toban,
            "name": name,
            "course_stats": data.get("course_stats", []),
            "updated_at": now_str
        }])
    except Exception as e:
        print(f"[SYNC ERROR] _sync_racer_profile_to_db: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
