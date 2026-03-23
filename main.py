import math
import itertools
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
import random
import os
import json
import concurrent.futures
import hashlib
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from live_scraper import (
    fetch_racer_profile, fetch_live_odds, fetch_match_result,
    fetch_exhibition_data, fetch_all_odds
)
import scraper
from scraper import scrape_today, update_all_active_races
import time
import asyncio

from app_config import JST, LOCK_FILE, STATIC_DIR
from database import get_db_connection, init_db

app = FastAPI(title="Kyotei Advisor MVP")
init_db()

@app.on_event("startup")
async def startup_event():
    # ロックファイルのクリーンアップ
    if LOCK_FILE.exists():
        try:
            LOCK_FILE.unlink()
        except:
            pass
    
    # バックグラウンドワーカーの起動
    asyncio.create_task(background_worker())

async def background_worker():
    """5分おきに全レース場のデータを自動更新する"""
    print("[SYSTEM] Background worker started.")
    # 初回起動時は少し待機してから開始する（APIリクエスト時の「取得中」表示を避けるため）
    await asyncio.sleep(5)
    
    while True:
        try:
            # 実行
            # loop.run_in_executor を使ってブロッキングなスクレイピングを別スレッドで実行
            loop = asyncio.get_event_loop()
            
            # 当日の更新
            await loop.run_in_executor(None, update_all_active_races)
            
            # 18時以降であれば翌日のデータも自動取得（出走表のみ）
            now_jst = datetime.now(JST)
            if now_jst.hour >= 18:
                tomorrow_dt = now_jst + datetime.timedelta(days=1)
                await loop.run_in_executor(None, update_all_active_races, tomorrow_dt)
                
        except Exception as e:
            print(f"[WORKER ERROR] {e}")
        
        # 300秒（5分）待機
        await asyncio.sleep(300)

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

class ExhibitionUpdate(BaseModel):
    boat_number: int
    exhibition_time: float
    start_timing: float
    entry_course: int
    tilt: Optional[float] = None
    is_absent: bool = False

class PredictSettings(BaseModel):
    max_items: int = 8
    bet_type: str = "3連単"
    fixed_1st: int = 0

class PredictRequest(BaseModel):
    weights: CustomWeights
    settings: PredictSettings = PredictSettings()
    overrides: Optional[List[PlayerOverride]] = None
    recalculate_ai: bool = False


# ─────────────────────────── Places ─────────────────────────────────────────

places_dict_order = [
    "桐生", "戸田", "江戸川", "平和島", "多摩川", "浜名湖",
    "蒲郡", "常滑", "津", "三国", "びわこ", "住之江",
    "尼崎", "鳴門", "丸亀", "児島", "宮島", "徳山",
    "下関", "若松", "芦屋", "福岡", "唐津", "大村"
]

_active_places_cache_time = 0.0
_active_places_cache_jcds = []


@app.get("/api/places")
def get_places(date: Optional[str] = Query(None)):
    global _active_places_cache_time, _active_places_cache_jcds
    now = time.time()
    today_str = date if date else get_today_str()

    conn = get_db_connection()
    try:
        db_places = conn.execute(
            'SELECT DISTINCT place_code FROM races WHERE race_date = ?',
            (today_str,)
        ).fetchall()
        db_place_codes = {row['place_code'] for row in db_places}

        # Fetch all races for today to determine best grade across all race numbers
        all_race_rows = conn.execute(
            'SELECT place_code, place_name, race_title, race_number FROM races WHERE race_date = ?',
            (today_str,)
        ).fetchall()
    finally:
        conn.close()

    def _grade_priority(title: str) -> int:
        """Grade priority: higher = more important. Used to pick best title per meet."""
        if not title:
            return 0
        t = title.upper()
        # SG
        if any(kw in t for kw in ['SG', 'ＳＧ', 'グランプリ', 'ダービー', 'メモリアル', 'チャンピオンシップ', 'オールスター', 'ピーターリング']):
            return 5
        # G1
        if any(g in t for g in ['G1', 'GⅠ', 'G１', 'GI', 'Ｇ１', 'ＧⅠ', 'ＧＩ']) or any(kw in title for kw in ['クラシック', '龍王', '王者']):
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

    # Build grade_map: per place_code, keep the title with highest grade priority
    # and track max race_number separately
    grade_map_by_code: dict = {}
    for row in all_race_rows:
        pc = row['place_code']
        rn = row['race_number'] or 0
        rt = row['race_title'] or ''
        pname = row['place_name']
        if pc not in grade_map_by_code:
            grade_map_by_code[pc] = (rt, rn, pname)
        else:
            existing_title, existing_max, _ = grade_map_by_code[pc]
            best_title = rt if _grade_priority(rt) > _grade_priority(existing_title) else existing_title
            best_max = max(existing_max, rn)
            grade_map_by_code[pc] = (best_title, best_max, pname)

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
        if place_code and place_code in grade_map_by_code:
            race_title, max_race, _ = grade_map_by_code[place_code]

        result.append({
            "place": place,
            "place_code": place_code or '',
            "is_active": is_active,
            "grade": race_title,
            "max_race": max_race,
        })
    return result


@app.get("/api/places/{place_name}/races")
def get_races(place_name: str, date: Optional[str] = Query(None)):
    today_str = date if date else get_today_str()
    conn = get_db_connection()
    races = conn.execute(
        'SELECT id, race_date, place_code, place_name, race_number, race_title, is_finished '
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
        motor = player.get("motor_2_quinella") or 30.0
        ex_time = player["calc_ex"]
        tilt = player.get("tilt") or 0.0

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

def compute_players_hash(players_data: List[dict]) -> str:
    """AIの計算に影響する項目のみを抽出してハッシュ化する"""
    relevant = []
    # 艇番、展示タイム、ST、進入、チルト、欠場状況がAI計算の入力値
    for p in sorted(players_data, key=lambda x: x.get('boat_number', 0)):
        relevant.append({
            "b": p.get("boat_number"),
            "e": p.get("exhibition_time"),
            "s": p.get("start_timing"),
            "c": p.get("entry_course"),
            "t": p.get("tilt"),
            "a": p.get("is_absent")
        })
    return hashlib.md5(json.dumps(relevant, sort_keys=True).encode()).hexdigest()

def calculate_predictions(race_data, players_data, weights: CustomWeights, settings: PredictSettings = None, ai_cache: dict = None):
    if settings is None:
        settings = PredictSettings()

    # シード値を固定 (race_id) して結果のジッターを防ぐ
    race_id = race_data.get("id", 0)
    rng = random.Random(race_id)

    scored_players = []
    active_players_for_sim = []

    for row in players_data:
        p = dict(row)
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

        ex_time = p.get("exhibition_time") or 6.80
        # ★ ST=0.0 は有効値なので None のみデフォルト補填
        st_time = p.get("start_timing")
        if st_time is None:
            st_time = 0.15
        course = p.get("entry_course") or p["boat_number"]
        g_win = p.get("global_win_rate") or 4.0
        m_rate = p.get("motor_2_quinella") or 30.0
        tilt = p.get("tilt") or 0.0

        tilt_adj = tilt * 2.5
        course_score = max(0, 7 - course) * 3.0 * weights.course
        win_score = g_win * 10.0 * weights.win_rate
        motor_score = m_rate * 0.5 * weights.motor
        exhibition_score = max(0, (7.0 - ex_time) * 50) * weights.exhibition
        st_score = max(0, (0.3 - st_time) * 100) * weights.st
        
        # 風の影響計算
        wind_adj = 0.0
        if weights.wind > 0:
            w_speed = float(race_data.get("wind_speed") or 0)
            w_dir = race_data.get("wind_direction") or ""
            if "追い風" in w_dir:
                if w_speed <= 4:
                    if course == 1: wind_adj = 2.0
                    elif course == 2: wind_adj = 1.0
                else:
                    if course == 1: wind_adj = -2.0
                    elif course == 2: wind_adj = 2.0
                    elif course == 3: wind_adj = 1.0
            elif "向かい風" in w_dir:
                if w_speed <= 4:
                    if course == 1: wind_adj = -0.5
                    elif course == 3: wind_adj = 0.5
                    elif course == 4: wind_adj = 1.5
                else:
                    if course == 1: wind_adj = -3.5
                    elif course in [3, 4]: wind_adj = 2.5
                    elif course in [5, 6]: wind_adj = 1.5
            elif w_dir: # 横風など
                if course == 1: wind_adj = -0.5
        
        rule_score = course_score + win_score + motor_score + exhibition_score + st_score + tilt_adj + (wind_adj * weights.wind)

        # AIの計算 (キャッシュがあれば優先的に使用)
        race_id = race_data.get("id", 0)
        p_hash = compute_players_hash(players_data)
        
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
            ai_base = (p.get("local_win_rate") or 4.0) * 12 + (m_rate * 0.7)
            # 競艇におけるコース（枠）の絶対的な有利度をAI基礎点に加算 (1コースが圧倒的有利)
            course_bonus = {1: 45, 2: 25, 3: 15, 4: 10, 5: 5, 6: 0}.get(course, 0)
            ai_base += course_bonus

            if course >= 4 and st_time < 0.12:
                ai_base += 15
            if tilt >= 1.0 and course >= 3:
                ai_base += tilt * 4
            
            ai_base += max(0, (7.0 - ex_time) * 30)
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
    p_hash = compute_players_hash(players_data)
    
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

        for _ in range(NUM_SIMS):
            # ガウス分布でばらつきを持たせる
            sim_scores = [(p["boat_number"], p["ai_base"] + rng.gauss(0, 25.0)) for p in active_for_sim]
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
        return [{"pattern": r["pattern"], "prob": round(r["prob"]*100, 1)} for r in results[:max_items]]

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
        return results[:max_items]

    predictions = {
        "rule_focus": generate_rule_combinations(rule_probs, settings.bet_type, settings.max_items, 0),
        "ai_focus": generate_ai_combinations(ai_pattern_counts, settings.bet_type, settings.max_items, NUM_SIMS, settings.fixed_1st),
        "ai_default_focus": generate_ai_combinations(ai_pattern_counts, "3連単", 8, NUM_SIMS, 0),
        "ai_win_probs": ai_win_probs,
        "scenario": legacy_scenario,
        "scenarios": scenarios,
        "_ai_pattern_counts_list": [[",".join(map(str, k)), v] for k, v in ai_pattern_counts.items()],
        "_num_sims": NUM_SIMS
    }
    return scored_players, predictions


# ─────────────────────────── Race APIs ──────────────────────────────────────

@app.get("/api/racers/{toban}")
def api_get_racer(toban: str):
    return fetch_racer_profile(toban)


@app.get("/api/races/{race_id}/options")
def api_get_race_live_data(race_id: int):
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date, is_finished, result_json FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    
    # すでに終了して結果が保存されていればそれを返す
    if race["is_finished"] and race["result_json"]:
        try:
            result = json.loads(race["result_json"])
            return {"result": result, "odds": None}
        except:
            pass

    date_str = race["race_date"].replace("-", "")
    result = fetch_match_result(race["place_code"], race["race_number"], date_str)
    
    if result and result.get("finished"):
        conn = get_db_connection()
        try:
            ranking_str = result.get("ranking_str", "")
            result_json = json.dumps(result)
            conn.execute('UPDATE races SET is_finished = 1, ranking_str = ?, result_json = ? WHERE id = ?', 
                         (ranking_str, result_json, race_id))
            conn.commit()
        finally:
            conn.close()

    odds = None
    if not result or "error" in result or not result.get("finished"):
        odds = fetch_live_odds(race["place_code"], race_number=race["race_number"], date_str=date_str)
    
    return {"result": result, "odds": odds}


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
    conn = get_db_connection()
    try:
        races = conn.execute(
            'SELECT * FROM races WHERE race_date = ? AND is_finished = 1 AND ranking_str IS NOT NULL AND ranking_str != ""',
            (date,)
        ).fetchall()
        if not races:
            return {}
        
        race_ids = [r['id'] for r in races]
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
        hit = any(is_hit(p["pattern"], ranking) for p in preds["rule_focus"])
        if not hit:
            hit = any(is_hit(p["pattern"], ranking) for p in preds["ai_focus"])
            
        hits[rid] = hit
        
    return hits

@app.get("/api/races/{race_id}/odds")
def api_get_race_odds(race_id: int, bet_type: str = Query(default="3t")):
    """指定賭式オッズ取得。bet_type: 3t/3f/2t/2f/1t"""
    conn = get_db_connection()
    race = conn.execute(
        'SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    date_str = race["race_date"].replace("-", "")
    return fetch_all_odds(race["place_code"], race["race_number"], date_str, bet_type)


@app.get("/api/races/{race_id}/weather")
def api_get_race_weather(race_id: int):
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
def get_race_detail(race_id: int):
    return get_custom_predict(race_id, PredictRequest(weights=CustomWeights(), settings=PredictSettings()))


@app.post("/api/races/{race_id}/predict")
def get_custom_predict(race_id: int, req: PredictRequest):
    conn = get_db_connection()
    try:
        race = conn.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
        if not race:
            raise HTTPException(status_code=404, detail="Race not found")
        players = conn.execute(
            'SELECT * FROM entries WHERE race_id = ? ORDER BY boat_number', (race_id,)
        ).fetchall()

        race_dict = dict(race)
        
        # Overrides 適用
        scored_players_raw = [dict(p) for p in players]
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

        ai_cache = None
        # overrides があっても recalculate_ai が False なら、
        # もしDBにキャッシュがあればそれを利用する（表示用）
        if race_dict.get("ai_predictions_json") and not req.recalculate_ai:
            try:
                temp_cache = json.loads(race_dict["ai_predictions_json"])
                # 通常は展示完了フラグが一致する場合のみ。
                # ただし overrides がある場合は、ユーザーが意図的に数値をいじっているので、
                # 計算済みAIデータをそのまま「参考」として出すことを許容する
                ai_cache = temp_cache
            except Exception as e:
                pass

        scored_players, predictions = calculate_predictions(race_dict, scored_players_raw, req.weights, req.settings, ai_cache=ai_cache)

        # 新しく計算した場合（キャッシュがなかった、または無効だった場合）はDBに保存
        if not ai_cache:
            new_ai_cache = {
                "is_exhibition_done": race_dict.get("is_exhibition_done"),
                "ai_player_data": {
                    str(p["boat_number"]): {
                        "ai_base": p["ai_base"],
                        "ai_score": p["ai_score"],
                        "ai_mark": p["ai_mark"]
                    } for p in scored_players
                },
                "ai_win_probs": predictions["ai_win_probs"],
                "ai_pattern_counts": predictions.get("_ai_pattern_counts_list", []),
                "num_sims": predictions.get("_num_sims", 10000),
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
def scrape_exhibition(race_id: int):
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
        conn.commit()
    finally:
        conn.close()

    race_data = get_race_detail(race_id)
    race_data["scraped_exhibition"] = exhibition
    race_data["scraped_weather"] = data.get("weather_info", {})
    race_data["tilt_info"] = _get_tilt_info(race_id)
    return race_data


@app.post("/api/races/{race_id}/exhibition")
def update_exhibition(race_id: int, updates: List[ExhibitionUpdate]):
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
