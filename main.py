import math
import itertools
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
import random
import os
import json
import concurrent.futures
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from live_scraper import (
    fetch_racer_profile, fetch_live_odds, fetch_match_result,
    fetch_exhibition_data, fetch_all_odds
)
import scraper
from scraper import scrape_today
import time

from app_config import JST, LOCK_FILE, STATIC_DIR
from database import get_db_connection, init_db

app = FastAPI(title="Kyotei Advisor MVP")
init_db()

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

class ExhibitionUpdate(BaseModel):
    boat_number: int
    exhibition_time: float
    start_timing: float
    entry_course: int
    tilt: Optional[float] = None

class PredictSettings(BaseModel):
    max_items: int = 8
    bet_type: str = "3連単"

class PredictRequest(BaseModel):
    weights: CustomWeights
    settings: PredictSettings = PredictSettings()


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
def get_places():
    global _active_places_cache_time, _active_places_cache_jcds
    now = time.time()
    today_str = get_today_str()

    conn = get_db_connection()
    try:
        db_places = conn.execute(
            'SELECT DISTINCT place_code FROM races WHERE race_date = ?',
            (today_str,)
        ).fetchall()
        db_place_codes = {row['place_code'] for row in db_places}

        grade_rows = conn.execute(
            'SELECT place_code, place_name, race_title, MAX(race_number) as max_race '
            'FROM races WHERE race_date = ? GROUP BY place_code',
            (today_str,)
        ).fetchall()
    finally:
        conn.close()

    grade_map_by_code = {
        row['place_code']: (row['race_title'] or '', row['max_race'] or 0, row['place_name'])
        for row in grade_rows
    }

    if now - _active_places_cache_time > 300:
        try:
            active_jcds = scraper.scrape_index()
            _active_places_cache_jcds = active_jcds
            _active_places_cache_time = now
        except Exception as e:
            print(f"Error fetching active places index: {e}")

    active_place_codes = set(db_place_codes) | set(_active_places_cache_jcds)

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
def get_races(place_name: str):
    today_str = get_today_str()
    conn = get_db_connection()
    races = conn.execute(
        'SELECT id, race_date, place_code, place_name, race_number, race_title '
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
def trigger_scrape_today(background_tasks: BackgroundTasks):
    if LOCK_FILE.exists():
        raise HTTPException(status_code=400, detail="既にデータ取得処理が実行中です。")
    background_tasks.add_task(scrape_today)
    return {"status": "started", "message": "データ取得を開始しました。"}


# ─────────────────────────── Scenario Engine (全艇対応) ─────────────────────

def calculate_scenarios(scored_players):
    """
    2〜6号艇それぞれの「捲り」「差し」「捲り差し」確率を計算する。
    """
    scenarios = []

    for player in scored_players:
        course = player["calc_course"]
        if course < 2:
            continue

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


# ─────────────────────────── Prediction Engine ──────────────────────────────

def calculate_predictions(race_data, players_data, weights: CustomWeights, settings: PredictSettings = None):
    if settings is None:
        settings = PredictSettings()

    scored_players = []

    for row in players_data:
        p = dict(row)

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
        rule_score = course_score + win_score + motor_score + exhibition_score + st_score + tilt_adj

        ai_base = (p.get("local_win_rate") or 4.0) * 12 + (m_rate * 0.7)
        if course >= 4 and st_time < 0.12:
            ai_base += 15
        if tilt >= 1.0 and course >= 3:
            ai_base += tilt * 4
        ai_score = ai_base + max(0, (7.0 - ex_time) * 30) + random.uniform(-2, 2)

        p["rule_score"] = round(rule_score, 2)
        p["ai_score"] = round(ai_score, 2)
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

    scored_players.sort(key=lambda x: x["ai_score"], reverse=True)
    for i, p in enumerate(scored_players):
        p["ai_mark"] = marks[i] if i < len(marks) else ""

    scored_players.sort(key=lambda x: x["boat_number"])

    scenarios = calculate_scenarios(scored_players)

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
    ai_probs = get_probabilities("ai_score")

    def generate_combinations(boat_probs, bet_type, max_items):
        boats = list(boat_probs.keys())
        results = []
        if bet_type == "3連単":
            for c in itertools.permutations(boats, 3):
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                p3 = boat_probs[c[2]] / (1 - p1 - boat_probs[c[1]] + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}-{c[2]}", "prob": p1*p2*p3})
        elif bet_type == "3連複":
            for c in itertools.combinations(boats, 3):
                total = 0.0
                for perm in itertools.permutations(c, 3):
                    p1 = boat_probs[perm[0]]; p2 = boat_probs[perm[1]] / (1 - p1 + 1e-9)
                    p3 = boat_probs[perm[2]] / (1 - p1 - boat_probs[perm[1]] + 1e-9)
                    total += p1*p2*p3
                results.append({"pattern": f"{c[0]}={c[1]}={c[2]}", "prob": total})
        elif bet_type == "2連単":
            for c in itertools.permutations(boats, 2):
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}", "prob": p1*p2})
        elif bet_type == "2連複":
            for c in itertools.combinations(boats, 2):
                total = sum(boat_probs[p[0]] * boat_probs[p[1]] / (1 - boat_probs[p[0]] + 1e-9)
                            for p in itertools.permutations(c, 2))
                results.append({"pattern": f"{c[0]}={c[1]}", "prob": total})
        elif bet_type == "単勝":
            for b in boats:
                results.append({"pattern": str(b), "prob": boat_probs[b]})
        else:
            for c in itertools.permutations(boats, 3):
                p1 = boat_probs[c[0]]; p2 = boat_probs[c[1]] / (1 - p1 + 1e-9)
                p3 = boat_probs[c[2]] / (1 - p1 - boat_probs[c[1]] + 1e-9)
                results.append({"pattern": f"{c[0]}-{c[1]}-{c[2]}", "prob": p1*p2*p3})
        results.sort(key=lambda x: x["prob"], reverse=True)
        return [{"pattern": r["pattern"], "prob": round(r["prob"]*100, 1)} for r in results[:max_items]]

    predictions = {
        "rule_focus": generate_combinations(rule_probs, settings.bet_type, settings.max_items),
        "ai_focus": generate_combinations(ai_probs, settings.bet_type, settings.max_items),
        "scenario": legacy_scenario,
        "scenarios": scenarios,
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
        'SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)
    ).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    date_str = race["race_date"].replace("-", "")
    result = fetch_match_result(race["place_code"], race["race_number"], date_str)
    odds = None
    if not result or "error" in result:
        odds = fetch_live_odds(race["place_code"], race_number=race["race_number"], date_str=date_str)
    return {"result": result, "odds": odds}


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
    race = conn.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race:
        conn.close()
        raise HTTPException(status_code=404, detail="Race not found")
    players = conn.execute(
        'SELECT * FROM entries WHERE race_id = ? ORDER BY boat_number', (race_id,)
    ).fetchall()
    conn.close()
    scored_players, predictions = calculate_predictions(dict(race), players, req.weights, req.settings)
    return {
        "race": dict(race),
        "players": scored_players,
        "predictions": predictions,
        "current_weights": req.weights.dict(),
        "current_settings": req.settings.dict(),
    }


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
                'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=? '
                'WHERE race_id=? AND boat_number=?',
                (info["exhibition_time"], info["start_timing"], info["entry_course"],
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
                    'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=?, tilt=? '
                    'WHERE race_id=? AND boat_number=?',
                    (update.exhibition_time, update.start_timing, update.entry_course,
                     update.tilt if update.tilt is not None else 0.0,
                     race_id, update.boat_number)
                )
            except Exception:
                conn.execute(
                    'UPDATE entries SET exhibition_time=?, start_timing=?, entry_course=? '
                    'WHERE race_id=? AND boat_number=?',
                    (update.exhibition_time, update.start_timing, update.entry_course,
                     race_id, update.boat_number)
                )
        conn.commit()
    finally:
        conn.close()
    return get_race_detail(race_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
