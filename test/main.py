import math
import itertools
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
import random
import os
import json
import concurrent.futures
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from live_scraper import fetch_racer_profile, fetch_live_odds, fetch_match_result, fetch_exhibition_data
import scraper
from scraper import scrape_today
import time

from app_config import JST, LOCK_FILE, STATIC_DIR
from database import get_db_connection, init_db

app = FastAPI(title="Kyotei Advisor MVP")
init_db()

def get_today_str() -> str:
    """Returns today's date in JST as YYYY-MM-DD"""
    now_jst = datetime.now(JST)
    return now_jst.strftime('%Y-%m-%d')

allowed_origins = [origin.strip() for origin in os.environ.get("CORS_ALLOW_ORIGINS", "*").split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# フロントエンド静的ファイルのサーブ
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

from fastapi.responses import RedirectResponse
@app.get("/")
def read_root():
    return RedirectResponse(url="/static/index.html")

class CustomWeights(BaseModel):
    win_rate: float = 1.0       # 勝率の重み
    motor: float = 1.0          # モーターの重み
    exhibition: float = 1.0     # 展示の重み
    st: float = 1.0             # STの重み
    course: float = 1.0         # コース有利度の重み

class ExhibitionUpdate(BaseModel):
    boat_number: int
    exhibition_time: float
    start_timing: float
    entry_course: int

class PredictSettings(BaseModel):
    max_items: int = 8
    bet_type: str = "3連単"

class PredictRequest(BaseModel):
    weights: CustomWeights
    settings: PredictSettings = PredictSettings()

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
    """本日の開催場一覧を取得（全24場を返し、開催有無のフラグをつける）"""
    global _active_places_cache_time, _active_places_cache_jcds
    now = time.time()
    today_str = get_today_str()

    # DBからレースのある場を取得
    conn = get_db_connection()
    try:
        db_places = conn.execute(
            'SELECT DISTINCT place_code FROM races WHERE race_date = ?',
            (today_str,)
        ).fetchall()
        db_place_codes = {row['place_code'] for row in db_places}

        # 各会場の代表グレードと最大レース番号を取得
        grade_rows = conn.execute(
            'SELECT place_code, place_name, race_title, MAX(race_number) as max_race FROM races WHERE race_date = ? GROUP BY place_code',
            (today_str,)
        ).fetchall()
    finally:
        conn.close()

    # grade_map: place_code -> (race_title, max_race)
    grade_map_by_code = {row['place_code']: (row['race_title'] or '', row['max_race'] or 0, row['place_name']) for row in grade_rows}

    # キャッシュ付きでインデックスページをスクレイプ（DBにない場合のフォールバック）
    # また、DBにある場でもキャッシュを更新しておく（追加開催対応）
    if now - _active_places_cache_time > 300:
        try:
            active_jcds = scraper.scrape_index()
            _active_places_cache_jcds = active_jcds
            _active_places_cache_time = now
        except Exception as e:
            print(f"Error fetching active places index: {e}")

    # スクレイプで得た場コードもマージ
    active_place_codes = set(db_place_codes) | set(_active_places_cache_jcds)
    # place_code -> place_name の逆引き
    code_to_name = {v: k for k, v in {v: k for k, v in scraper.places_dict.items()}.items()}
    # 正しい逆引き
    code_to_name = scraper.places_dict  # code -> name

    result = []
    for place in places_dict_order:
        # place_name -> place_code を探す
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
    """特定の場の本日のレース一覧を取得"""
    today_str = get_today_str()
    conn = get_db_connection()
    # GROUP BY で重複除去、race_number 順に並べる
    races = conn.execute('''
        SELECT id, race_date, place_code, place_name, race_number, race_title
        FROM races
        WHERE place_name = ? AND race_date = ?
        GROUP BY race_number
        ORDER BY race_number
    ''', (place_name, today_str)).fetchall()
    conn.close()
    if not races:
        return []
    return [dict(r) for r in races]

@app.get("/api/status")
def get_status():
    """スクレイピング中かどうかを返す"""
    return {"is_scraping": LOCK_FILE.exists()}

@app.post("/api/scrape/today")
def trigger_scrape_today(background_tasks: BackgroundTasks):
    """手動で本日の全レースデータを一括スクレイピングする"""
    if LOCK_FILE.exists():
        raise HTTPException(status_code=400, detail="既にデータ取得処理が実行中です。")

    background_tasks.add_task(scrape_today)
    return {"status": "started", "message": "データ取得を開始しました。"}

# --- メインとなる予想計算ロジック（重み可変＋AIモック） ---
def calculate_predictions(race_data, players_data, weights: CustomWeights, settings: PredictSettings = None):
    if settings is None:
        settings = PredictSettings()

    scored_players = []

    for row in players_data:
        p = dict(row)

        # Noneのデータをハンドリング（展示前などはデータがない場合あり）
        ex_time = p.get("exhibition_time") or 6.80
        st_time = p.get("start_timing") or 0.15
        course = p.get("entry_course") or p["boat_number"]
        g_win = p.get("global_win_rate") or 4.0
        m_rate = p.get("motor_2_quinella") or 30.0

        # カスタムルールベースのスコア化
        # 1. コース有利度(インコースほど点が高い)
        course_score = max(0, 7 - course) * 3.0 * weights.course
        # 2. 全国勝率
        win_score = g_win * 10.0 * weights.win_rate
        # 3. モーター2連対率 (0~100) -> 係数調整
        motor_score = m_rate * 0.5 * weights.motor
        # 4. 展示タイム (6.50に近いほど良い)
        exhibition_score = max(0, (7.0 - ex_time) * 50) * weights.exhibition
        # 5. ST (0.00に近いほど良い)
        st_score = max(0, (0.3 - st_time) * 100) * weights.st

        rule_score = course_score + win_score + motor_score + exhibition_score + st_score

        # AIモック予測 (LightGBMやNNを想定した別軸の評価。今回は固定の重みづけによる非線形風モック)
        ai_base = (p.get("local_win_rate") or 4.0) * 12 + (m_rate * 0.7)
        # モックの非線形性：外枠でSTが良いとAIは高評価する
        if course >= 4 and st_time < 0.12:
            ai_base += 15
        ai_score = ai_base + (max(0, (7.0 - ex_time) * 30)) + random.uniform(-2, 2)

        p["rule_score"] = round(rule_score, 2)
        p["ai_score"] = round(ai_score, 2)

        # 計算結果として利用するため欠損値補填済みの値もセット
        p["calc_st"] = st_time
        p["calc_course"] = course
        p["calc_ex"] = ex_time

        scored_players.append(p)

    if not scored_players:
        return [], {
            "rule_focus": [],
            "ai_focus": [],
            "scenario": {"active": False, "probability": 0, "text": "", "focus": ""}
        }

    # rule印字 (◎ 〇 ▲ △ × )
    scored_players.sort(key=lambda x: x["rule_score"], reverse=True)
    marks = ["◎", "〇", "▲", "△", "×", ""]
    for i, p in enumerate(scored_players):
        p["rule_mark"] = marks[i] if i < len(marks) else ""

    # AI印字
    scored_players.sort(key=lambda x: x["ai_score"], reverse=True)
    for i, p in enumerate(scored_players):
        p["ai_mark"] = marks[i] if i < len(marks) else ""

    # 番号順に戻す
    scored_players.sort(key=lambda x: x["boat_number"])

    # === 展開シナリオ（捲り）===
    out_players = [p for p in scored_players if p["calc_course"] >= 4]
    in_players = [p for p in scored_players if p["calc_course"] <= 3]

    makuri_scenario = {"active": False, "probability": 0, "text": "", "focus": ""}

    if out_players and in_players:
        best_out = min(out_players, key=lambda x: x["calc_st"])
        avg_in_st = sum(p["calc_st"] for p in in_players) / len(in_players)

        st_diff = avg_in_st - best_out["calc_st"]
        makuri_prob = 10 + (st_diff * 400) + (((best_out.get("motor_2_quinella") or 30.0) - 30) * 1.5)

        makuri_prob = max(5.0, min(85.0, makuri_prob))
        makuri_prob = round(makuri_prob, 1)

        if makuri_prob >= 25.0:
            attacker = best_out["boat_number"]
            others = [p["boat_number"] for p in scored_players if p["boat_number"] != attacker]
            focus_2nd = random.choice(others)
            others.remove(focus_2nd)
            focus_3rd = random.choice(others)

            makuri_scenario = {
                "active": True,
                "probability": makuri_prob,
                "text": f"【特注シナリオ】{attacker}号艇（{best_out.get('racer_name','')}）のスタートとモーターに注目。一撃捲りが決まる確率が【{makuri_prob}%】。",
                "focus": f"{attacker}-{focus_2nd}-{focus_3rd}, {attacker}-全-全"
            }

    def get_probabilities(score_key):
        max_score = max(p[score_key] for p in scored_players)
        exps = [math.exp((p[score_key] - max_score)/50.0) for p in scored_players]
        sum_exps = sum(exps)
        probs = [e / sum_exps for e in exps]
        return {p["boat_number"]: prob for p, prob in zip(scored_players, probs)}

    rule_probs = get_probabilities("rule_score")
    ai_probs = get_probabilities("ai_score")

    def generate_combinations(boat_probs, bet_type, max_items):
        boats = list(boat_probs.keys())
        results = []

        if bet_type == "3連単":
            for comb in itertools.permutations(boats, 3):
                p1 = boat_probs[comb[0]]
                p2 = boat_probs[comb[1]] / (1.0 - p1 + 1e-9)
                p3 = boat_probs[comb[2]] / (1.0 - p1 - boat_probs[comb[1]] + 1e-9)
                results.append({"pattern": f"{comb[0]}-{comb[1]}-{comb[2]}", "prob": p1*p2*p3})
        elif bet_type == "3連複":
            for comb in itertools.combinations(boats, 3):
                total_prob = 0.0
                for perm in itertools.permutations(comb, 3):
                    p1 = boat_probs[perm[0]]
                    p2 = boat_probs[perm[1]] / (1.0 - p1 + 1e-9)
                    p3 = boat_probs[perm[2]] / (1.0 - p1 - boat_probs[perm[1]] + 1e-9)
                    total_prob += p1*p2*p3
                results.append({"pattern": f"{comb[0]}={comb[1]}={comb[2]}", "prob": total_prob})
        elif bet_type == "2連単":
            for comb in itertools.permutations(boats, 2):
                p1 = boat_probs[comb[0]]
                p2 = boat_probs[comb[1]] / (1.0 - p1 + 1e-9)
                results.append({"pattern": f"{comb[0]}-{comb[1]}", "prob": p1*p2})
        elif bet_type == "2連複":
            for comb in itertools.combinations(boats, 2):
                total_prob = 0.0
                for perm in itertools.permutations(comb, 2):
                    p1 = boat_probs[perm[0]]
                    p2 = boat_probs[perm[1]] / (1.0 - p1 + 1e-9)
                    total_prob += p1*p2
                results.append({"pattern": f"{comb[0]}={comb[1]}", "prob": total_prob})
        elif bet_type == "単勝":
            for b in boats:
                results.append({"pattern": f"{b}", "prob": boat_probs[b]})
        else:  # Default 3連単
            for comb in itertools.permutations(boats, 3):
                p1 = boat_probs[comb[0]]
                p2 = boat_probs[comb[1]] / (1.0 - p1 + 1e-9)
                p3 = boat_probs[comb[2]] / (1.0 - p1 - boat_probs[comb[1]] + 1e-9)
                results.append({"pattern": f"{comb[0]}-{comb[1]}-{comb[2]}", "prob": p1*p2*p3})

        results.sort(key=lambda x: x["prob"], reverse=True)
        return [{"pattern": r["pattern"], "prob": round(r["prob"]*100, 1)} for r in results[:max_items]]

    predictions = {
        "rule_focus": generate_combinations(rule_probs, settings.bet_type, settings.max_items),
        "ai_focus": generate_combinations(ai_probs, settings.bet_type, settings.max_items),
        "scenario": makuri_scenario
    }

    return scored_players, predictions

@app.get("/api/racers/{toban}")
def api_get_racer(toban: str):
    """レーサーの詳細情報（コース別勝率など）を取得"""
    return fetch_racer_profile(toban)

@app.get("/api/races/{race_id}/options")
def api_get_race_live_data(race_id: int):
    """レースのオッズまたは結果を取得"""
    conn = get_db_connection()
    race = conn.execute('SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)).fetchone()
    conn.close()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")

    date_str = race["race_date"].replace("-", "")  # YYYYMMDD
    # 結果を先に確認
    result = fetch_match_result(race["place_code"], race["race_number"], date_str)

    odds = None
    if not result or "error" in result:
        odds = fetch_live_odds(race["place_code"], race_number=race["race_number"], date_str=date_str)

    return {"result": result, "odds": odds}

@app.get("/api/races/{race_id}")
def get_race_detail(race_id: int):
    """レース詳細をデフォルトのウェイトで取得"""
    return get_custom_predict(race_id, PredictRequest(weights=CustomWeights(), settings=PredictSettings()))

@app.post("/api/races/{race_id}/predict")
def get_custom_predict(race_id: int, req: PredictRequest):
    """カスタムウェイトと設定を適用したレース詳細と予想データを取得"""
    conn = get_db_connection()
    race = conn.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if not race:
        conn.close()
        raise HTTPException(status_code=404, detail="Race not found")

    players = conn.execute('SELECT * FROM entries WHERE race_id = ? ORDER BY boat_number', (race_id,)).fetchall()
    conn.close()

    scored_players, predictions = calculate_predictions(dict(race), players, req.weights, req.settings)

    return {
        "race": dict(race),
        "players": scored_players,
        "predictions": predictions,
        "current_weights": req.weights.dict(),
        "current_settings": req.settings.dict()
    }

@app.get("/api/races/{race_id}/exhibition/scrape")
def scrape_exhibition(race_id: int):
    """公式サイトから展示情報を自動取得し、DBに保存してから予想を返す"""
    conn = get_db_connection()
    race = conn.execute('SELECT place_code, race_number, race_date FROM races WHERE id = ?', (race_id,)).fetchone()
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

    # DBへ保存
    conn = get_db_connection()
    try:
        for boat_number, info in exhibition.items():
            conn.execute('''
                UPDATE entries
                SET exhibition_time = ?, start_timing = ?, entry_course = ?
                WHERE race_id = ? AND boat_number = ?
            ''', (info["exhibition_time"], info["start_timing"], info["entry_course"], race_id, int(boat_number)))

        weather_info = data.get("weather_info", {})
        if weather_info:
            conn.execute('''
                UPDATE races
                SET weather = ?, wind_direction = ?, wind_speed = ?, wave_height = ?, is_exhibition_done = 1
                WHERE id = ?
            ''', (weather_info.get("weather"), weather_info.get("wind_direction"),
                  weather_info.get("wind_speed"), weather_info.get("wave_height"), race_id))
        conn.commit()
    finally:
        conn.close()

    race_data = get_race_detail(race_id)
    race_data["scraped_exhibition"] = exhibition
    race_data["scraped_weather"] = data.get("weather_info", {})
    return race_data


@app.post("/api/races/{race_id}/exhibition")
def update_exhibition(race_id: int, updates: List[ExhibitionUpdate]):
    """展示情報を手動更新し、再計算（デフォルトウェイトで）して結果を返す"""
    conn = get_db_connection()
    try:
        for update in updates:
            conn.execute('''
                UPDATE entries
                SET exhibition_time = ?, start_timing = ?, entry_course = ?
                WHERE race_id = ? AND boat_number = ?
            ''', (update.exhibition_time, update.start_timing, update.entry_course, race_id, update.boat_number))
        conn.commit()
    finally:
        conn.close()

    return get_race_detail(race_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
