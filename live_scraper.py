"""
Live scraper for on-demand data from boatrace.jp official site.
"""
import requests
from bs4 import BeautifulSoup
import itertools

BASE_URL = "https://www.boatrace.jp/owpc/pc"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def fetch_racer_profile(toban: str) -> dict:
    url = f"{BASE_URL}/data/racersearch/course?toban={toban}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        tables = soup.select("table.is-w400")
        if not tables:
            return {"error": "Stats table not found", "toban": toban}

        def parse_table(t):
            result = {}
            for row in t.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) >= 2:
                    c = cells[0].text.strip()
                    if c.isdigit() and 1 <= int(c) <= 6:
                        result[f"course_{c}"] = cells[1].text.strip()
            return result

        win_rates  = parse_table(tables[0])
        trio_rates = parse_table(tables[1]) if len(tables) > 1 else {}
        avg_st     = parse_table(tables[2]) if len(tables) > 2 else {}
        starts     = parse_table(tables[3]) if len(tables) > 3 else {}

        name = ""
        name_tag = soup.select_one(".mainTitle01 h2")
        if name_tag:
            name = name_tag.text.strip().split("（")[0].strip()

        courses = []
        for i in range(1, 7):
            courses.append({
                "course": i,
                "win_rate":  win_rates.get(f"course_{i}", "--"),
                "trio_rate": trio_rates.get(f"course_{i}", "--"),
                "avg_st":    avg_st.get(f"course_{i}", "--"),
                "starts":    starts.get(f"course_{i}", "--"),
            })
        return {"toban": toban, "name": name, "course_stats": courses}
    except Exception as e:
        return {"error": str(e), "toban": toban}


# ── 3連単オッズ (既存互換) ──────────────────────────────────────────────────
def fetch_live_odds(place_code: str, race_number: int, date_str: str) -> dict:
    return fetch_all_odds(place_code, race_number, date_str, "3t")


# ── 賭式別オッズ取得 ────────────────────────────────────────────────────────
def _parse_3t(soup):
    """3連単: 120通り"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None

    def page_order():
        res = []
        for first in range(1, 7):
            for second in range(1, 7):
                if second == first:
                    continue
                for third in range(1, 7):
                    if third == first or third == second:
                        continue
                    res.append((first, second, third))
        return res

    ordered = page_order()
    odds_list = []
    for idx, (f, s, t) in enumerate(ordered):
        if idx < len(cells):
            odds_list.append({
                "pattern": f"{f}-{s}-{t}",
                "odds": cells[idx].text.strip()
            })
    return odds_list


def _parse_3f(soup):
    """3連複: 20通り"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None
    combos = list(itertools.combinations(range(1, 7), 3))
    odds_list = []
    for idx, (a, b, c) in enumerate(combos):
        if idx < len(cells):
            odds_list.append({
                "pattern": f"{a}={b}={c}",
                "odds": cells[idx].text.strip()
            })
    return odds_list


def _parse_2t(soup):
    """2連単: 30通り (同じページ odds2tf に 2連単/2連複 混在)"""
    # 2連単は上半分
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None
    combos = list(itertools.permutations(range(1, 7), 2))
    odds_list = []
    for idx, (a, b) in enumerate(combos):
        if idx < len(cells):
            odds_list.append({
                "pattern": f"{a}-{b}",
                "odds": cells[idx].text.strip()
            })
    return odds_list


def _parse_2f(soup):
    """2連複: 15通り"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None
    # 2連複は 2連単の後にある場合が多いが、同 URL で別テーブルの場合も
    # 安全のため 30以降を取る
    combos = list(itertools.combinations(range(1, 7), 2))
    offset = 30  # 2連単の後
    odds_list = []
    for idx, (a, b) in enumerate(combos):
        ci = offset + idx
        if ci < len(cells):
            odds_list.append({
                "pattern": f"{a}={b}",
                "odds": cells[ci].text.strip()
            })
    # もし offset で取れなければ最初から
    if not odds_list:
        for idx, (a, b) in enumerate(combos):
            if idx < len(cells):
                odds_list.append({
                    "pattern": f"{a}={b}",
                    "odds": cells[idx].text.strip()
                })
    return odds_list


def _parse_1t(soup):
    """単勝: 6通り"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None
    odds_list = []
    for i in range(6):
        if i < len(cells):
            odds_list.append({
                "pattern": str(i + 1),
                "odds": cells[i].text.strip()
            })
    return odds_list


def fetch_all_odds(place_code: str, race_number: int, date_str: str, bet_type: str = "3t") -> dict:
    """
    任意の賭式のオッズを取得。
    bet_type: "3t"=3連単, "3f"=3連複, "2t"=2連単, "2f"=2連複, "1t"=単勝
    """
    url_map = {
        "3t": "odds3t",
        "3f": "odds3f",
        "2t": "odds2tf",
        "2f": "odds2tf",
        "1t": "odds1tf",
    }
    label_map = {
        "3t": "3連単", "3f": "3連複", "2t": "2連単", "2f": "2連複", "1t": "単勝"
    }
    parser_map = {
        "3t": _parse_3t, "3f": _parse_3f, "2t": _parse_2t, "2f": _parse_2f, "1t": _parse_1t
    }

    url_key = url_map.get(bet_type, "odds3t")
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/{url_key}?rno={race_number}&jcd={jcd}&hd={date_str}"

    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")

        if "データはありません" in res.text:
            return {"error": "No odds data available", "bet_type": bet_type}

        parser = parser_map.get(bet_type, _parse_3t)
        odds_list = parser(soup)

        if not odds_list:
            return {"error": "Could not parse odds", "bet_type": bet_type}

        def sort_key(item):
            try:
                return float(item["odds"])
            except Exception:
                return 9999.0

        odds_sorted = sorted(odds_list, key=sort_key)

        return {
            "all_odds": odds_list,          # 全件（元の順番）
            "sorted_odds": odds_sorted,     # 安い順
            "total": len(odds_list),
            "bet_type": bet_type,
            "bet_label": label_map.get(bet_type, bet_type),
        }
    except Exception as e:
        return {"error": str(e), "bet_type": bet_type}


def fetch_exhibition_data(place_code: str, race_number: int, date_str: str) -> dict:
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/beforeinfo?rno={race_number}&jcd={jcd}&hd={date_str}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")

        if "データはありません" in res.text:
            return {"error": "展示情報はまだありません"}

        results = {}
        # ----- Parse Ex Time and Tilt (Table 1) -----
        for tbody in soup.select("tbody.is-fs12"):
            tds = tbody.find_all("td")
            if len(tds) >= 6:
                try:
                    boat_str = tds[0].text.strip()
                    if not boat_str.isdigit(): continue
                    boat_number = int(boat_str)
                    if not (1 <= boat_number <= 6): continue
                    
                    ex_time_str = tds[4].text.strip()
                    ex_time = float(ex_time_str) if ex_time_str.replace('.', '', 1).isdigit() else 6.80
                    
                    tilt_str = tds[5].text.strip()
                    tilt = float(tilt_str) if tilt_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
                    
                    if boat_number not in results:
                        results[boat_number] = {"boat_number": boat_number}
                    results[boat_number]["exhibition_time"] = ex_time
                    results[boat_number]["tilt"] = tilt
                    
                    # Default values for st and course in case they aren't parsed below
                    if "entry_course" not in results[boat_number]:
                        results[boat_number]["entry_course"] = boat_number
                    if "start_timing" not in results[boat_number]:
                        results[boat_number]["start_timing"] = 0.15
                except Exception:
                    pass

        # ----- Parse ST and Course (Table 2 / image layout) -----
        st_spans = soup.select(".table1_boatImage1Time")
        boat_spans = soup.select(".table1_boatImage1Number")
        
        for i, (b_span, st_span) in enumerate(zip(boat_spans, st_spans)):
            try:
                b_class = [c for c in b_span.get("class", []) if c.startswith("is-type")]
                if b_class:
                    boat_number = int(b_class[0].replace("is-type", ""))
                    course = i + 1
                    
                    st_str = st_span.text.strip().lstrip('F').lstrip('L')
                    st_time = float(st_str) if st_str.replace('.', '', 1).isdigit() else 0.15
                    
                    if boat_number not in results:
                        results[boat_number] = {"boat_number": boat_number}
                    results[boat_number]["entry_course"] = course
                    results[boat_number]["start_timing"] = st_time
            except Exception:
                pass

        if not results:
            return {"error": "展示データの解析に失敗しました", "url": url}

        weather_info = _parse_weather(soup)
        return {"exhibition": results, "weather_info": weather_info, "url": url}
    except Exception as e:
        return {"error": str(e)}


def _parse_weather(soup) -> dict:
    weather_info = {
        "weather": "不明",
        "wind_direction": "",
        "wind_speed": 0.0,
        "wave_height": 0.0,
    }
    try:
        w_span = soup.select_one('.weather1_bodyUnitLabelData span[class^="is-weather"]')
        if w_span:
            w_class = w_span.get('class', [''])[0]
            w_map = {
                "is-weather1": "晴れ", "is-weather2": "曇り",
                "is-weather3": "雨",   "is-weather4": "雪",
                "is-weather5": "霧",
            }
            weather_info["weather"] = w_map.get(w_class, "不明")

        for unit in soup.select('.weather1_bodyUnitLabelData'):
            text = unit.text.strip()
            if 'm' in text and 'cm' not in text:
                spd = text.replace('m', '').strip()
                if spd.isdigit():
                    weather_info["wind_speed"] = float(spd)
            if 'cm' in text:
                wv = text.replace('cm', '').strip()
                if wv.isdigit():
                    weather_info["wave_height"] = float(wv)

        wind_img = soup.select_one('.weather1_bodyUnitImage')
        if wind_img:
            classes = wind_img.get('class', [])
            dir_class = next((c for c in classes if c.startswith('is-direction')), None)
            if dir_class:
                d_idx = dir_class.replace('is-direction', '')
                dir_map = {
                    "1": "北",   "2": "北東", "3": "東",   "4": "南東",
                    "5": "南",   "6": "南西", "7": "西",   "8": "北西",
                    "9": "無風",
                }
                weather_info["wind_direction"] = dir_map.get(d_idx, d_idx)
    except Exception as e:
        print(f"Weather parsing error: {e}")
    return weather_info


def fetch_match_result(place_code: str, race_number: int, date_str: str):
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/raceresult?rno={race_number}&jcd={jcd}&hd={date_str}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        if "データはありません" in res.text:
            return None

        result = {"finished": True, "ranking": [], "payouts": [], "url": url,
                  "racer_names": {}, "race_times": {}}

        # ── 着順パース（着/枠/選手/タイム）──────────────────────────────────
        tables = soup.find_all("table")
        for t in tables:
            for row in t.find_all("tr"):
                cells = row.find_all("td")
                if cells and len(cells) >= 3:
                    rank = cells[0].text.strip()
                    boat = cells[1].text.strip() if len(cells) > 1 else ""
                    if rank.isdigit() and boat.isdigit():
                        entry = {"rank": int(rank), "boat": int(boat)}
                        # 選手名を取得（boat番号の次のセルから）
                        if len(cells) >= 3:
                            import re
                            name_raw = cells[2].text.strip()
                            # "4320 \n 峰 竜太" のように登録番号が含まれる場合があるので除去
                            name_clean = re.sub(r'^\d+\s+', '', name_raw)
                            entry["name"] = " ".join(name_clean.split())
                        # レースタイムを取得
                        for c in cells:
                            ct = c.text.strip()
                            # タイム形式: 1'53"1 or 1:53.1
                            if "'" in ct and '"' in ct:
                                entry["time"] = ct
                                break
                        result["ranking"].append(entry)
                        if entry.get("name"):
                            result["racer_names"][int(boat)] = entry.get("name", "")
                        if entry.get("time"):
                            result["race_times"][int(boat)] = entry.get("time", "")

        ranking_boats = [str(r["boat"]) for r in sorted(result["ranking"], key=lambda x: x["rank"])]
        result["ranking_str"] = "-".join(ranking_boats[:3]) if ranking_boats else "--"

        # ── 全賭式の払戻金パース ──────────────────────────
        # 対象となる賭式名
        BET_TYPES = {"3連単", "3連複", "2連単", "2連複", "単勝", "複勝"}
        seen_types = set()

        for t in tables:
            text = t.get_text()
            if not any(bt in text for bt in BET_TYPES):
                continue
            for row in t.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    bet_type_raw = cells[0].text.strip()
                    if bet_type_raw in BET_TYPES and bet_type_raw not in seen_types:
                        # 組番: 2番目セル（数字を含む）
                        combo_raw = cells[1].text.strip() if len(cells) > 1 else ""
                        
                        # 払戻金は¥マークを含むセルを探す
                        payout_str = ""
                        for c in cells[1:]:
                            txt = c.text.strip()
                            if "¥" in txt or "円" in txt:
                                payout_str = txt
                                break
                        
                        if payout_str:
                            val = payout_str.replace(",", "").replace("¥", "").replace("円", "")
                            if val.isdigit() and int(val) > 0:
                                fmt_val = f"{int(val):,}円"
                                result["payouts"].append({
                                    "type": bet_type_raw,
                                    "payout": fmt_val,
                                    "combination": combo_raw
                                })
                                seen_types.add(bet_type_raw)


        # 払戻金が取れなかった場合、別のパース方法を試みる (旧ロジック)
        if not result["payouts"]:
            for t in tables:
                text = t.get_text()
                if "払戻金" in text or "3連単" in text:
                    for row in t.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            bet_type = cells[0].text.strip()
                            for c in cells[1:]:
                                val = c.text.strip()
                                if "¥" in val or "円" in val or val.replace(",", "").isdigit():
                                    result["payouts"].append({"type": bet_type, "payout": val})
                                    break

        return result if result["ranking"] else None
    except Exception as e:
        print(f"fetch_match_result error: {e}")
        return None
