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
            for third in range(1, 7):
                if third == first:
                    continue
                for second in range(1, 7):
                    if second == first or second == third:
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
        tables = soup.find_all("table")
        exhibition_table = None
        for t in tables:
            text = t.get_text()
            if "展示タイム" in text or "展示T" in text:
                exhibition_table = t
                break

        if not exhibition_table:
            tbodies = soup.select("tbody.is-fs12")
            for idx, tbody in enumerate(tbodies):
                if idx >= 6:
                    break
                boat_number = idx + 1
                tds = tbody.select("td")
                if len(tds) >= 4:
                    try:
                        entry_course = int(tds[0].text.strip()) if tds[0].text.strip().isdigit() else boat_number
                        ex_time = float(tds[1].text.strip()) if tds[1].text.strip().replace('.', '').isdigit() else 6.80
                        st_time = float(tds[3].text.strip()) if tds[3].text.strip().replace('.', '').isdigit() else 0.15
                        results[boat_number] = {
                            "boat_number": boat_number,
                            "entry_course": entry_course,
                            "exhibition_time": ex_time,
                            "start_timing": st_time,
                        }
                    except Exception:
                        pass
            weather_info = _parse_weather(soup)
            return {"exhibition": results, "weather_info": weather_info, "url": url}

        rows = exhibition_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                try:
                    boat_cell_text = cells[0].text.strip()
                    if not boat_cell_text.isdigit():
                        continue
                    boat_number = int(boat_cell_text)
                    if not (1 <= boat_number <= 6):
                        continue
                    entry_course_text = cells[1].text.strip()
                    ex_time_text = cells[2].text.strip()
                    st_time_text = cells[3].text.strip()
                    entry_course = int(entry_course_text) if entry_course_text.isdigit() else boat_number
                    ex_time = float(ex_time_text) if ex_time_text.replace('.', '', 1).isdigit() else 6.80
                    st_time = float(st_time_text) if st_time_text.replace('.', '', 1).lstrip('-').isdigit() else 0.15
                    results[boat_number] = {
                        "boat_number": boat_number,
                        "entry_course": entry_course,
                        "exhibition_time": ex_time,
                        "start_timing": st_time,
                    }
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
        result = {"finished": True, "ranking": [], "payouts": [], "url": url}
        tables = soup.find_all("table")
        for t in tables:
            for row in t.find_all("tr"):
                cells = row.find_all("td")
                if cells and len(cells) >= 3:
                    rank = cells[0].text.strip()
                    boat = cells[2].text.strip()
                    if rank.isdigit() and boat.isdigit():
                        result["ranking"].append({"rank": int(rank), "boat": int(boat)})
        ranking_boats = [str(r["boat"]) for r in sorted(result["ranking"], key=lambda x: x["rank"])]
        result["ranking_str"] = "-".join(ranking_boats[:3]) if ranking_boats else "--"
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
        return None
