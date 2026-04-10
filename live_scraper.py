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
        
        # 1. 統計テーブルを取得 (最新の構造: div.table1 table)
        tables = soup.select("div.table1 table")
        if not tables:
            # フォールバック (旧構造: table.is-w400)
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

        # 2. 選手名の取得
        name = ""
        # 12/27追記: p.racerName 内に <span> でカナが入っている場合があるため
        name_tag = soup.select_one("p.racerName")
        if name_tag:
            # カナ(span)を除去して漢字氏名のみ取得
            for span in name_tag.find_all("span"):
                span.decompose()
            name = name_tag.get_text(strip=True)
        
        if not name:
            # フォールバック: .mainTitle01 h2 (旧構造)
            name_tag = soup.select_one(".mainTitle01 h2")
            if name_tag:
                name = name_tag.get_text(strip=True).split("（")[0].strip()
            
        if name:
            # "4320" などの番号が含まれる場合があるので名前だけ抽出
            import re
            name = re.sub(r'^\d+\s*', '', name)
            # 全角スペース等を整理
            name = " ".join(name.split())

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


def fetch_racer_past_results(toban: str) -> list:
    """
    指定した選手の過去成績を取得する。
    PCサイトの back3 (過去3節成績) ページから詳細な着順データを抽出する。
    """
    url = f"{BASE_URL}/data/racersearch/back3?toban={toban}"
    print(f"[SCRAPE] Fetching racer past results (back3) from: {url}")
    results = []
    
    # 必要な定数やインポート
    from scraper import places_dict
    from app_config import REQUEST_TIMEOUT

    try:
        # 参照元（Referer）をセットすることでシステムエラーを回避しやすくする
        headers = HEADERS.copy()
        headers["Referer"] = f"{BASE_URL}/data/racersearch/profile?toban={toban}"
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # 過去3節成績のテーブルを取得
        tables = soup.select(".is-p_result, table.table1")
        if not tables:
            print(f"  [WARN] No back3 tables found for toban {toban}")
            return []

        for table in tables:
            tbody = table.select_one("tbody")
            if not tbody: continue
            
            # 各節の行を処理
            for tr in tbody.select("tr"):
                tds = tr.find_all("td")
                if len(tds) < 4: continue
                
                # 第2カラム: 開催場
                place_val = tds[1].text.strip()
                # 第3カラム（付近）: タイトル
                # タイトルはtds[2]〜tds[4]のいずれか（列結合による）
                # 確実に取るために、tds[-1]以外の名称が含まれるtdを探す
                title_val = ""
                for i in range(2, len(tds)-1):
                    txt = tds[i].text.strip()
                    if txt: title_val = txt; break
                
                # 最後のカラム: 節間成績 (<a>タグのリスト)
                res_td = tds[-1]
                res_links = res_td.select("a")
                for link in res_links:
                    href = link.get("href", "")
                    rank_txt = link.get_text(strip=True)
                    
                    # hrefから日付、場コード、レース番号を抽出
                    # 例: /owpc/pc/race/raceresult?rno=8&jcd=24&hd=20260313
                    import re
                    rno_m = re.search(r'rno=(\d+)', href)
                    jcd_m = re.search(r'jcd=(\d+)', href)
                    hd_m = re.search(r'hd=(\d+)', href)
                    
                    if rno_m and jcd_m and hd_m:
                        rno = rno_m.group(1)
                        jcd = jcd_m.group(1).zfill(2)
                        hd = hd_m.group(1) # YYYYMMDD
                        
                        date_iso = f"{hd[:4]}-{hd[4:6]}-{hd[6:8]}"
                        
                        # 重複追加を避ける (同日同場同レース)
                        if any(r['race_date'] == date_iso and r['place_code'] == jcd and r['race_no'] == int(rno) for r in results):
                            continue

                        # place_codeから場コードを特定（なければリンクから）
                        p_code = next((c for c, n in places_dict.items() if n in place_val), jcd)

                        results.append({
                            "race_date": date_iso,
                            "place_code": p_code,
                            "place_name": place_val,
                            "race_no": int(rno),
                            "rank": rank_txt,
                            "race_title": title_val,
                            "entry_course": None, 
                            "start_timing": None
                        })

        print(f"  -> Found {len(results)} historical race results for toban {toban}")
        return results

    except Exception as e:
        print(f"[LIVE SCRAPE ERROR] fetch_racer_past_results: {e}")
        return []


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
        # Boatrace.jp の3連単オッズ表は、横に1〜6号艇の1着が並び、縦に2着、3着が展開される。
        # td.oddsPoint を抽出すると、行優先（各行の1号艇1着、2号艇1着…）の順で取得される。
        for row_idx in range(20):
            for f in range(1, 7):
                others = [b for b in range(1, 7) if b != f]
                s_idx = row_idx // 4  # 2着のインデックス
                t_idx = row_idx % 4  # 3着のインデックス
                s = others[s_idx]
                rem = [b for b in others if b != s]
                t = rem[t_idx]
                res.append((f, s, t))
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
    """3連複: 20通り (特殊な並び順)"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None
    
    # 公式サイトの並び順 (1=2=3, 1=2=4, 1=2=5, 1=2=6, 1=3=4, 2=3=4, 1=3=5, 2=3=5, ...)
    ordered = [
        (1,2,3), (1,2,4), (1,2,5), (1,2,6),
        (1,3,4), (2,3,4), (1,3,5), (2,3,5), (1,3,6), (2,3,6),
        (1,4,5), (2,4,5), (3,4,5), (1,4,6), (2,4,6), (3,4,6),
        (1,5,6), (2,5,6), (3,5,6), (4,5,6)
    ]
    
    odds_list = []
    for idx, (a, b, c) in enumerate(ordered):
        if idx < len(cells):
            odds_list.append({
                "pattern": f"{a}={b}={c}",
                "odds": cells[idx].text.strip()
            })
    return odds_list


def _parse_2t(soup):
    """2連単: 30通り (行優先: 1-2, 2-1, 3-1, 4-1, 5-1, 6-1, 1-3, ...)"""
    cells = soup.select("td.oddsPoint")
    if not cells:
        return None

    def page_order():
        res = []
        for row in range(5):
            for first in range(1, 7):
                others = [b for b in range(1, 7) if b != first]
                second = others[row]
                res.append((first, second))
        return res

    ordered = page_order()
    odds_list = []
    for idx, (a, b) in enumerate(ordered):
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
    # ページ内のtd.oddsPointを全取得
    all_cells = soup.select("td.oddsPoint")
    if not all_cells:
        return None
    
    odds_list = []
    # 単勝は通常、最初のテーブルの6件
    for i in range(min(6, len(all_cells))):
        val = all_cells[i].text.strip()
        if not val or "---" in val: val = "-"
        odds_list.append({
            "pattern": str(i + 1),
            "odds": val
        })
    return odds_list if odds_list else None


def _parse_1f(soup):
    """複勝: 6通り (通常 単勝の後のテーブル)"""
    all_cells = soup.select("td.oddsPoint")
    if len(all_cells) < 7:
        return None
    
    odds_list = []
    # 複勝は通常、7番目から12番目。
    # ただし公式は 1.0-1.2 のような形式
    for i in range(6):
        idx = 6 + i
        if idx < len(all_cells):
            val = all_cells[idx].text.strip()
            if not val or "---" in val: val = "-"
            odds_list.append({
                "pattern": f"({i + 1})",
                "odds": val
            })
    return odds_list if odds_list else None


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
        "1f": "odds1tf",
    }
    label_map = {
        "3t": "3連単", "3f": "3連複", "2t": "2連単", "2f": "2連複", "1t": "単勝", "1f": "複勝"
    }
    parser_map = {
        "3t": _parse_3t, "3f": _parse_3f, "2t": _parse_2t, "2f": _parse_2f, "1t": _parse_1t, "1f": _parse_1f
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
                    is_absent = ("欠" in ex_time_str or "-" in ex_time_str)
                    ex_time = float(ex_time_str) if ex_time_str.replace('.', '', 1).isdigit() else 6.80
                    
                    tilt_str = tds[5].text.strip()
                    tilt = float(tilt_str) if tilt_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
                    
                    if boat_number not in results:
                        results[boat_number] = {"boat_number": boat_number}
                    results[boat_number]["exhibition_time"] = ex_time
                    results[boat_number]["tilt"] = tilt
                    results[boat_number]["is_absent"] = is_absent
                    if "entry_course" not in results[boat_number]:
                        results[boat_number]["entry_course"] = boat_number
                    if "start_timing" not in results[boat_number]:
                        results[boat_number]["start_timing"] = 0.15
                    
                    # --- New: Parts Exchange, Weight Adj, Propeller ---
                    # 1. Weight Adjustment (tds[1])
                    weight_str = tds[1].text.strip()
                    weight_adj = float(weight_str) if weight_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
                    results[boat_number]["weight_adjustment"] = weight_adj

                    # 2. Propeller (tds[3])
                    results[boat_number]["propeller"] = tds[3].text.strip()

                    # 3. Parts Exchange (tds[6] or nested list)
                    parts_ul = tds[6].find("ul")
                    if parts_ul:
                        parts = [li.text.strip() for li in parts_ul.find_all("li")]
                        results[boat_number]["parts_exchange"] = " ".join(parts)
                    else:
                        parts_txt = tds[6].text.strip()
                        results[boat_number]["parts_exchange"] = parts_txt if parts_txt else ""
                    
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
                    
                    raw_st = st_span.text.strip()
                    is_flying = 'F' in raw_st
                    st_str = raw_st.lstrip('F').lstrip('L')
                    st_time = float(st_str) if st_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.15
                    if is_flying:
                        st_time = -st_time
                    
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
                    "9": "無風", "10": "追い風", "11": "向かい風", "12": "左横風", "13": "右横風",
                    "14": "左斜め追い風", "15": "右斜め追い風", "16": "左斜め向かい風", "17": "右斜め向かい風"
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
        # 特定のテーブルクラスを指定することで、払戻金テーブルなど他テーブルの混入を防ぐ
        ranking_table = soup.select_one("table.is-p_resultRanking3")
        if not ranking_table:
            # フォールバック: 「着」と「ボートレーサー」が含まれる最初のテーブルを探す
            for t in soup.find_all("table"):
                txt = t.get_text()
                if "着" in txt and "ボートレーサー" in txt:
                    ranking_table = t
                    break
        
        if ranking_table:
            for row in ranking_table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                # ヘッダー行(th)を考慮
                if cells and len(cells) >= 3:
                    rank_raw = cells[0].get_text(strip=True)
                    boat_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    
                    # 着順が1-6の数字、またはF, L, K, Sなどの失格・欠場コードであることをチェック
                    # ただし、「単勝」「複勝」などの項目は除外する
                    if rank_raw and not any(kw in rank_raw for kw in ["単勝", "複勝", "3連", "2連", "払戻"]):
                        is_rank_valid = rank_raw.isdigit() or (len(rank_raw) == 1 and rank_raw.isalpha())
                        
                        if is_rank_valid and boat_raw.isdigit():
                            boat_val = int(boat_raw)
                            entry = {"rank": rank_raw, "boat": boat_val}
                            
                            # 選手名を2列目(index 2)以降から探す
                            import re
                            potential_name = ""
                            for i in range(2, min(len(cells), 5)):
                                raw = cells[i].get_text(separator=' ', strip=True)
                                # 登録番号（4桁）を削除
                                clean = re.sub(r'^\d{4}\s*', '', raw)
                                # 漢字・カナ・空白のみで構成されているかチェック（記号や金額を除外）
                                if clean and len(clean) >= 2 and len(clean) < 20:
                                    if re.match(r'^[\u4e00-\u9faf\u3040-\u309f\u30a0-\u30ff\s・]+$', clean):
                                        potential_name = clean
                                        break
                            entry["name"] = " ".join(potential_name.split())
                            
                            # 名前が空の場合の最終手段（「枠」の文字などが含まれないもの）
                            if not entry.get("name") and len(cells) > 3:
                                for c in cells[3:]:
                                    txt = c.get_text(strip=True)
                                    if txt and len(txt) >= 2 and not any(ch in txt for ch in ["円", "¥", "￥", "着", "枠"]):
                                        if not txt.replace(".", "").isdigit():
                                            entry["name"] = txt
                                            break

                            # レースタイムを取得 (DQなどの場合はタイムが '--' になる)
                            for c in cells:
                                ct = c.get_text(strip=True)
                                if ("'" in ct and '"' in ct) or (":" in ct and "." in ct):
                                    entry["time"] = ct
                                    break
                            
                            result["ranking"].append(entry)
                            if entry.get("name"):
                                result["racer_names"][boat_val] = entry.get("name", "")
                            if entry.get("time"):
                                result["race_times"][boat_val] = entry.get("time", "")

        # 3着まで確定しているかどうかの厳密なチェック
        ranking_boats = [str(r["boat"]) for r in sorted(result["ranking"], key=lambda x: x["rank"])]
        if len(ranking_boats) >= 3:
            result["ranking_str"] = "-".join(ranking_boats[:3])
        else:
            result["ranking_str"] = "--"
            result["finished"] = False # 3着まで確定していなければ不完全とみなす

        # ── 全賭式の払戻金パース ──────────────────────────
        # 払戻金テーブル(is-p_resultBetting)を対象にする
        betting_tables = soup.select("table.is-p_resultBetting")
        if not betting_tables:
            # フォールバック: すべてのテーブルから配当らしきものを探す
            betting_tables = soup.find_all("table")

        BET_TYPES = {"3連単", "3連複", "2連単", "2連複", "単勝", "複勝"}
        seen_types = set()

        for t in betting_tables:
            text = t.get_text()
            if not any(bt in text for bt in BET_TYPES):
                continue
            for row in t.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    bet_type_raw = cells[0].get_text(strip=True)
                    if bet_type_raw in BET_TYPES and bet_type_raw not in seen_types:
                        # 組番: 2番目セル
                        combo_raw = cells[1].get_text(strip=True).replace(' ', '')
                        
                        # 払戻金は「円」または「¥」を含むセルを探す
                        payout_str = ""
                        for c in cells[1:]:
                            txt = c.get_text(strip=True)
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
            for t in soup.find_all("table"):
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
