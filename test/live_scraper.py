"""
Live scraper for on-demand data from boatrace.jp official site:
- Racer detailed profile (course statistics)
- Real-time odds (3連単)
- Race results & payout
"""
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.boatrace.jp/owpc/pc"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fetch_racer_profile(toban: str) -> dict:
    """
    選手のコース別成績をスクレイピング
    URL: /owpc/pc/data/racersearch/course?toban=XXXX
    Tables (all .is-w400):
      [0] コース別1着率      : 1コース='逃げ率'
      [1] コース別3連率
      [2] コース別平均ST
      [3] コース別スタート数
    """
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
            rows = t.find_all("tr")
            for row in rows:
                cells = row.find_all(["th", "td"])
                if len(cells) >= 2:
                    course = cells[0].text.strip()
                    if course.isdigit() and 1 <= int(course) <= 6:
                        result[f"course_{course}"] = cells[1].text.strip()
            return result
        
        # Parse title from table header
        titles = soup.select("table.is-w400 th:first-child")
        
        win_rates = parse_table(tables[0])      # コース別1着率 – 1コースの値 = 逃げ率
        trio_rates = parse_table(tables[1]) if len(tables) > 1 else {}     # コース別3連率
        avg_st = parse_table(tables[2]) if len(tables) > 2 else {}         # 平均ST    
        starts = parse_table(tables[3]) if len(tables) > 3 else {}         # スタート数
        
        # Get racer name from page title
        name = ""
        name_tag = soup.select_one(".mainTitle01 h2")
        if name_tag:
            name = name_tag.text.strip().split("（")[0].strip()
        
        courses = []
        for i in range(1, 7):
            courses.append({
                "course": i,
                "win_rate": win_rates.get(f"course_{i}", "--"),   # 1着率 (逃げ率)
                "trio_rate": trio_rates.get(f"course_{i}", "--"), # 3連率
                "avg_st": avg_st.get(f"course_{i}", "--"),        # 平均ST
                "starts": starts.get(f"course_{i}", "--"),        # スタート数
            })
        
        return {
            "toban": toban,
            "name": name,
            "course_stats": courses
        }
    except Exception as e:
        print(f"Error fetching racer profile {toban}: {e}")
        return {"error": str(e), "toban": toban}


def fetch_live_odds(place_code: str, race_number: int, date_str: str) -> dict:
    """
    3連単の現在オッズを取得 (120通り)
    URL: /owpc/pc/race/odds3t?rno=X&jcd=Y&hd=YYYYMMDD
    120 td.oddsPoint cells, ordered as:
       1-2-3, 1-2-4, 1-2-5, 1-2-6, 1-3-2, ... (all permutations of 6 boats)
    """
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/odds3t?rno={race_number}&jcd={jcd}&hd={date_str}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        
        # Check for "no data" case
        if "データはありません" in res.text:
            return {"error": "No odds data available"}
        
        cells = soup.select("td.oddsPoint")
        if not cells:
            return {"error": "Could not parse odds"}
        
        # Generate all 120 permutations of boats 1-6 in canonical order
        import itertools
        patterns = list(itertools.permutations([1, 2, 3, 4, 5, 6], 3))
        
        # Sort patterns: 1st boat, then 2nd, then 3rd to match page order
        # The table reads left-to-right, top-to-bottom
        # For 3連単: fix 1st place, iterate 2nd, then rows for 3rd
        # The official layout is grouped by 1st place, then sweeps 2nd/3rd
        # Let's match by generating in page order (1-2-3, 1-2-4, 1-2-5, 1-2-6, 1-3-2, ...)
        def page_order():
            results = []
            for first in range(1, 7):
                for third in range(1, 7):
                    if third == first:
                        continue
                    for second in range(1, 7):
                        if second == first or second == third:
                            continue
                        results.append((first, second, third))
            return results
        
        ordered = page_order()
        odds_list = []
        for idx, (f, s, t) in enumerate(ordered):
            if idx < len(cells):
                odds_val = cells[idx].text.strip()
                odds_list.append({
                    "pattern": f"{f}-{s}-{t}",
                    "odds": odds_val
                })
        
        # Sort by odds value (ascending = favorites first)
        def odds_sort_key(item):
            try:
                return float(item["odds"])
            except:
                return 9999
        
        odds_list_sorted = sorted(odds_list, key=odds_sort_key)
        
        return {
            "all_odds": odds_list_sorted[:30],  # Top 30 cheapest (most likely) combos
            "total": len(odds_list)
        }
    except Exception as e:
        print(f"Error fetching odds: {e}")
        return {"error": str(e)}


def fetch_exhibition_data(place_code: str, race_number: int, date_str: str) -> dict:
    """
    展示情報（進入コース・展示タイム・ST）を取得
    URL: /owpc/pc/race/beforeinfo?rno=X&jcd=Y&hd=YYYYMMDD
    テーブル構造: 艇番 | 進入 | 展示タイム | ST
    """
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/beforeinfo?rno={race_number}&jcd={jcd}&hd={date_str}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")

        if "データはありません" in res.text:
            return {"error": "展示情報はまだありません"}

        # 展示情報テーブルを探す: class="is-w748" or table containing 展示タイム
        results = {}

        # 展示タイム表: 対象テーブルを探す
        # 公式サイト: table.is-w488 以下に 艇番・進入・展示T・STが含まれる
        tables = soup.find_all("table")
        exhibition_table = None
        for t in tables:
            text = t.get_text()
            if "展示タイム" in text or "展示T" in text:
                exhibition_table = t
                break

        if not exhibition_table:
            # フォールバック: tbody.is-fs12 クラスから読む
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
                            "start_timing": st_time
                        }
                    except Exception:
                        pass
            return {"exhibition": results, "url": url}

        # テーブルから行を読む
        rows = exhibition_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                try:
                    # 艇番を特定 (最初のセル or 艇番セル)
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
                        "start_timing": st_time
                    }
                except Exception:
                    pass

        if not results:
            return {"error": "展示データの解析に失敗しました", "url": url}

        # 気象情報（天候、風向、風速、波高）を取得
        weather_info = {
            "weather": "不明",
            "wind_direction": "",
            "wind_speed": 0.0,
            "wave_height": 0.0
        }
        
        try:
            # 天候
            weather_el = soup.select_one('.weather1_bodyUnitLabelTitle')
            if weather_el:
                # '気温                  25.0℃' のように入っているので、アイコンのクラスやテキストから取るのが確実
                # 簡略化して span の is-weatherX から取得
                w_span = soup.select_one('.weather1_bodyUnitLabelData span[class^="is-weather"]')
                if w_span:
                    w_class = w_span.get('class', [''])[0]
                    # is-weather1 (晴れ), is-weather2 (曇り), is-weather3 (雨), is-weather4 (雪)
                    w_map = {"is-weather1": "晴れ", "is-weather2": "曇り", "is-weather3": "雨", "is-weather4": "雪", "is-weather5": "霧"}
                    weather_info["weather"] = w_map.get(w_class, "不明")

            # 風速、波高の抽出
            for unit in soup.select('.weather1_bodyUnitLabelData'):
                text = unit.text.strip()
                if 'm' in text and not 'cm' in text:
                    spd = text.replace('m', '').strip()
                    if spd.isdigit(): weather_info["wind_speed"] = float(spd)
                if 'cm' in text:
                    wv = text.replace('cm', '').strip()
                    if wv.isdigit(): weather_info["wave_height"] = float(wv)
            
            # 風向の抽出 (is-directionX)
            wind_img = soup.select_one('.weather1_bodyUnitImage')
            if wind_img:
                classes = wind_img.get('class', [])
                dir_class = next((c for c in classes if c.startswith('is-direction')), None)
                if dir_class:
                    d_idx = dir_class.replace('is-direction', '')
                    # 1:北, 2:北東, 3:東, 4:南東, 5:南, 6:南西, 7:西, 8:北西, 9:無風 等のアサイン（boatrace公式仕様）
                    # 画面表示用にはCSSクラスをそのまま返すか角度にするか。今回は単純な数値を方角にマッピング
                    dir_map = {"1": "北", "2": "北東", "3": "東", "4": "南東", "5": "南", "6": "南西", "7": "西", "8": "北西", "9": "無風"}
                    weather_info["wind_direction"] = dir_map.get(d_idx, d_idx)

        except Exception as e:
            print(f"Weather parsing error: {e}")

        return {"exhibition": results, "weather_info": weather_info, "url": url}
    except Exception as e:
        print(f"Error fetching exhibition data: {e}")
        return {"error": str(e)}


def fetch_match_result(place_code: str, race_number: int, date_str: str):

    """
    レース結果・払戻金を取得
    URL: /owpc/pc/race/raceresult?rno=X&jcd=Y&hd=YYYYMMDD
    Returns None if race not finished
    """
    jcd = str(int(place_code)).zfill(2)
    url = f"{BASE_URL}/race/raceresult?rno={race_number}&jcd={jcd}&hd={date_str}"
    try:
        res = requests.get(url, timeout=15, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        
        if "データはありません" in res.text:
            return None  # Race not yet finished
        
        result = {"finished": True, "ranking": [], "payouts": [], "url": url}
        
        tables = soup.find_all("table")
        
        # --- Ranking table: usually first main data table ---
        for t in tables:
            rows = t.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if cells and len(cells) >= 3:
                    rank = cells[0].text.strip()
                    boat = cells[2].text.strip()
                    if rank.isdigit() and boat.isdigit():
                        result["ranking"].append({"rank": int(rank), "boat": int(boat)})
        
        ranking_boats = [str(r["boat"]) for r in sorted(result["ranking"], key=lambda x: x["rank"])]
        result["ranking_str"] = "-".join(ranking_boats[:3]) if ranking_boats else "--"
        
        # --- Payouts: extract from 払戻金 info ---
        for t in tables:
            text = t.get_text()
            if "払戻金" in text or "3連単" in text:
                rows = t.find_all("tr")
                for row in rows:
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
        print(f"Error fetching race result: {e}")
        return None
