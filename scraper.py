import requests
from bs4 import BeautifulSoup
import datetime
import time
import concurrent.futures
import sqlite3
import live_scraper

from app_config import JST, LOCK_FILE, REQUEST_TIMEOUT, USER_AGENT
from database import get_db_connection, init_db, push_race_to_supabase
import re

# SSEプッシュ用コールバック (main.py側からセットされる)
sse_broadcast_callback = None


def get_current_date(target_dt: datetime.datetime = None):
    """Returns (now_jst, target_date_str as YYYYMMDD, iso_date as YYYY-MM-DD)"""
    if target_dt is None:
        target_dt = datetime.datetime.now(JST)
    target_date_str = target_dt.strftime('%Y%m%d')
    iso_date = target_dt.strftime('%Y-%m-%d')
    return target_dt, target_date_str, iso_date

places_dict = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川", "06": "浜名湖",
    "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
    "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島", "17": "宮島", "18": "徳山",
    "19": "下関", "20": "若松", "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村"
}

HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept-Language': 'ja,en;q=0.9',
}

def scrape_index(target_dt: datetime.datetime = None):
    """指定日の開催場JCDコード一覧を公式サイトから取得する"""
    _, target_date_str, _ = get_current_date(target_dt)
    url = f"https://www.boatrace.jp/owpc/pc/race/index?hd={target_date_str}"
    print(f"[INFO] Fetching index from: {url}")
    try:
        # 開催場一覧取得にもリトライを適用
        response = _fetch_with_retry(url)
        print(f"[INFO] scrape_index status: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        active_jcd_list = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            # jcd= が含まれ、かつ raceindex, raceresult, racelist のいずれかが含まれる場合に開催場とみなす
            if 'jcd=' in href and any(kw in href for kw in ['raceindex', 'raceresult', 'racelist']):
                jcd = href.split('jcd=')[1].split('&')[0].zfill(2)
                if jcd not in active_jcd_list:
                    active_jcd_list.append(jcd)
        print(f"[INFO] Found {len(active_jcd_list)} active places.")
        return active_jcd_list
    except Exception as e:
        # requests.exceptions.HTTPError などの詳細を取得
        status_code = "Unknown"
        if hasattr(e, 'response') and e.response is not None:
            status_code = e.response.status_code
        
        print(f"[ERROR] scrape_index error (status {status_code}): {e}")
        if status_code == 403:
            print("[CRITICAL] IP Blocked (403 Forbidden). Boatrace site might be blocking this server.")
        return []

def _get_grade_from_soup(soup):
    """ページHTMLからレースグレード/タイトルを返す"""
    # 1. ページ内の全要素からグレードを示すクラスを探す
    # 1. ページ内の全要素からグレードを示すクラスを探す
    found_priority = 0
    found_grade = ""
    
    for el in soup.find_all(True):
        cls_list = el.get('class', [])
        cls_str = " ".join(cls_list) if isinstance(cls_list, list) else str(cls_list)
        
        if 'is-SGb' in cls_str or 'is-sg' in cls_str or 'is-gradeSG' in cls_str:
            if found_priority < 5:
                found_grade = "SG"; found_priority = 5
        elif 'is-G1b' in cls_str or 'is-grade1' in cls_str:
            if found_priority < 4:
                found_grade = "G1"; found_priority = 4
        elif 'is-G2b' in cls_str or 'is-grade2' in cls_str:
            if found_priority < 3:
                found_grade = "G2"; found_priority = 3
        elif 'is-G3b' in cls_str or 'is-grade3' in cls_str:
            if found_priority < 2:
                found_grade = "G3"; found_priority = 2
        elif 'is-lady' in cls_str or 'is-Lady' in cls_str:
            if found_priority < 1:
                found_grade = "女子"; found_priority = 1

    # 2. タイトルテキストの特定
    main_content = soup.select_one('.contents, main, #main, #contents') or soup
    title_el = main_content.select_one('.heading2_titleName, .heading2_title, .title_race__titleName, h2, h3')
    found_title = title_el.get_text(separator=' ', strip=True) if title_el else ""

    # 3. 強制文字列判定 (クラスが取れない場合の最終手段)
    t_up = found_title.upper()
    if not found_grade:
        if '尼崎' in t_up and 'センプル' in t_up: found_grade = "G1"
        elif 'SG' in t_up or 'ＳＧ' in t_up or "クラシック" in t_up: found_grade = "SG"
        elif 'G1' in t_up or 'Ｇ１' in t_up: found_grade = "G1"
        elif 'G2' in t_up or 'Ｇ２' in t_up: found_grade = "G2"
        elif 'G3' in t_up or 'Ｇ３' in t_up: found_grade = "G3"

    if found_title:
        if found_grade and found_grade not in found_title:
            res = f"{found_grade} {found_title}"
            print(f"DEBUG: Found Grade '{found_grade}', Prepending to '{found_title}' -> '{res}'")
            return res
        return found_title
    
    return ""

    # 優先度2: title_race クラス
    el = soup.select_one('.title_race__titleName')
    if el:
        text = el.get_text(separator=' ', strip=True)
        img = el.find('img')
        if img and img.get('alt'):
            text = f"{img.get('alt')} {text}".strip()
        if text:
            return text
            
    # 優先度3: h2, h3タグからキーワード検索 (メインコンテンツ内に限定)
    for h in soup.select('.contents h2, .contents h3, main h2, main h3'):
        # グレードに関係しそうなキーワードが含まれているかチェック
        txt = h.get_text(strip=True)
        # サブテーブルの「一般」を拾わないようにする工夫が必要
        if 'TABLE1_TITLE' in [c.upper() for c in h.get('class', [])]:
            continue
            
        img = h.find('img')
        if img and img.get('alt'):
            txt = f"{img.get('alt')} {txt}".strip()
            
        for kw in ['SG', 'G1', 'GⅠ', 'G１', 'G2', 'GⅡ', 'G２', 'G3', 'GⅢ', 'G３', 'グランプリ', 'ダービー', 'メモリアル', 'レディース', 'ヴィーナス', '女子']:
            if kw in txt.upper() or kw in txt:
                return txt
    return ""

def _get_day_label_from_soup(soup):
    """ページHTMLから『初日』『2日目』『最終日』などの開催日指定ラベルを返す"""
    # 1. アクティブな日程タブから取得（最も正確）
    # is-active または is-active2 クラスを探す
    active_tab = soup.select_one('.tab2 li[class*="is-active"] span')
    if active_tab:
        txt = active_tab.get_text(strip=True)
        import re
        m = re.search(r'(初日|\d+日目|最終日)', txt)
        if m:
            return m.group(1)

    # 2. 典型的なラベルクラスを探す (互換性用)
    labels = soup.select('.label1, .label2')
    for l in labels:
        txt = l.get_text(strip=True)
        if any(kw in txt for kw in ['初日', '日目', '最終日']):
            import re
            m = re.search(r'(初日|\d+日目|最終日)', txt)
            if m:
                return m.group(1)
    
    # 3. 全体から正規表現で検索 (フォールバック)
    import re
    text = soup.get_text()
    match = re.search(r'(初日|\d+日目|最終日)', text)
    if match:
        return match.group(1)
    
    return ""

def _fetch_with_retry(url, retries=3, wait=2):
    """HTTPリクエストをリトライ付きで実行する"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except Exception as e:
            if attempt < retries - 1:
                print(f"  [RETRY {attempt+1}/{retries}] {url} – {e}")
                time.sleep(wait)
            else:
                raise

def scrape_race_syusso(jcd, race_no, target_dt: datetime.datetime = None):
    """特定のレースの出走表データを取得しDBに保存。race_titleも取得・更新する。"""
    _, target_date_str, iso_date = get_current_date(target_dt)
    url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={race_no}&jcd={jcd}&hd={target_date_str}"
    place_name = places_dict.get(jcd, jcd)
    print(f"  Scraping {place_name} {race_no}R ...")

    try:
        response = _fetch_with_retry(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            # レースタイトル取得
            race_title = _get_grade_from_soup(soup)
            day_label = _get_day_label_from_soup(soup)

            # 締切時刻の取得 (最新の安定した方法: 12レース一覧表から当該レース番号のものを抽出)
            scheduled_time = ""
            try:
                # 12レース一覧表の「締切予定時刻」行を探す
                import re
                for tr in soup.select('.table1 table tr'):
                    tds = tr.find_all('td')
                    if any('締切予定時刻' in td.get_text() for td in tds):
                        # この行の tds の中から、race_no に対応するものを取得
                        # 最初の td は colspan="2" なので、インデックスは race_no になる
                        if len(tds) > race_no:
                            target_td = tds[race_no]
                            m = re.search(r'(\d{1,2}:\d{2})', target_td.get_text())
                            if m:
                                scheduled_time = m.group(1)
                                break
            except Exception as e:
                print(f"      [DEBUG] New deadline scraper failed: {e}")

            # フォールバック (既存のロジック: 複数のパターンの要素から「締切」を含むものを探す）
            if not scheduled_time:
                time_elements = soup.find_all(lambda tag: tag.name in ['span', 'div', 'p', 'td'] and '締切' in tag.get_text())
                for el in time_elements:
                    import re
                    t_text = el.get_text(separator=' ', strip=True)
                    m = re.search(r'(\d{1,2}:\d{2})', t_text)
                    if m:
                        scheduled_time = m.group(1)
                        break
            # 既存レースを確認
            cursor.execute(
                'SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?',
                (iso_date, jcd, race_no)
            )
            existing = cursor.fetchone()

            if existing:
                race_id = existing[0]
                # 既存でも race_title / scheduled_time を更新（グレード情報や時刻を最新に保つ）
                if race_title or scheduled_time or day_label:
                    cursor.execute(
                        'UPDATE races SET race_title = ?, scheduled_time = ?, day_label = ? WHERE id = ?',
                        (race_title, scheduled_time, day_label, race_id)
                    )
            else:
                try:
                    cursor.execute('''
                        INSERT INTO races (race_date, place_code, place_name, race_number,
                                           weather, wind_direction, wind_speed, wave_height, race_title, scheduled_time, day_label)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (iso_date, jcd, place_name, race_no,
                          '不明', '', 0.0, 0.0, race_title, scheduled_time, day_label))
                    race_id = cursor.lastrowid
                except sqlite3.IntegrityError:
                    cursor.execute(
                        'SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?',
                        (iso_date, jcd, race_no)
                    )
                    race_id = cursor.fetchone()[0]

            tbody_list = soup.select('tbody.is-fs12')
            if not tbody_list:
                # データなし→レコードも不要なら削除
                if not existing:
                    cursor.execute('DELETE FROM races WHERE id = ?', (race_id,))
                conn.commit()
                print(f"    -> No entry data for {place_name} {race_no}R")
                return

            for tbody in tbody_list:
                # 艇番をHTMLから直接取得 (最初のtdのテキスト)
                try:
                    tds = tbody.select('td')
                    if not tds:
                        continue
                    boat_str = tds[0].get_text(strip=True)
                    # "1", "2" などの全角数字を半角に変換、または数値のみ抽出
                    import re
                    m = re.search(r'[1-6１-６]', boat_str)
                    if m:
                        b_val = m.group(0)
                        # 全角→半角変換
                        b_map = {'１':'1','２':'2','３':'3','４':'4','５':'5','６':'6'}
                        boat_number = int(b_map.get(b_val, b_val))
                    else:
                        continue # 艇番が不明な場合はスキップ
                except Exception as e:
                    print(f"    -> Error identifying boat number: {e}")
                    continue
                try:
                    name_el = tbody.select_one('.is-fs18')
                    racer_name = name_el.get_text(strip=True).replace('\u3000', ' ') if name_el else ''

                    # 登録番号（toban）取得
                    racer_id = ''
                    toban_link = tbody.select_one('a[href*="toban="]')
                    if toban_link:
                        href = toban_link.get('href', '')
                        if 'toban=' in href:
                            racer_id = href.split('toban=')[1].split('&')[0]

                    class_el = tbody.select_one('.is-fs11')
                    racer_class = class_el.get_text(strip=True) if class_el else ''

                    tds = tbody.select('td')
                    global_win_rate = global_2_quinella = 0.0
                    local_win_rate = local_2_quinella = 0.0
                    motor_number = boat_number_machine = 0
                    motor_2_quinella = boat_2_quinella = 0.0

                    if len(tds) >= 8:
                        global_rates = tds[4].get_text(separator=' ', strip=True).split()
                        global_win_rate = float(global_rates[0]) if len(global_rates) > 0 else 0.0
                        global_2_quinella = float(global_rates[1]) if len(global_rates) > 1 else 0.0

                        local_rates = tds[5].get_text(separator=' ', strip=True).split()
                        local_win_rate = float(local_rates[0]) if len(local_rates) > 0 else 0.0
                        local_2_quinella = float(local_rates[1]) if len(local_rates) > 1 else 0.0

                        motor_data = tds[6].get_text(separator=' ', strip=True).split()
                        motor_number = int(motor_data[0]) if len(motor_data) > 0 and motor_data[0].isdigit() else 0
                        motor_2_quinella = float(motor_data[1]) if len(motor_data) > 1 else 0.0

                        boat_data = tds[7].get_text(separator=' ', strip=True).split()
                        boat_number_machine = int(boat_data[0]) if len(boat_data) > 0 and boat_data[0].isdigit() else 0
                        boat_2_quinella = float(boat_data[1]) if len(boat_data) > 1 else 0.0

                    cursor.execute('''
                        INSERT OR REPLACE INTO entries (
                            race_id, boat_number, racer_name, racer_class, racer_id,
                            global_win_rate, global_2_quinella, local_win_rate, local_2_quinella,
                            motor_number, motor_2_quinella, boat_number_machine, boat_2_quinella
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        race_id, boat_number, racer_name, racer_class, racer_id,
                        global_win_rate, global_2_quinella, local_win_rate, local_2_quinella,
                        motor_number, motor_2_quinella, boat_number_machine, boat_2_quinella
                    ))
                except Exception as e:
                    print(f"    -> Error parsing boat {boat_number}: {e}")

            conn.commit()
            print(f"    -> OK: {place_name} {race_no}R ({len(tbody_list)} boats, title='{race_title}')")
            # Supabase同期
            push_race_to_supabase(race_id)
        except Exception as db_e:
            print(f"  [DB ERROR] {jcd} {race_no}R: {db_e}")
        finally:
            conn.close()

    except Exception as e:
        print(f"  [ERROR] scrape_race_syusso({jcd},{race_no}): {e}")


def scrape_today(target_dt: datetime.datetime = None):
    """指定日の全開催場、全レース(1~12R)のデータを取得してDBに保存"""
    target_dt, target_date_str, iso_date = get_current_date(target_dt)
    print(f"=== {target_date_str} のレース情報を取得開始 ===")

    init_db()

    # ロックファイル: 20分以内なら二重起動を防ぐ
    if LOCK_FILE.exists():
        age = time.time() - LOCK_FILE.stat().st_mtime
        if age < 1200:
            print("Already scraping. Skipping.")
            return
        LOCK_FILE.unlink()

    LOCK_FILE.write_text("1", encoding="utf-8")

    try:
        active_jcds = scrape_index(target_dt)
        print(f"[INFO] 本日開催場: {[places_dict.get(j) for j in active_jcds]}")

        if not active_jcds:
            print("[WARN] 開催中の場が見つかりませんでした。取得を中止します。")
            return

        # 並列スクレイピング (max_workers=3 に減らして安定化)
        print(f"[INFO] Starting parallel scraping with 3 workers for {len(active_jcds)} places.")
        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for jcd in active_jcds:
                for race_no in range(1, 13):
                    tasks.append(executor.submit(scrape_race_syusso, jcd, race_no, target_dt))
            
            completed_count = 0
            total_tasks = len(tasks)
            for future in concurrent.futures.as_completed(tasks):
                completed_count += 1
                try:
                    future.result()
                    if completed_count % 10 == 0:
                        print(f"[INFO] Progress: {completed_count}/{total_tasks} tasks completed.")
                except Exception as e:
                    print(f"[ERROR] Task failed: {e}")

        print("=== データ取得完了 ===")
        # === SSEプッシュ: 開催場リストが更新されたことを通知 ===
        if sse_broadcast_callback:
            sse_broadcast_callback("places_updated", {"date": iso_date})
    except Exception as e:
        print(f"[CRITICAL] scrape_today failed: {e}")

def scrape_missing_today(target_dt: datetime.datetime = None):
    """DBをチェックし、足りないレースデータ(出走表)のみを公式サイトから並列取得する"""
    target_dt, target_date_str, iso_date = get_current_date(target_dt)
    print(f"=== {target_date_str} の不足データを確認・補填開始 ===")
    
    active_jcds = scrape_index(target_dt)
    if not active_jcds:
        print("[WARN] 開催中の場が見つかりませんでした。")
        return

    # DB内の既存レース番号を取得 (entriesが存在するもののみ)
    conn = get_db_connection()
    try:
        # entriesと紐付いている本日分の(place_code, race_number)を取得
        existing_data = conn.execute('''
            SELECT DISTINCT r.place_code, r.race_number 
            FROM races r
            JOIN entries e ON r.id = e.race_id
            WHERE r.race_date = ?
        ''', (iso_date,)).fetchall()
        existing_set = {(row['place_code'], row['race_number']) for row in existing_data}
    finally:
        conn.close()

    to_scrape = []
    for jcd in active_jcds:
        for rno in range(1, 13):
            if (jcd, rno) not in existing_set:
                to_scrape.append((jcd, rno))

    if not to_scrape:
        print("[INFO] 不足しているレースデータはありません。")
        return

    print(f"[INFO] {len(to_scrape)} 件のデータが不足しています。並列取得を開始します...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(scrape_race_syusso, jcd, rno, target_dt) for jcd, rno in to_scrape]
        concurrent.futures.wait(futures)

    print("=== 不足データの補填完了 ===")
    # === SSEプッシュ: 開催場リストが更新されたことを通知 ===
    if sse_broadcast_callback:
        sse_broadcast_callback("places_updated", {"date": iso_date})

def update_exhibition(jcd, race_no, target_dt: datetime.datetime = None):
    """展示データのみを更新する"""
    _, target_date_str, iso_date = get_current_date(target_dt)
    print(f"  Updating exhibition: {places_dict.get(jcd, jcd)} {race_no}R...")
    data = live_scraper.fetch_exhibition_data(jcd, race_no, target_date_str)
    
    if "exhibition" in data:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            # racesテーブルの天気・風・波を更新
            w = data.get("weather_info", {})
            cursor.execute('''
                UPDATE races SET 
                    weather = ?, wind_direction = ?, wind_speed = ?, wave_height = ?,
                    is_exhibition_done = 1
                WHERE race_date = ? AND place_code = ? AND race_number = ?
            ''', (w.get("weather", "不明"), w.get("wind_direction", ""), 
                  w.get("wind_speed", 0.0), w.get("wave_height", 0.0),
                  iso_date, jcd, race_no))

            # entriesテーブルの展示タイム・ST・コース・チルト・新規項目を更新
            for boat_num, ex_data in data["exhibition"].items():
                cursor.execute('''
                    UPDATE entries SET
                        exhibition_time = ?, start_timing = ?, entry_course = ?, tilt = ?,
                        parts_exchange = ?, weight_adjustment = ?, propeller = ?
                    WHERE race_id = (
                        SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?
                    ) AND boat_number = ?
                ''', (ex_data.get("exhibition_time", 0.0), ex_data.get("start_timing", 0.15),
                      ex_data.get("entry_course", boat_num), ex_data.get("tilt", 0.0),
                      ex_data.get("parts_exchange", ""), ex_data.get("weight_adjustment", 0.0), ex_data.get("propeller", ""),
                      iso_date, jcd, race_no, boat_num))
            # 展示データ更新時はAIキャッシュを無効化して再計算させる
            cursor.execute('''
                UPDATE races SET ai_predictions_json = NULL 
                WHERE race_date = ? AND place_code = ? AND race_number = ?
            ''', (iso_date, jcd, race_no))
            # Supabase同期
            cursor.execute('SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?', (iso_date, jcd, race_no))
            row = cursor.fetchone()
            if row:
                race_id = row[0]
                push_race_to_supabase(race_id)
                
                # === SSEプッシュ: 展示更新をリアルタイム通知 ===
                if sse_broadcast_callback:
                    try:
                        sse_broadcast_callback("exhibition_updated", {
                            "race_id": race_id,
                            "place_name": places_dict.get(jcd, jcd),
                            "race_number": race_no
                        })
                    except Exception as e:
                        print(f"    [SSE ERROR] {e}")
        except Exception as e:
            print(f"    [DB ERROR] update_exhibition: {e}")
        finally:
            conn.close()
    else:
        print(f"    -> {data.get('error', 'No data')}")

def update_result(jcd, race_no, target_dt: datetime.datetime = None):
    """レース結果（着順）のみを更新する"""
    _, target_date_str, iso_date = get_current_date(target_dt)
    print(f"  Checking result: {places_dict.get(jcd, jcd)} {race_no}R...")
    data = live_scraper.fetch_match_result(jcd, race_no, target_date_str)
    
    if data and data.get("finished"):
        # 3着まで確定しているか二重チェック（live_scraper側でも行っているが念のため）
        if not data.get("ranking_str") or data.get("ranking_str") == "--":
            print(f"    -> Warning: Result for {race_no}R is marked finished but ranking_str is invalid. Skipping update.")
            return

        conn = get_db_connection()
        race_id = None
        try:
            cursor = conn.cursor()
            # racesテーブルの終了フラグと着順文字列、詳細結果JSONを更新
            import json
            # レースが終了したので最終オッズを取得して保存
            # 基本の3連単(3t)に加えて、主要な賭式のオッズを並行して取得
            new_odds_cache = {}
            for bt in ["3t", "3f", "2t", "2f", "1t"]:
                try:
                    o = live_scraper.fetch_all_odds(jcd, race_no, target_date_str, bt)
                    if o and "error" not in o:
                        new_odds_cache[bt] = o
                except: pass
            
            odds_json_val = json.dumps(new_odds_cache) if new_odds_cache else None

            cursor.execute('''
                UPDATE races SET is_finished = 1, ranking_str = ?, result_json = ?, odds_json = ?
                WHERE race_date = ? AND place_code = ? AND race_number = ?
            ''', (data.get("ranking_str", ""), json.dumps(data), odds_json_val, iso_date, jcd, race_no))
            
            # race_id を取得（SSEプッシュ用）
            row = cursor.execute(
                'SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?',
                (iso_date, jcd, race_no)
            ).fetchone()
            if row:
                race_id = row[0]
            
            # entriesテーブルの到着順位とタイムを更新
            for rank_item in data.get("ranking", []):
                boat = rank_item.get("boat")
                rank = rank_item.get("rank")
                time_str = data.get("race_times", {}).get(boat, "")
                cursor.execute('''
                    UPDATE entries SET arrival_order = ?, race_time = ?
                    WHERE race_id = ? AND boat_number = ?
                ''', (rank, time_str, race_id, boat))
            
            conn.commit()
            print(f"    -> OK: Race finished. Result: {data.get('ranking_str')}. Odds cached: {list(new_odds_cache.keys())}")
            # Supabase同期
            if race_id:
                push_race_to_supabase(race_id)
        except Exception as e:
            print(f"    [DB ERROR] update_result: {e}")
        finally:
            conn.close()
        
        # === SSEプッシュ: 結果が確定したらブラウザへリアルタイム通知 ===
        if race_id and sse_broadcast_callback:
            try:
                sse_broadcast_callback("race_finished", {
                    "race_id": race_id,
                    "place_name": places_dict.get(jcd, jcd),
                    "race_number": race_no,
                    "ranking_str": data.get("ranking_str", ""),
                })
            except Exception as e:
                print(f"    [SSE ERROR] {e}")
    else:
        print(f"    -> Still no result.")

def update_venue_races(jcd, target_dt: datetime.datetime = None):
    """
    特定会場の全てのレース（1〜12R）に対して、展示・結果の更新を試みる。
    並列実行されることを想定。
    """
    target_dt, target_date_str, iso_date = get_current_date(target_dt)
    
    # DBの状態を1回だけ取得（効率化）
    conn = get_db_connection()
    try:
        # entriesの数も一緒にカウントする
        races_in_db = conn.execute('''
            SELECT r.race_number, r.is_exhibition_done, r.is_finished, r.ranking_str, COUNT(e.id) as entry_count
            FROM races r
            LEFT JOIN entries e ON r.id = e.race_id
            WHERE r.race_date = ? AND r.place_code = ?
            GROUP BY r.id
        ''', (iso_date, jcd)).fetchall()
    finally:
        conn.close()
    
    db_race_nums = {r["race_number"] for r in races_in_db}
    finished_nums = {r["race_number"] for r in races_in_db if r["is_finished"]}
    # 着順が不完全な（"--" や 3着分揃っていない等）レース番号も再取得対象にする
    incomplete_result_nums = {
        r["race_number"] for r in races_in_db 
        if r["is_finished"] and (
            not r["ranking_str"] or 
            r["ranking_str"] == "--" or 
            str(r["ranking_str"] or "").count("-") < 2
        )
    }
    ex_done_nums = {r["race_number"] for r in races_in_db if r["is_exhibition_done"]}
    # 6艇揃っていないレース番号のセット
    incomplete_nums = {r["race_number"] for r in races_in_db if r["entry_count"] < 6}

    for rno in range(1, 13):
        # 1. 出走表がない、あるいは6艇揃っていない場合は取得/再取得
        if rno not in db_race_nums or rno in incomplete_nums:
            scrape_race_syusso(jcd, rno, target_dt)
            time.sleep(0.2)
        
        # 2. すでに正常に終了済みの場合はスキップ (不完全な結果はスキップしない)
        if rno in finished_nums and rno not in incomplete_result_nums:
            continue
            
        # 3. 展示更新
        if rno not in ex_done_nums:
            update_exhibition(jcd, rno, target_dt)
            time.sleep(0.2)
        
        # 4. 結果更新
        update_result(jcd, rno, target_dt)
        time.sleep(0.2)

def update_all_active_races(target_dt: datetime.datetime = None):
    """全開催場の展示・結果を巡回更新する (バックグラウンドワーカー用)"""
    start_time = time.time()
    target_dt, target_date_str, iso_date = get_current_date(target_dt)
    print(f"\n[WORKER] Periodic update started at {datetime.datetime.now(JST)}")
    
    active_jcds = scrape_index(target_dt)
    if not active_jcds:
        print("[WORKER] No active places found.")
        return

    # 優先順位付け：締切時刻が近い会場から先に処理する
    # DBから各会場の「次に始まる未終了レースの時刻」を取得
    prioritized_jcds = []
    conn = get_db_connection()
    try:
        for jcd in active_jcds:
            next_race = conn.execute(
                "SELECT scheduled_time FROM races WHERE race_date = ? AND place_code = ? AND is_finished = 0 ORDER BY race_number LIMIT 1",
                (iso_date, jcd)
            ).fetchone()
            # 時刻がない、または終了済みの場合は後回しにするための大きな値
            sort_key = next_race['scheduled_time'] if next_race and next_race['scheduled_time'] else "23:59"
            prioritized_jcds.append((sort_key, jcd))
    finally:
        conn.close()
    
    # 時刻順にソート
    prioritized_jcds.sort()
    sorted_jcds = [item[1] for item in prioritized_jcds]

    # 全開催場を並列で巡回 (最大5スレッド)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(update_venue_races, jcd, target_dt): jcd for jcd in sorted_jcds}
        for future in concurrent.futures.as_completed(futures):
            jcd = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[WORKER ERROR] Parallel update failed for {places_dict.get(jcd, jcd)}: {e}")

    duration = time.time() - start_time
    print(f"[WORKER] Periodic update finished in {duration:.1f}s at {datetime.datetime.now(JST)}\n")

def get_racer_results_stats(toban: str, jcd: str = None, date_str: str = None):
    """
    選手の詳細情報を取得する。
    1. プロフィール・期別成績 (data/racersearch/profile)
    2. 過去3節成績 (data/racersearch/back3)
    3. 今節成績 (特定レースのracelistページから抽出)
    """
    stats = {
        "racer_id": toban,
        "profile": {},
        "seasonal": {},
        "back3": [],
        "current_series": []
    }

    # 1. プロフィール & 期別成績
    profile_url = f"https://www.boatrace.jp/owpc/pc/data/racersearch/profile?toban={toban}"
    try:
        res = requests.get(profile_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        if res.status_code == 200:
            soup = BeautifulSoup(res.content, "html.parser")
            # 氏名
            name_el = soup.select_one(".racer_name")
            if name_el:
                stats["profile"]["name"] = name_el.get_text(strip=True)
            
            # 詳細情報 (級別、出身地など)
            profile_table = soup.select_one(".is-p_profile")
            if profile_table:
                dls = profile_table.select("dl")
                for dl in dls:
                    dt = dl.select_one("dt").get_text(strip=True)
                    dd = dl.select_one("dd").get_text(strip=True)
                    if "級別" in dt: stats["profile"]["class"] = dd
                    if "支部" in dt: stats["profile"]["branch"] = dd
                    if "出身地" in dt: stats["profile"]["hometown"] = dd
            
            # 期別成績
            season_url = f"https://www.boatrace.jp/owpc/pc/data/racersearch/season?toban={toban}"
            sres = requests.get(season_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
            if sres.status_code == 200:
                ssoup = BeautifulSoup(sres.content, "html.parser")
                table = ssoup.select_one(".is-p_result")
                if table:
                    rows = table.select("tr")
                    for row in rows:
                        th = row.select_one("th")
                        td = row.select_one("td")
                        if th and td:
                            txt = th.get_text(strip=True)
                            val = td.get_text(strip=True)
                            if "勝率" in txt: stats["seasonal"]["win_rate"] = val
                            if "2連対率" in txt: stats["seasonal"]["quinella_rate"] = val
                            if "平均ST" in txt: stats["seasonal"]["avg_st"] = val
    except Exception as e:
        print(f"Error scraping profile: {e}")

    # 2. 過去3節成績
    back3_url = f"https://www.boatrace.jp/owpc/pc/data/racersearch/back3?toban={toban}"
    try:
        res = requests.get(back3_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        if res.status_code == 200:
            soup = BeautifulSoup(res.content, "html.parser")
            # 過去3節のテーブルを取得
            # 構造を詳細にみると is-p_result クラスのテーブルが複数並んでいる
            tables = soup.select(".is-p_result")
            for table in tables:
                caption = table.select_one("caption")
                if not caption: continue
                title = caption.get_text(strip=True)
                
                rows = table.select("tbody tr")
                ranks = []
                for r in rows:
                    tds = r.select("td")
                    if len(tds) >= 4:
                        # 最後のカラムが節間成績 (<a>タグのリスト)
                        res_td = tds[-1]
                        # すべての<a>タグから着順を取得
                        r_links = res_td.select("a")
                        for a in r_links:
                            r_txt = a.get_text(strip=True)
                            if r_txt: ranks.append(r_txt)
                
                if ranks:
                    stats["back3"].append({
                        "series_title": title,
                        "ranks": ranks
                    })
    except Exception as e:
        print(f"Error scraping back3: {e}")

    # 3. 今節成績 (出走表から)
    if jcd and date_str:
        hd = date_str.replace("-", "")
        # 出走表ページに今節成績が載っている
        racelist_url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd={jcd}&hd={hd}"
        try:
            res = requests.get(racelist_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
            if res.status_code == 200:
                soup = BeautifulSoup(res.content, "html.parser")
                racer_links = soup.select(f"a[href*='toban={toban}']")
                if racer_links:
                    target_tr = None
                    for link in racer_links:
                        parent = link.find_parent("tr")
                        if parent: 
                            target_tr = parent
                            break
                    
                    if target_tr:
                        perf_tds = target_tr.select("td.is-over1024")
                        for td in perf_tds:
                            txt = td.get_text("|", strip=True)
                            parts = txt.split("|")
                            # レース番号, コース/ST, 着順 の3要素セットでループ
                            for i in range(0, len(parts) - 2, 3):
                                stats["current_series"].append({
                                    "race_no": parts[i],
                                    "course_st": parts[i+1],
                                    "rank": parts[i+2]
                                })
        except Exception as e:
            print(f"Error scraping current series: {e}")

    return stats

def repair_corrupted_races(days_back=2):
    """
    過去N日間のレースを調査し、出走艇データが6艇に満たない不完全なものを再取得する。
    """
    print(f"[REPAIR] Checking for corrupted races in the last {days_back} days...")
    from datetime import datetime, timedelta
    from app_config import JST
    
    conn = get_db_connection()
    try:
        # 直近N日の不完全なレースを取得
        threshold_date = (datetime.now(JST) - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        corrupted = conn.execute('''
            SELECT r.id, r.race_date, r.place_code, r.race_number, COUNT(e.id) as cnt
            FROM races r
            LEFT JOIN entries e ON r.id = e.race_id
            WHERE r.race_date >= ?
            GROUP BY r.id
            HAVING cnt < 6
        ''', (threshold_date,)).fetchall()
        
        if not corrupted:
            print("[REPAIR] No corrupted races found.")
            return
            
        print(f"[REPAIR] Found {len(corrupted)} corrupted races. Repairing...")
        for row in corrupted:
            date_str = row['race_date']
            jcd = row['place_code']
            race_no = row['race_number']
            
            print(f"[REPAIR] Repairing {date_str} {jcd} {race_no}R (current boats: {row['cnt']})...")
            target_dt = datetime.strptime(date_str, '%Y-%m-%d')
            scrape_race_syusso(jcd, race_no, target_dt)
            
        print("[REPAIR] Repair completed.")
    except Exception as e:
        print(f"[REPAIR ERROR] {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    from database import init_db
    init_db()
