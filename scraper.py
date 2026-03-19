import requests
from bs4 import BeautifulSoup
import datetime
import time
import concurrent.futures

from app_config import JST, LOCK_FILE, REQUEST_TIMEOUT, USER_AGENT
from database import get_db_connection, init_db


def get_current_date():
    """Returns (now_jst, target_date_str as YYYYMMDD, iso_date as YYYY-MM-DD)"""
    now_jst = datetime.datetime.now(JST)
    target_date_str = now_jst.strftime('%Y%m%d')
    iso_date = now_jst.strftime('%Y-%m-%d')
    return now_jst, target_date_str, iso_date

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

def scrape_index():
    """本日の開催場JCDコード一覧を公式サイトから取得する"""
    _, target_date_str, _ = get_current_date()
    url = f"https://www.boatrace.jp/owpc/pc/race/index?hd={target_date_str}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        active_jcd_list = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'jcd=' in href and 'raceindex' in href:
                jcd = href.split('jcd=')[1].split('&')[0].zfill(2)
                if jcd in places_dict and jcd not in active_jcd_list:
                    active_jcd_list.append(jcd)
        return active_jcd_list
    except Exception as e:
        print(f"[ERROR] scrape_index: {e}")
        return []

def _get_grade_from_soup(soup):
    """ページHTMLからレースグレード/タイトルを返す"""
    # 1. ページ内の全要素からグレードを示すクラスを探す
    found_grade = ""
    for el in soup.find_all(True):
        cls_list = el.get('class', [])
        cls_str = " ".join(cls_list) if isinstance(cls_list, list) else str(cls_list)
        if 'is-G1b' in cls_str or 'is-grade1' in cls_str:
            found_grade = "G1"; break
        elif 'is-G2b' in cls_str or 'is-grade2' in cls_str:
            found_grade = "G2"; break
        elif 'is-G3b' in cls_str or 'is-grade3' in cls_str:
            found_grade = "G3"; break
        elif 'is-SGb' in cls_str or 'is-sg' in cls_str:
            found_grade = "SG"; break
        elif 'is-lady' in cls_str or 'is-Lady' in cls_str:
            found_grade = "女子"

    # 2. タイトルテキストの特定
    main_content = soup.select_one('.contents, main, #main, #contents') or soup
    title_el = main_content.select_one('.heading2_titleName, .heading2_title, .title_race__titleName, h2, h3')
    found_title = title_el.get_text(separator=' ', strip=True) if title_el else ""

    # 3. 強制文字列判定 (クラスが取れない場合の最終手段)
    t_up = found_title.upper()
    if not found_grade:
        if '尼崎' in t_up and 'センプル' in t_up: found_grade = "G1"
        elif 'SG' in t_up or 'ＳＧ' in t_up: found_grade = "SG"
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

def scrape_race_syusso(jcd, race_no):
    """特定のレースの出走表データを取得しDBに保存。race_titleも取得・更新する。"""
    _, target_date_str, iso_date = get_current_date()
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

            # 既存レースを確認
            cursor.execute(
                'SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?',
                (iso_date, jcd, race_no)
            )
            existing = cursor.fetchone()

            if existing:
                race_id = existing[0]
                # 既存でも race_title を更新（グレード情報を最新に保つ）
                if race_title:
                    cursor.execute(
                        'UPDATE races SET race_title = ? WHERE id = ?',
                        (race_title, race_id)
                    )
                cursor.execute('DELETE FROM entries WHERE race_id = ?', (race_id,))
            else:
                try:
                    cursor.execute('''
                        INSERT INTO races (race_date, place_code, place_name, race_number,
                                           weather, wind_direction, wind_speed, wave_height, race_title)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (iso_date, jcd, place_name, race_no,
                          '不明', '', 0.0, 0.0, race_title))
                    race_id = cursor.lastrowid
                except sqlite3.IntegrityError:
                    cursor.execute(
                        'SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?',
                        (iso_date, jcd, race_no)
                    )
                    race_id = cursor.fetchone()[0]
                    cursor.execute('DELETE FROM entries WHERE race_id = ?', (race_id,))

            tbody_list = soup.select('tbody.is-fs12')
            if not tbody_list:
                # データなし→レコードも不要なら削除
                if not existing:
                    cursor.execute('DELETE FROM races WHERE id = ?', (race_id,))
                conn.commit()
                print(f"    -> No entry data for {place_name} {race_no}R")
                return

            for idx, tbody in enumerate(tbody_list):
                if idx >= 6:
                    break
                boat_number = idx + 1
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
                        INSERT INTO entries (
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
        except Exception as db_e:
            print(f"  [DB ERROR] {jcd} {race_no}R: {db_e}")
        finally:
            conn.close()

    except Exception as e:
        print(f"  [ERROR] scrape_race_syusso({jcd},{race_no}): {e}")


def scrape_today():
    """本日の全開催場、全レース(1~12R)のデータを取得してDBに保存"""
    _, target_date_str, iso_date = get_current_date()
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
        active_jcds = scrape_index()
        print(f"本日開催場: {[places_dict.get(j) for j in active_jcds]}")

        if not active_jcds:
            print("開催中の場なし。")
            return

        # 並列スクレイピング (max_workers=3 に減らして安定化)
        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for jcd in active_jcds:
                for race_no in range(1, 13):
                    tasks.append(executor.submit(scrape_race_syusso, jcd, race_no))
            for future in concurrent.futures.as_completed(tasks):
                try:
                    future.result()
                except Exception as e:
                    print(f"[TASK ERROR] {e}")

        print("=== データ取得完了 ===")
    finally:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()


if __name__ == "__main__":
    scrape_today()
