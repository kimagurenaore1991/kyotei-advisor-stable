import sqlite3
import json
from datetime import datetime, timedelta
from app_config import DB_NAME, JST
from supabase_client import get_supabase_client, is_supabase_enabled, upsert_races, upsert_entries, upsert_racer_results, cleanup_supabase_storage, delete_very_old_races

MIGRATIONS = [
    "ALTER TABLE races ADD COLUMN race_title TEXT DEFAULT ''",
    "ALTER TABLE entries ADD COLUMN racer_id TEXT DEFAULT ''",
    "ALTER TABLE entries ADD COLUMN tilt REAL DEFAULT 0.0",
    "ALTER TABLE races ADD COLUMN is_finished BOOLEAN DEFAULT 0",
    "ALTER TABLE races ADD COLUMN ranking_str TEXT DEFAULT ''",
    "ALTER TABLE races ADD COLUMN ai_predictions_json TEXT",
    "ALTER TABLE entries ADD COLUMN is_absent BOOLEAN DEFAULT 0",
    "ALTER TABLE races ADD COLUMN result_json TEXT",
    "ALTER TABLE races ADD COLUMN odds_json TEXT",
    "ALTER TABLE races ADD COLUMN scheduled_time TEXT DEFAULT ''",
    "ALTER TABLE entries ADD COLUMN parts_exchange TEXT DEFAULT ''",
    "ALTER TABLE entries ADD COLUMN weight_adjustment REAL DEFAULT 0.0",
    "ALTER TABLE entries ADD COLUMN pre_inspection_time REAL",
    "ALTER TABLE entries ADD COLUMN propeller TEXT DEFAULT ''",
    "ALTER TABLE races ADD COLUMN day_label TEXT DEFAULT ''"
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_races_date_place ON races(race_date, place_code, race_number)",
    "CREATE INDEX IF NOT EXISTS idx_entries_race_id ON entries(race_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_race_boat ON entries(race_id, boat_number)",
    "CREATE INDEX IF NOT EXISTS idx_racer_results_racer ON racer_results(racer_id)",
    "CREATE INDEX IF NOT EXISTS idx_racer_results_place ON racer_results(racer_id, place_code)",
]


def get_db_connection(timeout: float = 30.0) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME, timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)};")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_date TEXT NOT NULL,
            place_code TEXT NOT NULL,
            place_name TEXT NOT NULL,
            race_number INTEGER NOT NULL,
            race_title TEXT DEFAULT '',
            weather TEXT,
            wind_direction TEXT,
            wind_speed REAL,
            wave_height REAL,
            is_exhibition_done BOOLEAN DEFAULT 0,
            scheduled_time TEXT DEFAULT '',
            UNIQUE(race_date, place_code, race_number)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER,
            boat_number INTEGER,
            racer_name TEXT,
            racer_class TEXT,
            racer_id TEXT DEFAULT '',
            global_win_rate REAL,
            global_2_quinella REAL,
            local_win_rate REAL,
            local_2_quinella REAL,
            motor_number INTEGER,
            motor_2_quinella REAL,
            boat_number_machine INTEGER,
            boat_2_quinella REAL,
            exhibition_time REAL,
            start_timing REAL,
            entry_course INTEGER,
            arrival_order INTEGER,
            race_time TEXT,
            tilt REAL DEFAULT 0.0,
            parts_exchange TEXT DEFAULT '',
            weight_adjustment REAL DEFAULT 0.0,
            pre_inspection_time REAL,
            propeller TEXT DEFAULT '',
            FOREIGN KEY (race_id) REFERENCES races(id) ON DELETE CASCADE,
            UNIQUE(race_id, boat_number)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS racer_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            racer_id TEXT NOT NULL,
            place_code TEXT NOT NULL,
            place_name TEXT DEFAULT '',
            race_date TEXT NOT NULL,
            race_no INTEGER NOT NULL,
            course INTEGER,
            start_timing REAL,
            rank INTEGER,
            race_title TEXT DEFAULT '',
            updated_at TEXT DEFAULT '',
            UNIQUE(racer_id, place_code, race_date, race_no)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS racer_profiles (
            toban TEXT PRIMARY KEY,
            name TEXT,
            course_stats_json TEXT,
            updated_at TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS favorite_racers (
            toban TEXT PRIMARY KEY,
            name TEXT,
            created_at TEXT
        )
        """
    )

    for sql in MIGRATIONS:
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError:
            pass

    for sql in INDEXES:
        cursor.execute(sql)

    conn.commit()
    conn.close()
    print(f"データベース初期化完了: {DB_NAME}")


def cleanup_old_data(threshold_date_iso: str) -> None:
    """指定された日付より古い（昨日以前の）データを削除する"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.execute("DELETE FROM races WHERE race_date < ?", (threshold_date_iso,))
        deleted_count = cursor.rowcount
        conn.commit()
        if deleted_count > 0:
            print(f"[DATABASE] {deleted_count}件の古いレースデータを削除しました (基準日: {threshold_date_iso})")
        
        # Supabase側のクリーンアップ設定（3日以上前を完全削除）
        # threshold_date_iso は today - 2日 なので、それより小さい（=3日以上前）を削除
        delete_very_old_races(threshold_date_iso)
    except Exception as e:
        print(f"[DATABASE ERROR] cleanup_old_data: {e}")
    finally:
        conn.close()


# ─────────────────────────── Supabase Sync ───────────────────────────
_syncing_dates = set()

def sync_specific_date_from_supabase(date_iso: str):
    """
    指定された日付のデータのみをSupabaseから取得し、ローカルSQLiteに反映する。
    初回起動時や日付切り替え時のパフォーマン向上を目的に、対象範囲を絞って高速に同期する。
    """
    if date_iso in _syncing_dates:
        print(f"[SUPABASE] Sync already in progress for {date_iso}. Skipping.")
        return
    
    _syncing_dates.add(date_iso)
    try:
        from supabase_client import get_supabase_client
        supabase = get_supabase_client()
        if not supabase:
            print(f"[SUPABASE] Sync disabled or client not initialized for {date_iso}.")
            return
        
        # 1. Racesの取得 (指定日のみ)
        races_res = supabase.table("races").select("*").eq("race_date", date_iso).execute()
        races = races_res.data
        if not races:
            print(f"[SUPABASE] No race data found for {date_iso}. Falling back to official scraper.")
            import scraper
            import datetime
            try:
                target_dt = datetime.datetime.strptime(date_iso, '%Y-%m-%d').replace(tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
                scraper.scrape_today(target_dt)
            except Exception as e:
                print(f"[SCRAPER FALLBACK ERROR] {e}")
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for r in races:
            # SQLite用に変換
            ai_pred = json.dumps(r['ai_predictions_json']) if r.get('ai_predictions_json') else None
            result_json = json.dumps(r['result_json']) if r.get('result_json') else None
            odds_json = json.dumps(r['odds_json']) if r.get('odds_json') else None
            
            cursor.execute('''
                INSERT OR REPLACE INTO races (
                    race_date, place_code, place_name, race_number, race_title,
                    weather, wind_direction, wind_speed, wave_height,
                    is_exhibition_done, scheduled_time, is_finished, ranking_str,
                    ai_predictions_json, result_json, odds_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (r['race_date'], r['place_code'], r['place_name'], r['race_number'], r.get('race_title', ''),
                  r.get('weather'), r.get('wind_direction'), r.get('wind_speed'), r.get('wave_height'),
                  r.get('is_exhibition_done'), r.get('scheduled_time'), r.get('is_finished'), r.get('ranking_str'),
                  ai_pred, result_json, odds_json))
        
        # 2. Entriesの取得 (指定日のみ)
        entries_res = supabase.table("entries").select("*").eq("race_date", date_iso).execute()
        entries = entries_res.data
        
        if entries:
            for e in entries:
                # race_idをSQLiteから逆引き
                row = cursor.execute(
                    "SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?",
                    (e['race_date'], e['place_code'], e['race_number'])
                ).fetchone()
                
                if row:
                    race_id = row[0]
                    cursor.execute('''
                        INSERT OR REPLACE INTO entries (
                            race_id, boat_number, racer_name, racer_class, racer_id,
                            global_win_rate, global_2_quinella, local_win_rate, local_2_quinella,
                            motor_number, motor_2_quinella, boat_number_machine, boat_2_quinella,
                            exhibition_time, start_timing, entry_course, arrival_order, race_time,
                            tilt, is_absent, parts_exchange, weight_adjustment, pre_inspection_time, propeller
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (race_id, e['boat_number'], e.get('racer_name'), e.get('racer_class'), e.get('racer_id'),
                          e.get('global_win_rate'), e.get('global_2_quinella'), e.get('local_win_rate'), e.get('local_2_quinella'),
                          e.get('motor_number'), e.get('motor_2_quinella'), e.get('boat_number_machine'), e.get('boat_2_quinella'),
                          e.get('exhibition_time'), e.get('start_timing'), e.get('entry_course'), e.get('arrival_order'), e.get('race_time'),
                          e.get('tilt', 0.0), 1 if e.get('is_absent') else 0,
                          e.get('parts_exchange', ''), e.get('weight_adjustment', 0.0), e.get('pre_inspection_time'), e.get('propeller', '')))
        
        conn.commit()
        conn.close()
        print(f"[SUPABASE] Targeted sync success for {date_iso}: {len(races)} races, {len(entries) if entries else 0} entries.")
    except Exception as e:
        print(f"[SUPABASE ERROR] sync_specific_date_from_supabase: {e}")
    finally:
        if date_iso in _syncing_dates:
            _syncing_dates.remove(date_iso)


def sync_from_supabase(days=1):
    """Supabaseから過去N日分のデータを取得し、ローカルSQLiteを更新する"""
    print(f"[SUPABASE] Syncing last {days} days from Supabase...")
    supabase = get_supabase_client()
    if not supabase:
        print("[SUPABASE] Sync disabled or client not initialized.")
        return
    
    # 基準日の計算
    threshold_dt = datetime.now(JST) - timedelta(days=days)
    threshold_date_iso = threshold_dt.strftime('%Y-%m-%d')
    
    try:
        # 1. Racesの取得
        races_res = supabase.table("races").select("*").gte("race_date", threshold_date_iso).execute()
        races = races_res.data
        
        if not races:
            print("[SUPABASE] No data found for sync.")
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for r in races:
            # SQLite用に変換
            ai_pred = json.dumps(r['ai_predictions_json']) if r.get('ai_predictions_json') else None
            result_json = json.dumps(r['result_json']) if r.get('result_json') else None
            odds_json = json.dumps(r['odds_json']) if r.get('odds_json') else None
            
            cursor.execute('''
                INSERT OR REPLACE INTO races (
                    race_date, place_code, place_name, race_number, race_title,
                    weather, wind_direction, wind_speed, wave_height,
                    is_exhibition_done, scheduled_time, is_finished, ranking_str,
                    ai_predictions_json, result_json, odds_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (r['race_date'], r['place_code'], r['place_name'], r['race_number'], r.get('race_title', ''),
                  r.get('weather'), r.get('wind_direction'), r.get('wind_speed'), r.get('wave_height'),
                  r.get('is_exhibition_done'), r.get('scheduled_time'), r.get('is_finished'), r.get('ranking_str'),
                  ai_pred, result_json, odds_json))
        
        # 2. Entriesの取得
        entries_res = supabase.table("entries").select("*").gte("race_date", threshold_date_iso).execute()
        entries = entries_res.data
        
        if entries:
            for e in entries:
                # race_idをSQLiteから逆引き
                row = cursor.execute(
                    "SELECT id FROM races WHERE race_date = ? AND place_code = ? AND race_number = ?",
                    (e['race_date'], e['place_code'], e['race_number'])
                ).fetchone()
                
                if row:
                    race_id = row[0]
                    cursor.execute('''
                        INSERT OR REPLACE INTO entries (
                            race_id, boat_number, racer_name, racer_class, racer_id,
                            global_win_rate, global_2_quinella, local_win_rate, local_2_quinella,
                            motor_number, motor_2_quinella, boat_number_machine, boat_2_quinella,
                            exhibition_time, start_timing, entry_course, arrival_order, race_time,
                            tilt, is_absent, parts_exchange, weight_adjustment, pre_inspection_time, propeller
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (race_id, e['boat_number'], e.get('racer_name'), e.get('racer_class'), e.get('racer_id'),
                          e.get('global_win_rate'), e.get('global_2_quinella'), e.get('local_win_rate'), e.get('local_2_quinella'),
                          e.get('motor_number'), e.get('motor_2_quinella'), e.get('boat_number_machine'), e.get('boat_2_quinella'),
                          e.get('exhibition_time'), e.get('start_timing'), e.get('entry_course'), e.get('arrival_order'), e.get('race_time'),
                          e.get('tilt', 0.0), 1 if e.get('is_absent') else 0,
                          e.get('parts_exchange', ''), e.get('weight_adjustment', 0.0), e.get('pre_inspection_time'), e.get('propeller', '')))
        
        conn.commit()
        conn.close()
        print(f"[SUPABASE] Sync success: {len(races)} races, {len(entries) if entries else 0} entries.")
    except Exception as e:
        print(f"[SUPABASE ERROR] sync_from_supabase: {e}")


def push_race_to_supabase(race_id_local: int):
    """特定のレース（と全出走艇）をSupabaseに同期する"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Race
        row = cursor.execute("SELECT * FROM races WHERE id = ?", (race_id_local,)).fetchone()
        if not row:
            conn.close()
            return
        r = dict(row)
        race_data = r.copy()
        race_data.pop('id', None)
        for col in ['ai_predictions_json', 'result_json', 'odds_json']:
            val = race_data.get(col)
            if val is not None:
                try: race_data[col] = json.loads(val)
                except: pass
            else:
                race_data[col] = None
        if is_supabase_enabled():
            upsert_races([race_data])
        
        # Entries
        rows = cursor.execute("SELECT * FROM entries WHERE race_id = ?", (race_id_local,)).fetchall()
        entries_data = []
        for erow in rows:
            e = dict(erow)
            e.pop('id', None)
            e.pop('race_id', None)
            e['race_date'] = r['race_date']
            e['place_code'] = r['place_code']
            e['race_number'] = int(r['race_number'])
            try: e['boat_number'] = int(e['boat_number'])
            except: pass
            try:
                if e.get('arrival_order'):
                    e['arrival_order'] = int(e['arrival_order'])
            except: pass
            e['is_absent'] = bool(e.get('is_absent', False))
            entries_data.append(e)
        
        if entries_data and is_supabase_enabled():
            upsert_entries(entries_data)
        
        conn.close()
    except Exception as e:
        print(f"[SUPABASE ERROR] push_race_to_supabase: {e}")


def save_racer_results(racer_id: str, results: list[dict]):
    """選手の過去着順データをSQLiteとSupabaseに保存する"""
    if not results:
        return
    conn = get_db_connection()
    try:
        now_str = datetime.now(JST).isoformat()
        cursor = conn.cursor()
        for r in results:
            cursor.execute('''
                INSERT OR REPLACE INTO racer_results (
                    racer_id, place_code, place_name, race_date, race_no,
                    course, start_timing, rank, race_title, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (racer_id, r.get('place_code', ''), r.get('place_name', ''),
                  r.get('race_date', ''), r.get('race_no', 0),
                  r.get('course'), r.get('start_timing'),
                  r.get('rank'), r.get('race_title', ''), now_str))
        conn.commit()

        # Supabase同期
        supa_data = []
        for r in results:
            supa_data.append({
                'racer_id': racer_id,
                'place_code': r.get('place_code', ''),
                'place_name': r.get('place_name', ''),
                'race_date': r.get('race_date', ''),
                'race_no': r.get('race_no', 0),
                'course': r.get('course'),
                'start_timing': r.get('start_timing'),
                'rank': r.get('rank'),
                'race_title': r.get('race_title', ''),
                'updated_at': now_str
            })
        if is_supabase_enabled():
            upsert_racer_results(supa_data)
    except Exception as e:
        print(f"[DB ERROR] save_racer_results: {e}")
    finally:
        conn.close()


def get_racer_results(racer_id: str, place_code: str = None) -> list[dict]:
    """選手の過去着順データをDBから取得（place_code指定時はそのレース場のみ）
    racer_resultsテーブルと、既にDBにあるentriesテーブル（確定分）の両方から取得して統合する。
    """
    conn = get_db_connection()
    try:
        # racer_results から取得
        query_base = 'SELECT race_date, place_code, place_name, race_no, course, start_timing, rank, race_title FROM racer_results WHERE racer_id = ?'
        params_base = [racer_id]
        if place_code:
            query_base += ' AND place_code = ?'
            params_base.append(place_code)

        rows_historical = conn.execute(query_base, params_base).fetchall()
        results = [dict(r) for r in rows_historical]

        # entries/races から今節などの確定分を取得（重複を避けるため後でマージ検討するが一旦単純取得）
        query_entries = '''
            SELECT r.race_date, r.place_code, r.place_name, r.race_number as race_no, 
                   e.entry_course as course, e.start_timing, e.arrival_order as rank, r.race_title
            FROM entries e
            JOIN races r ON e.race_id = r.id
            WHERE e.racer_id = ? AND e.arrival_order IS NOT NULL
        '''
        params_entries = [racer_id]
        if place_code:
            query_entries += ' AND r.place_code = ?'
            params_entries.append(place_code)
        
        rows_entries = conn.execute(query_entries, params_entries).fetchall()
        
        seen_keys = set()
        for x in results:
            date_part = x['race_date'][-5:] if x['race_date'] else ""
            seen_keys.add((date_part, x['place_code'], x['race_no']))

        for r in rows_entries:
            d = dict(r)
            date_part = d['race_date'][-5:] if d['race_date'] else ""
            key = (date_part, d['place_code'], d['race_no'])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            results.append(d)

        # 日付とレース番号でソート（新しい順）
        # 日付形式が混在している可能性（YYYY-MM-DD vs MM/DD）があるため正規化してソート
        def sort_key(x):
            date_str = x['race_date']
            if '/' in date_str and '-' not in date_str:
                # MM/DD 形式なら仮に今年の年を付ける
                month, day = date_str.split('/')
                date_str = f"{datetime.now().year}-{month.zfill(2)}-{day.zfill(2)}"
            return (date_str, x['race_no'])

        results.sort(key=sort_key, reverse=True)
        return results
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
