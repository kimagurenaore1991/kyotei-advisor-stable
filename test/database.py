import sqlite3
from app_config import DB_NAME

MIGRATIONS = [
    "ALTER TABLE races ADD COLUMN race_title TEXT DEFAULT ''",
    "ALTER TABLE entries ADD COLUMN racer_id TEXT DEFAULT ''",
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_races_date_place ON races(race_date, place_code, race_number)",
    "CREATE INDEX IF NOT EXISTS idx_entries_race_id ON entries(race_id)",
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
            FOREIGN KEY (race_id) REFERENCES races(id) ON DELETE CASCADE
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


if __name__ == "__main__":
    init_db()
