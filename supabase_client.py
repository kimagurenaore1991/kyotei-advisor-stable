from supabase import create_client, Client
from app_config import SUPABASE_URL, SUPABASE_KEY, USE_SUPABASE

_client = None

def get_supabase_client(force_refresh=False) -> Client | None:
    global _client
    if not USE_SUPABASE:
        return None
    if _client is None or force_refresh:
        if not SUPABASE_URL or not SUPABASE_KEY:
            return None
        try:
            if force_refresh:
                print("[SUPABASE] Refreshing client connection...")
            _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            print(f"[SUPABASE ERROR] Initializing client: {e}")
            _client = None
            return None
    return _client

def is_supabase_enabled() -> bool:
    return USE_SUPABASE and get_supabase_client() is not None

def upsert_races(races_data: list[dict], retry=True):
    if not is_supabase_enabled(): return
    supabase = get_supabase_client()
    try:
        return supabase.table("races").upsert(races_data, on_conflict="race_date,place_code,race_number").execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return upsert_races(races_data, retry=False)
        print(f"[SUPABASE ERROR] upsert_races: {e}")
        return None

def upsert_entries(entries_data: list[dict], retry=True):
    if not is_supabase_enabled(): return
    supabase = get_supabase_client()
    try:
        return supabase.table("entries").upsert(entries_data, on_conflict="race_date,place_code,race_number,boat_number").execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return upsert_entries(entries_data, retry=False)
        print(f"[SUPABASE ERROR] upsert_entries: {e}")
        return None

def upsert_racer_results(results_data: list[dict], retry=True):
    if not is_supabase_enabled(): return
    supabase = get_supabase_client()
    try:
        return supabase.table("racer_results").upsert(results_data, on_conflict="racer_id,place_code,race_date,race_no").execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return upsert_racer_results(results_data, retry=False)
        print(f"[SUPABASE ERROR] upsert_racer_results: {e}")
        return None

def upsert_racer_profiles(profiles_data: list[dict], retry=True):
    if not is_supabase_enabled(): return
    supabase = get_supabase_client()
    try:
        return supabase.table("racer_profiles").upsert(profiles_data, on_conflict="toban").execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return upsert_racer_profiles(profiles_data, retry=False)
        print(f"[SUPABASE ERROR] upsert_racer_profiles: {e}")
        return None

def upsert_favorites(favorites_data: list[dict], retry=True):
    if not is_supabase_enabled(): return
    supabase = get_supabase_client()
    try:
        return supabase.table("favorite_racers").upsert(favorites_data, on_conflict="toban,user_id").execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return upsert_favorites(favorites_data, retry=False)
        print(f"[SUPABASE ERROR] upsert_favorites: {e}")
        return None

def cleanup_supabase_storage(threshold_date_iso: str, retry=True):
    """
    基準日より古いデータの重いカラム（JSONデータ）をNULL化して容量を節約する。
    行自体（出走表や選手情報）は残す。
    """
    if not is_supabase_enabled(): return
    try:
        supabase = get_supabase_client()
        print(f"[SUPABASE] Thinning data older than {threshold_date_iso}...")
        
        # オッズ、結果、予測などの重いJSONデータをクリア
        response = supabase.table("races").update({
            "odds_json": None,
            "result_json": None,
            "ai_predictions_json": None
        }).lt("race_date", threshold_date_iso).execute()
        
        return response
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return cleanup_supabase_storage(threshold_date_iso, retry=False)
        print(f"[SUPABASE ERROR] cleanup_supabase_storage: {e}")
        return None

def delete_very_old_races(threshold_date_iso: str, retry=True):
    """
    古いデータを完全に削除する。
    """
    if not is_supabase_enabled(): return
    try:
        supabase = get_supabase_client()
        print(f"[SUPABASE] Deleting data before {threshold_date_iso}...")
        
        # 1. 出走艇データの削除 (entries)
        # 外部キー制約がない場合でも、論理的な整合性のために先に削除
        supabase.table("entries").delete().lt("race_date", threshold_date_iso).execute()
        
        # 2. レース本体の削除 (races)
        return supabase.table("races").delete().lt("race_date", threshold_date_iso).execute()
    except Exception as e:
        if retry and ("disconnected" in str(e).lower() or "connection" in str(e).lower()):
            get_supabase_client(force_refresh=True)
            return delete_very_old_races(threshold_date_iso, retry=False)
        print(f"[SUPABASE ERROR] delete_very_old_races: {e}")
        return None
