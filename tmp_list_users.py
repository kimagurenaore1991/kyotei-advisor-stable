from supabase_client import get_supabase_client
from datetime import datetime, timezone

def list_users():
    supabase = get_supabase_client()
    if not supabase:
        print("Supabase client not initialized.")
        return

    try:
        # profilesテーブルから全ユーザーを取得（デバッグ用）
        res = supabase.table("profiles").select("*").execute()
        users = res.data
        if not users:
            print("No users found in profiles table.")
            return

        print(f"Found {len(users)} users:")
        for u in users:
            print(f"ID: {u.get('id')}, Email: {u.get('email')}, Premium: {u.get('is_premium')}, Updated: {u.get('updated_at')}")
    except Exception as e:
        print(f"Error querying users: {e}")

if __name__ == "__main__":
    list_users()
