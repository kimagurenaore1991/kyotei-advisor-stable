from supabase_client import get_supabase_client
from datetime import datetime, timezone, timedelta

def unlock_user(email):
    supabase = get_supabase_client()
    if not supabase:
        print("Supabase client not initialized.")
        return

    try:
        # ユーザーを取得
        res = supabase.table("profiles").select("id").eq("email", email).execute()
        if not res.data:
            print(f"User with email {email} not found.")
            return
        
        user_id = res.data[0]['id']
        ends_at = (datetime.now(timezone.utc) + timedelta(days=31)).isoformat()
        
        data = {
            "subscription_status": "premium",
            "is_premium": True,
            "subscription_ends_at": ends_at,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        supabase.table("profiles").update(data).eq("id", user_id).execute()
        print(f"Successfully unlocked user: {email} (ID: {user_id})")
    except Exception as e:
        print(f"Error unlocking user {email}: {e}")

if __name__ == "__main__":
    unlock_user("hos19910209@gmail.com")
    unlock_user("noboribetupc1@gmail.com")
