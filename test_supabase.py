import os
import json
from datetime import datetime, timezone
from supabase import create_client

# Load config
from app_config import SUPABASE_URL, SUPABASE_KEY

def test_insert_profile():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Mock user_id (needs to be a valid UUID or existing auth user ID if RLS is strict)
    # But here we just want to see the error message from Supabase
    test_user_id = "00000000-0000-0000-0000-000000000000" 
    
    new_profile_data = {
        "id": test_user_id,
        "is_premium": False,
        "trial_started_at": datetime.now(timezone.utc).isoformat()
    }
    
    print(f"Attempting to insert into profiles with URL: {SUPABASE_URL}")
    try:
        res = supabase.table("profiles").insert(new_profile_data).execute()
        print("Success!")
        print(res.data)
    except Exception as e:
        print("Failed!")
        print(f"Error: {e}")

if __name__ == "__main__":
    test_insert_profile()
