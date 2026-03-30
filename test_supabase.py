import os
import json
from supabase import create_client, Client
from app_config import SUPABASE_URL, SUPABASE_KEY

def test_supabase():
    print(f"Testing Supabase connection to: {SUPABASE_URL}")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Test 1: Fetch from races
        print("Testing: Fetching from 'races' table...")
        res = supabase.table("races").select("count", count="exact").limit(1).execute()
        print(f"Success! Exact count of races: {res.count}")
        
        # Test 2: Fetch recent races
        print("Testing: Fetching recent 3 races...")
        res_data = supabase.table("races").select("*").order("race_date", desc=True).limit(3).execute()
        print(f"Fetched {len(res_data.data)} recent races.")
        for r in res_data.data:
            print(f" - {r.get('race_date')} {r.get('place_name')} {r.get('race_number')}R")
            
        if not res_data.data:
            print("WARNING: 'races' table is EMPTY on Supabase.")
            
    except Exception as e:
        print(f"FAILED to connect or query Supabase: {e}")

if __name__ == "__main__":
    test_supabase()
