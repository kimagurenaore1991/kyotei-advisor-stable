import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.getcwd())

import scraper
import json

def test_search():
    print("Searching for '峰' (Global)...")
    results = scraper.search_racers_global("峰")
    print(f"Results found: {len(results)}")
    for r in results[:5]:
        print(f" - {r['toban']} {r['name']} ({r['class']})")
    
    if not results:
        print("FAIL: No results found for '峰'")
        return

    print("\nSearching for '4320' (Global)...")
    results_id = scraper.search_racers_global("4320")
    print(f"Results found: {len(results_id)}")
    for r in results_id:
        print(f" - {r['toban']} {r['name']} ({r['class']})")
    
    if not any(r['toban'] == '4320' for r in results_id):
        print("FAIL: Toban 4320 not found")
        return

    print("\nSUCCESS: Global search functions as expected.")

if __name__ == "__main__":
    test_search()
