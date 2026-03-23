import requests
import datetime
from app_config import JST, USER_AGENT

HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept-Language': 'ja,en;q=0.9',
}

def test_scrape_index():
    target_date_str = datetime.datetime.now(JST).strftime('%Y%m%d')
    url = f"https://www.boatrace.jp/owpc/pc/race/index?hd={target_date_str}"
    print(f"Testing URL: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        print(f"Status Code: {response.status_code}")
        print(f"Response Length: {len(response.content)}")
        if response.status_code == 200:
            print("Successfully reached boatrace.jp")
        else:
            print(f"Failed to reach boatrace.jp: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_scrape_index()
