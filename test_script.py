import urllib.request
import json
from datetime import datetime, timedelta

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print('Yesterday:', yesterday)

body = {
    'weights': {},
    'settings': {
        'max_items': 3,
        'hit_type': 'ai',
        'bet_type': '3t',
        'fixed_1st': False,
        'ai_prediction_mode': 0,
        'custom_prediction_mode': 0
    }
}

req = urllib.request.Request(f'http://localhost:8000/api/daily_hits?date={yesterday}',
    data=json.dumps(body).encode('utf-8'),
    headers={'Content-Type': 'application/json'})

try:
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read().decode('utf-8'))
        for k, v in data.items():
            if v.get('hit'):
                print(f"Race {k} ({v['place']} {v['race_no']}R): hit={v['hit']}, payout={v['payout']}, ranking={v['ranking']}")
except Exception as e:
    print('Error:', e)
