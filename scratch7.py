with open('static/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

elements = [
    'menu-btn', 'overlay', 'setting-bet-type', 'recalc-btn', 
    'reset-btn', 'manual-apply-btn', 'ai-recalc-btn'
]

for el in elements:
    if f'id="{el}"' not in content and f"id='{el}'" not in content:
        print(f"Element {el} NOT FOUND!")
