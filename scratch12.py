import sys
with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if 1280 < i < 1500:
        print(f'{i+1}: {line.rstrip()}')
