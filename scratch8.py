with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if ' overlay ' in line or 'overlay=' in line or 'const overlay' in line:
        print(f'Line {i+1}: {line.strip()}')
