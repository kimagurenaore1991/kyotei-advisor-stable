with open('static/index.html', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if 'switchTab' in line:
            print(f'Line {i+1}: {line.strip()}')
