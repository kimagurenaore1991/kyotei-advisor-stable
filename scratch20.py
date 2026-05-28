with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

depth = 0
for i, line in enumerate(lines):
    if i < 1280 or i > 4130: continue
    l = line.split('//')[0]
    old_depth = depth
    depth += l.count('{')
    depth -= l.count('}')
    
    if old_depth == 0 and depth == 1:
        print(f"Opened at line {i+1}: {line.strip()}")
