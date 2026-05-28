with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

depth = 0
for i, line in enumerate(lines):
    if i < 1280 or i > 4200: continue
    # Extremely basic depth counter (ignores comments/strings, just for a rough idea)
    l = line.split('//')[0]
    depth += l.count('{')
    depth -= l.count('}')
    
    if i == 4128: # Line 4129
        print(f"Depth at line 4129: {depth}")
        break
