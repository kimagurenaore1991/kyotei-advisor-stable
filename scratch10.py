import re
with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

in_func = 0
for i, line in enumerate(lines):
    if i < 1280 or i > 4200: continue
    
    # Very basic tracking
    if '{' in line and ('function' in line or '=>' in line or 'try' in line or 'for' in line or 'if' in line):
        in_func += line.count('{')
    if '}' in line:
        in_func -= line.count('}')
        if in_func < 0: in_func = 0
        
    if in_func == 0 and 'document.getElementById(' in line:
        print(f"Line {i+1}: {line.strip()}")
