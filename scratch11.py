import re

with open('static/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Find all document.getElementById('...') at top level
lines = content.split('\n')
in_func = 0
for i, line in enumerate(lines):
    if i < 1280 or i > 4200: continue
    
    if '{' in line and ('function' in line or '=>' in line or 'try' in line or 'for' in line or 'if' in line):
        in_func += line.count('{')
    if '}' in line:
        in_func -= line.count('}')
        if in_func < 0: in_func = 0
        
    if in_func == 0:
        matches = re.findall(r"document\.getElementById\(['\"](.*?)['\"]\)", line)
        for m in matches:
            if f'id="{m}"' not in content and f"id='{m}'" not in content:
                print(f"ERROR: {m} not found in HTML! (Used at line {i+1})")
