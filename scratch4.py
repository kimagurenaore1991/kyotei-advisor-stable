with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "'" in line or '"' in line or '' in line or '/' in line:
        continue # skip lines with strings or comments/regexes to avoid noise
    o = line.count('(')
    c = line.count(')')
    if o != c:
        print(f'Line {i+1}: {line.strip()}')
