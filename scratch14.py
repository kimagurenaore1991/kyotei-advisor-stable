with open('main.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'Content-Security-Policy' in line or 'CSP' in line or 'Helmet' in line or 'middleware' in line.lower():
        print(f"Line {i+1}: {line.strip()}")
