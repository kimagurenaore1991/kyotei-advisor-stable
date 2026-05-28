with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'eval(' in line or 'new Function(' in line or "setTimeout('" in line or 'setTimeout("' in line or "setInterval('" in line or 'setInterval("' in line:
        print(f"Line {i+1}: {line.strip()}")
