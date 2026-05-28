import re
with open('static/index.html', 'r', encoding='utf-8') as f:
    content = f.read()
script_start = content.rfind('<script>')
script_end = content.find('</script>', script_start)
script = content[script_start+8:script_end]

lines = script.split('\n')
for i, line in enumerate(lines):
    l = line.strip()
    if not l or l.startswith('//') or l.startswith('/*') or l.startswith('let ') or l.startswith('const ') or l.startswith('var ') or l.startswith('function ') or l.startswith('async '):
        continue
    if l.startswith('}') or l.startswith(']') or l.startswith('//'):
        continue
    # Check if the line is not indented by 8 spaces or more (assuming inside function is 12)
    # The script block is indented by 4 spaces. So top level is 8 spaces.
    if len(line) - len(line.lstrip()) == 8:
        print(f"Line {1281+i}: {l}")
