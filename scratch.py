import sys
with open('static/index.html', 'r', encoding='utf-8') as f:
    content = f.read()
script_start = content.rfind('<script>')
script = content[script_start+8:content.find('</script>', script_start+8)]

# Basic tokenizer
import re
tokens = re.split(r'(/\\*.*?\\*/|//.*?$|\'[^\']*?\'|"[^"]*?"|[^]*?)', script, flags=re.MULTILINE|re.DOTALL)
clean_script = ""
for t in tokens:
    if t.startswith('/*') or t.startswith('//') or t.startswith("'") or t.startswith('"') or t.startswith(''):
        clean_script += " " * len(t.split('\n')) # keep lines aligned
    else:
        clean_script += t

lines = clean_script.split('\n')
stack = []
for i, line in enumerate(lines):
    for char in line:
        if char in '{[(':
            stack.append((char, i+1))
        elif char in '}])':
            if not stack:
                print(f"Error: unmatched {char} at line {i+1}")
                sys.exit(1)
            last, line_num = stack.pop()
            if (last == '{' and char != '}') or (last == '[' and char != ']') or (last == '(' and char != ')'):
                print(f"Error: mismatched {last} from line {line_num} with {char} at line {i+1}")
                sys.exit(1)

if stack:
    print(f"Unclosed braces/parens: {stack}")
else:
    print("No obvious bracket mismatch found.")
