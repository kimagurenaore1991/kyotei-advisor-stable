import sys, re

def check_brackets(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    script_start = content.rfind('<script>')
    script_end = content.find('</script>', script_start)
    script = content[script_start+8:script_end]
    
    # regex to remove strings and comments
    # remove block comments
    script = re.sub(r'/\*.*?\*/', lambda m: ' ' * len(m.group(0)), script, flags=re.DOTALL)
    # remove line comments
    script = re.sub(r'//.*', '', script)
    # remove strings and regexes
    script = re.sub(r'(["\'])(?:(?=(\\\\?))\2.)*?\1', lambda m: ' ' * len(m.group(0)), script, flags=re.DOTALL)
    
    lines = script.split('\n')
    stack = []
    
    for i, line in enumerate(lines):
        for char in line:
            if char in '{[(':
                stack.append((char, i+1))
            elif char in '}])':
                if not stack:
                    print(f"Error: unmatched {char} at line {i+1}")
                    return
                last, line_num = stack.pop()
                if (last == '{' and char != '}') or (last == '[' and char != ']') or (last == '(' and char != ')'):
                    print(f"Error: mismatched {last} from line {line_num} with {char} at line {i+1}")
                    return
                    
    if stack:
        print(f"Unclosed braces/parens: {stack[-5:]}")
    else:
        print("No obvious bracket mismatch found.")

check_brackets('static/index.html')
