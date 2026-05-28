with open('static/index.html', 'r', encoding='utf-8') as f:
    content = f.read()
if 'id="drawer-overlay"' not in content and "id='drawer-overlay'" not in content:
    print("drawer-overlay NOT FOUND")
else:
    print("drawer-overlay FOUND")
