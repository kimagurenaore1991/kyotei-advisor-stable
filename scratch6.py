with open('static/index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if '<script>' in line and i > 500:
        print(f'Script starts at {i+1}')
        # print 5 lines before it
        for j in range(i-5, i):
            print(f'{j+1}: {lines[j].strip()}')
        break
