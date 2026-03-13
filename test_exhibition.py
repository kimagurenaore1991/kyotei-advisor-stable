from bs4 import BeautifulSoup
import json

def main():
    with open('test_beforeinfo.html', 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    output = []
    
    for i, t in enumerate(soup.find_all('table')):
        headers = [th.text.strip().replace('\n', '') for th in t.find_all('th')]
        out_rows = []
        for tr in t.find_all('tr'):
            cells = tr.find_all(['th', 'td'])
            out_rows.append([c.text.strip().replace('\n', '').replace(' ', '') for c in cells])
            
        output.append({
            "table_index": i,
            "headers": headers,
            "rows": out_rows
        })
        
    with open('test_tables.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()
