import sqlite3
import json

db_path = 'kyotei.db'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("Checking for finished races in the database...")
cursor.execute("SELECT r.id, r.place_name, r.race_number, res.ranking FROM races r JOIN race_results res ON r.id = res.race_id LIMIT 10")
rows = cursor.fetchall()

if not rows:
    print("No finished races found. Checking all results...")
    cursor.execute("SELECT * FROM race_results LIMIT 5")
    results = cursor.fetchall()
    for res in results:
        print(dict(res))
else:
    for row in rows:
        print(f"ID: {row['id']}, Place: {row['place_name']}, Race: {row['race_number']}R, Result: {row['ranking']}")

conn.close()
