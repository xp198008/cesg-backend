import json
import sqlite3
from pathlib import Path

db = Path(__file__).resolve().parents[1] / "data" / "cesg.db"
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
for q, args in [
    ("id=7610", (7610,)),
    ("plate like %7610%", ("%7610%",)),
    ("device like %7610%", ("%7610%",)),
]:
    rows = conn.execute(
        f"SELECT id, plate_no, device_no, jt808_car_id FROM vehicle WHERE {q.split()[0]} {'LIKE' if 'like' in q else '='} ?",
        args,
    ).fetchall()
    print(q, "->", len(rows))
    for r in rows:
        print(dict(r))
