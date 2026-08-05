import sqlite3
c = sqlite3.connect("data/cesg.db")
cur = c.cursor()
for q in [
    ("id=7610", "select id, plate_no from vehicle where id=7610"),
    ("id near", "select id, plate_no from vehicle where id between 7605 and 7615"),
    ("device 7610", "select v.id, v.plate_no, d.device_no from vehicle v join vehicle_device d on d.vehicle_id=v.id where d.device_no like '%7610%' limit 10"),
]:
    print("---", q[0])
    cur.execute(q[1])
    for row in cur.fetchall():
        print(row)
