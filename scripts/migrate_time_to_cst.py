"""历史数据时区迁移：把过去以 UTC 写入的时间字段统一 +8 小时（东八区）。

背景：
- 旧代码里各表 created_at/updated_at 用 SQLite 的 CURRENT_TIMESTAMP（恒为 UTC）填充；
- 2026-07-05 之后新代码统一改为应用层写入东八区时间（china_now_naive）；
- 本脚本用于把改造前的存量记录一次性 +8 小时，避免新老数据混着差 8 小时。

用法（先 dry-run 看影响面，确认无误后再加 --apply）：
    python scripts/migrate_time_to_cst.py --db data/cesg.db
    python scripts/migrate_time_to_cst.py --db data/cesg.db --cutoff "2026-07-05 18:00:00" --apply

参数：
    --db       SQLite 数据库文件路径（必填）
    --cutoff   只处理该时刻（东八区）之前写入的行；应填【新后端上线的北京时间】。
               不填则处理全部行——仅适用于上线前一次性迁移。
    --columns  额外需要 +8 的列名（逗号分隔），默认只处理明确由 CURRENT_TIMESTAMP
               写入的审计列：created_at, updated_at, first_auth_at, last_auth_at, login_at
    --apply    真正执行 UPDATE；不加只打印将影响的表/列/行数（dry-run）

注意：
- 执行前务必先备份数据库文件（cp cesg.db cesg.db.bak-$(date +%s)）；
- 若服务器系统时区不是 UTC，datetime.now() 写入的列（如 logout_at、handled_at）
  本来就是北京时间，不要加进 --columns；
- 脚本幂等性：不具备。跑两次会 +16 小时，务必只跑一次。
"""
from __future__ import annotations

import argparse
import sqlite3
import sys

DEFAULT_COLUMNS = {"created_at", "updated_at", "first_auth_at", "last_auth_at", "login_at"}


def find_datetime_columns(conn: sqlite3.Connection, wanted: set[str]) -> list[tuple[str, str]]:
    """返回 (表名, 列名) 列表：库中所有类型为 DATETIME/TIMESTAMP 且列名在 wanted 内的列。"""
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    ]
    hits: list[tuple[str, str]] = []
    for t in tables:
        for cid, name, coltype, notnull, dflt, pk in conn.execute(f'PRAGMA table_info("{t}")'):
            if name in wanted and ("DATE" in (coltype or "").upper() or "TIME" in (coltype or "").upper()):
                hits.append((t, name))
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description="存量 UTC 时间字段 +8 小时迁移")
    ap.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    ap.add_argument("--cutoff", default="", help="只处理早于该北京时间的行，如 '2026-07-05 18:00:00'")
    ap.add_argument("--columns", default="", help="额外处理的列名，逗号分隔")
    ap.add_argument("--apply", action="store_true", help="真正执行 UPDATE（默认 dry-run）")
    args = ap.parse_args()

    wanted = set(DEFAULT_COLUMNS)
    for c in args.columns.split(","):
        c = c.strip()
        if c:
            wanted.add(c)

    conn = sqlite3.connect(args.db)
    try:
        targets = find_datetime_columns(conn, wanted)
        if not targets:
            print("未找到匹配的时间列，退出。")
            return 0

        total = 0
        for table, col in targets:
            where = f'"{col}" IS NOT NULL'
            params: list[str] = []
            if args.cutoff:
                # 存量值是 UTC，cutoff 是北京时间：先把 cutoff 换算成 UTC 再比较
                where += f' AND "{col}" < datetime(?, \'-8 hours\')'
                params.append(args.cutoff)
            (count,) = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE {where}', params
            ).fetchone()
            if not count:
                continue
            total += count
            action = "UPDATE" if args.apply else "DRY-RUN"
            print(f"[{action}] {table}.{col}: {count} 行 +8 小时")
            if args.apply:
                conn.execute(
                    f'UPDATE "{table}" SET "{col}" = datetime("{col}", \'+8 hours\') WHERE {where}',
                    params,
                )
        if args.apply:
            conn.commit()
            print(f"完成：共更新 {total} 行。")
        else:
            print(f"dry-run 结束：共 {total} 行将被 +8 小时。确认无误后加 --apply 执行。")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
