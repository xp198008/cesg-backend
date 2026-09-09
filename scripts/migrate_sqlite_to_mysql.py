"""把 CESG SQLite 整库导入本机已有 MySQL 的 cesg 库（不写入 jt808）。

用法（在服务器 backend 目录、后端已停写后）:
  CESG_SQLITE=/home/huanwei/cesg/backend/data/cesg.db \\
  CESG_MYSQL_URL=mysql+pymysql://root:xxx@127.0.0.1:3306/cesg?charset=utf8mb4 \\
  python scripts/migrate_sqlite_to_mysql.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import JSON, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app.models  # noqa: F401
from app.database import Base

BATCH = 500


def _sqlite_path() -> Path:
    raw = os.environ.get("CESG_SQLITE") or str(ROOT / "data" / "cesg.db")
    return Path(raw).expanduser().resolve()


def _mysql_url() -> str:
    url = (os.environ.get("CESG_MYSQL_URL") or "").strip()
    if url:
        return url
    user = os.environ.get("CESG_MYSQL_USER", "root")
    password = os.environ.get("CESG_MYSQL_PASSWORD", "")
    host = os.environ.get("CESG_MYSQL_HOST", "127.0.0.1")
    port = os.environ.get("CESG_MYSQL_PORT", "3306")
    db = os.environ.get("CESG_MYSQL_DATABASE", "cesg")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"


def _ensure_database(mysql_url: str) -> None:
    parsed = make_url(mysql_url)
    dbname = parsed.database
    if not dbname:
        raise SystemExit("MySQL URL 缺少库名")
    admin = parsed.set(database="mysql")
    eng = create_engine(admin)
    with eng.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(
            f"CREATE DATABASE IF NOT EXISTS `{dbname}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        ))
    eng.dispose()


def _norm(value, column):
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if value == "" and bool(getattr(column, "unique", False)):
        return None
    if isinstance(column.type, JSON):
        if value is None:
            return None if column.nullable else []
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None if column.nullable else []
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
    return value


def migrate() -> None:
    sqlite_path = _sqlite_path()
    if not sqlite_path.exists():
        raise SystemExit(f"sqlite 不存在: {sqlite_path}")
    mysql_url = _mysql_url()
    print(f"SRC {sqlite_path}")
    print(f"DST {make_url(mysql_url).set(password=None)}")

    _ensure_database(mysql_url)

    src: Engine = create_engine(f"sqlite:///{sqlite_path.as_posix()}")
    dst: Engine = create_engine(mysql_url, pool_pre_ping=True)

    Base.metadata.create_all(dst)
    src_insp = inspect(src)
    src_tables = set(src_insp.get_table_names())

    with src.connect() as src_conn, dst.begin() as conn:
        conn.execute(text("SET NAMES utf8mb4"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for table in Base.metadata.sorted_tables:
            name = table.name
            if name not in src_tables:
                print(f"SKIP {name} (sqlite 无此表)")
                continue
            conn.execute(table.delete())
            rows = src_conn.execute(table.select()).mappings()
            batch = []
            total = 0
            max_id = 0
            pk = [c.name for c in table.primary_key.columns]
            for row in rows:
                item = {c.name: _norm(row[c.name], c) for c in table.columns}
                batch.append(item)
                if pk == ["id"] and item.get("id") is not None:
                    max_id = max(max_id, int(item["id"] or 0))
                if len(batch) >= BATCH:
                    conn.execute(table.insert(), batch)
                    total += len(batch)
                    batch = []
            if batch:
                conn.execute(table.insert(), batch)
                total += len(batch)
            print(f"OK {name} {total}")
            if pk == ["id"] and max_id > 0:
                conn.execute(text(f"ALTER TABLE `{name}` AUTO_INCREMENT = {max_id + 1}"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))

    print("==== 对账 ====")
    with src.connect() as s, dst.connect() as d:
        failed = False
        for table in Base.metadata.sorted_tables:
            if table.name not in src_tables:
                continue
            sc = s.execute(text(f"SELECT COUNT(*) FROM `{table.name}`")).scalar()
            dc = d.execute(text(f"SELECT COUNT(*) FROM `{table.name}`")).scalar()
            mark = "OK" if int(sc or 0) == int(dc or 0) else "DIFF"
            print(f"{mark} {table.name} sqlite={sc} mysql={dc}")
            if mark == "DIFF":
                failed = True
        if failed:
            raise SystemExit("对账失败")
    print("DONE")


if __name__ == "__main__":
    migrate()
