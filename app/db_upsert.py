"""按当前库方言生成 upsert / insert-ignore（SQLite 与 MySQL）。"""
from __future__ import annotations

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.database import engine


def _is_mysql() -> bool:
    return (engine.dialect.name or "").startswith("mysql")


def upsert_stmt(table, values: dict, index_elements: list[str], update_keys: list[str]):
    if _is_mysql():
        stmt = mysql_insert(table).values(**values)
        updates = {key: getattr(stmt.inserted, key) for key in update_keys}
        return stmt.on_duplicate_key_update(**updates)
    stmt = sqlite_insert(table).values(**values)
    return stmt.on_conflict_do_update(
        index_elements=index_elements,
        set_={key: getattr(stmt.excluded, key) for key in update_keys},
    )


def insert_ignore_stmt(table, values: dict, index_elements: list[str]):
    if _is_mysql():
        stmt = mysql_insert(table).values(**values)
        return stmt.prefix_with("IGNORE")
    stmt = sqlite_insert(table).values(**values)
    return stmt.on_conflict_do_nothing(index_elements=index_elements)
