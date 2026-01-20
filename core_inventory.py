# core_inventory.py
# Складские документы
# IN приходная накладная
# OUT расходная накладная
# В пути партии поставок
#
# Требование
# Никаких прямых правок qty руками
# Любое изменение qty только через документ

import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple

DB_FILE = "data.sqlite"


def _conn():
    return sqlite3.connect(DB_FILE)


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_inventory_schema():
    con = _conn()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,               -- IN or OUT
        name TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        sort_order INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS docs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,               -- IN or OUT
        doc_date TEXT NOT NULL,           -- YYYY-MM-DD
        reason TEXT NOT NULL,
        comment TEXT NOT NULL DEFAULT "",
        created_ts TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS doc_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id INTEGER NOT NULL,
        sku TEXT NOT NULL,
        qty INTEGER NOT NULL,
        comment TEXT NOT NULL DEFAULT "",
        FOREIGN KEY(doc_id) REFERENCES docs(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS in_transit_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT NOT NULL,
        qty INTEGER NOT NULL,
        eta_date TEXT NOT NULL,           -- YYYY-MM-DD
        comment TEXT NOT NULL DEFAULT "",
        is_active INTEGER NOT NULL DEFAULT 1,
        created_ts TEXT NOT NULL
    )
    """)

    con.commit()
    con.close()


def seed_default_reasons():
    ensure_inventory_schema()
    defaults_in = ["Поставка", "Возврат", "Пересорт", "Инвентаризация"]
    defaults_out = ["OLX", "Prom", "Rozetka", "Списание", "Возврат", "Пересорт", "Инвентаризация"]

    con = _conn()
    cur = con.cursor()

    for i, name in enumerate(defaults_in):
        cur.execute("SELECT COUNT(1) FROM reasons WHERE kind='IN' AND name=?", (name,))
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO reasons(kind,name,is_active,sort_order) VALUES('IN',?,1,?)", (name, i))

    for i, name in enumerate(defaults_out):
        cur.execute("SELECT COUNT(1) FROM reasons WHERE kind='OUT' AND name=?", (name,))
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO reasons(kind,name,is_active,sort_order) VALUES('OUT',?,1,?)", (name, i))

    con.commit()
    con.close()


def list_reasons(kind: str) -> List[str]:
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        SELECT name
        FROM reasons
        WHERE kind=? AND is_active=1
        ORDER BY sort_order, name
    """, (kind,))
    rows = [r[0] for r in cur.fetchall()]
    con.close()
    return rows


def upsert_reason(kind: str, name: str, is_active: int = 1, sort_order: int = 0):
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("SELECT id FROM reasons WHERE kind=? AND name=?", (kind, name))
    r = cur.fetchone()
    if r:
        cur.execute("UPDATE reasons SET is_active=?, sort_order=? WHERE id=?", (int(is_active), int(sort_order), int(r[0])))
    else:
        cur.execute("INSERT INTO reasons(kind,name,is_active,sort_order) VALUES(?,?,?,?)",
                    (kind, name, int(is_active), int(sort_order)))
    con.commit()
    con.close()


def create_doc(kind: str, doc_date: str, reason: str, comment: str) -> int:
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO docs(kind, doc_date, reason, comment, created_ts)
        VALUES(?,?,?,?,?)
    """, (kind, doc_date, reason, comment or "", _now_ts()))
    doc_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return doc_id


def add_doc_line(doc_id: int, sku: str, qty: int, comment: str):
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO doc_lines(doc_id, sku, qty, comment)
        VALUES(?,?,?,?)
    """, (int(doc_id), sku, int(qty), comment or ""))
    con.commit()
    con.close()


def apply_doc_to_stock(doc_id: int) -> Tuple[bool, str]:
    """
    Применяет документ к остаткам
    IN прибавляет qty
    OUT отнимает qty
    Разрешаем уход в минус, как вы хотели
    """
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()

    cur.execute("SELECT kind FROM docs WHERE id=?", (int(doc_id),))
    row = cur.fetchone()
    if not row:
        con.close()
        return False, "Документ не найден"

    kind = str(row[0]).upper().strip()
    if kind not in ("IN", "OUT"):
        con.close()
        return False, "Неверный тип документа"

    cur.execute("SELECT sku, qty FROM doc_lines WHERE doc_id=?", (int(doc_id),))
    lines = cur.fetchall()
    if not lines:
        con.close()
        return False, "В документе нет строк"

    for sku, q in lines:
        cur.execute("SELECT qty FROM products WHERE sku=?", (sku,))
        pr = cur.fetchone()
        if not pr:
            con.close()
            return False, f"SKU не найден в товарах: {sku}"

        current = int(pr[0] or 0)
        delta = int(q or 0)
        if kind == "OUT":
            delta = -delta

        new_qty = current + delta
        cur.execute("UPDATE products SET qty=? WHERE sku=?", (int(new_qty), sku))

    con.commit()
    con.close()
    return True, "Документ применен"


def create_in_transit_batch(sku: str, qty: int, eta_date: str, comment: str = "") -> int:
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO in_transit_batches(sku, qty, eta_date, comment, is_active, created_ts)
        VALUES(?,?,?,?,1,?)
    """, (sku, int(qty), eta_date, comment or "", _now_ts()))
    batch_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return batch_id


def list_in_transit_batches(active_only: bool = True) -> List[Dict]:
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    if active_only:
        cur.execute("""
            SELECT id, sku, qty, eta_date, comment, is_active, created_ts
            FROM in_transit_batches
            WHERE is_active=1
            ORDER BY eta_date, id
        """)
    else:
        cur.execute("""
            SELECT id, sku, qty, eta_date, comment, is_active, created_ts
            FROM in_transit_batches
            ORDER BY eta_date, id
        """)
    rows = []
    for r in cur.fetchall():
        rows.append({
            "id": int(r[0]),
            "sku": r[1],
            "qty": int(r[2]),
            "eta_date": r[3],
            "comment": r[4],
            "is_active": int(r[5]),
            "created_ts": r[6],
        })
    con.close()
    return rows


def set_batch_active(batch_id: int, is_active: int):
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()
    cur.execute("UPDATE in_transit_batches SET is_active=? WHERE id=?", (int(is_active), int(batch_id)))
    con.commit()
    con.close()


def recompute_products_in_transit():
    """
    Пересчитывает products.in_transit как сумму активных партий
    Если у вас пока в таблице products нет поля in_transit, добавьте его в core_db миграции
    """
    ensure_inventory_schema()
    con = _conn()
    cur = con.cursor()

    cur.execute("SELECT sku, SUM(qty) FROM in_transit_batches WHERE is_active=1 GROUP BY sku")
    sums = {r[0]: int(r[1] or 0) for r in cur.fetchall()}

    cur.execute("SELECT sku FROM products")
    all_skus = [r[0] for r in cur.fetchall()]

    for sku in all_skus:
        val = int(sums.get(sku, 0))
        cur.execute("UPDATE products SET in_transit=? WHERE sku=?", (val, sku))

    con.commit()
    con.close()
