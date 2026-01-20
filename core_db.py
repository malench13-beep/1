# core_db.py
# Работа с SQLite базой данных
# Гарантированная инициализация структуры
# Без падений при пустой или новой базе

import os
import sqlite3
from datetime import datetime


# -------------------------
# БАЗОВЫЕ ФУНКЦИИ
# -------------------------

def app_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def db_path() -> str:
    return os.path.join(app_dir(), "data.sqlite")


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def connect():
    con = sqlite3.connect(db_path())
    con.row_factory = sqlite3.Row
    return con


# -------------------------
# ИНИЦИАЛИЗАЦИЯ БД
# -------------------------

def init_db():
    con = connect()
    cur = con.cursor()

    # Товары
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        sku TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        qty INTEGER NOT NULL DEFAULT 0,
        safety_stock INTEGER NOT NULL DEFAULT 0,
        in_transit INTEGER NOT NULL DEFAULT 0,
        lead_time_days INTEGER NOT NULL DEFAULT 0,
        price REAL,
        status TEXT NOT NULL DEFAULT 'active',
        updated_at TEXT NOT NULL
    )
    """)

    # Настройки приложения
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    # Связь товаров с платформами
    cur.execute("""
    CREATE TABLE IF NOT EXISTS platform_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT NOT NULL,
        platform_item_id TEXT NOT NULL,
        sku TEXT NOT NULL,
        comment TEXT,
        updated_at TEXT NOT NULL
    )
    """)

    con.commit()
    con.close()


# -------------------------
# НАСТРОЙКИ
# -------------------------

def set_setting(key: str, value: str):
    init_db()
    con = connect()
    cur = con.cursor()
    cur.execute("""
    INSERT INTO settings(key, value, updated_at)
    VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET
        value=excluded.value,
        updated_at=excluded.updated_at
    """, (key, value, now_ts()))
    con.commit()
    con.close()


def get_setting(key: str, default: str = "") -> str:
    init_db()
    con = connect()
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    con.close()
    if row is None:
        return default
    return str(row["value"])


# -------------------------
# ТОВАРЫ
# -------------------------

def clear_products():
    init_db()
    con = connect()
    cur = con.cursor()
    cur.execute("DELETE FROM products")
    con.commit()
    con.close()


def upsert_product(prod: dict):
    init_db()
    con = connect()
    cur = con.cursor()
    cur.execute("""
    INSERT INTO products(
        sku, name, qty, safety_stock, in_transit,
        lead_time_days, price, status, updated_at
    )
    VALUES(?,?,?,?,?,?,?,?,?)
    ON CONFLICT(sku) DO UPDATE SET
        name=excluded.name,
        qty=excluded.qty,
        safety_stock=excluded.safety_stock,
        in_transit=excluded.in_transit,
        lead_time_days=excluded.lead_time_days,
        price=excluded.price,
        status=excluded.status,
        updated_at=excluded.updated_at
    """, (
        prod["sku"],
        prod["name"],
        int(prod.get("qty", 0)),
        int(prod.get("safety_stock", 0)),
        int(prod.get("in_transit", 0)),
        int(prod.get("lead_time_days", 0)),
        prod.get("price", None),
        prod.get("status", "active"),
        now_ts(),
    ))
    con.commit()
    con.close()


def list_products(limit: int = 5000):
    init_db()
    con = connect()
    cur = con.cursor()
    cur.execute("""
        SELECT
            sku,
            name,
            qty,
            safety_stock,
            in_transit,
            lead_time_days,
            price,
            status
        FROM products
        ORDER BY name COLLATE NOCASE ASC
        LIMIT ?
    """, (int(limit),))
    rows = cur.fetchall()
    con.close()
    return rows


def search_products(query: str, everywhere: bool = False, limit: int = 8):
    init_db()
    q = (query or "").strip()
    if q == "":
        return []

    like = f"%{q}%"
    con = connect()
    cur = con.cursor()

    if everywhere:
        cur.execute("""
            SELECT
                sku,
                name,
                qty,
                safety_stock,
                in_transit,
                lead_time_days,
                price,
                status
            FROM products
            WHERE
                sku LIKE ?
                OR name LIKE ?
                OR status LIKE ?
            ORDER BY
                CASE WHEN name LIKE ? THEN 0 ELSE 1 END,
                name COLLATE NOCASE ASC
            LIMIT ?
        """, (like, like, like, like, int(limit)))
    else:
        cur.execute("""
            SELECT
                sku,
                name,
                qty,
                safety_stock,
                in_transit,
                lead_time_days,
                price,
                status
            FROM products
            WHERE name LIKE ?
            ORDER BY
                CASE WHEN name LIKE ? THEN 0 ELSE 1 END,
                name COLLATE NOCASE ASC
            LIMIT ?
        """, (like, like, int(limit)))

    rows = cur.fetchall()
    con.close()
    return rows

# --- inventory schema helper, add to the end of core_db.py ---

def ensure_columns_for_inventory():
    import sqlite3
    con = sqlite3.connect("data.sqlite")
    cur = con.cursor()

    cur.execute("PRAGMA table_info(products)")
    cols = [r[1] for r in cur.fetchall()]

    def add_col(name, ddl):
        if name not in cols:
            cur.execute(ddl)

    add_col("in_transit", "ALTER TABLE products ADD COLUMN in_transit INTEGER NOT NULL DEFAULT 0")
    add_col("safety_stock", "ALTER TABLE products ADD COLUMN safety_stock INTEGER NOT NULL DEFAULT 0")
    add_col("lead_time_days", "ALTER TABLE products ADD COLUMN lead_time_days INTEGER NOT NULL DEFAULT 0")

    con.commit()
    con.close()

