import csv
import os
import uuid
from typing import Callable, Dict, List

from core_db import init_db, clear_products, upsert_product


Logger = Callable[[str, str], None]


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _to_int(val, default=0) -> int:
    try:
        if val is None:
            return default
        s = str(val).strip()
        if s == "":
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default


def _to_float(val):
    try:
        if val is None:
            return None
        s = str(val).strip()
        if s == "":
            return None
        return float(s.replace(",", "."))
    except Exception:
        return None


def _gen_sku(existing: set) -> str:
    while True:
        sku = "GEN-" + uuid.uuid4().hex[:10].upper()
        if sku not in existing:
            existing.add(sku)
            return sku


def _detect_delimiter(sample: str) -> str:
    if sample.count(";") > sample.count(","):
        return ";"
    return ","


def _read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = _detect_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delim)
        rows = [r for r in reader]
    return rows


def _resolve_columns(fieldnames: List[str]) -> Dict[str, str]:
    fn = [_norm(x) for x in fieldnames]

    def find_any(candidates):
        for c in candidates:
            if c in fn:
                return fieldnames[fn.index(c)]
        return ""

    col_sku = find_any(["sku", "артикул", "код", "код товара", "id", "product_id"])
    col_name = find_any(["name", "название", "наименование", "title", "товар"])
    col_qty = find_any(["qty", "quantity", "количество", "остаток", "stock", "available"])
    col_price = find_any(["price", "цена", "стоимость"])
    col_safety = find_any(["safety_stock", "страховой", "страховой остаток", "min_stock", "минимум"])
    col_lead = find_any(["lead_time_days", "lead time", "leadtime", "срок поставки", "lead", "дней доставки"])
    col_status = find_any(["status", "статус"])

    return {
        "sku": col_sku,
        "name": col_name,
        "qty": col_qty,
        "price": col_price,
        "safety_stock": col_safety,
        "lead_time_days": col_lead,
        "status": col_status,
    }


def import_products_csv(path: str, logger: Logger):
    logger("INFO", f"Импорт CSV начат: {path}")

    if not os.path.exists(path):
        logger("ERROR", "Файл CSV не найден")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}

    try:
        rows = _read_csv_rows(path)
    except UnicodeDecodeError:
        logger("ERROR", "Ошибка кодировки CSV. Сохраните файл как UTF-8")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}
    except Exception as e:
        logger("ERROR", f"Не удалось прочитать CSV: {e}")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}

    if not rows:
        logger("WARN", "CSV пустой или нет строк данных")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}

    fieldnames = list(rows[0].keys())
    cols = _resolve_columns(fieldnames)

    if cols["name"] == "":
        logger("ERROR", "Не найдена колонка названия товара. Ожидается name или Название")
        logger("INFO", f"Колонки в файле: {fieldnames}")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}

    init_db()

    logger("INFO", "Очистка таблицы товаров")
    try:
        clear_products()
    except Exception as e:
        logger("ERROR", f"Не удалось очистить таблицу товаров: {e}")
        return {"ok": False, "imported": 0, "skugen": 0, "skipped": 0}

    existing_sku = set()
    imported = 0
    skugen = 0
    skipped = 0

    for idx, r in enumerate(rows, start=2):
        try:
            name = str(r.get(cols["name"], "")).strip()
            if name == "":
                skipped += 1
                logger("WARN", f"Строка {idx} пропущена. Пустое название")
                continue

            sku_raw = str(r.get(cols["sku"], "")).strip() if cols["sku"] else ""
            if sku_raw == "":
                sku = _gen_sku(existing_sku)
                skugen += 1
            else:
                sku = sku_raw
                if sku in existing_sku:
                    skipped += 1
                    logger("WARN", f"Строка {idx} пропущена. Дубликат SKU {sku}")
                    continue
                existing_sku.add(sku)

            qty = _to_int(r.get(cols["qty"], 0)) if cols["qty"] else 0
            price = _to_float(r.get(cols["price"], "")) if cols["price"] else None
            safety = _to_int(r.get(cols["safety_stock"], 0)) if cols["safety_stock"] else 0
            lead = _to_int(r.get(cols["lead_time_days"], 0)) if cols["lead_time_days"] else 0
            status = str(r.get(cols["status"], "active")).strip() if cols["status"] else "active"
            if status == "":
                status = "active"

            upsert_product({
                "sku": sku,
                "name": name,
                "qty": qty,
                "safety_stock": safety,
                "in_transit": 0,
                "lead_time_days": lead,
                "price": price,
                "status": status,
            })
            imported += 1

        except Exception as e:
            skipped += 1
            logger("ERROR", f"Строка {idx} ошибка импорта: {e}")

    logger("INFO", f"Импорт завершен. Импортировано {imported}, SKU сгенерировано {skugen}, пропущено {skipped}")
    return {"ok": True, "imported": imported, "skugen": skugen, "skipped": skipped}
