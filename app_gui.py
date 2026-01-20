import os
import json
import sqlite3
import tkinter as tk
from tkinter import ttk, filedialog, simpledialog, messagebox
from datetime import datetime, date
from queue import Queue, Empty

from core_tokens import load_tokens, get_tokens_dict, validate_tokens
from core_db import init_db, list_products, set_setting, get_setting, search_products
from core_import_csv import import_products_csv
from core_bot_telegram import TelegramBotRunner
import core_inventory as inv

APP_TITLE = "LLM ERP"
STATE_FILE = "config_ui.json"
DB_FILE = "data.sqlite"


def app_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def state_path() -> str:
    return os.path.join(app_dir(), STATE_FILE)


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_ymd() -> str:
    return date.today().strftime("%Y-%m-%d")


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def ensure_inventory_columns():
    con = sqlite3.connect(DB_FILE)
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


def rv(row, key, default=None):
    """
    row может быть sqlite3.Row или dict.
    """
    try:
        return row[key]
    except Exception:
        pass
    try:
        return row.get(key, default)
    except Exception:
        return default


def db_get_product_by_sku(sku: str):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        SELECT sku, name, qty, safety_stock, in_transit, lead_time_days, price, status
        FROM products
        WHERE sku=?
    """, (sku,))
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {
        "sku": r[0],
        "name": r[1],
        "qty": int(r[2] or 0),
        "safety_stock": int(r[3] or 0),
        "in_transit": int(r[4] or 0),
        "lead_time_days": int(r[5] or 0),
        "price": r[6],
        "status": r[7],
    }


class AppState:
    def __init__(self) -> None:
        self.geometry = "1400x860"
        self.active_tab = " Товары "
        self.right_width = 420
        self.right_status_height = 170
        self.bot_answer_mode = "ai"
        self.table_cols = {}
        self.last_in_reason = ""
        self.last_out_reason = ""
        self.last_dates = {
            "in_doc_date": today_ymd(),
            "out_doc_date": today_ymd(),
            "eta_date": today_ymd(),
        }

    def load(self) -> None:
        p = state_path()
        if not os.path.exists(p):
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.geometry = data.get("geometry", self.geometry)
            self.active_tab = data.get("active_tab", self.active_tab)
            self.right_width = int(data.get("right_width", self.right_width))
            self.right_status_height = int(data.get("right_status_height", self.right_status_height))
            self.bot_answer_mode = data.get("bot_answer_mode", self.bot_answer_mode)
            self.table_cols = data.get("table_cols", self.table_cols) or {}
            self.last_in_reason = data.get("last_in_reason", self.last_in_reason) or ""
            self.last_out_reason = data.get("last_out_reason", self.last_out_reason) or ""
            self.last_dates = data.get("last_dates", self.last_dates) or self.last_dates
        except Exception:
            return

    def save(self) -> None:
        data = {
            "geometry": self.geometry,
            "active_tab": self.active_tab,
            "right_width": self.right_width,
            "right_status_height": self.right_status_height,
            "bot_answer_mode": self.bot_answer_mode,
            "table_cols": self.table_cols,
            "last_in_reason": self.last_in_reason,
            "last_out_reason": self.last_out_reason,
            "last_dates": self.last_dates,
        }
        try:
            with open(state_path(), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


class Tooltip:
    def __init__(self, widget: tk.Widget, title: str, body: str) -> None:
        self.widget = widget
        self.title = title
        self.body = body
        self.tip = None
        self.after_id = None

        widget.bind("<Enter>", self._on_enter, add="+")
        widget.bind("<Leave>", self._on_leave, add="+")
        widget.bind("<ButtonPress>", self._on_leave, add="+")

    def _on_enter(self, _event=None) -> None:
        self.after_id = self.widget.after(450, self._show)

    def _on_leave(self, _event=None) -> None:
        if self.after_id is not None:
            try:
                self.widget.after_cancel(self.after_id)
            except Exception:
                pass
            self.after_id = None
        self._hide()

    def _show(self) -> None:
        if self.tip is not None:
            return

        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 10

        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")

        frame = ttk.Frame(self.tip, padding=10)
        frame.pack(fill="both", expand=True)

        title_lbl = ttk.Label(frame, text=self.title, font=("Segoe UI", 11, "bold"))
        title_lbl.pack(anchor="w")

        body_lbl = ttk.Label(frame, text=self.body, font=("Segoe UI", 9), wraplength=900, justify="left")
        body_lbl.pack(anchor="w", pady=(6, 0))

    def _hide(self) -> None:
        if self.tip is not None:
            try:
                self.tip.destroy()
            except Exception:
                pass
            self.tip = None


class LogPanel(ttk.Frame):
    def __init__(self, master: tk.Misc, queue: Queue) -> None:
        super().__init__(master)
        self._entries = []
        self._paused = False
        self._queue = queue

        self.var_info = tk.BooleanVar(value=True)
        self.var_warn = tk.BooleanVar(value=True)
        self.var_error = tk.BooleanVar(value=True)
        self.var_search = tk.StringVar(value="")

        top = ttk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        self.btn_copy = ttk.Button(top, text="Копировать", command=self.copy_all)
        self.btn_copy.pack(side="left")

        self.btn_save = ttk.Button(top, text="Сохранить", command=self.save_to_file)
        self.btn_save.pack(side="left", padx=(6, 0))

        self.btn_pause = ttk.Button(top, text="Пауза", command=self.toggle_pause)
        self.btn_pause.pack(side="left", padx=(6, 0))

        filters = ttk.Frame(top)
        filters.pack(side="right")

        self.cb_info = ttk.Checkbutton(filters, text="Инфо", variable=self.var_info, command=self.refresh)
        self.cb_warn = ttk.Checkbutton(filters, text="Предупр", variable=self.var_warn, command=self.refresh)
        self.cb_err = ttk.Checkbutton(filters, text="Ошибки", variable=self.var_error, command=self.refresh)
        self.cb_info.pack(side="left")
        self.cb_warn.pack(side="left", padx=(6, 0))
        self.cb_err.pack(side="left", padx=(6, 0))

        Tooltip(self.cb_info, "ИНФО", "Обычные сообщения. Импорт, пересчет, создание документов, обновление таблиц.")
        Tooltip(self.cb_warn, "ПРЕДУПРЕЖДЕНИЯ", "Некритичные проблемы. Не выбран товар, пустой поиск, неверное число.")
        Tooltip(self.cb_err, "ОШИБКИ", "Критичные ошибки. Отсутствуют ключи, сбой SQL, невозможен запуск бота.")

        search_row = ttk.Frame(self)
        search_row.pack(fill="x", padx=8, pady=(0, 6))
        ttk.Label(search_row, text="Поиск по логу").pack(side="left")
        ent = ttk.Entry(search_row, textvariable=self.var_search)
        ent.pack(side="left", fill="x", expand=True, padx=(8, 0))
        ent.bind("<KeyRelease>", lambda _e: self.refresh())
        Tooltip(ent, "ПОИСК ПО ЛОГУ", "Фильтрует лог по подстроке. Полезно искать SKU, название, ERROR, warn.")

        self.text = tk.Text(self, height=14, wrap="word")
        self.text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.text.configure(state="disabled")

        Tooltip(self.btn_copy, "КОПИРОВАТЬ", "Копирует текущий видимый лог в буфер обмена.")
        Tooltip(self.btn_save, "СОХРАНИТЬ", "Сохраняет текущий видимый лог в TXT.")
        Tooltip(self.btn_pause, "ПАУЗА", "Останавливает автообновление окна лога. Включите обратно для продолжения.")

    def toggle_pause(self) -> None:
        self._paused = not self._paused
        self.btn_pause.configure(text="Пауза" if not self._paused else "Пауза включена")
        if not self._paused:
            self.refresh()

    def add(self, level: str, msg: str) -> None:
        level = level.upper().strip()
        if level not in ("INFO", "WARN", "ERROR"):
            level = "INFO"
        self._entries.append({"ts": now_ts(), "level": level, "msg": msg})
        if not self._paused:
            self.refresh()

    def poll_queue(self):
        try:
            while True:
                lvl, msg = self._queue.get_nowait()
                self.add(lvl, msg)
        except Empty:
            return

    def copy_all(self) -> None:
        text = self._build_visible_text()
        self.clipboard_clear()
        self.clipboard_append(text)

    def save_to_file(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Сохранить лог",
            defaultextension=".txt",
            filetypes=[("Text", "*.txt"), ("All", "*.*")]
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self._build_visible_text())
            self.add("INFO", f"Лог сохранен: {path}")
        except Exception as e:
            self.add("ERROR", f"Не удалось сохранить лог: {e}")

    def refresh(self) -> None:
        text = self._build_visible_text()
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.insert("1.0", text)
        self.text.configure(state="disabled")
        self.text.see("end")

    def _build_visible_text(self) -> str:
        q = (self.var_search.get() or "").strip().lower()
        allow_info = self.var_info.get()
        allow_warn = self.var_warn.get()
        allow_err = self.var_error.get()

        lines = []
        for e in self._entries:
            lvl = e["level"]
            if lvl == "INFO" and not allow_info:
                continue
            if lvl == "WARN" and not allow_warn:
                continue
            if lvl == "ERROR" and not allow_err:
                continue

            line = f'{e["ts"]} {lvl}: {e["msg"]}'
            if q and q not in line.lower():
                continue
            lines.append(line)
        return "\n".join(lines) + ("\n" if lines else "")


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self.state_obj = AppState()
        self.state_obj.load()

        self.title(APP_TITLE)
        self.geometry(self.state_obj.geometry)
        self.minsize(1200, 760)

        self._queue = Queue()
        self.bot = None

        self.var_bot_mode = tk.StringVar(value=self.state_obj.bot_answer_mode)

        self._sort_state = {}

        self._apply_theme()
        self._build_layout()
        self._bind_events()

        init_db()
        ensure_inventory_columns()
        inv.ensure_inventory_schema()
        inv.seed_default_reasons()
        inv.recompute_products_in_transit()

        self._load_tokens_to_settings()

        self.after(200, self._tick)
        self.after(250, self._apply_state_to_sashes_safe)

        self.log.add("INFO", "Приложение запущено. Импортируйте CSV. Документы меняют остатки. В пути считает in_transit.")

        autostart = get_setting("bot_autostart", "0").strip() == "1"
        if autostart:
            self.after(400, self._try_autostart_bot)

    def _tick(self):
        self.log.poll_queue()
        self.after(200, self._tick)

    def _emit_from_thread(self, level: str, msg: str):
        self._queue.put((level, msg))

    def _apply_theme(self) -> None:
        try:
            style = ttk.Style(self)
            if "vista" in style.theme_names():
                style.theme_use("vista")
        except Exception:
            pass
        try:
            style = ttk.Style(self)
            style.configure("TNotebook.Tab", font=("Segoe UI", 18, "bold"))
        except Exception:
            pass

    def _build_layout(self) -> None:
        self.outer = ttk.Panedwindow(self, orient="horizontal")
        self.outer.pack(fill="both", expand=True)

        self.left = ttk.Frame(self.outer)
        self.right = ttk.Frame(self.outer)

        self.outer.add(self.left, weight=8)
        self.outer.add(self.right, weight=3)

        self.right_split = ttk.Panedwindow(self.right, orient="vertical")
        self.right_split.pack(fill="both", expand=True)

        self.status = ttk.Frame(self.right_split)
        self.log = LogPanel(self.right_split, self._queue)

        self.right_split.add(self.status, weight=1)
        self.right_split.add(self.log, weight=4)

        self._build_status_panel()
        self._build_left_tabs()

    def _build_status_panel(self) -> None:
        box = ttk.Frame(self.status, padding=10)
        box.pack(fill="both", expand=True)

        title = ttk.Label(box, text="Статус", font=("Segoe UI", 11, "bold"))
        title.grid(row=0, column=0, sticky="w")

        self.lbl_db = ttk.Label(box, text=f"База: {os.path.join(app_dir(), DB_FILE)}")
        self.lbl_db.grid(row=1, column=0, sticky="w", pady=(8, 0))

        self.lbl_bot = ttk.Label(box, text="Telegram бот: Остановлен")
        self.lbl_bot.grid(row=2, column=0, sticky="w", pady=(6, 0))

        botmode_row = ttk.Frame(box)
        botmode_row.grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Label(botmode_row, text="Режим ответа").pack(side="left")

        cbm = ttk.Combobox(
            botmode_row,
            textvariable=self.var_bot_mode,
            values=["operator", "triggers", "ai"],
            width=16,
            state="readonly",
        )
        cbm.pack(side="left", padx=(10, 0))
        cbm.bind("<<ComboboxSelected>>", lambda _e: self._on_bot_mode_change())

        Tooltip(
            cbm,
            "РЕЖИМЫ БОТА",
            "operator\n"
            "Только оператор. Бот не ищет в базе и не отвечает по товарам, сразу эскалирует.\n"
            "\n"
            "triggers\n"
            "Только триггеры. Бот ищет совпадения по таблице триггеров. Если не найдено, эскалирует оператору.\n"
            "\n"
            "ai\n"
            "Максимальный режим по правилам. Бот ищет по базе товаров. Если найден один товар, отвечает.\n"
            "Если найдено несколько, задает один уточняющий вопрос и показывает варианты.\n"
            "Если не найдено, эскалирует оператору.\n"
        )

        self.lbl_last = ttk.Label(box, text="Последняя операция: нет")
        self.lbl_last.grid(row=4, column=0, sticky="w", pady=(8, 0))

    def _build_left_tabs(self) -> None:
        self.nb = ttk.Notebook(self.left)
        self.nb.pack(fill="both", expand=True, padx=8, pady=8)

        self.tab_products = ttk.Frame(self.nb)
        self.tab_in = ttk.Frame(self.nb)
        self.tab_out = ttk.Frame(self.nb)
        self.tab_transit = ttk.Frame(self.nb)
        self.tab_reasons = ttk.Frame(self.nb)
        self.tab_bots = ttk.Frame(self.nb)
        self.tab_ai = ttk.Frame(self.nb)

        self.nb.add(self.tab_products, text=" Товары ")
        self.nb.add(self.tab_in, text=" Приход ")
        self.nb.add(self.tab_out, text=" Расход ")
        self.nb.add(self.tab_transit, text=" В пути ")
        self.nb.add(self.tab_reasons, text=" Причины ")
        self.nb.add(self.tab_bots, text=" Боты ")
        self.nb.add(self.tab_ai, text=" AI ")

        self._build_tab_products()
        self._build_tab_in()
        self._build_tab_out()
        self._build_tab_transit()
        self._build_tab_reasons()
        self._build_tab_bots()
        self._build_tab_ai()

        want = (self.state_obj.active_tab or "").strip()
        for i in range(self.nb.index("end")):
            cur = (self.nb.tab(i, "text") or "").strip()
            if cur == want:
                self.nb.select(i)
                break

        self.nb.bind("<<NotebookTabChanged>>", lambda _e: self._on_tab_change())

    def _build_button_row(self, parent, buttons):
        row = ttk.Frame(parent)
        row.pack(fill="x", padx=10, pady=(10, 6))
        for btn in buttons:
            b = ttk.Button(row, text=btn["text"], command=btn["cmd"])
            b.pack(side="left", padx=(0, 8))
            btn["ref"][0] = b
            Tooltip(b, btn["tip_title"], btn["tip_body"])
        return row

    def _build_table(self, parent, columns, table_key):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        tv = ttk.Treeview(frame, columns=columns, show="headings")
        vs = ttk.Scrollbar(frame, orient="vertical", command=tv.yview)
        hs = ttk.Scrollbar(frame, orient="horizontal", command=tv.xview)
        tv.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)

        tv.grid(row=0, column=0, sticky="nsew")
        vs.grid(row=0, column=1, sticky="ns")
        hs.grid(row=1, column=0, sticky="ew")

        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        saved = self.state_obj.table_cols.get(table_key, {})
        for c in columns:
            tv.heading(c, text=c)
            w = int(saved.get(c, 120))
            tv.column(c, width=w, minwidth=1, stretch=False)

        def store_widths(_event=None):
            widths = {}
            for c in columns:
                widths[c] = tv.column(c, "width")
            self.state_obj.table_cols[table_key] = widths

        tv.bind("<ButtonRelease-1>", store_widths, add="+")
        tv.bind("<Configure>", store_widths, add="+")

        self._attach_sorting(tv, columns, table_key)
        return tv

    def _attach_sorting(self, tv: ttk.Treeview, columns, table_key: str):
        def try_num(v):
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                return None

        def sort_by(col_name: str):
            items = tv.get_children("")
            if not items:
                return
            col_index = columns.index(col_name)

            asc = True
            key = (table_key, col_name)
            if key in self._sort_state:
                asc = not self._sort_state[key]
            self._sort_state[key] = asc

            def item_key(iid):
                vals = tv.item(iid, "values")
                if col_index >= len(vals):
                    return ""
                v = vals[col_index]
                n = try_num(v)
                if n is None:
                    return str(v).lower()
                return n

            sorted_items = sorted(items, key=item_key, reverse=not asc)
            for idx, iid in enumerate(sorted_items):
                tv.move(iid, "", idx)

        for c in columns:
            tv.heading(c, command=lambda cc=c: sort_by(cc))

    def _build_tab_products(self):
        self._build_button_row(
            self.tab_products,
            [
                {
                    "text": "Импорт CSV",
                    "cmd": self._import_csv,
                    "tip_title": "ИМПОРТ CSV",
                    "tip_body": "Вы выбираете CSV. Таблица товаров очищается и заполняется заново. Затем пересчитывается В пути (in_transit).",
                    "ref": [None],
                },
                {
                    "text": "Обновить",
                    "cmd": self._refresh_products_table,
                    "tip_title": "ОБНОВИТЬ",
                    "tip_body": "Перечитывает товары из базы и обновляет таблицу.",
                    "ref": [None],
                },
                {
                    "text": "Пересчитать В пути",
                    "cmd": self._recompute_in_transit,
                    "tip_title": "ПЕРЕСЧЕТ В ПУТИ",
                    "tip_body": "Суммирует активные партии В пути и записывает сумму в products.in_transit. Затем обновляет таблицы.",
                    "ref": [None],
                },
            ],
        )

        self.tv_products = self._build_table(
            self.tab_products,
            columns=["SKU", "Название", "Остаток", "Страховой", "В пути", "Lead", "Цена", "Статус"],
            table_key="products",
        )
        self._refresh_products_table()

    def _build_doc_header(self, parent, kind: str):
        frm = ttk.LabelFrame(parent, text="Шапка документа", padding=10)
        frm.pack(fill="x", padx=10, pady=(10, 8))

        if kind == "IN":
            self.var_in_date = tk.StringVar(value=self.state_obj.last_dates.get("in_doc_date", today_ymd()))
            self.var_in_reason = tk.StringVar(value=self.state_obj.last_in_reason or "")
            self.var_in_comment = tk.StringVar(value="")
            date_var = self.var_in_date
            reason_var = self.var_in_reason
            comment_var = self.var_in_comment
            reasons = inv.list_reasons("IN")
            title = "Дата прихода"
        else:
            self.var_out_date = tk.StringVar(value=self.state_obj.last_dates.get("out_doc_date", today_ymd()))
            self.var_out_reason = tk.StringVar(value=self.state_obj.last_out_reason or "")
            self.var_out_comment = tk.StringVar(value="")
            date_var = self.var_out_date
            reason_var = self.var_out_reason
            comment_var = self.var_out_comment
            reasons = inv.list_reasons("OUT")
            title = "Дата расхода"

        r0 = ttk.Frame(frm)
        r0.pack(fill="x")

        ttk.Label(r0, text=title).pack(side="left")
        e_date = ttk.Entry(r0, textvariable=date_var, width=14)
        e_date.pack(side="left", padx=(10, 0))
        Tooltip(e_date, "ДАТА", "Формат YYYY-MM-DD. Пример 2026-01-20. Позже добавим календарь.")

        ttk.Label(r0, text="Причина").pack(side="left", padx=(18, 0))
        cb = ttk.Combobox(r0, textvariable=reason_var, values=reasons, width=22, state="readonly")
        cb.pack(side="left", padx=(10, 0))
        Tooltip(cb, "ПРИЧИНА", "Причина документа. Список редактируется во вкладке Причины. Последний выбор сохраняется.")

        ttk.Label(r0, text="Комментарий").pack(side="left", padx=(18, 0))
        e_c = ttk.Entry(r0, textvariable=comment_var)
        e_c.pack(side="left", fill="x", expand=True, padx=(10, 0))
        Tooltip(e_c, "КОММЕНТАРИЙ", "Комментарий к документу. Записывается в шапку накладной.")

        if not reason_var.get() and reasons:
            reason_var.set(reasons[0])

        return frm

    def _build_product_picker(self, parent, title_text: str):
        frm = ttk.LabelFrame(parent, text=title_text, padding=10)
        frm.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        top = ttk.Frame(frm)
        top.pack(fill="x")

        var_q = tk.StringVar(value="")
        ttk.Label(top, text="Поиск").pack(side="left")
        ent = ttk.Entry(top, textvariable=var_q)
        Rbtn = ttk.Button(top, text="Найти", command=lambda: self._picker_search(var_q.get(), tv))

        Rbtn.pack(side="right")
        Rbtn.configure(width=12)

        Rbtn.pack_configure(padx=(10, 0))
        Rbtn.pack_configure(pady=(0, 0))

        Rbtn.pack(side="right")

        Rbtn.pack_forget()
        Rbtn.pack(side="left", padx=(10, 0))

        ent = ttk.Entry(top, textvariable=var_q)
        ent.pack(side="left", fill="x", expand=True, padx=(10, 0))

        btn = ttk.Button(top, text="Найти", command=lambda: self._picker_search(var_q.get(), tv))
        btn.pack(side="left", padx=(10, 0))

        Tooltip(ent, "ПОИСК ТОВАРА", "Поиск по названию. Можно вводить часть слова. Пример: nokia 1280.")
        Tooltip(btn, "НАЙТИ", "Ищет в базе. Если пусто, попробуйте более короткий запрос.")

        tv = self._build_table(frm, ["SKU", "Название", "Остаток", "В пути", "Lead"], table_key=f"picker_{title_text}")
        return frm, var_q, tv

    def _picker_search(self, query: str, tv: ttk.Treeview):
        q = (query or "").strip()
        if not q:
            messagebox.showwarning("Поиск", "Введите текст")
            return
        rows = search_products(q, everywhere=True, limit=100)
        tv.delete(*tv.get_children())
        for r in rows:
            tv.insert("", "end", values=[
                rv(r, "sku", ""),
                rv(r, "name", ""),
                safe_int(rv(r, "qty", 0), 0),
                safe_int(rv(r, "in_transit", 0), 0),
                safe_int(rv(r, "lead_time_days", 0), 0),
            ])
        self.log.add("INFO", f"Поиск товаров: '{q}', найдено {len(rows)}")

    def _build_doc_lines(self, parent, kind: str):
        frm = ttk.LabelFrame(parent, text="Строки документа", padding=10)
        frm.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        if kind == "IN":
            self.in_lines = []
        else:
            self.out_lines = []

        top = ttk.Frame(frm)
        top.pack(fill="x")

        ttk.Label(top, text="Количество").pack(side="left")
        var_qty = tk.StringVar(value="1")
        e_qty = ttk.Entry(top, textvariable=var_qty, width=8)
        e_qty.pack(side="left", padx=(8, 0))
        Tooltip(e_qty, "КОЛИЧЕСТВО", "Количество для строки. Только целое число больше 0.")

        ttk.Label(top, text="Комментарий строки").pack(side="left", padx=(18, 0))
        var_c = tk.StringVar(value="")
        e_c = ttk.Entry(top, textvariable=var_c)
        e_c.pack(side="left", fill="x", expand=True, padx=(8, 0))
        Tooltip(e_c, "КОММЕНТАРИЙ СТРОКИ", "Комментарий к конкретной строке накладной.")

        btn_add = ttk.Button(top, text="Добавить выбранный товар",
                             command=lambda: self._doc_add_selected(kind, var_qty.get(), var_c.get()))
        btn_add.pack(side="left", padx=(10, 0))
        Tooltip(btn_add, "ДОБАВИТЬ СТРОКУ",
                "Берет выбранный товар из таблицы поиска и добавляет строку в накладную. На склад не влияет, пока не нажмете Создать и применить.")

        tv = self._build_table(frm, ["SKU", "Название", "Количество", "Комментарий"], table_key=f"{kind}_lines")

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=(8, 0))

        btn_make = ttk.Button(btns, text="Создать и применить", command=lambda: self._doc_create_apply(kind))
        btn_make.pack(side="left")

        btn_clear = ttk.Button(btns, text="Очистить строки", command=lambda: self._doc_clear_lines(kind, tv))
        btn_clear.pack(side="left", padx=(10, 0))

        Tooltip(btn_make, "СОЗДАТЬ И ПРИМЕНИТЬ",
                "Создает документ, записывает строки, применяет изменения к остаткам товаров сразу. Логи пишутся справа.")
        Tooltip(btn_clear, "ОЧИСТИТЬ СТРОКИ", "Очищает строки в форме. На базу не влияет.")

        return frm, tv

    def _build_tab_in(self):
        self._build_doc_header(self.tab_in, "IN")
        self.frm_in_picker, self.var_in_q, self.tv_in_picker = self._build_product_picker(self.tab_in, "Выбор товара для прихода")
        self.frm_in_lines, self.tv_in_lines = self._build_doc_lines(self.tab_in, "IN")

    def _build_tab_out(self):
        self._build_doc_header(self.tab_out, "OUT")

        quick = ttk.LabelFrame(self.tab_out, text="Быстрое списание", padding=10)
        quick.pack(fill="x", padx=10, pady=(8, 8))

        self.var_quick_q = tk.StringVar(value="")
        self.var_quick_qty = tk.StringVar(value="1")
        self.var_quick_comment = tk.StringVar(value="")

        r = ttk.Frame(quick)
        r.pack(fill="x")

        ttk.Label(r, text="Поиск").pack(side="left")
        e_q = ttk.Entry(r, textvariable=self.var_quick_q)
        e_q.pack(side="left", fill="x", expand=True, padx=(10, 0))

        ttk.Label(r, text="Кол").pack(side="left", padx=(12, 0))
        e_qty = ttk.Entry(r, textvariable=self.var_quick_qty, width=8)
        e_qty.pack(side="left", padx=(8, 0))

        ttk.Label(r, text="Комментарий").pack(side="left", padx=(12, 0))
        e_c = ttk.Entry(r, textvariable=self.var_quick_comment, width=24)
        e_c.pack(side="left", padx=(8, 0))

        btn = ttk.Button(r, text="Списать", command=self._quick_out_apply)
        btn.pack(side="left", padx=(10, 0))

        Tooltip(e_q, "ПОИСК", "Введите часть названия и нажмите Enter. Списывает быстро, без ручного добавления строк.")
        Tooltip(e_qty, "КОЛИЧЕСТВО", "Если пусто, по умолчанию 1.")
        Tooltip(e_c, "КОММЕНТАРИЙ", "Комментарий к строке расхода. Пример: OLX, клиент, пересылка.")
        Tooltip(btn, "СПИСАТЬ",
                "Ищет товар. Если один кандидат, создаст OUT документ и применит. Если несколько, попросит выбрать номер.")

        e_q.bind("<Return>", lambda _e: self._quick_out_apply())

        self.frm_out_picker, self.var_out_q, self.tv_out_picker = self._build_product_picker(self.tab_out, "Выбор товара для расхода")
        self.frm_out_lines, self.tv_out_lines = self._build_doc_lines(self.tab_out, "OUT")

    def _build_tab_transit(self):
        top = ttk.LabelFrame(self.tab_transit, text="Добавить партию в пути", padding=10)
        top.pack(fill="x", padx=10, pady=(10, 8))

        self.var_eta_sku = tk.StringVar(value="")
        self.var_eta_qty = tk.StringVar(value="1")
        self.var_eta_date = tk.StringVar(value=self.state_obj.last_dates.get("eta_date", today_ymd()))
        self.var_eta_comment = tk.StringVar(value="")

        r = ttk.Frame(top)
        r.pack(fill="x")

        ttk.Label(r, text="SKU").pack(side="left")
        e_sku = ttk.Entry(r, textvariable=self.var_eta_sku, width=18)
        e_sku.pack(side="left", padx=(8, 0))
        Tooltip(e_sku, "SKU", "SKU товара. Можно скопировать из вкладки Товары.")

        ttk.Label(r, text="Кол").pack(side="left", padx=(12, 0))
        e_qty = ttk.Entry(r, textvariable=self.var_eta_qty, width=8)
        e_qty.pack(side="left", padx=(8, 0))

        ttk.Label(r, text="ETA").pack(side="left", padx=(12, 0))
        e_date = ttk.Entry(r, textvariable=self.var_eta_date, width=14)
        e_date.pack(side="left", padx=(8, 0))
        Tooltip(e_date, "ETA ДАТА", "Ожидаемая дата прихода YYYY-MM-DD. Позже добавим календарь.")

        ttk.Label(r, text="Комментарий").pack(side="left", padx=(12, 0))
        e_c = ttk.Entry(r, textvariable=self.var_eta_comment)
        e_c.pack(side="left", fill="x", expand=True, padx=(8, 0))

        btn_add = ttk.Button(r, text="Добавить", command=self._transit_add_batch)
        btn_add.pack(side="left", padx=(10, 0))

        btn_re = ttk.Button(r, text="Пересчитать В пути", command=self._recompute_in_transit)
        btn_re.pack(side="left", padx=(10, 0))

        Tooltip(btn_add, "ДОБАВИТЬ ПАРТИЮ",
                "Создает запись В пути. Она попадет в сумму В пути только если партия активна и вы сделали Пересчитать.")
        Tooltip(btn_re, "ПЕРЕСЧЕТ В ПУТИ",
                "Суммирует активные партии В пути по SKU и записывает сумму в products.in_transit, затем обновляет таблицы.")

        self.tv_transit = self._build_table(
            self.tab_transit,
            ["ID", "SKU", "Количество", "ETA", "Активна", "Комментарий", "Создано"],
            table_key="transit",
        )

        btns = ttk.Frame(self.tab_transit)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        b_off = ttk.Button(btns, text="Сделать неактивной", command=lambda: self._transit_set_active(0))
        b_on = ttk.Button(btns, text="Сделать активной", command=lambda: self._transit_set_active(1))

        b_off.pack(side="left")
        b_on.pack(side="left", padx=(10, 0))

        Tooltip(b_off, "НЕАКТИВНА", "Помечает выбранную партию неактивной. Она перестанет учитываться в сумме В пути.")
        Tooltip(b_on, "АКТИВНА", "Помечает выбранную партию активной. Она начнет учитываться в сумме В пути.")

        self._refresh_transit()

    def _build_tab_reasons(self):
        frm = ttk.Frame(self.tab_reasons, padding=10)
        frm.pack(fill="both", expand=True)

        left = ttk.LabelFrame(frm, text="Причины прихода IN", padding=10)
        right = ttk.LabelFrame(frm, text="Причины расхода OUT", padding=10)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        right.pack(side="left", fill="both", expand=True, padx=(6, 0))

        self.lb_in = tk.Listbox(left, height=18)
        self.lb_out = tk.Listbox(right, height=18)
        self.lb_in.pack(fill="both", expand=True)
        self.lb_out.pack(fill="both", expand=True)

        btns_in = ttk.Frame(left)
        btns_out = ttk.Frame(right)
        btns_in.pack(fill="x", pady=(8, 0))
        btns_out.pack(fill="x", pady=(8, 0))

        b1 = ttk.Button(btns_in, text="Добавить", command=lambda: self._reason_add("IN"))
        b2 = ttk.Button(btns_in, text="Отключить", command=lambda: self._reason_disable("IN"))
        b3 = ttk.Button(btns_out, text="Добавить", command=lambda: self._reason_add("OUT"))
        b4 = ttk.Button(btns_out, text="Отключить", command=lambda: self._reason_disable("OUT"))

        b1.pack(side="left")
        b2.pack(side="left", padx=(10, 0))
        b3.pack(side="left")
        b4.pack(side="left", padx=(10, 0))

        Tooltip(left, "ПРИЧИНЫ IN", "Справочник причин прихода. Используется при создании приходных накладных.")
        Tooltip(right, "ПРИЧИНЫ OUT", "Справочник причин расхода. Используется при создании расходных накладных.")
        Tooltip(b2, "ОТКЛЮЧИТЬ", "Отключает выбранную причину. Она исчезнет из выбора, но история документов сохранится.")
        Tooltip(b4, "ОТКЛЮЧИТЬ", "Отключает выбранную причину. Она исчезнет из выбора, но история документов сохранится.")

        self._refresh_reasons()

    def _build_tab_bots(self):
        self.btn_toggle_ref = [None]
        self._build_button_row(
            self.tab_bots,
            [
                {
                    "text": "Запустить Telegram",
                    "cmd": self._bot_toggle,
                    "tip_title": "ЗАПУСК И ОСТАНОВКА TELEGRAM",
                    "tip_body": "Одна кнопка. Если бот запущен, кнопка станет Остановить. При закрытии программы запоминается состояние и при следующем запуске может автозапуститься.",
                    "ref": self.btn_toggle_ref,
                },
            ],
        )

    def _build_tab_ai(self):
        top = ttk.Frame(self.tab_ai)
        top.pack(fill="x", padx=12, pady=(10, 8))

        btn_tokens = ttk.Button(top, text="Перечитать tokens.py", command=self._load_tokens_to_settings)
        btn_tokens.pack(side="left")
        Tooltip(btn_tokens, "TOKENS", "Проверяет ключи. Если ключ пустой, пишет ERROR в лог.")

        btn_save_rules = ttk.Button(top, text="Сохранить правила", command=self._save_bot_rules)
        btn_save_rules.pack(side="left", padx=(8, 0))
        Tooltip(btn_save_rules, "СОХРАНИТЬ ПРАВИЛА",
                "Сохраняет текст правил. Эти правила использует режим ai. Пишите правила строками: одна строка одно правило.")

        frm = ttk.Frame(self.tab_ai)
        frm.pack(fill="both", expand=True, padx=12, pady=(0, 10))

        lbl = ttk.Label(frm, text="Правила бота", font=("Segoe UI", 10, "bold"))
        lbl.pack(anchor="w")

        self.txt_rules = tk.Text(frm, height=14, wrap="word")
        self.txt_rules.pack(fill="both", expand=True, pady=(6, 0))

        cur = get_setting("bot_rules", "")
        self.txt_rules.insert("1.0", cur)

        Tooltip(
            self.txt_rules,
            "ПРАВИЛА БОТА",
            "Формат: одна строка одно правило.\n"
            "Пример:\n"
            "R01 Формат ответа: Название товара - 250 гр. или 4,50 гр.\n"
            "R02 Если спросили сколько в наличии: до 10 точное, иначе Больше 10.\n"
            "R03 Если qty=0 и in_transit>0 и lead_time_days>0: скажи нет в наличии, но приход через lead..lead+3 дней.\n"
            "R04 Если lead_time_days=0: скажи нет и неизвестно будет ли.\n"
        )

    def _save_bot_rules(self):
        t = self.txt_rules.get("1.0", "end").strip()
        set_setting("bot_rules", t)
        self.log.add("INFO", "Правила бота сохранены")

    def _refresh_products_table(self):
        rows = list_products(limit=5000)
        tv = self.tv_products
        tv.delete(*tv.get_children())

        for r in rows:
            sku = rv(r, "sku", "")
            name = rv(r, "name", "")
            qty = safe_int(rv(r, "qty", 0), 0)
            safety_stock = safe_int(rv(r, "safety_stock", 0), 0)
            in_transit = safe_int(rv(r, "in_transit", 0), 0)
            lead = safe_int(rv(r, "lead_time_days", 0), 0)
            price = rv(r, "price", None)
            status = rv(r, "status", "")

            price_txt = "" if price is None else str(price).replace(".", ",")

            tv.insert("", "end", values=[
                sku, name, qty, safety_stock, in_transit, lead, price_txt, status
            ])

        self.log.add("INFO", f"Таблица товаров обновлена. Строк {len(rows)}")

    def _import_csv(self):
        path = filedialog.askopenfilename(
            title="Выберите CSV файл",
            filetypes=[("CSV", "*.csv"), ("All", "*.*")]
        )
        if not path:
            self.log.add("WARN", "Импорт отменен. Файл не выбран")
            return

        res = import_products_csv(path, self._emit_from_thread)
        if res.get("ok", False):
            inv.recompute_products_in_transit()
            self._refresh_products_table()
            self._refresh_transit()
            self._set_last(f"Импорт CSV ok. {res.get('imported', 0)} шт")
        else:
            self._set_last("Импорт CSV ошибка")

    def _recompute_in_transit(self):
        inv.recompute_products_in_transit()
        self._refresh_products_table()
        self._refresh_transit()
        self.log.add("INFO", "Пересчет В пути выполнен")

    def _doc_add_selected(self, kind: str, qty_s: str, comment: str):
        qty = safe_int(qty_s, 0)
        if qty <= 0:
            self.log.add("WARN", "Количество должно быть больше 0")
            return

        if kind == "IN":
            picker = self.tv_in_picker
            lines = self.in_lines
            tv_lines = self.tv_in_lines
        else:
            picker = self.tv_out_picker
            lines = self.out_lines
            tv_lines = self.tv_out_lines

        sel = picker.selection()
        if not sel:
            self.log.add("WARN", "Не выбран товар в таблице поиска")
            return
        vals = picker.item(sel[0], "values")
        sku = vals[0]
        name = vals[1]

        lines.append({"sku": sku, "name": name, "qty": qty, "comment": comment or ""})
        tv_lines.insert("", "end", values=[sku, name, qty, comment or ""])
        self.log.add("INFO", f"Строка добавлена {kind}: {sku} qty {qty}")

    def _doc_clear_lines(self, kind: str, tv: ttk.Treeview):
        if kind == "IN":
            self.in_lines = []
        else:
            self.out_lines = []
        tv.delete(*tv.get_children())
        self.log.add("INFO", f"Строки очищены: {kind}")

    def _doc_create_apply(self, kind: str):
        if kind == "IN":
            doc_date = (self.var_in_date.get() or "").strip()
            reason = (self.var_in_reason.get() or "").strip()
            comment = (self.var_in_comment.get() or "").strip()
            lines = self.in_lines
        else:
            doc_date = (self.var_out_date.get() or "").strip()
            reason = (self.var_out_reason.get() or "").strip()
            comment = (self.var_out_comment.get() or "").strip()
            lines = self.out_lines

        try:
            parse_ymd(doc_date)
        except Exception:
            self.log.add("ERROR", "Неверная дата. Формат YYYY-MM-DD")
            return

        if not reason:
            self.log.add("ERROR", "Не выбрана причина")
            return
        if not lines:
            self.log.add("WARN", "Нет строк в документе")
            return

        doc_id = inv.create_doc(kind, doc_date, reason, comment)
        for ln in lines:
            inv.add_doc_line(doc_id, ln["sku"], ln["qty"], ln.get("comment", ""))

        ok, msg = inv.apply_doc_to_stock(doc_id)
        if not ok:
            self.log.add("ERROR", f"Документ не применен: {msg}")
            return

        if kind == "IN":
            self.state_obj.last_in_reason = reason
            self.state_obj.last_dates["in_doc_date"] = doc_date
        else:
            self.state_obj.last_out_reason = reason
            self.state_obj.last_dates["out_doc_date"] = doc_date

        self._refresh_products_table()
        self._set_last(f"Документ {kind} применен id {doc_id}")
        self.log.add("INFO", f"Документ {kind} применен id {doc_id}. {msg}")

    def _quick_out_apply(self):
        q = (self.var_quick_q.get() or "").strip()
        if not q:
            self.log.add("WARN", "Введите поиск для списания")
            return

        qty = safe_int(self.var_quick_qty.get(), 1)
        if qty <= 0:
            qty = 1

        comment = (self.var_quick_comment.get() or "").strip()

        rows = search_products(q, everywhere=True, limit=20)
        if not rows:
            self.log.add("WARN", f"Не найдено: {q}")
            return

        if len(rows) > 1:
            names = "\n".join([f"{i+1}. {rv(r,'name','')}" for i, r in enumerate(rows[:10])])
            n = simpledialog.askinteger("Выбор", f"Найдено несколько. Введите номер:\n{names}",
                                        minvalue=1, maxvalue=min(10, len(rows)))
            if not n:
                return
            pick = rows[n - 1]
        else:
            pick = rows[0]

        reason = (self.var_out_reason.get() or "").strip()
        doc_date = (self.var_out_date.get() or today_ymd()).strip()
        if not reason:
            outs = inv.list_reasons("OUT")
            reason = outs[0] if outs else "OLX"
            self.var_out_reason.set(reason)

        doc_id = inv.create_doc("OUT", doc_date, reason, (self.var_out_comment.get() or "").strip())
        inv.add_doc_line(doc_id, rv(pick, "sku", ""), qty, comment)
        ok, msg = inv.apply_doc_to_stock(doc_id)
        if not ok:
            self.log.add("ERROR", f"Списание не применено: {msg}")
            return

        self.state_obj.last_out_reason = reason
        self.state_obj.last_dates["out_doc_date"] = doc_date

        self._refresh_products_table()
        self._set_last(f"Быстрое списание OUT id {doc_id}")
        self.log.add("INFO", f"Быстрое списание: {rv(pick,'sku','')} qty {qty} причина {reason}")

    def _transit_add_batch(self):
        sku = (self.var_eta_sku.get() or "").strip()
        if not sku:
            self.log.add("WARN", "Введите SKU")
            return
        qty = safe_int(self.var_eta_qty.get(), 0)
        if qty <= 0:
            self.log.add("WARN", "Количество должно быть больше 0")
            return
        eta = (self.var_eta_date.get() or "").strip()
        try:
            parse_ymd(eta)
        except Exception:
            self.log.add("ERROR", "Неверная ETA дата. Формат YYYY-MM-DD")
            return
        comment = (self.var_eta_comment.get() or "").strip()

        pr = db_get_product_by_sku(sku)
        if pr is None:
            self.log.add("ERROR", f"SKU не найден в товарах: {sku}")
            return

        bid = inv.create_in_transit_batch(sku, qty, eta, comment)
        self.state_obj.last_dates["eta_date"] = eta
        self._refresh_transit()
        self.log.add("INFO", f"Партия в пути добавлена id {bid} sku {sku} qty {qty} eta {eta}")

    def _refresh_transit(self):
        rows = inv.list_in_transit_batches(active_only=False)
        tv = self.tv_transit
        tv.delete(*tv.get_children())
        for r in rows:
            tv.insert("", "end", values=[
                rv(r, "id", ""),
                rv(r, "sku", ""),
                rv(r, "qty", 0),
                rv(r, "eta_date", ""),
                "Да" if safe_int(rv(r, "is_active", 0), 0) == 1 else "Нет",
                rv(r, "comment", ""),
                rv(r, "created_ts", ""),
            ])
        self.log.add("INFO", f"В пути обновлено. Строк {len(rows)}")

    def _transit_set_active(self, val: int):
        sel = self.tv_transit.selection()
        if not sel:
            self.log.add("WARN", "Не выбрана партия")
            return
        vals = self.tv_transit.item(sel[0], "values")
        batch_id = safe_int(vals[0], 0)
        if batch_id <= 0:
            return
        inv.set_batch_active(batch_id, val)
        self._refresh_transit()
        self.log.add("INFO", f"Партия {batch_id} активность {val}. Теперь пересчитайте В пути.")

    def _refresh_reasons(self):
        self.lb_in.delete(0, "end")
        self.lb_out.delete(0, "end")
        for r in inv.list_reasons("IN"):
            self.lb_in.insert("end", r)
        for r in inv.list_reasons("OUT"):
            self.lb_out.insert("end", r)

        ins = inv.list_reasons("IN")
        outs = inv.list_reasons("OUT")
        if hasattr(self, "var_in_reason") and not self.var_in_reason.get() and ins:
            self.var_in_reason.set(ins[0])
        if hasattr(self, "var_out_reason") and not self.var_out_reason.get() and outs:
            self.var_out_reason.set(outs[0])

        self.log.add("INFO", "Причины обновлены")

    def _reason_add(self, kind: str):
        name = simpledialog.askstring("Причина", "Введите название причины")
        if not name:
            return
        inv.upsert_reason(kind, name.strip(), 1, 0)
        self._refresh_reasons()

    def _reason_disable(self, kind: str):
        lb = self.lb_in if kind == "IN" else self.lb_out
        sel = lb.curselection()
        if not sel:
            self.log.add("WARN", "Не выбрана причина")
            return
        name = lb.get(sel[0])
        if not messagebox.askyesno("Отключить", f"Отключить причину: {name}"):
            return
        inv.upsert_reason(kind, name, 0, 0)
        self._refresh_reasons()

    def _load_tokens_to_settings(self):
        tp = os.path.join(app_dir(), "tokens.py")
        try:
            mod = load_tokens(tp)
            td = get_tokens_dict(mod)
            errs = validate_tokens(td)
            if errs:
                for e in errs:
                    self.log.add("ERROR", e)
            else:
                self.log.add("INFO", "Все ключи в tokens.py заполнены")

            set_setting("telegram_api_key", (td.get("TELEGRAM_API_KEY") or "").strip())
            set_setting("deepseek_api_key", (td.get("DEEPSEEK_API_KEY") or "").strip())
        except Exception as e:
            self.log.add("ERROR", f"Не удалось прочитать tokens.py: {e}")

    def _try_autostart_bot(self):
        try:
            self._bot_start_internal(autostart_call=True)
        except Exception as e:
            self.log.add("ERROR", f"Автозапуск бота не удался: {e}")

    def _bot_start_internal(self, autostart_call: bool = False):
        token = (get_setting("telegram_api_key", "") or "").strip()
        if token == "":
            self.log.add("ERROR", "TELEGRAM_API_KEY пустой. Запуск невозможен")
            return

        if self.bot is None:
            self.bot = TelegramBotRunner(token, self._emit_from_thread)

        if not self.bot.is_running():
            self.bot.start()
            self.lbl_bot.configure(text="Telegram бот: Запущен")
            if hasattr(self, "btn_toggle_ref") and self.btn_toggle_ref[0] is not None:
                self.btn_toggle_ref[0].configure(text="Остановить Telegram")
            set_setting("bot_autostart", "1")
            self._set_last("Telegram бот автозапущен" if autostart_call else "Telegram бот запущен")

    def _bot_toggle(self):
        token = (get_setting("telegram_api_key", "") or "").strip()
        if token == "":
            self.log.add("ERROR", "TELEGRAM_API_KEY пустой. Запуск невозможен")
            return

        if self.bot is None:
            self.bot = TelegramBotRunner(token, self._emit_from_thread)

        if self.bot.is_running():
            self.bot.stop()
            self.lbl_bot.configure(text="Telegram бот: Остановлен")
            if self.btn_toggle_ref[0] is not None:
                self.btn_toggle_ref[0].configure(text="Запустить Telegram")
            set_setting("bot_autostart", "0")
            self._set_last("Telegram бот остановлен")
            return

        self._bot_start_internal()

    def _on_bot_mode_change(self):
        m = (self.var_bot_mode.get() or "").strip()
        if m not in {"operator", "triggers", "ai"}:
            m = "ai"
            self.var_bot_mode.set("ai")
        self.state_obj.bot_answer_mode = m
        set_setting("bot_answer_mode", m)
        self.log.add("INFO", f"Режим ответа бота сохранен: {m}")

    def _on_tab_change(self):
        idx = self.nb.index(self.nb.select())
        name = self.nb.tab(idx, "text")
        self.state_obj.active_tab = name
        self._set_last(f"Вкладка: {name.strip()}")

    def _bind_events(self):
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bind("<Configure>", self._on_configure, add="+")
        self.bind("<ButtonRelease-1>", self._capture_sashes, add="+")

    def _on_configure(self, _event=None):
        self.state_obj.geometry = self.geometry()

    def _capture_sashes(self, _event=None):
        try:
            sash_left = self.outer.sashpos(0)
            total_w = max(self.winfo_width(), 1200)
            self.state_obj.right_width = max(360, total_w - int(sash_left))
        except Exception:
            pass
        try:
            status_h = self.right_split.sashpos(0)
            self.state_obj.right_status_height = int(status_h)
        except Exception:
            pass

    def _apply_state_to_sashes_safe(self):
        self.update_idletasks()
        self._reset_layout(apply_state=True)

    def _reset_layout(self, apply_state=False):
        self.update_idletasks()

        total_w = max(self.winfo_width(), 1200)
        min_left = 620
        min_right = 360

        desired_right = 420
        if apply_state:
            desired_right = max(min_right, min(int(self.state_obj.right_width), total_w - min_left))

        desired_left = total_w - desired_right
        desired_left = max(min_left, min(desired_left, total_w - min_right))
        try:
            self.outer.sashpos(0, desired_left)
        except Exception:
            pass

        total_h = max(self.right.winfo_height(), 650)
        min_status = 140
        min_log = 240

        desired_status = 170
        if apply_state:
            desired_status = max(min_status, min(int(self.state_obj.right_status_height), total_h - min_log))

        try:
            self.right_split.sashpos(0, desired_status)
        except Exception:
            pass

    def _on_close(self):
        self._save_state()
        self.destroy()

    def _save_state(self):
        self._capture_sashes()
        self.state_obj.bot_answer_mode = self.var_bot_mode.get()
        self.state_obj.active_tab = self.nb.tab(self.nb.index(self.nb.select()), "text")
        self.state_obj.geometry = self.geometry()

        if hasattr(self, "var_in_reason"):
            self.state_obj.last_in_reason = self.var_in_reason.get()
        if hasattr(self, "var_out_reason"):
            self.state_obj.last_out_reason = self.var_out_reason.get()
        if hasattr(self, "var_in_date"):
            self.state_obj.last_dates["in_doc_date"] = self.var_in_date.get()
        if hasattr(self, "var_out_date"):
            self.state_obj.last_dates["out_doc_date"] = self.var_out_date.get()
        if hasattr(self, "var_eta_date"):
            self.state_obj.last_dates["eta_date"] = self.var_eta_date.get()

        self.state_obj.save()

    def _set_last(self, text: str):
        self.lbl_last.configure(text=f"Последняя операция: {text}")


if __name__ == "__main__":
    app = App()
    app.mainloop()
