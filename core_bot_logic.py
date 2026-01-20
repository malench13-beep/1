# core_bot_logic.py
# Общая логика ответа по базе товаров
#
# Режимы
# operator  всегда перевод на оператора
# triggers  обрабатывается в транспорте
# ai        поиск по базе, уточнения, строгий формат ответа

import re
from dataclasses import dataclass
from datetime import datetime, time as dtime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import List, Optional, Tuple, Callable

from core_db import search_products, get_setting

Logger = Callable[[str, str], None]


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_uah(price) -> str:
    if price is None or str(price).strip() == "":
        return "нет"
    try:
        d = Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return str(price)

    if d == d.to_integral():
        return f"{int(d)} гр."
    s = f"{d:.2f}".replace(".", ",")
    return f"{s} гр."


def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9\s]", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(s: str) -> List[str]:
    s = normalize_text(s)
    return [p for p in s.split(" ") if len(p) >= 2]


def parse_time_hhmm(s: str) -> Optional[dtime]:
    s = (s or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return dtime(hour=hh, minute=mm)


def is_within_work_hours() -> bool:
    start_s = get_setting("work_start_hhmm", "09:00")
    end_s = get_setting("work_end_hhmm", "18:00")
    st = parse_time_hhmm(start_s)
    en = parse_time_hhmm(end_s)
    if st is None or en is None:
        return True

    now = datetime.now().time()
    if st <= en:
        return st <= now <= en
    return now >= st or now <= en


def is_greeting(q: str) -> bool:
    qn = normalize_text(q)
    greetings = [
        "привет", "здравствуйте", "добрый", "доброго", "доброе", "хай", "hello",
        "спасибо", "благодарю"
    ]
    return any(g in qn for g in greetings)


def extract_requested_qty(q: str) -> Optional[int]:
    qn = normalize_text(q)
    nums = re.findall(r"\b(\d{1,4})\b", qn)
    if not nums:
        return None
    try:
        n = int(nums[0])
    except Exception:
        return None
    if n <= 0:
        return None
    return n


def detect_intent(q: str) -> str:
    qn = normalize_text(q)

    if any(w in qn for w in ["сколько", "количество", "ск-ко"]):
        return "qty"
    if any(w in qn for w in ["есть", "налич", "в наличии", "есть ли"]):
        return "in_stock"
    if any(w in qn for w in ["цена", "сто", "сколько стоит", "почем"]):
        return "price"
    if any(w in qn for w in ["доставка", "когда придет", "когда будет", "привез", "срок", "приход"]):
        return "delivery"
    if any(w in qn for w in ["гарант", "возврат", "обмен", "брак", "не работает", "полом"]):
        return "policy"

    rq = extract_requested_qty(q)
    if rq is not None and any(w in qn for w in ["надо", "нужно", "хочу", "возьму", "беру", "закажу", "заказать", "куплю", "купить"]):
        return "need_qty"

    return "general"


def best_match_from_rows(q: str, rows: List[dict]) -> Tuple[Optional[dict], List[dict]]:
    if not rows:
        return None, []

    qt = tokenize(q)
    if not qt:
        return rows[0], rows[:5]

    scored = []
    qn = normalize_text(q)

    for r in rows:
        name = str(r.get("name", ""))
        nn = normalize_text(name)
        nt_set = set(tokenize(name))

        score = 0
        if qn and qn in nn:
            score += 60

        overlap = len(set(qt) & nt_set)
        score += overlap * 12

        for t in qt:
            if t in nn:
                score += 4

        scored.append((score, r))

    scored.sort(key=lambda x: x[0], reverse=True)
    best = scored[0][1]
    top = [x[1] for x in scored[:5]]
    return best, top


def product_line(r: dict) -> str:
    return f"{r.get('name','')} - {format_uah(r.get('price'))}"


def qty_to_public(qty: int) -> str:
    if qty <= 0:
        return "0"
    if qty <= 10:
        return str(qty)
    return "Больше 10"


def semantic_expand_queries(q: str) -> List[str]:
    toks = [t for t in tokenize(q) if len(t) >= 3]
    out: List[str] = []
    if toks:
        out.append(" ".join(toks))
        out.extend(toks[:5])

    nums = re.findall(r"\b(\d{1,6})\b", normalize_text(q))
    for n in nums[:3]:
        out.append(n)

    seen = set()
    res = []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            res.append(s)
    return res[:8]


def build_ticket_summary(platform: str, customer_text: str, found: List[dict], bot_text: str) -> str:
    lines = []
    lines.append(f"Help,{platform}, вопрос клиента: {customer_text}")
    if found:
        lines.append("Найдено в базе:")
        for r in found[:5]:
            qty = r.get("qty", 0)
            it = r.get("in_transit", 0)
            lt = r.get("lead_time_days", 0)
            lines.append(f"- {product_line(r)}; остаток {qty}; в пути {it}; lead {lt}")
    else:
        lines.append("В базе не найдено совпадений.")
    lines.append(f"Ответ бота клиенту: {bot_text}")
    lines.append("Поставьте + если берете в работу.")
    return "\n".join(lines)


def build_admin_large_order(platform: str, customer_text: str, found: Optional[dict], requested_qty: int) -> str:
    line = product_line(found) if found else "Товар не определен"
    return (
        f"Info,{platform}, крупный запрос.\n"
        f"Клиент хочет: {requested_qty} шт.\n"
        f"Запрос: {customer_text}\n"
        f"Позиция: {line}\n"
        f"Время: {now_ts()}"
    )


@dataclass
class BotDecision:
    reply_text: str = ""
    ticket_needed: bool = False
    ticket_summary: str = ""
    notify_main: bool = False
    main_message: str = ""
    notify_admin: bool = False
    admin_message: str = ""


def handle_customer_message(platform: str, customer_text: str, logger: Logger, mode: str = "ai") -> BotDecision:
    q = (customer_text or "").strip()
    if q == "":
        return BotDecision()

    mode = (mode or "ai").strip().lower()

    if is_greeting(q):
        return BotDecision(reply_text="Здравствуйте. Напишите название товара или модель. Я подскажу цену и наличие.")

    if mode == "operator":
        txt = (
            "Я бот программы автоответов. К сожалению, не могу дать ответ на ваш вопрос. "
            "Я уже отправил ваш вопрос оператору, он ответит в рабочее время."
        )
        summ = build_ticket_summary(platform, q, [], txt)
        return BotDecision(reply_text=txt, ticket_needed=True, ticket_summary=summ)

    within = is_within_work_hours()
    intent = detect_intent(q)
    requested_qty = extract_requested_qty(q) if intent == "need_qty" else None

    rows = search_products(q, everywhere=False, limit=30)
    if not rows:
        rows = search_products(q, everywhere=True, limit=30)

    if not rows:
        for qq in semantic_expand_queries(q):
            rows = search_products(qq, everywhere=True, limit=30)
            if rows:
                break

    if not rows:
        txt = (
            "Я бот программы автоответов. К сожалению, не могу дать ответ на ваш вопрос. "
            "Я уже отправил ваш вопрос оператору, он ответит в рабочее время."
        )
        summ = build_ticket_summary(platform, q, [], txt)
        return BotDecision(reply_text=txt, ticket_needed=True, ticket_summary=summ)

    best, top = best_match_from_rows(q, rows)
    if best is None:
        txt = (
            "Я бот программы автоответов. К сожалению, не могу дать ответ на ваш вопрос. "
            "Я уже отправил ваш вопрос оператору, он ответит в рабочее время."
        )
        summ = build_ticket_summary(platform, q, [], txt)
        return BotDecision(reply_text=txt, ticket_needed=True, ticket_summary=summ)

    qn = normalize_text(q)
    if len(top) >= 2 and (len(qn) <= 4 or intent in ("general", "policy")):
        opts = []
        for i, r in enumerate(top[:5], start=1):
            opts.append(f"{i}. {product_line(r)}")
        txt = "Уточните, какой вариант нужен. Ответьте номером из списка.\n" + "\n".join(opts)
        soft = build_ticket_summary(platform, q, top[:5], txt)
        return BotDecision(reply_text=txt, notify_main=True, main_message=soft)

    if intent == "policy" and not within:
        txt = "Я бот программы автоответов. Я передал ваш вопрос оператору, он ответит в рабочее время."
        summ = build_ticket_summary(platform, q, [best], txt)
        return BotDecision(reply_text=txt, ticket_needed=True, ticket_summary=summ)

    qty = int(best.get("qty", 0) or 0)
    in_transit = int(best.get("in_transit", 0) or 0)
    lead = int(best.get("lead_time_days", 0) or 0)

    if intent == "price":
        txt = product_line(best)

    elif intent == "in_stock":
        txt = "Да" if qty > 0 else "Нет"

    elif intent == "qty":
        txt = qty_to_public(qty)

    elif intent == "need_qty":
        need = int(requested_qty or 0)
        if need > 0 and qty >= need:
            txt = "Да, такое количество есть"
        elif need > 0 and qty < need:
            txt = f"К сожалению, такого количества нет. В наличии {qty_to_public(qty)}."
        else:
            txt = product_line(best)

    elif intent == "delivery":
        if qty > 0:
            txt = "Есть в наличии"
        else:
            if in_transit > 0 and lead > 0:
                txt = f"Сейчас товара нет. Ориентировочно следующий приход через {lead},{lead+3} дней. Напишите, я сообщу, когда появится."
            elif lead <= 0:
                txt = "Извините, товара нет в наличии. К сожалению, сейчас не могу сказать, будет ли он еще."
            else:
                txt = "Извините, товара нет в наличии. К сожалению, сейчас не могу сказать, будет ли он еще."

    else:
        txt = product_line(best)

    out = BotDecision(reply_text=txt)

    if intent == "need_qty" and requested_qty is not None:
        threshold = int(get_setting("large_order_qty", "10") or "10")
        if int(requested_qty) >= threshold:
            out.notify_admin = True
            out.admin_message = build_admin_large_order(platform, q, best, int(requested_qty))

    return out
