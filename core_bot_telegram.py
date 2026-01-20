# core_bot_telegram.py
# Telegram транспорт
# Эскалация запускается только когда decision.ticket_needed = True
# Уведомление главному оператору без тикета это decision.notify_main = True

import threading
import time
import requests
import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from core_db import get_setting
from core_bot_logic import handle_customer_message

Logger = Callable[[str, str], None]


def _bot_prefix(text: str) -> str:
    t = (text or "").strip()
    if t == "":
        return ""
    return "Bot:\n" + t


@dataclass
class Ticket:
    ticket_id: str
    platform: str
    customer_chat_id: int
    customer_text: str
    summary: str
    created_ts: float
    claimed_by: Optional[int] = None
    last_ping_ts: float = 0.0
    stage: int = 0
    resolved: bool = False


class TelegramBotRunner:
    def __init__(self, token: str, logger: Logger):
        self.token = token
        self.logger = logger

        self._stop = threading.Event()
        self._thread = None
        self._offset = 0
        self._session = requests.Session()
        self.base = f"https://api.telegram.org/bot{self.token}"

        self._tickets_lock = threading.Lock()
        self._tickets: Dict[str, Ticket] = {}

    def start(self):
        if self._thread and self._thread.is_alive():
            self.logger("WARN", "Telegram бот уже запущен")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self.logger("INFO", "Telegram бот запущен")

    def stop(self):
        self._stop.set()
        self.logger("INFO", "Telegram бот остановка запрошена")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _send(self, chat_id: int, text: str):
        if not text:
            return
        try:
            r = self._session.post(
                self.base + "/sendMessage",
                data={"chat_id": chat_id, "text": text},
                timeout=20,
            )
            if r.status_code != 200:
                self.logger("ERROR", f"Telegram sendMessage HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            self.logger("ERROR", f"Telegram sendMessage ошибка: {e}")

    def _get_updates(self):
        try:
            r = self._session.get(
                self.base + "/getUpdates",
                params={"timeout": 25, "offset": self._offset},
                timeout=35,
            )
            if r.status_code != 200:
                self.logger("ERROR", f"Telegram getUpdates HTTP {r.status_code}: {r.text[:200]}")
                return []
            data = r.json()
            if not data.get("ok", False):
                self.logger("ERROR", f"Telegram getUpdates ok=false: {data}")
                return []
            return data.get("result", [])
        except Exception as e:
            self.logger("ERROR", f"Telegram getUpdates ошибка: {e}")
            return []

    def _load_ids(self, key: str) -> List[int]:
        raw = get_setting(key, "[]")
        try:
            arr = json.loads(raw)
            out = []
            for x in arr:
                try:
                    out.append(int(x))
                except Exception:
                    pass
            return out
        except Exception:
            return []

    def _load_ops(self) -> Dict[str, List[int]]:
        return {
            "main": self._load_ids("ops_main_ids"),
            "all": self._load_ids("ops_all_ids"),
            "admin": self._load_ids("ops_admin_ids"),
        }

    def _is_operator_chat(self, chat_id: int) -> bool:
        ops = self._load_ops()
        return chat_id in ops["main"] or chat_id in ops["all"] or chat_id in ops["admin"]

    def _create_ticket(self, platform: str, customer_chat_id: int, customer_text: str, summary: str) -> str:
        tid = f"{int(time.time())}_{customer_chat_id}"
        t = Ticket(
            ticket_id=tid,
            platform=platform,
            customer_chat_id=customer_chat_id,
            customer_text=customer_text,
            summary=summary,
            created_ts=time.time(),
            last_ping_ts=0.0,
            stage=0,
            resolved=False,
        )
        with self._tickets_lock:
            self._tickets[tid] = t
        return tid

    def _claim_oldest(self, operator_chat_id: int) -> Optional[Ticket]:
        with self._tickets_lock:
            open_tickets = [t for t in self._tickets.values() if not t.resolved and t.claimed_by is None]
            open_tickets.sort(key=lambda x: x.created_ts)
            if not open_tickets:
                return None
            t = open_tickets[0]
            t.claimed_by = operator_chat_id
            return t

    def _ping_ticket_stage(self, t: Ticket, target_ids: List[int]):
        for cid in target_ids:
            self._send(cid, t.summary)
        t.last_ping_ts = time.time()

    def _process_escalations(self):
        ops = self._load_ops()
        now = time.time()

        with self._tickets_lock:
            tickets = list(self._tickets.values())

        for t in tickets:
            if t.resolved:
                continue
            if t.claimed_by is not None:
                continue

            age = now - t.created_ts

            if t.stage == 0 and t.last_ping_ts == 0.0:
                if ops["main"]:
                    self._ping_ticket_stage(t, ops["main"])
                    self.logger("INFO", f"Ticket {t.ticket_id} отправлен главному оператору")
                t.stage = 1
                continue

            if t.stage == 1 and age >= 60 and (now - t.last_ping_ts) >= 60:
                targets = list(set(ops["main"] + ops["all"]))
                if targets:
                    self._ping_ticket_stage(t, targets)
                    self.logger("WARN", f"Ticket {t.ticket_id} повтор через 60 сек всем операторам")
                t.stage = 2
                continue

            if t.stage == 2 and age >= 60 + 180 and (now - t.last_ping_ts) >= 180:
                targets = list(set(ops["main"] + ops["all"]))
                if targets:
                    self._ping_ticket_stage(t, targets)
                    self.logger("WARN", f"Ticket {t.ticket_id} повтор через 3 минуты всем операторам")
                t.stage = 3
                continue

            if t.stage == 3 and age >= 60 + 180 + 420:
                self._send(t.customer_chat_id, _bot_prefix("Извините, сейчас оператор занят. Как освободится главный оператор, он вам напишет."))
                t.stage = 4
                self.logger("ERROR", f"Ticket {t.ticket_id} финал клиенту после 7 минут")
                continue

            if age >= 3600:
                if ops["admin"]:
                    self._ping_ticket_stage(t, ops["admin"])
                    self.logger("ERROR", f"Ticket {t.ticket_id} 1 час без реакции, отправлено админу")
                t.resolved = True

    def _send_admin_info(self, msg: str):
        ops = self._load_ops()
        for cid in ops["admin"]:
            self._send(cid, msg)

    def _send_main_info(self, msg: str):
        ops = self._load_ops()
        for cid in ops["main"]:
            self._send(cid, msg)

    def _load_triggers(self) -> List[dict]:
        raw = get_setting("bot_triggers_json", "[]")
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                out = []
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    trig = str(row.get("triggers", "")).strip()
                    ans = str(row.get("answer", "")).strip()
                    if trig and ans:
                        out.append({"triggers": trig, "answer": ans})
                return out
        except Exception:
            pass
        return []

    def _match_trigger(self, text: str) -> Optional[str]:
        q = (text or "").lower()
        q = q.replace("ё", "е")
        rows = self._load_triggers()
        for row in rows:
            trig = row["triggers"]
            ans = row["answer"]
            keys = [t.strip().lower() for t in trig.split(",") if t.strip()]
            for k in keys:
                if k and k in q:
                    return ans
        return None

    def _loop(self):
        self.logger("INFO", "Telegram цикл запущен")
        while not self._stop.is_set():
            updates = self._get_updates()
            for u in updates:
                try:
                    self._offset = max(self._offset, int(u["update_id"]) + 1)
                    msg = u.get("message", None)
                    if not msg:
                        continue

                    chat = msg.get("chat", {})
                    chat_id = chat.get("id", None)
                    text = msg.get("text", "")

                    if chat_id is None:
                        continue
                    chat_id = int(chat_id)

                    if self._is_operator_chat(chat_id):
                        if (text or "").strip() == "+":
                            t = self._claim_oldest(chat_id)
                            if t is None:
                                self._send(chat_id, "Нет активных запросов")
                            else:
                                self._send(chat_id, f"Взято в работу. Ticket {t.ticket_id}. Платформа: {t.platform}")
                                self.logger("INFO", f"Ticket {t.ticket_id} взят оператором {chat_id}")
                            continue
                        continue

                    self.logger("INFO", f"Telegram входящее: chat_id {chat_id} text {text}")

                    mode = (get_setting("bot_answer_mode", "ai") or "ai").strip().lower()

                    if mode == "triggers":
                        ans = self._match_trigger(text)
                        if ans is not None:
                            self._send(chat_id, _bot_prefix(ans))
                        else:
                            txt = (
                                "Я бот программы автоответов. К сожалению, не могу дать ответ на ваш вопрос. "
                                "Я уже отправил ваш вопрос оператору, он ответит в рабочее время."
                            )
                            self._send(chat_id, _bot_prefix(txt))
                            summ = f"Help,telegram, триггеры не сработали. Вопрос клиента: {text}\nОтвет бота клиенту: {txt}\nПоставьте + если берете в работу."
                            tid = self._create_ticket("telegram", chat_id, text, summ)
                            self.logger("WARN", f"Создан тикет {tid} для эскалации")
                        continue

                    decision = handle_customer_message(platform="telegram", customer_text=text, logger=self.logger, mode=mode)

                    if decision.reply_text:
                        self._send(chat_id, _bot_prefix(decision.reply_text))

                    if decision.notify_admin and decision.admin_message:
                        self._send_admin_info(decision.admin_message)
                        self.logger("INFO", "Отправлено админу инфо")

                    if decision.notify_main and decision.main_message:
                        self._send_main_info(decision.main_message)
                        self.logger("INFO", "Отправлено главному оператору мягкое уведомление без тикета")

                    if decision.ticket_needed and decision.ticket_summary:
                        tid = self._create_ticket("telegram", chat_id, text, decision.ticket_summary)
                        self.logger("WARN", f"Создан тикет {tid} для эскалации")

                except Exception as e:
                    self.logger("ERROR", f"Telegram обработка update ошибка: {e}")

            self._process_escalations()
            time.sleep(0.3)

        self.logger("INFO", "Telegram цикл остановлен")
