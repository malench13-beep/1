import re
import json
import requests
from typing import Callable, Dict, Any, List


Logger = Callable[[str, str], None]


def extract_numbers(text: str) -> List[str]:
    return re.findall(r"\d+(?:[.,]\d+)?", text or "")


def deepseek_chat(
    api_key: str,
    base_url: str,
    model: str,
    system_text: str,
    user_text: str,
    logger: Logger,
    timeout_sec: int = 25,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + "/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
        "temperature": 0.2,
        "max_tokens": 500,
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
    except requests.exceptions.Timeout:
        logger("ERROR", "Deepseek таймаут")
        return {"ok": False, "error": "timeout"}
    except Exception as e:
        logger("ERROR", f"Deepseek ошибка запроса: {e}")
        return {"ok": False, "error": str(e)}

    if r.status_code != 200:
        logger("ERROR", f"Deepseek HTTP {r.status_code}: {r.text[:280]}")
        return {"ok": False, "error": f"http_{r.status_code}"}

    try:
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        return {"ok": True, "text": content}
    except Exception as e:
        logger("ERROR", f"Deepseek неверный ответ: {e}")
        return {"ok": False, "error": "bad_response"}


def safe_ai_answer_or_fallback(
    ai_text: str,
    facts_text: str,
    fallback_text: str,
    logger: Logger
) -> str:
    facts_nums = set(extract_numbers(facts_text))
    ai_nums = set(extract_numbers(ai_text))

    if len(ai_nums - facts_nums) > 0:
        logger("WARN", "Deepseek попытался вывести числа, которых нет в фактах. Использую строгий ответ")
        return fallback_text

    banned = ["возможно", "примерно", "скорее всего"]
    low = (ai_text or "").lower()
    for w in banned:
        if w in low:
            logger("WARN", "Deepseek использовал запрещенные слова. Использую строгий ответ")
            return fallback_text

    t = (ai_text or "").strip()
    if t == "":
        logger("WARN", "Deepseek вернул пустой ответ. Использую строгий ответ")
        return fallback_text

    return t


def parse_json_safely(text: str) -> Dict[str, Any]:
    try:
        t = (text or "").strip()
        if t.startswith("```"):
            t = t.strip("`")
        return json.loads(t)
    except Exception:
        return {}
