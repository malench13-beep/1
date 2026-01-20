import importlib.util
import os


REQUIRED_KEYS = [
    "PROM_API_TOKEN",
    "OPENAI_API_KEY",
    "TELEGRAM_API_KEY",
    "ROZETKA_API_KEY",
    "OLX_API_KEY",
    "DEEPSEEK_API_KEY",
]


def load_tokens(tokens_path: str):
    if not os.path.exists(tokens_path):
        raise FileNotFoundError(f"Не найден файл tokens.py по пути: {tokens_path}")

    spec = importlib.util.spec_from_file_location("tokens", tokens_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Не удалось загрузить tokens.py как модуль")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def get_tokens_dict(tokens_mod) -> dict:
    d = {}
    for k in REQUIRED_KEYS:
        d[k] = getattr(tokens_mod, k, None)
    return d


def validate_tokens(tokens_dict: dict) -> list:
    errors = []
    for k in REQUIRED_KEYS:
        v = tokens_dict.get(k, None)
        if v is None:
            errors.append(f"Отсутствует переменная {k} в tokens.py")
        elif isinstance(v, str) and v.strip() == "":
            errors.append(f"Пустой ключ {k} в tokens.py")
    return errors
