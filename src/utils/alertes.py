"""Alertes Telegram (bot) : un message quand une collecte ou un job échoue.

Configuration dans .env (voir scripts/configurer_telegram.py) :
  TELEGRAM_BOT_TOKEN : jeton donné par @BotFather
  TELEGRAM_CHAT_ID   : identifiant de la conversation qui reçoit les alertes
Le jeton n'est jamais affiché ni journalisé (il donne le contrôle du bot).
"""

from __future__ import annotations

import os

import requests
from dotenv import find_dotenv, load_dotenv

API = "https://api.telegram.org/bot{token}/{method}"
MAX_LEN = 4000  # limite Telegram : 4096 caractères par message


def _token() -> str | None:
    load_dotenv(find_dotenv(usecwd=True), override=False)
    return os.getenv("TELEGRAM_BOT_TOKEN") or None


def masked(token: str | None) -> str:
    """'123456:ABCdef...' -> '123456:…' : de quoi reconnaître le bot, pas le piloter."""
    return f"{token.split(':')[0]}:…" if token else "(absent)"


def call(method: str, token: str | None = None, **params) -> dict:
    token = token or _token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN manquant dans .env")
    resp = requests.post(
        API.format(token=token, method=method), json=params, timeout=20
    )
    data = resp.json()
    if not data.get("ok"):
        # Message d'erreur de Telegram, jamais l'URL (elle contient le jeton)
        raise RuntimeError(
            f"Telegram {method} : {data.get('description', resp.status_code)}"
        )
    return data["result"]


def send_alert(text: str) -> bool:
    """Envoie une alerte. Ne lève jamais : une alerte ratée ne doit pas masquer
    l'erreur d'origine. Retourne False (et l'écrit sur la sortie) en cas d'échec."""
    load_dotenv(find_dotenv(usecwd=True), override=False)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    try:
        if not chat_id:
            raise RuntimeError("TELEGRAM_CHAT_ID manquant dans .env")
        call("sendMessage", chat_id=chat_id, text=text[:MAX_LEN])
        return True
    except Exception as exc:  # noqa: BLE001 - voir docstring
        print(f"[alerte] envoi Telegram impossible : {exc}")
        return False
