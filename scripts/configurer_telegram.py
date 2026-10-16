"""Aide à la configuration des alertes Telegram.

1. Après avoir mis TELEGRAM_BOT_TOKEN dans .env et envoyé un message à ton bot :
     docker compose run --rm --no-deps app python -m scripts.configurer_telegram
   -> affiche l'identifiant de la conversation (à copier dans TELEGRAM_CHAT_ID).
2. Une fois TELEGRAM_CHAT_ID renseigné :
     docker compose run --rm --no-deps app python -m scripts.configurer_telegram --test
   -> envoie un message de test.
"""

from __future__ import annotations

import os
import sys

from src.utils.alertes import _token, call, masked, send_alert


def find_chats() -> int:
    token = _token()
    if not token:
        print("TELEGRAM_BOT_TOKEN absent de .env (étape 1 de la procédure).")
        return 1
    bot = call("getMe", token)
    print(f"Bot reconnu : @{bot['username']} (jeton {masked(token)})")
    chats = {}
    for update in call("getUpdates", token):
        msg = update.get("message") or update.get("channel_post") or {}
        chat = msg.get("chat")
        if chat:
            name = chat.get("title") or " ".join(
                x for x in (chat.get("first_name"), chat.get("last_name")) if x
            )
            chats[chat["id"]] = (chat["type"], name)
    if not chats:
        print("Aucun message reçu : envoie « bonjour » à ton bot sur Telegram, "
              "puis relance cette commande.")  # fmt: skip
        return 1
    print("Conversations trouvées (copie l'identifiant dans TELEGRAM_CHAT_ID) :")
    for chat_id, (kind, name) in chats.items():
        print(f"  TELEGRAM_CHAT_ID={chat_id}    ({kind} : {name})")
    return 0


def test() -> int:
    if not os.getenv("TELEGRAM_CHAT_ID"):
        print(
            "TELEGRAM_CHAT_ID absent de .env : lance d'abord la commande sans --test."
        )
        return 1
    ok = send_alert("✅ JobMarket MA : les alertes Telegram fonctionnent.")
    print("Message de test envoyé." if ok else "Échec de l'envoi (voir ci-dessus).")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(test() if "--test" in sys.argv else find_chats())
