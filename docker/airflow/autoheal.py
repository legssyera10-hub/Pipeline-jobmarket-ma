"""Contrôle de santé Docker qui se répare : redémarre un service Airflow figé.

  healthcheck: python /opt/airflow/autoheal.py <url de santé> <nom du service>

Pourquoi : Docker ne redémarre JAMAIS un conteneur « unhealthy », seulement un
conteneur arrêté. Au jour 8, le scheduler s'est figé 35 min (plus de battement de
coeur) : sans intervention, le run de 18:00 n'aurait pas été créé.

Fonctionnement (une exécution par contrôle, l'état est gardé dans un fichier) :
  - succès : compteur remis à zéro ; si le service vient d'être redémarré par ce
    script, alerte Telegram « rétabli » (envoyée une fois) ;
  - échec pendant la période de grâce après démarrage : non compté (un démarrage
    lent n'est pas une panne) ;
  - MAX_FAILURES échecs consécutifs : SIGTERM au processus 1 (arrêt propre) ;
    toujours en échec au contrôle suivant : SIGKILL des processus du service.
    Le conteneur s'arrête et `restart: unless-stopped` le relance.
Aucune dépendance hors bibliothèque standard ; ne journalise jamais le jeton Telegram.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

MAX_FAILURES = int(os.getenv("AUTOHEAL_MAX_FAILURES", "6"))  # x 30 s = 3 min
GRACE_S = int(os.getenv("AUTOHEAL_GRACE_S", "300"))
STATE_DIR = Path("/opt/airflow/logs/.autoheal")  # volume : survit au redémarrage


def healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status == 200
    except Exception:
        return False


def pid1_start_ticks() -> int:
    """Instant de démarrage du processus 1 : identifie une « vie » du conteneur."""
    return int(Path("/proc/1/stat").read_text().rsplit(")", 1)[1].split()[19])


def pid1_uptime() -> float:
    """Secondes écoulées depuis le démarrage du conteneur (processus 1)."""
    boot_uptime = float(Path("/proc/uptime").read_text().split()[0])
    return boot_uptime - pid1_start_ticks() / os.sysconf("SC_CLK_TCK")


def log(msg: str) -> None:
    """Dans le journal du conteneur (docker logs), pas seulement dans la sonde."""
    try:
        with open("/proc/1/fd/1", "w") as out:
            out.write(f"[autoheal] {msg}\n")
    except OSError:
        pass


def telegram(text: str) -> None:
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        return
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        urllib.request.urlopen(url, data=data, timeout=10).close()
    except Exception as exc:  # une alerte ratée ne doit pas fausser la sonde
        log(f"alerte Telegram non envoyée ({type(exc).__name__})")


def kill_service(sig: int) -> None:
    if sig == signal.SIGTERM:
        os.kill(1, sig)  # dumb-init transmet au service puis se termine
        return
    # Le processus 1 ignore SIGKILL venu de son propre conteneur : on vise les autres
    for p in Path("/proc").iterdir():
        if p.name.isdigit() and int(p.name) not in (1, os.getpid()):
            try:
                os.kill(int(p.name), sig)
            except OSError:
                pass


def main(url: str, service: str) -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f"{service}.json"
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError):
        state = {}
    life = pid1_start_ticks()
    if state.get("vie") != life:  # conteneur (re)démarré : nouveau décompte
        state.update(vie=life, failures=0)
    failures = state["failures"]

    if healthy(url):
        if state.get("redemarre_le"):
            telegram(
                f"♻️ JobMarket MA : {service} figé, redémarré automatiquement "
                f"({state['redemarre_le']}), de nouveau sain."
            )
            log("service de nouveau sain après redémarrage automatique")
        path.write_text(json.dumps({"vie": life, "failures": 0}))
        return 0

    if pid1_uptime() < GRACE_S:
        return 1  # démarrage en cours : pas compté
    failures += 1
    state["failures"] = failures
    if failures >= MAX_FAILURES:
        sig = signal.SIGTERM if failures == MAX_FAILURES else signal.SIGKILL
        now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        state.setdefault("redemarre_le", now)
        log(f"{failures} échecs d'affilée : arrêt ({signal.Signals(sig).name})")
        path.write_text(json.dumps(state))
        kill_service(sig)
        return 1
    path.write_text(json.dumps(state))
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
