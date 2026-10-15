"""Client HTTP « poli » : la seule porte de sortie réseau de la collecte.

Règles appliquées à chaque requête (cadre légal du plan) :
- User-Agent identifiable avec un moyen de contact ;
- robots.txt lu et respecté (Protego : gère les jokers, contrairement à
  urllib.robotparser qui a déclaré à tort Jooblebot autorisé chez ReKrute) ;
  Crawl-delay respecté s'il est plus long que notre délai minimal ;
- au moins `min_interval` secondes entre deux requêtes vers le même hôte, jamais de
  parallélisme ;
- réessais espacés (2 s, 4 s, 8 s…) sur erreurs temporaires (429, 5xx, réseau), en
  respectant l'en-tête Retry-After ; une erreur 4xx définitive n'est pas réessayée.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from urllib.parse import urlsplit

import requests
from protego import Protego

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RobotsDenied(PermissionError):
    """robots.txt interdit cette URL à notre User-Agent."""


class TransientHTTPError(RuntimeError):
    """Erreur temporaire qui a survécu à tous les réessais."""


class PoliteClient:
    def __init__(
        self,
        user_agent: str,
        min_interval: float = 2.0,
        max_attempts: int = 4,
        timeout: float = 30.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.user_agent = user_agent
        self.min_interval = min_interval
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = user_agent
        self._sleep, self._clock = sleep, clock
        self._last_call: dict[str, float] = {}
        self._robots: dict[str, Protego | None] = {}

    # --- robots.txt ---------------------------------------------------------------

    def _robots_for(self, base: str) -> Protego | None:
        """None = pas de robots.txt (404) : tout est autorisé (norme RFC 9309)."""
        if base not in self._robots:
            resp = self._request(f"{base}/robots.txt", check_robots=False)
            if resp.status_code == 200:
                self._robots[base] = Protego.parse(resp.text)
            elif 400 <= resp.status_code < 500:
                self._robots[base] = None
            else:  # 5xx après réessais : prudence, on n'avance pas à l'aveugle
                raise TransientHTTPError(f"robots.txt indisponible pour {base}")
        return self._robots[base]

    def _host_interval(self, base: str) -> float:
        rp = self._robots.get(base)
        delay = rp.crawl_delay(self.user_agent) if rp else None
        return max(self.min_interval, float(delay or 0))

    # --- requêtes -------------------------------------------------------------------

    def _wait_turn(self, base: str) -> None:
        last = self._last_call.get(base)
        if last is not None:
            remaining = self._host_interval(base) - (self._clock() - last)
            if remaining > 0:
                self._sleep(remaining)
        self._last_call[base] = self._clock()

    def _request(self, url: str, check_robots: bool = True) -> requests.Response:
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"
        if check_robots:
            rp = self._robots_for(base)
            if rp is not None and not rp.can_fetch(url, self.user_agent):
                raise RobotsDenied(f"robots.txt interdit {url}")

        backoff = 2.0
        for attempt in range(1, self.max_attempts + 1):
            self._wait_turn(base)
            try:
                resp = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == self.max_attempts:
                    raise TransientHTTPError(f"{url} : {exc}") from exc
                self._sleep(backoff)
                backoff *= 2
                continue
            if resp.status_code not in RETRYABLE_STATUS:
                return resp
            if attempt == self.max_attempts:
                raise TransientHTTPError(f"{url} : HTTP {resp.status_code}")
            retry_after = resp.headers.get("Retry-After", "")
            wait = float(retry_after) if retry_after.isdigit() else backoff
            self._sleep(min(wait, 300))
            backoff *= 2
        raise AssertionError("inatteignable")

    def get_json(self, url: str):
        resp = self._request(url)
        resp.raise_for_status()  # 4xx définitif : pas de réessai
        return resp.json()
