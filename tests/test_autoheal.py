"""Contrôle de santé auto-réparateur des services Airflow (docker/airflow/autoheal.py).

Aucun signal réel ni message Telegram : tout est simulé.
"""

import importlib.util
import signal
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "autoheal", Path(__file__).parents[1] / "docker" / "airflow" / "autoheal.py"
)


@pytest.fixture
def ah(tmp_path, monkeypatch):
    mod = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(mod)
    calls = {"kills": [], "alerts": [], "healthy": False, "uptime": 1000.0, "life": 1}
    monkeypatch.setattr(mod, "STATE_DIR", tmp_path)
    monkeypatch.setattr(mod, "healthy", lambda url: calls["healthy"])
    monkeypatch.setattr(mod, "pid1_uptime", lambda: calls["uptime"])
    monkeypatch.setattr(mod, "pid1_start_ticks", lambda: calls["life"])
    monkeypatch.setattr(mod, "kill_service", calls["kills"].append)
    monkeypatch.setattr(mod, "telegram", calls["alerts"].append)
    monkeypatch.setattr(mod, "log", lambda msg: None)
    mod.calls = calls
    return mod


def check(ah, n=1):
    return [ah.main("http://x/health", "scheduler") for _ in range(n)]


def test_sain_ne_fait_rien(ah):
    ah.calls["healthy"] = True
    assert check(ah, 3) == [0, 0, 0]
    assert ah.calls["kills"] == [] and ah.calls["alerts"] == []


def test_demarrage_lent_non_compte(ah):
    ah.calls["uptime"] = 10.0  # dans la période de grâce
    assert check(ah, ah.MAX_FAILURES + 3) == [1] * (ah.MAX_FAILURES + 3)
    assert ah.calls["kills"] == []


def test_seuil_puis_sigterm_puis_sigkill(ah):
    check(ah, ah.MAX_FAILURES - 1)
    assert ah.calls["kills"] == []  # sous le seuil : patience
    check(ah)
    assert ah.calls["kills"] == [signal.SIGTERM]  # arrêt propre d'abord
    check(ah)
    assert ah.calls["kills"] == [signal.SIGTERM, signal.SIGKILL]  # toujours figé


def test_un_succes_remet_le_compteur_a_zero(ah):
    check(ah, ah.MAX_FAILURES - 1)
    ah.calls["healthy"] = True
    check(ah)
    ah.calls["healthy"] = False
    check(ah, ah.MAX_FAILURES - 1)
    assert ah.calls["kills"] == []


def test_nouvelle_vie_puis_retablissement_alerte_une_fois(ah):
    check(ah, ah.MAX_FAILURES)
    assert ah.calls["kills"] == [signal.SIGTERM]
    ah.calls["life"] = 2  # Docker a relancé le conteneur
    check(ah)  # encore en échec : nouveau décompte, pas de SIGKILL immédiat
    assert ah.calls["kills"] == [signal.SIGTERM]
    ah.calls["healthy"] = True
    check(ah, 2)
    assert len(ah.calls["alerts"]) == 1 and "redémarré" in ah.calls["alerts"][0]
