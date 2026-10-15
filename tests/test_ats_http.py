"""Client HTTP poli : robots.txt, espacement des requêtes, réessais. Aucun réseau."""

import pytest
import requests

from src.ingestion.ats.http import PoliteClient, RobotsDenied, TransientHTTPError

ROBOTS = "User-agent: *\nDisallow: /prive/\nCrawl-delay: 5\n"


class FakeResponse:
    def __init__(self, status=200, text="", json_body=None, headers=None):
        self.status_code = status
        self.text = text
        self._json = json_body
        self.headers = headers or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Renvoie des réponses programmées par URL, et journalise les appels."""

    def __init__(self, routes):
        self.routes = {k: list(v) for k, v in routes.items()}
        self.headers = {}
        self.calls = []

    def get(self, url, timeout):
        self.calls.append(url)
        responses = self.routes[url]
        resp = responses.pop(0) if len(responses) > 1 else responses[0]
        if isinstance(resp, Exception):
            raise resp
        return resp


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, s):
        self.sleeps.append(s)
        self.now += s

    def __call__(self):
        return self.now


def client(routes, robots=ROBOTS):
    routes = {"https://api.test/robots.txt": [robots], **routes}
    clock = FakeClock()
    c = PoliteClient(
        "TestBot/1.0", min_interval=2.0, session=FakeSession(routes),
        sleep=clock.sleep, clock=clock,
    )  # fmt: skip
    return c, clock


def test_user_agent_is_sent():
    c, _ = client({"https://api.test/a": [FakeResponse(json_body={})]})
    assert c.session.headers["User-Agent"] == "TestBot/1.0"


def test_robots_disallow_blocks_without_calling_the_url():
    c, _ = client({})
    with pytest.raises(RobotsDenied):
        c.get_json("https://api.test/prive/x")
    assert c.session.calls == ["https://api.test/robots.txt"]


def test_crawl_delay_longer_than_min_interval_is_respected():
    c, clock = client(
        {"https://api.test/a": [FakeResponse(json_body=1)],
         "https://api.test/b": [FakeResponse(json_body=2)]}
    )  # fmt: skip
    c.get_json("https://api.test/a")
    c.get_json("https://api.test/b")
    # Dès que robots.txt est lu, son Crawl-delay (5 s) remplace min_interval (2 s) :
    # robots.txt -> a : 5 s ; a -> b : 5 s
    assert clock.sleeps == [5.0, 5.0]


def test_missing_robots_means_allowed():
    c, _ = client(
        {"https://api.test/x": [FakeResponse(json_body={"ok": 1})]},
        robots=FakeResponse(404),
    )
    assert c.get_json("https://api.test/x") == {"ok": 1}


def test_429_is_retried_honoring_retry_after():
    c, clock = client(
        {"https://api.test/a": [
            FakeResponse(429, headers={"Retry-After": "30"}),
            FakeResponse(json_body={"ok": 1}),
        ]}
    )  # fmt: skip
    assert c.get_json("https://api.test/a") == {"ok": 1}
    assert 30.0 in clock.sleeps


def test_network_errors_are_retried_then_reported():
    c, _ = client({"https://api.test/a": [requests.ConnectionError("coupé")]})
    with pytest.raises(TransientHTTPError):
        c.get_json("https://api.test/a")
    # 1 appel robots.txt + 4 tentatives
    assert c.session.calls.count("https://api.test/a") == 4


def test_404_is_not_retried():
    c, _ = client({"https://api.test/a": [FakeResponse(404)]})
    with pytest.raises(requests.HTTPError):
        c.get_json("https://api.test/a")
    assert c.session.calls.count("https://api.test/a") == 1


# Le robots.txt de robots.txt est servi par une chaîne : on l'adapte ici
@pytest.fixture(autouse=True)
def _robots_as_response(monkeypatch):
    original = FakeSession.get

    def get(self, url, timeout):
        resp = original(self, url, timeout)
        return FakeResponse(text=resp) if isinstance(resp, str) else resp

    monkeypatch.setattr(FakeSession, "get", get)
