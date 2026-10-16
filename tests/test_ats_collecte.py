"""Collecte brute : date du jour seulement, immuabilité, échec isolé par employeur."""

import gzip
import json
from datetime import date

import pytest
from botocore.exceptions import ClientError

from src.ingestion.ats.collecte import Employeur, alert_message, collect, raw_key
from src.ingestion.ats.http import RobotsDenied

TODAY = date(2026, 10, 15)
EMPLOYEURS = [
    Employeur("Acme", "greenhouse", "acme"),
    Employeur("Beta", "lever", "beta"),
]


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "HeadObject",
            )

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None):
        self.objects[Key] = Body


class FakeHttp:
    user_agent = "TestBot/1.0"

    def __init__(self, bodies):
        self.bodies = bodies
        self.calls = []

    def get_json(self, url):
        self.calls.append(url)
        body = self.bodies[url]
        if isinstance(body, Exception):
            raise body
        return body


GH = "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true"
LV = "https://api.lever.co/v0/postings/beta?mode=json"
GH_BODY = {
    "jobs": [
        {
            "id": 1,
            "title": "Dev",
            "location": {"name": "Rabat, Maroc"},
            "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/1",
        }
    ]
}


def test_past_date_is_refused(settings):
    with pytest.raises(ValueError, match="rejouer le parsing"):
        collect(date(2026, 10, 1), FakeHttp({}), settings, EMPLOYEURS, FakeS3(), TODAY)


def test_raw_response_is_stored_with_metadata(settings):
    s3 = FakeS3()
    http = FakeHttp({GH: GH_BODY, LV: []})
    results = collect(TODAY, http, settings, EMPLOYEURS, s3, TODAY)
    assert [r["statut"] for r in results] == ["written", "written"]
    assert results[0]["offres_maroc"] == 1
    env = json.loads(gzip.decompress(s3.objects[raw_key(TODAY, EMPLOYEURS[0])]))
    assert env["corps"] == GH_BODY  # réponse brute intacte
    assert env["url"] == GH and env["user_agent"] == "TestBot/1.0"


def test_rerun_makes_no_network_call(settings):
    s3 = FakeS3()
    collect(TODAY, FakeHttp({GH: GH_BODY, LV: []}), settings, EMPLOYEURS, s3, TODAY)
    http = FakeHttp({GH: GH_BODY, LV: []})
    results = collect(TODAY, http, settings, EMPLOYEURS, s3, TODAY)
    assert [r["statut"] for r in results] == ["skipped", "skipped"]
    assert http.calls == []


def test_one_failing_employer_does_not_stop_the_others(settings):
    s3 = FakeS3()
    http = FakeHttp({GH: RobotsDenied("interdit"), LV: []})
    results = collect(TODAY, http, settings, EMPLOYEURS, s3, TODAY)
    assert [r["statut"] for r in results] == ["failed", "written"]
    assert "RobotsDenied" in results[0]["erreur"]
    assert raw_key(TODAY, EMPLOYEURS[0]) not in s3.objects


def test_schema_drift_does_not_fail_collection(settings):
    # La réponse brute est sauvegardée ; c'est le parsing (Bronze) qui échouera
    s3 = FakeS3()
    drifted = [{"id": "x", "titre": "Dev", "categories": {}, "hostedUrl": "u"}]
    results = collect(TODAY, FakeHttp({GH: GH_BODY, LV: drifted}), settings,
                      EMPLOYEURS, s3, TODAY)  # fmt: skip
    assert [r["statut"] for r in results] == ["written", "written"]
    assert "'text' absent" in results[1]["avertissement"]
    assert raw_key(TODAY, EMPLOYEURS[1]) in s3.objects


def test_alert_message():
    ok = [{"statut": "written", "employeur": "A", "ats": "lever"}]
    assert alert_message(TODAY, ok, rattrapage=False) is None
    failed = ok + [{"statut": "failed", "employeur": "B", "ats": "greenhouse",
                    "erreur": "TransientHTTPError: HTTP 503"}]  # fmt: skip
    msg = alert_message(TODAY, failed, rattrapage=False)
    assert "B (greenhouse) : TransientHTTPError: HTTP 503" in msg
    assert "rattrapage de 18:00" in msg
    assert "employeurs disponibles" in alert_message(TODAY, failed, rattrapage=True)
