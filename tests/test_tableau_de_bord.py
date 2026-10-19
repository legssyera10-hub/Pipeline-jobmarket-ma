"""Générateur du tableau de bord web : injection sûre des données dans la page."""

import json

from scripts.tableau_de_bord import render


def test_donnees_injectees_sans_pouvoir_fermer_le_script():
    html = render({"sources": {"x": {"libelle": "</script><script>alert(1)"}}})
    assert "/*__DONNEES__*/null" not in html  # remplacé
    assert "</script><script>alert" not in html  # neutralisé
    payload = html.split("const DONNEES = ", 1)[1].split(";\n", 1)[0]
    assert json.loads(payload.replace("<\/", "</"))["sources"]["x"][
        "libelle"
    ].startswith("</script>")


def test_page_autonome_et_accessible():
    html = render({"sources": {}})
    assert html.lower().startswith("<!doctype html>")
    assert '<html lang="fr">' in html
    assert 'role="tablist"' in html and "Voir les données" in html
