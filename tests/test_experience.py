"""parse_experience : cas réels du dataset (EN) + cas du plan (FR) + pièges."""

import pytest

from src.transform.experience import find_mentions, parse_experience


@pytest.mark.parametrize(
    "text, expected",
    [
        # --- anglais : formes courantes
        ("3+ years of experience in data engineering", (3, None)),
        ("5 years experience with Python", (5, None)),
        ("3-5 years of experience required", (3, 5)),
        ("3 to 5 years of relevant experience", (3, 5)),
        ("Minimum of 2 years of experience", (2, None)),
        ("At least 4 yrs. experience", (4, None)),
        ("Experience: 7+ years", (7, None)),
        ("8-10+ years PR/related experience", (8, 10)),
        ("2–4 years' experience", (2, 4)),  # tiret demi-cadratin + apostrophe
        ("five (5) years of experience", (5, None)),
        ("Three years of professional experience", (3, None)),
        ("0-2 years of experience, new grads welcome", (0, 2)),
        # --- texte collé (HTML retiré sans espace) : vu dans les données
        ("degree in computer science1+ years experience with SDLC", (1, None)),
        # --- plusieurs mentions : la plus faible (alternatives "OR")
        (
            "BS and 14 years of related experience; OR Master's and 12 years "
            "of related experience",
            (12, None),
        ),
        # --- français (cas du plan)
        ("3 à 5 ans d'expérience", (3, 5)),
        ("Expérience : minimum 2 ans", (2, None)),
        ("au moins 4 ans d’expérience en data", (4, None)),  # apostrophe typographique
        ("10 ans d'expérience exigés", (10, None)),
        # --- pièges : ne PAS extraire
        ("Must be at least 18 years of age", (None, None)),
        ("Candidates must be 21 years old", (None, None)),
        ("a family-owned business with 40+ years of experience", (None, None)),
        ("We have served clients for 25 years", (None, None)),
        ("2 years commitment to the program", (None, None)),
        (
            "Great benefits. 5 years ago we started. Experience the difference",
            (None, None),
        ),
        ("Must have answers within 2 answers", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_parse_experience(text, expected):
    assert parse_experience(text) == expected


def test_inconsistent_range_is_ignored():
    assert find_mentions("5-3 years of experience") == []


def test_all_valid_mentions_are_listed():
    text = "3+ years of SQL experience and 5 years of Python experience"
    assert find_mentions(text) == [(3, None), (5, None)]
