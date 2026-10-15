import pytest

from src.transform.maroc import find_city, is_morocco, region_of


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Casablanca, Morocco", ("Casablanca", "Casablanca-Settat")),
        ("Capital Tower, Boulevard Mly Youssef, Casablanca, Maroc",
         ("Casablanca", "Casablanca-Settat")),
        ("Tangier", ("Tanger", "Tanger-Tétouan-Al Hoceïma")),
        ("Fez, Morocco", ("Fès", "Fès-Meknès")),
        ("Marrakesh", ("Marrakech", "Marrakech-Safi")),
        ("Kénitra", ("Kénitra", "Rabat-Salé-Kénitra")),
        ("الرباط", ("Rabat", "Rabat-Salé-Kénitra")),
        ("Rabat ou Casablanca", ("Rabat", "Rabat-Salé-Kénitra")),  # la 1re citée
        ("Casablanca, Kenitra or Agadir", ("Casablanca", "Casablanca-Settat")),
        ("Remote - Morocco", None),
        ("Rabato, Malta", None),  # mot entier seulement
        (None, None),
    ],
)  # fmt: skip
def test_find_city(raw, expected):
    assert find_city(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Remote - Morocco", True), ("Maroc", True), ("المغرب", True),
        ("Agadir", True), ("Algiers, Algeria", False), ("Paris, France", False),
        ("", False),
    ],
)  # fmt: skip
def test_is_morocco(raw, expected):
    assert is_morocco(raw) is expected


def test_region_of():
    assert region_of("Agadir") == "Souss-Massa"
    assert region_of("Inconnue") is None
