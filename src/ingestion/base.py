"""Interface commune à toutes les sources de collecte."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class Collector(ABC):
    """Une source d'offres. Ne transforme rien : c'est le rôle de la couche Silver."""

    name: str

    @abstractmethod
    def fetch(self, run_date: date) -> pd.DataFrame:
        """Offres brutes de la journée `run_date`, colonnes en texte, sans nettoyage."""

    def available_dates(self) -> list[date]:
        """Dates pour lesquelles la source a des offres (utile pour un backfill)."""
        raise NotImplementedError
