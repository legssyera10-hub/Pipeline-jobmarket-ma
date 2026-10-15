"""Règles qualité ligne à ligne : quarantaine (bloquant) et anomalies (signalement).

Deux niveaux, parce qu'une offre au salaire aberrant reste une offre exploitable
(titre, technologies, ville sont justes) alors qu'une offre sans date ne l'est pas :

- BLOQUANT    -> la ligne part dans silver/rejets avec TOUS ses motifs ;
- ANOMALIE    -> la ligne reste dans silver/offres, le motif est listé dans
                 `_anomalies` ; chaque usage décide (ex. le modèle salarial les exclut).

Corrections par rapport au plan :
- motifs cumulés dans un tableau, pas un `when` en cascade qui masque les suivants ;
- une condition NULL ne fait jamais disparaître une ligne : `filter(cond)` et
  `filter(~cond)` écartent TOUS DEUX les lignes où cond est NULL ; ici le test porte
  sur la taille d'un tableau, jamais NULL ;
- « date future » mesurée par rapport à la date de COLLECTE, pas à current_date() :
  le résultat ne dépend pas du jour d'exécution (idempotence).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Bornes plausibles d'un salaire annualisé (USD). Mesuré sur Silver : 442 salaires
# "YEARLY" < 10 000 (taux horaires mal étiquetés) et 49 > 1 M (montant annuel saisi
# comme horaire puis x 2080, ex. 41 M$ pour un stage infirmier).
SALAIRE_MIN_PLAUSIBLE = 10_000
SALAIRE_MAX_PLAUSIBLE = 1_000_000

# Fonctions plutôt que constantes : F.col() exige une SparkContext active, un dict
# construit à l'import ferait échouer `import src.quality.rules` avant Spark.


def blocking_rules() -> dict[str, Column]:
    col = F.col
    return {
        "offre_id_manquant": col("offre_id").isNull(),
        "titre_manquant": col("titre_normalise").isNull(),
        "date_publication_manquante": col("date_publication").isNull(),
        "date_collecte_manquante": col("date_collecte").isNull(),
        "publication_apres_collecte": col("date_publication") > col("date_collecte"),
    }


def anomaly_rules() -> dict[str, Column]:
    col = F.col
    return {
        "salaire_min_superieur_max": col("salaire_min_annuel")
        > col("salaire_max_annuel"),
        "salaire_hors_bornes": (col("salaire_min_annuel") < SALAIRE_MIN_PLAUSIBLE)
        | (col("salaire_max_annuel") > SALAIRE_MAX_PLAUSIBLE),
        "salaire_devise_non_usd": col("salaire_min_annuel").isNotNull()
        & (F.coalesce(col("devise"), F.lit("")) != "USD"),
        "expiration_avant_publication": col("date_expiration")
        < col("date_publication"),
        "experience_min_superieure_max": col("experience_min_annees")
        > col("experience_max_annees"),
    }


def _motifs(rules: dict[str, Column]) -> Column:
    # when(NULL) -> NULL, retiré par array_compact : le tableau n'est jamais NULL
    return F.array_compact(F.array(*[F.when(c, F.lit(n)) for n, c in rules.items()]))


def apply_quality_rules(df: DataFrame) -> DataFrame:
    return df.withColumn("_motifs_rejet", _motifs(blocking_rules())).withColumn(
        "_anomalies", _motifs(anomaly_rules())
    )


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valides, rejets). Chaque ligne d'entrée est dans exactement une des deux."""
    rejected = F.size("_motifs_rejet") > 0
    return df.where(~rejected).drop("_motifs_rejet"), df.where(rejected)
