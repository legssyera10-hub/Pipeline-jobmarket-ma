import pandas as pd
import pytest

from scripts.annotation_technologies import score_frame


def test_score_counts_true_and_false_positives():
    df = pd.DataFrame(
        {
            "technologies_extraites": ["python;sql;excel", "java", ""],
            "technologies_reelles": ["python;sql", "java;spring", ""],
        }
    )
    r = score_frame(df)
    # VP = python, sql, java (3) ; FP = excel (1) ; FN = spring (1)
    assert r["precision"] == pytest.approx(3 / 4)
    assert r["rappel"] == pytest.approx(3 / 4)
    assert r["faux_positifs"] == [("excel", 1)]
    assert r["faux_negatifs"] == [("spring", 1)]
