"""Vérifie le comportement Spark sur lequel repose l'idempotence du pipeline."""

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark


def test_dynamic_overwrite_replaces_only_target_partition(spark, tmp_path):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    path = str(tmp_path / "table")

    jour1 = spark.createDataFrame(
        [("a", "2026-01-01"), ("b", "2026-01-01")], ["id", "date"]
    )
    jour2 = spark.createDataFrame([("c", "2026-01-02")], ["id", "date"])
    jour1.write.mode("overwrite").partitionBy("date").parquet(path)
    jour2.write.mode("overwrite").partitionBy("date").parquet(path)

    # On rejoue le jour 1 : il est remplacé, le jour 2 reste intact
    jour1.write.mode("overwrite").partitionBy("date").parquet(path)

    counts = {
        r["date"].isoformat(): r["count"]
        for r in spark.read.parquet(path).groupBy("date").count().collect()
    }
    assert counts == {"2026-01-01": 2, "2026-01-02": 1}


def test_partition_pruning_reads_only_requested_partition(spark, tmp_path):
    path = str(tmp_path / "pruned")
    df = spark.createDataFrame(
        [("a", "2026-01-01"), ("b", "2026-01-02")], ["id", "date"]
    )
    df.write.mode("overwrite").partitionBy("date").parquet(path)

    filtered = spark.read.parquet(path).filter(F.col("date") == "2026-01-02")
    plan = filtered._jdf.queryExecution().executedPlan().toString()

    assert "PartitionFilters" in plan and "2026-01-02" in plan
    assert filtered.count() == 1
