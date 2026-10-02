"""SparkSession factory and table I/O that hides the Delta-vs-Parquet difference."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from sentinel.common.config import Settings

# Resolved by spark-submit/ivy inside the Spark image; kept here so local runs match.
DELTA_PACKAGES = [
    "io.delta:delta-spark_2.12:3.2.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def build_spark(
    app_name: str, settings: Settings, *, packages: bool = False, master: str | None = None
) -> SparkSession:
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    b = (
        b.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    )
    if packages:
        b = b.config("spark.jars.packages", ",".join(DELTA_PACKAGES))
    if settings.table_format == "delta":
        b = (
            b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        )
    if settings.lake_root.startswith("s3"):
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.s3_endpoint.startswith("https")).lower(),
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return b.getOrCreate()


def write_batch(
    df: DataFrame,
    path: str,
    settings: Settings,
    *,
    mode: str = "append",
    partition_by: list[str] | None = None,
) -> None:
    w = df.write.format(settings.table_format).mode(mode)
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)


def read_table(spark: SparkSession, path: str, settings: Settings) -> DataFrame:
    return spark.read.format(settings.table_format).load(path)
