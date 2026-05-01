import os
from typing import Any, Dict

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_pipeline_config() -> Dict[str, Any]:
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    if not os.path.exists(config_path):
        config_path = "/app/config/pipeline_config.yaml"
    return load_yaml(config_path)


def load_dq_rules() -> Dict[str, Any]:
    configured_path = os.environ.get("DQ_RULES_CONFIG", "/data/config/dq_rules.yaml")
    if not os.path.exists(configured_path):
        configured_path = "/app/config/dq_rules.yaml"
    if not os.path.exists(configured_path):
        return {}
    return load_yaml(configured_path) or {}


def get_spark_session(config: Dict[str, Any], app_suffix: str) -> SparkSession:
    spark_cfg = config.get("spark", {})
    master = spark_cfg.get("master", "local[2]")
    app_name = f"{spark_cfg.get('app_name', 'nedbank-de-pipeline')}-{app_suffix}"

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)
