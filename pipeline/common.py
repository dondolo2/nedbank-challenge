import glob
import logging
import os
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_pipeline_config() -> Dict[str, Any]:
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "pipeline_config.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Pipeline config not found: {config_path}")
    return load_yaml(config_path)


def load_dq_rules() -> Dict[str, Any]:
    configured_path = os.environ.get("DQ_RULES_CONFIG", "/data/config/dq_rules.yaml")
    if not os.path.exists(configured_path):
        configured_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "dq_rules.yaml")
    if not os.path.exists(configured_path):
        logger.warning("DQ rules file not found; using empty rules")
        return {}
    return load_yaml(configured_path) or {}


def ensure_output_dirs(config: Dict[str, Any]) -> None:
    out = config.get("output", {})
    for key in ("bronze_path", "silver_path", "gold_path"):
        path = out.get(key)
        if path:
            os.makedirs(path, exist_ok=True)


def get_spark_session(config: Dict[str, Any], app_suffix: str) -> SparkSession:
    spark_cfg = config.get("spark", {})
    master = spark_cfg.get("master", "local[2]")
    app_name = f"{spark_cfg.get('app_name', 'nedbank-de-pipeline')}-{app_suffix}"

    extra_jars = os.environ.get("SPARK_EXTRA_JARS", "").strip()
    if extra_jars:
        jar_list = [j.strip() for j in extra_jars.split(",") if j.strip()]
    else:
        jar_list = sorted(glob.glob("/app/spark-jars/*.jar"))

    builder = SparkSession.builder.master(master).appName(app_name)
    if jar_list:
        builder = builder.config("spark.jars", ",".join(jar_list))

    builder = (
        builder.config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Avoid Snappy native libs in /tmp (often blocked under hardened docker --tmpfs).
        .config("spark.sql.parquet.compression.codec", "gzip")
    )

    # Do not use configure_spark_with_delta_pip — it hits Ivy/Maven and fails under --network=none.
    return builder.getOrCreate()


def write_delta(df: DataFrame, path: str) -> None:
    os.makedirs(path, exist_ok=True)
    df.write.format("delta").mode("overwrite").save(path)
