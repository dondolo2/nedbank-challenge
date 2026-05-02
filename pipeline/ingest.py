import logging
import os

from pyspark.sql.functions import lit, monotonically_increasing_id

from pipeline.common import ensure_output_dirs, get_spark_session, load_pipeline_config, write_delta

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _require_file(path: str) -> None:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Required input file not found: {path}")


def run_ingestion():
    config = load_pipeline_config()
    ensure_output_dirs(config)
    spark = get_spark_session(config, "ingest")

    input_cfg = config["input"]
    bronze_root = config["output"]["bronze_path"]
    run_ts = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

    _require_file(input_cfg["accounts_path"])
    _require_file(input_cfg["customers_path"])
    _require_file(input_cfg["transactions_path"])

    accounts = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(input_cfg["accounts_path"])
        .withColumn("ingestion_timestamp", lit(run_ts))
        .withColumn("_bronze_row_id", monotonically_increasing_id())
    )
    customers = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(input_cfg["customers_path"])
        .withColumn("ingestion_timestamp", lit(run_ts))
        .withColumn("_bronze_row_id", monotonically_increasing_id())
    )
    transactions = (
        spark.read.option("mergeSchema", True)
        .json(input_cfg["transactions_path"])
        .withColumn("ingestion_timestamp", lit(run_ts))
        .withColumn("_bronze_row_id", monotonically_increasing_id())
    )

    write_delta(accounts, f"{bronze_root}/accounts")
    write_delta(customers, f"{bronze_root}/customers")
    write_delta(transactions, f"{bronze_root}/transactions")

    logger.info("Bronze ingest complete: accounts, customers, transactions")
    spark.stop()
