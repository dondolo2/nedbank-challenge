from pyspark.sql.functions import lit

from pipeline.common import get_spark_session, load_pipeline_config, write_delta


def run_ingestion():
    config = load_pipeline_config()
    spark = get_spark_session(config, "ingest")

    input_cfg = config["input"]
    bronze_root = config["output"]["bronze_path"]
    run_ts = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

    accounts = spark.read.option("header", True).csv(input_cfg["accounts_path"])
    customers = spark.read.option("header", True).csv(input_cfg["customers_path"])
    transactions = spark.read.json(input_cfg["transactions_path"])

    accounts_bronze = accounts.withColumn("ingestion_timestamp", lit(run_ts))
    customers_bronze = customers.withColumn("ingestion_timestamp", lit(run_ts))
    transactions_bronze = transactions.withColumn("ingestion_timestamp", lit(run_ts))

    write_delta(accounts_bronze, f"{bronze_root}/accounts")
    write_delta(customers_bronze, f"{bronze_root}/customers")
    write_delta(transactions_bronze, f"{bronze_root}/transactions")

    spark.stop()
