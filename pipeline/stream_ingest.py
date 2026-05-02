import glob
import os
import time
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, row_number, sum as spark_sum, max as spark_max, to_timestamp, trim, upper, when
from pyspark.sql.types import DecimalType

from pipeline.common import get_spark_session, load_pipeline_config
from pipeline.transform import parse_flexible_date


def _table_exists(path: str) -> bool:
    return os.path.isdir(path) and os.path.exists(os.path.join(path, "_delta_log"))


def _stream_files(stream_dir: str) -> List[str]:
    return sorted(glob.glob(os.path.join(stream_dir, "*.jsonl")))


def _create_or_merge_current_balances(spark: SparkSession, path: str, events_df):
    balances_df = (
        events_df.groupBy("account_id")
        .agg(
            spark_sum("signed_amount").cast(DecimalType(18, 2)).alias("signed_amount"),
            spark_max(col("transaction_timestamp")).alias("transaction_timestamp"),
            spark_max(col("updated_at")).alias("updated_at"),
        )
    )

    if not _table_exists(path):
        balances_df.select(
            col("account_id"),
            col("signed_amount").alias("current_balance"),
            col("transaction_timestamp").alias("last_transaction_timestamp"),
            col("updated_at"),
        ).write.format("delta").mode("overwrite").save(path)
        return

    delta_table = DeltaTable.forPath(spark, path)
    delta_table.alias("t").merge(
        balances_df.alias("s"),
        "t.account_id = s.account_id",
    ).whenMatchedUpdate(
        set={
            "current_balance": col("t.current_balance") + col("s.signed_amount"),
            "last_transaction_timestamp": when(
                col("s.transaction_timestamp") > col("t.last_transaction_timestamp"),
                col("s.transaction_timestamp"),
            ).otherwise(col("t.last_transaction_timestamp")),
            "updated_at": col("s.updated_at"),
        }
    ).whenNotMatchedInsert(
        values={
            "account_id": col("s.account_id"),
            "current_balance": col("s.signed_amount"),
            "last_transaction_timestamp": col("s.transaction_timestamp"),
            "updated_at": col("s.updated_at"),
        }
    ).execute()


def _create_or_merge_recent_transactions(spark: SparkSession, path: str, events_df):
    if not _table_exists(path):
        events_df.select(
            "account_id",
            "transaction_id",
            "transaction_timestamp",
            "amount",
            "transaction_type",
            "channel",
            "updated_at",
        ).write.format("delta").mode("overwrite").save(path)
        return

    delta_table = DeltaTable.forPath(spark, path)
    delta_table.alias("t").merge(
        events_df.alias("s"),
        "t.account_id = s.account_id AND t.transaction_id = s.transaction_id",
    ).whenNotMatchedInsert(
        values={
            "account_id": col("s.account_id"),
            "transaction_id": col("s.transaction_id"),
            "transaction_timestamp": col("s.transaction_timestamp"),
            "amount": col("s.amount"),
            "transaction_type": col("s.transaction_type"),
            "channel": col("s.channel"),
            "updated_at": col("s.updated_at"),
        }
    ).execute()

    recent = spark.read.format("delta").load(path)
    window = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").desc())
    rows_to_delete = (
        recent.withColumn("rn", row_number().over(window))
        .filter(col("rn") > 50)
        .select("account_id", "transaction_id")
        .collect()
    )

    if rows_to_delete:
        conditions = []
        for row in rows_to_delete:
            account_id = str(row["account_id"]).replace("'", "\\'")
            transaction_id = str(row["transaction_id"]).replace("'", "\\'")
            conditions.append(
                f"(account_id = '{account_id}' AND transaction_id = '{transaction_id}')"
            )
        condition = " OR ".join(conditions)
        DeltaTable.forPath(spark, path).delete(condition)


def _prepare_stream_events(spark: SparkSession, file_path: str):
    raw = spark.read.option("mergeSchema", True).json(file_path)
    events = raw.select(
        col("account_id").cast("string"),
        col("transaction_id").cast("string"),
        col("transaction_date"),
        col("transaction_time").cast("string"),
        upper(trim(col("transaction_type"))).alias("transaction_type"),
        trim(col("channel")).alias("channel"),
        col("amount").cast("string").alias("amount_raw"),
    )

    events = (
        events.withColumn("transaction_date", parse_flexible_date(col("transaction_date")))
        .withColumn(
            "transaction_timestamp",
            to_timestamp(
                concat_ws(" ", col("transaction_date"), col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("amount", col("amount_raw").cast(DecimalType(18, 2)))
        .withColumn(
            "signed_amount",
            when(col("transaction_type") == "DEBIT", -col("amount")).otherwise(col("amount")),
        )
        .withColumn("currency", lit("ZAR"))
        .withColumn("updated_at", current_timestamp())
        .select(
            "account_id",
            "transaction_id",
            "transaction_timestamp",
            "amount",
            "transaction_type",
            "channel",
            "updated_at",
            "signed_amount",
        )
    )

    return events


def run_stream_ingestion():
    config = load_pipeline_config()
    spark = get_spark_session(config, "stream_ingest")

    stream_dir = "/data/stream"
    current_balances_path = "/data/output/stream_gold/current_balances"
    recent_transactions_path = "/data/output/stream_gold/recent_transactions"

    os.makedirs(current_balances_path, exist_ok=True)
    os.makedirs(recent_transactions_path, exist_ok=True)

    processed_files = set()
    while True:
        stream_files = [p for p in _stream_files(stream_dir) if p not in processed_files]
        if not stream_files:
            time.sleep(30)
            stream_files = [p for p in _stream_files(stream_dir) if p not in processed_files]
            if not stream_files:
                break

        for file_path in stream_files:
            events = _prepare_stream_events(spark, file_path)
            _create_or_merge_current_balances(spark, current_balances_path, events)
            _create_or_merge_recent_transactions(spark, recent_transactions_path, events)
            processed_files.add(file_path)

    spark.stop()
