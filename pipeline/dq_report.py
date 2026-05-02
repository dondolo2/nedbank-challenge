import datetime
import json
import os
import time
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common import get_spark_session, load_dq_rules


def _load_count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def write_dq_report(spark: Optional[SparkSession], config: Dict[str, Any], pipeline_start_time: float) -> None:
    close_spark = False
    if spark is None:
        spark = get_spark_session(config, "dq_report")
        close_spark = True

    dq_rules = load_dq_rules()
    dq_handling = dq_rules.get("dq_handling", {})

    bronze_root = config["output"]["bronze_path"]
    silver_root = config["output"]["silver_path"]
    gold_root = config["output"]["gold_path"]

    accounts_raw = _load_count(spark, f"{bronze_root}/accounts")
    customers_raw = _load_count(spark, f"{bronze_root}/customers")
    transactions_raw = _load_count(spark, f"{bronze_root}/transactions")

    silver_transactions = spark.read.format("delta").load(f"{silver_root}/transactions")
    dq_rows = (
        silver_transactions.filter(col("dq_flag").isNotNull())
        .groupBy("dq_flag")
        .count()
        .collect()
    )

    gold_layer_record_counts = {
        "dim_accounts": _load_count(spark, f"{gold_root}/dim_accounts"),
        "dim_customers": _load_count(spark, f"{gold_root}/dim_customers"),
        "fact_transactions": _load_count(spark, f"{gold_root}/fact_transactions"),
    }

    dq_issues = []
    for row in dq_rows:
        issue_type = row["dq_flag"]
        records_affected = int(row["count"])
        denominator = accounts_raw if issue_type == "NULL_REQUIRED" else transactions_raw
        percentage_of_total = round((records_affected / denominator * 100) if denominator else 0.0, 2)
        handling_action = dq_handling.get(issue_type, {}).get("handling_action")
        records_in_output = 0 if issue_type in {"ORPHANED_ACCOUNT", "NULL_REQUIRED"} else records_affected

        dq_issues.append(
            {
                "issue_type": issue_type,
                "records_affected": records_affected,
                "percentage_of_total": percentage_of_total,
                "handling_action": handling_action,
                "records_in_output": records_in_output,
            }
        )

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "stage": "2",
        "source_record_counts": {
            "accounts_raw": accounts_raw,
            "customers_raw": customers_raw,
            "transactions_raw": transactions_raw,
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_layer_record_counts,
        "execution_duration_seconds": int(time.time() - pipeline_start_time),
    }

    output_path = "/data/output"
    os.makedirs(output_path, exist_ok=True)
    with open(os.path.join(output_path, "dq_report.json"), "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    if close_spark:
        spark.stop()
