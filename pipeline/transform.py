import logging
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import coalesce, col, concat_ws, from_unixtime, lit, row_number, to_date, to_timestamp, trim, upper, when
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.common import ensure_output_dirs, get_spark_session, load_dq_rules, load_pipeline_config, write_delta

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def dedupe_on_key(df: DataFrame, key_col: str) -> DataFrame:
    """Deterministic: last row wins by Bronze ingestion order (_bronze_row_id)."""
    window = Window.partitionBy(key_col).orderBy(
        col("ingestion_timestamp").desc(),
        col("_bronze_row_id").desc(),
    )
    return (
        df.filter(col(key_col).isNotNull())
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def _dq_flag_expr(dq_rules: dict):
    """Build dq_flag column from config-driven rules (see config/dq_rules.yaml)."""
    null_txn: List[str] = dq_rules.get("null_checks", {}).get(
        "transactions",
        ["transaction_id", "account_id", "transaction_type", "channel"],
    )
    domain = dq_rules.get("domain_checks", {})
    currency_cfg = dq_rules.get("currency_normalisation", {})
    target_ccy = currency_cfg.get("target_value", "ZAR")
    flag_variants = currency_cfg.get("flag_variants", True)

    types_allowed = domain.get("transaction_type", {}).get("allowed")
    if not types_allowed:
        types_allowed = ["DEBIT", "CREDIT", "FEE", "REVERSAL"]
    channels_allowed = domain.get("channel", {}).get("allowed")
    if not channels_allowed:
        channels_allowed = ["POS", "APP", "ATM", "EFT", "USSD", "INTERNAL"]

    null_cond = None
    for name in null_txn:
        c = col(name).isNull()
        null_cond = c if null_cond is None else (null_cond | c)

    domain_violation = (col("transaction_type").isNotNull() & ~col("transaction_type").isin(types_allowed)) | (
        col("channel").isNotNull() & ~col("channel").isin(channels_allowed)
    )

    ccy_bad = lit(False)
    if flag_variants:
        ccy_bad = col("currency_raw").isNotNull() & (upper(trim(col("currency_raw"))) != lit(target_ccy))

    return (
        when(null_cond if null_cond is not None else lit(False), lit("NULL_REQUIRED"))
        .when(col("transaction_date").isNull() | col("transaction_timestamp").isNull(), lit("DATE_FORMAT"))
        .when(col("amount").isNull(), lit("TYPE_MISMATCH"))
        .when(domain_violation, lit("TYPE_MISMATCH"))
        .when(ccy_bad, lit("CURRENCY_VARIANT"))
        .when(col("known_account").isNull(), lit("ORPHANED_ACCOUNT"))
        .otherwise(lit(None))
    )


def parse_flexible_date(col_expr):
    return coalesce(
        to_date(col_expr, "yyyy-MM-dd"),
        to_date(col_expr, "dd/MM/yyyy"),
        to_date(from_unixtime(col_expr.cast("long")), "yyyy-MM-dd"),
    )


def run_transformation():
    config = load_pipeline_config()
    dq_rules = load_dq_rules()
    ensure_output_dirs(config)
    spark = get_spark_session(config, "transform")

    bronze_root = config["output"]["bronze_path"]
    silver_root = config["output"]["silver_path"]

    accounts = spark.read.format("delta").load(f"{bronze_root}/accounts")
    customers = spark.read.format("delta").load(f"{bronze_root}/customers")
    transactions = spark.read.format("delta").load(f"{bronze_root}/transactions")

    accounts = dedupe_on_key(accounts, "account_id").select(
        col("account_id").cast("string"),
        col("customer_ref").cast("string"),
        upper(trim(col("account_type"))).alias("account_type"),
        upper(trim(col("account_status"))).alias("account_status"),
        parse_flexible_date(col("open_date")).alias("open_date"),
        upper(trim(col("product_tier"))).alias("product_tier"),
        upper(trim(col("digital_channel"))).alias("digital_channel"),
        col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
        col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
        parse_flexible_date(col("last_activity_date")).alias("last_activity_date"),
        col("ingestion_timestamp"),
    )

    customers = dedupe_on_key(customers, "customer_id").select(
        col("customer_id").cast("string"),
        upper(trim(col("gender"))).alias("gender"),
        trim(col("province")).alias("province"),
        upper(trim(col("income_band"))).alias("income_band"),
        upper(trim(col("segment"))).alias("segment"),
        col("risk_score").cast(IntegerType()).alias("risk_score"),
        upper(trim(col("kyc_status"))).alias("kyc_status"),
        parse_flexible_date(col("dob")).alias("dob"),
        col("ingestion_timestamp"),
    )

    account_ids = accounts.select("account_id").distinct()

    merchant_subcategory_col = col("merchant_subcategory") if "merchant_subcategory" in transactions.columns else lit(None)

    transactions = dedupe_on_key(transactions, "transaction_id").select(
        col("transaction_id").cast("string"),
        col("account_id").cast("string"),
        col("transaction_date").cast("string").alias("transaction_date_raw"),
        col("transaction_time").cast("string").alias("transaction_time_raw"),
        upper(trim(col("transaction_type"))).alias("transaction_type"),
        col("merchant_category").cast("string"),
        when(merchant_subcategory_col.isNotNull(), merchant_subcategory_col.cast("string")).otherwise(lit(None)).alias("merchant_subcategory"),
        col("amount").cast("string").alias("amount_raw"),
        upper(trim(col("currency"))).alias("currency_raw"),
        upper(trim(col("channel"))).alias("channel"),
        col("location.province").cast("string").alias("province"),
        col("ingestion_timestamp"),
    )

    transactions = (
        transactions.withColumn("transaction_date", parse_flexible_date(col("transaction_date_raw")))
        .withColumn(
            "transaction_timestamp",
            to_timestamp(
                concat_ws(" ", col("transaction_date"), col("transaction_time_raw")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("amount", col("amount_raw").cast(DecimalType(18, 2)))
        .withColumn("currency", lit("ZAR"))
        .drop("transaction_date_raw", "transaction_time_raw", "amount_raw")
    )

    transactions = transactions.join(account_ids.withColumn("known_account", lit(1)), on="account_id", how="left")

    transactions = (
        transactions.withColumn("dq_flag", _dq_flag_expr(dq_rules))
        .drop("known_account", "currency_raw")
    )

    write_delta(accounts, f"{silver_root}/accounts")
    write_delta(customers, f"{silver_root}/customers")
    write_delta(transactions, f"{silver_root}/transactions")

    logger.info("Silver transform complete")
    spark.stop()
