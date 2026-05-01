from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    row_number,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.common import (
    get_spark_session,
    load_dq_rules,
    load_pipeline_config,
    write_delta,
)


def dedupe_on_key(df, key_col: str):
    window = Window.partitionBy(key_col).orderBy(col("ingestion_timestamp").desc())
    return (
        df.filter(col(key_col).isNotNull())
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def run_transformation():
    config = load_pipeline_config()
    _dq_rules = load_dq_rules()  # Loaded by design requirement, even if empty.
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
        to_date(col("open_date"), "yyyy-MM-dd").alias("open_date"),
        upper(trim(col("product_tier"))).alias("product_tier"),
        upper(trim(col("digital_channel"))).alias("digital_channel"),
        col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
        col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
        to_date(col("last_activity_date"), "yyyy-MM-dd").alias("last_activity_date"),
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
        to_date(col("dob"), "yyyy-MM-dd").alias("dob"),
        col("ingestion_timestamp"),
    )

    account_ids = accounts.select("account_id").distinct()

    transactions = dedupe_on_key(transactions, "transaction_id").select(
        col("transaction_id").cast("string"),
        col("account_id").cast("string"),
        col("transaction_date").cast("string").alias("transaction_date_raw"),
        col("transaction_time").cast("string").alias("transaction_time_raw"),
        upper(trim(col("transaction_type"))).alias("transaction_type"),
        col("merchant_category").cast("string"),
        col("amount").cast("string").alias("amount_raw"),
        upper(trim(col("currency"))).alias("currency_raw"),
        upper(trim(col("channel"))).alias("channel"),
        col("location.province").cast("string").alias("province"),
        col("ingestion_timestamp"),
    )

    transactions = (
        transactions.withColumn("transaction_date", to_date(col("transaction_date_raw"), "yyyy-MM-dd"))
        .withColumn(
            "transaction_timestamp",
            to_timestamp(concat_ws(" ", col("transaction_date_raw"), col("transaction_time_raw")), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("amount", col("amount_raw").cast(DecimalType(18, 2)))
        .withColumn("currency", lit("ZAR"))
        .drop("transaction_date_raw", "transaction_time_raw", "amount_raw")
    )

    transactions = transactions.join(account_ids.withColumn("known_account", lit(1)), on="account_id", how="left")

    transactions = transactions.withColumn(
        "dq_flag",
        when(
            col("transaction_id").isNull()
            | col("account_id").isNull()
            | col("transaction_type").isNull()
            | col("channel").isNull(),
            lit("NULL_REQUIRED"),
        )
        .when(col("transaction_date").isNull() | col("transaction_timestamp").isNull(), lit("DATE_FORMAT"))
        .when(col("amount").isNull(), lit("TYPE_MISMATCH"))
        .when(col("currency_raw").isNotNull() & (upper(trim(col("currency_raw"))) != lit("ZAR")), lit("CURRENCY_VARIANT"))
        .when(col("known_account").isNull(), lit("ORPHANED_ACCOUNT"))
        .otherwise(lit(None)),
    ).drop("known_account", "currency_raw")

    write_delta(accounts, f"{silver_root}/accounts")
    write_delta(customers, f"{silver_root}/customers")
    write_delta(transactions, f"{silver_root}/transactions")

    spark.stop()
