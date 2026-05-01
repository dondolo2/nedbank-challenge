from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    current_date,
    floor,
    lit,
    months_between,
    row_number,
    when,
)

from pipeline.common import get_spark_session, load_pipeline_config, write_delta


def run_provisioning():
    config = load_pipeline_config()
    spark = get_spark_session(config, "provision")

    silver_root = config["output"]["silver_path"]
    gold_root = config["output"]["gold_path"]

    accounts = spark.read.format("delta").load(f"{silver_root}/accounts")
    customers = spark.read.format("delta").load(f"{silver_root}/customers")
    transactions = spark.read.format("delta").load(f"{silver_root}/transactions")

    cust_window = Window.orderBy(col("customer_id"))
    customer_age = floor(months_between(current_date(), col("dob")) / lit(12))
    dim_customers = (
        customers.filter(col("customer_id").isNotNull())
        .withColumn("customer_sk", row_number().over(cust_window).cast("bigint"))
        .withColumn(
            "age_band",
            when(customer_age >= 65, lit("65+"))
            .when(customer_age >= 56, lit("56-65"))
            .when(customer_age >= 46, lit("46-55"))
            .when(customer_age >= 36, lit("36-45"))
            .when(customer_age >= 26, lit("26-35"))
            .when(customer_age >= 18, lit("18-25"))
            .otherwise(lit("18-25")),
        )
        .select(
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        )
    )

    valid_customer_ids = dim_customers.select("customer_id").distinct()
    account_window = Window.orderBy(col("account_id"))
    dim_accounts = (
        accounts.filter(col("account_id").isNotNull())
        .join(valid_customer_ids, accounts.customer_ref == valid_customer_ids.customer_id, "inner")
        .withColumn("account_sk", row_number().over(account_window).cast("bigint"))
        .select(
            "account_sk",
            "account_id",
            col("customer_ref").alias("customer_id"),
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        )
    )

    fact_window = Window.orderBy(col("transaction_id"))
    fact_transactions = (
        transactions.join(dim_accounts.select("account_id", "account_sk", "customer_id"), on="account_id", how="inner")
        .join(dim_customers.select("customer_id", "customer_sk"), on="customer_id", how="inner")
        .filter(col("transaction_id").isNotNull())
        .withColumn("transaction_sk", row_number().over(fact_window).cast("bigint"))
        .select(
            "transaction_sk",
            "transaction_id",
            "account_sk",
            "customer_sk",
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "amount",
            "currency",
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
        )
    )

    write_delta(dim_customers, f"{gold_root}/dim_customers")
    write_delta(dim_accounts, f"{gold_root}/dim_accounts")
    write_delta(fact_transactions, f"{gold_root}/fact_transactions")

    spark.stop()
