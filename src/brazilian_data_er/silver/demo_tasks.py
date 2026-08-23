"""Dummy Silver tasks used to test the AutoHeal pipeline."""

from __future__ import annotations

import argparse
import os
from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, to_timestamp, trim

from brazilian_data_er.utils.self_healing import monitor_task

CUSTOMERS = [
    ("c-001", "Alice Silva", "Sao Paulo"),
    ("c-002", "Bruno Costa", "Rio de Janeiro"),
]
ORDERS = [
    ("o-001", "c-001", "2024-01-02 10:00:00", "delivered", 149.90),
    ("o-002", "c-002", "2024-01-03 11:30:00", "shipped", 89.50),
]


def _output_path(task_name: str) -> str:
    """Return the temporary output path used by the demo."""
    root = os.getenv(
        "DEMO_OUTPUT_PATH",
        "/Volumes/brazilian-e-commerce/bronze/raw_data/demo/silver",
    )
    return os.path.join(root, task_name)


def _write_demo_output(dataframe: DataFrame, task_name: str) -> DataFrame:
    """Write one demo Silver DataFrame to Parquet."""
    dataframe.write.mode("overwrite").parquet(_output_path(task_name))
    return dataframe


@monitor_task("silver_demo_customers")
def process_demo_customers(spark: SparkSession) -> DataFrame:
    """Clean dummy customer records."""
    dataframe = spark.createDataFrame(CUSTOMERS, ["customer_id", "customer_name", "city"])
    cleaned = dataframe.select(
        "customer_id",
        lower(trim(col("customer_name"))).alias("customer_name"),
        lower(trim(col("city"))).alias("city"),
    )
    return _write_demo_output(cleaned, "customers")


@monitor_task("silver_demo_orders")
def process_demo_orders(spark: SparkSession) -> DataFrame:
    """Cast dummy order records."""
    dataframe = spark.createDataFrame(
        ORDERS,
        ["order_id", "customer_id", "order_created_at", "order_status", "order_total"],
    )
    cleaned = dataframe.select(
        "order_id",
        "customer_id",
        to_timestamp("order_created_at").alias("order_created_at"),
        lower(trim(col("order_status"))).alias("order_status"),
        col("order_total").cast("double").alias("order_total"),
    )
    return _write_demo_output(cleaned, "orders")


@monitor_task("silver_demo_quality_check")
def process_demo_quality_check(spark: SparkSession) -> DataFrame:
    """Raise a deterministic missing-column Spark schema failure."""
    orders = spark.read.parquet(_output_path("orders"))
    invalid_projection = orders.select(
        "order_id", "customer_id", "missing_business_column"
    )
    invalid_projection.limit(1).count()
    return invalid_projection


TASKS: Dict[str, Callable[[SparkSession], DataFrame]] = {
    "customers": process_demo_customers,
    "orders": process_demo_orders,
    "quality-check": process_demo_quality_check,
}


def _run_task(task_name: str) -> None:
    """Create Spark and execute one named task."""
    spark = SparkSession.builder.appName("BrazilianDataERDemoSilver").getOrCreate()
    try:
        TASKS[task_name](spark).count()
    finally:
        spark.stop()


def customers_main() -> None:
    """Run the dummy customers task."""
    _run_task("customers")


def orders_main() -> None:
    """Run the dummy orders task."""
    _run_task("orders")


def quality_check_main() -> None:
    """Run the intentionally failing quality-check task."""
    _run_task("quality-check")


def run_demo_pipeline() -> None:
    """Run both successful tasks followed by the intentional failure."""
    spark = SparkSession.builder.appName("BrazilianDataERDemoSilver").getOrCreate()
    try:
        process_demo_customers(spark).count()
        process_demo_orders(spark).count()
        process_demo_quality_check(spark).count()
    finally:
        spark.stop()


def main() -> None:
    """Run one task or the complete demo using ``--task``."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", choices=[*sorted(TASKS), "all"], required=True)
    args = parser.parse_args()
    run_demo_pipeline() if args.task == "all" else _run_task(args.task)


if __name__ == "__main__":
    main()
