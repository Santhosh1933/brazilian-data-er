"""
Gold Layer: Daily Sales Table
Create daily sales aggregated analytics table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, avg, countDistinct
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_gold_daily_sales(spark: SparkSession) -> None:
    """
    Process daily sales: Aggregate sales by date and customer state.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("gold.daily_sales", config)
    
    logger.info("Starting processing of gold daily_sales dataset")
    
    # Read silver tables
    logger.debug("Reading silver tables: orders, order_items, customers")
    silver_orders_df = read_table_from_config(spark, "orders", layer="silver", config=config).alias("orders")
    silver_order_items_df = read_table_from_config(spark, "order_items", layer="silver", config=config).alias("order_items")
    silver_customers_df = read_table_from_config(spark, "customers", layer="silver", config=config).alias("customers")
    
    # Create daily sales table
    logger.debug("Aggregating daily sales by date and customer state")
    gold_daily_sales_df = (
        silver_orders_df
            .join(
                silver_order_items_df,
                col("orders.order_id") == col("order_items.order_id")
            )
            .join(
                silver_customers_df,
                col("orders.customer_id") == col("customers.customer_id")
            )
            .withColumn(
                "order_date",
                to_date(col("orders.order_created_ts"))
            )
            .groupBy(
                col("order_date"),
                col("customers.state").alias("customer_state")
            )
            .agg(
                countDistinct(col("orders.order_id")).alias("total_orders"),
                sum(col("order_items.price") + col("order_items.freight_value")).alias("total_revenue"),
                avg(col("order_items.price") + col("order_items.freight_value")).alias("avg_order_value")
            )
    )
    
    log_dataframe_info(gold_daily_sales_df, logger, config)
    
    # Save as gold table
    logger.debug("Saving gold daily_sales table")
    save_as_table_from_config(gold_daily_sales_df, "daily_sales", layer="gold", mode="overwrite", config=config)
    
    logger.info("Successfully processed gold daily_sales dataset")


def main() -> None:
    """Entry point for the daily_sales gold processing script."""
    spark = SparkSession.builder \
        .appName("GoldDailySales") \
        .getOrCreate()
    
    try:
        process_gold_daily_sales(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

