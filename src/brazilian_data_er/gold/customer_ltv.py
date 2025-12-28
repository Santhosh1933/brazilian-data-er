"""
Gold Layer: Customer LTV Table
Create customer lifetime value analytics table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, max
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_gold_customer_ltv(spark: SparkSession) -> None:
    """
    Process customer LTV: Calculate customer lifetime value metrics.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("gold.customer_ltv", config)
    
    logger.info("Starting processing of gold customer_ltv dataset")
    
    # Read silver tables
    logger.debug("Reading silver tables: orders, order_items, customers")
    silver_orders_df = read_table_from_config(spark, "orders", layer="silver", config=config).alias("orders")
    silver_order_items_df = read_table_from_config(spark, "order_items", layer="silver", config=config).alias("order_items")
    silver_customers_df = read_table_from_config(spark, "customers", layer="silver", config=config).alias("customers")
    
    # Create customer LTV table
    logger.debug("Calculating customer lifetime value metrics")
    gold_customer_ltv_df = (
        silver_orders_df
            .join(
                silver_order_items_df,
                col("orders.order_id") == col("order_items.order_id")
            )
            .join(
                silver_customers_df,
                col("orders.customer_id") == col("customers.customer_id")
            )
            .groupBy(
                col("orders.customer_id"),
                col("customers.state").alias("customer_state")
            )
            .agg(
                countDistinct(col("orders.order_id")).alias("total_orders"),
                sum(col("order_items.price") + col("order_items.freight_value")).alias("total_spent"),
                max(col("orders.order_created_ts")).alias("last_order_timestamp")
            )
    )
    
    log_dataframe_info(gold_customer_ltv_df, logger, config)
    
    # Save as gold table
    logger.debug("Saving gold customer_ltv table")
    save_as_table_from_config(gold_customer_ltv_df, "customer_ltv", layer="gold", mode="overwrite", config=config)
    
    logger.info("Successfully processed gold customer_ltv dataset")


def main() -> None:
    """Entry point for the customer_ltv gold processing script."""
    spark = SparkSession.builder \
        .appName("GoldCustomerLTV") \
        .getOrCreate()
    
    try:
        process_gold_customer_ltv(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

