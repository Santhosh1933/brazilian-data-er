"""
Silver Layer: Orders Dataset
Clean and process orders data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, when
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_orders(spark: SparkSession) -> None:
    """
    Process orders dataset: Cast types, add calculated fields.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.orders", config)
    
    logger.info("Starting processing of silver orders dataset")
    
    # Read bronze table
    logger.debug("Reading bronze orders table")
    orders = read_table_from_config(spark, "orders", layer="bronze", config=config)
    
    # Process orders
    logger.debug("Processing orders: casting timestamps and adding calculated fields")
    silver_orders = (
        orders.select(
            "order_id",
            "customer_id",
            to_timestamp("order_purchase_timestamp").alias("order_created_ts"),
            to_timestamp("order_delivered_customer_date").alias("order_delivered_ts"),
            "order_status"
        )
        .withColumn(
            "delivery_days",
            datediff(col("order_delivered_ts"), col("order_created_ts"))
        )
        .withColumn(
            "is_delivered",
            when(col("order_status") == "delivered", True).otherwise(False)
        )
    )
    
    log_dataframe_info(silver_orders, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver orders table")
    save_as_table_from_config(silver_orders, "orders", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver orders dataset")


def main() -> None:
    """Entry point for the orders silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverOrders") \
        .getOrCreate()
    
    try:
        process_silver_orders(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

