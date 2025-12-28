"""
Silver Layer: Order Items Dataset
Clean and process order items data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_order_items(spark: SparkSession) -> None:
    """
    Process order items dataset: Deduplicate and cast types.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.order_items", config)
    
    logger.info("Starting processing of silver order_items dataset")
    
    # Read bronze table
    logger.debug("Reading bronze order_items table")
    order_items = read_table_from_config(spark, "order_items", layer="bronze", config=config)
    
    # Process order items
    logger.debug("Processing order_items: deduplicating and casting types")
    silver_order_items = (
        order_items.dropDuplicates(["order_id", "order_item_id"])
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            col("price").cast("double"),
            col("freight_value").cast("double")
        )
    )
    
    log_dataframe_info(silver_order_items, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver order_items table")
    save_as_table_from_config(silver_order_items, "order_items", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver order_items dataset")


def main() -> None:
    """Entry point for the order items silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverOrderItems") \
        .getOrCreate()
    
    try:
        process_silver_order_items(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

