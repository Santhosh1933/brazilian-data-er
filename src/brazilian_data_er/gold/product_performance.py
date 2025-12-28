"""
Gold Layer: Product Performance Table
Create product performance analytics table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_gold_product_performance(spark: SparkSession) -> None:
    """
    Process product performance: Aggregate product metrics with reviews.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("gold.product_performance", config)
    
    logger.info("Starting processing of gold product_performance dataset")
    
    # Read silver tables
    logger.debug("Reading silver tables: order_items, products, reviews")
    silver_order_items_df = read_table_from_config(spark, "order_items", layer="silver", config=config).alias("order_items")
    silver_products_df = read_table_from_config(spark, "products", layer="silver", config=config).alias("products")
    silver_reviews_df = read_table_from_config(spark, "reviews", layer="silver", config=config).alias("reviews")
    
    # Create product performance table
    logger.debug("Aggregating product performance metrics with reviews")
    gold_product_performance_df = (
        silver_order_items_df
            .join(
                silver_products_df,
                col("order_items.product_id") == col("products.product_id")
            )
            .join(
                silver_reviews_df,
                col("order_items.order_id") == col("reviews.order_id"),
                "left"
            )
            .groupBy(
                col("order_items.product_id"),
                col("products.category")
            )
            .agg(
                count(col("order_items.order_id")).alias("total_units_sold"),
                sum(col("order_items.price") + col("order_items.freight_value")).alias("total_revenue"),
                avg(col("reviews.review_score")).alias("average_review_score")
            )
    )
    
    log_dataframe_info(gold_product_performance_df, logger, config)
    
    # Save as gold table
    logger.debug("Saving gold product_performance table")
    save_as_table_from_config(gold_product_performance_df, "product_performance", layer="gold", mode="overwrite", config=config)
    
    logger.info("Successfully processed gold product_performance dataset")


def main() -> None:
    """Entry point for the product_performance gold processing script."""
    spark = SparkSession.builder \
        .appName("GoldProductPerformance") \
        .getOrCreate()
    
    try:
        process_gold_product_performance(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

