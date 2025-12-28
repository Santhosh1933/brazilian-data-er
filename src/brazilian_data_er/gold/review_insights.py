"""
Gold Layer: Review Insights Table
Create review insights analytics table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_gold_review_insights(spark: SparkSession) -> None:
    """
    Process review insights: Join reviews with orders, items, and products.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("gold.review_insights", config)
    
    logger.info("Starting processing of gold review_insights dataset")
    
    # Read silver tables
    logger.debug("Reading silver tables: reviews, orders, order_items, products")
    silver_reviews_df = read_table_from_config(spark, "reviews", layer="silver", config=config).alias("reviews")
    silver_orders_df = read_table_from_config(spark, "orders", layer="silver", config=config).alias("orders")
    silver_order_items_df = read_table_from_config(spark, "order_items", layer="silver", config=config).alias("order_items")
    silver_products_df = read_table_from_config(spark, "products", layer="silver", config=config).alias("products")
    
    # Create review insights table
    logger.debug("Joining reviews with orders, items, and products")
    gold_review_insights_df = (
        silver_reviews_df
            .join(
                silver_orders_df,
                col("reviews.order_id") == col("orders.order_id")
            )
            .join(
                silver_order_items_df,
                col("orders.order_id") == col("order_items.order_id")
            )
            .join(
                silver_products_df,
                col("order_items.product_id") == col("products.product_id")
            )
            .select(
                col("reviews.review_id"),
                col("reviews.review_score"),
                col("reviews.review_text"),
                col("products.category"),
                col("orders.order_created_ts").alias("order_timestamp")
            )
    )
    
    log_dataframe_info(gold_review_insights_df, logger, config)
    
    # Save as gold table
    logger.debug("Saving gold review_insights table")
    save_as_table_from_config(gold_review_insights_df, "review_insights", layer="gold", mode="overwrite", config=config)
    
    logger.info("Successfully processed gold review_insights dataset")


def main() -> None:
    """Entry point for the review_insights gold processing script."""
    spark = SparkSession.builder \
        .appName("GoldReviewInsights") \
        .getOrCreate()
    
    try:
        process_gold_review_insights(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

