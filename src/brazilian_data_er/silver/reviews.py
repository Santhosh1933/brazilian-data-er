"""
Silver Layer: Reviews Dataset
Clean and process reviews data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, expr
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_reviews(spark: SparkSession) -> None:
    """
    Process reviews dataset: Clean and cast types.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.reviews", config)
    
    logger.info("Starting processing of silver reviews dataset")
    
    # Read bronze table
    logger.debug("Reading bronze order_reviews table")
    reviews = read_table_from_config(spark, "order_reviews", layer="bronze", config=config)
    
    # Process reviews
    logger.debug("Processing reviews: cleaning and casting types")
    silver_reviews = (
        reviews.select(
            "review_id",
            "order_id",
            expr("try_cast(review_score as int)").alias("review_score"),
            trim(col("review_comment_message")).alias("review_text")
        )
    )
    
    log_dataframe_info(silver_reviews, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver reviews table")
    save_as_table_from_config(silver_reviews, "reviews", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver reviews dataset")


def main() -> None:
    """Entry point for the reviews silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverReviews") \
        .getOrCreate()
    
    try:
        process_silver_reviews(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

