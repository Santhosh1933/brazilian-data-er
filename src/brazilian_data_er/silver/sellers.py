"""
Silver Layer: Sellers Dataset
Clean and process sellers data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, avg
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_sellers(spark: SparkSession) -> None:
    """
    Process sellers dataset: Clean data, cast types, deduplicate, standardize columns.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.sellers", config)
    
    logger.info("Starting processing of silver sellers dataset")
    
    # Read bronze tables
    logger.debug("Reading bronze sellers and geolocation tables")
    sellers = read_table_from_config(spark, "sellers", layer="bronze", config=config)
    geo = read_table_from_config(spark, "geolocation", layer="bronze", config=config)
    
    # Clean geolocation data - aggregate by zip code
    logger.debug("Cleaning and aggregating geolocation data")
    geo_clean = (
        geo.groupBy("geolocation_zip_code_prefix")
           .agg(
               avg("geolocation_lat").alias("latitude"),
               avg("geolocation_lng").alias("longitude")
           )
    )
    
    # Process sellers
    logger.debug("Processing sellers: deduplicating, joining with geo, and standardizing columns")
    silver_sellers = (
        sellers.dropDuplicates(["seller_id"])
        .join(
            geo_clean,
            sellers.seller_zip_code_prefix == geo_clean.geolocation_zip_code_prefix,
            "left"
        )
        .select(
            "seller_id",
            lower(trim(col("seller_city"))).alias("city"),
            lower(trim(col("seller_state"))).alias("state"),
            "latitude",
            "longitude"
        )
    )
    
    log_dataframe_info(silver_sellers, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver sellers table")
    save_as_table_from_config(silver_sellers, "sellers", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver sellers dataset")


def main() -> None:
    """Entry point for the sellers silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverSellers") \
        .getOrCreate()
    
    try:
        process_silver_sellers(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

