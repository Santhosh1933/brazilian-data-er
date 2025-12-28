"""
Silver Layer: Customers Dataset
Clean and process customers data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, avg
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_customers(spark: SparkSession) -> None:
    """
    Process customers dataset: Clean data, cast types, deduplicate, standardize columns.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.customers", config)
    
    logger.info("Starting processing of silver customers dataset")
    
    # Read bronze tables
    logger.debug("Reading bronze customers and geolocation tables")
    customers = read_table_from_config(spark, "customers", layer="bronze", config=config)
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
    
    # Process customers
    logger.debug("Processing customers: deduplicating, joining with geo, and standardizing columns")
    silver_customers = (
        customers.dropDuplicates(["customer_id"])
        .join(
            geo_clean,
            customers.customer_zip_code_prefix == geo_clean.geolocation_zip_code_prefix,
            "left"
        )
        .select(
            "customer_id",
            "customer_unique_id",
            lower(trim(col("customer_city"))).alias("city"),
            lower(trim(col("customer_state"))).alias("state"),
            "latitude",
            "longitude"
        )
    )
    
    log_dataframe_info(silver_customers, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver customers table")
    save_as_table_from_config(silver_customers, "customers", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver customers dataset")


def main() -> None:
    """Entry point for the customers silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverCustomers") \
        .getOrCreate()
    
    try:
        process_silver_customers(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

