"""
Silver Layer: Products Dataset
Clean and process products data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_products(spark: SparkSession) -> None:
    """
    Process products dataset: Join with category translation and clean data.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.products", config)
    
    logger.info("Starting processing of silver products dataset")
    
    # Read bronze tables
    logger.debug("Reading bronze products and product_category_translation tables")
    products = read_table_from_config(spark, "products", layer="bronze", config=config)
    category = read_table_from_config(spark, "product_category_translation", layer="bronze", config=config)
    
    # Process products
    logger.debug("Joining products with category translation and selecting columns")
    silver_products = (
        products.join(
            category,
            "product_category_name",
            "left"
        )
        .select(
            "product_id",
            col("product_category_name_english").alias("category"),
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )
    )
    
    log_dataframe_info(silver_products, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver products table")
    save_as_table_from_config(silver_products, "products", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver products dataset")


def main() -> None:
    """Entry point for the products silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverProducts") \
        .getOrCreate()
    
    try:
        process_silver_products(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

