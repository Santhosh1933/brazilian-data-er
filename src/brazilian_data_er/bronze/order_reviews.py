"""
Bronze Layer: Order Reviews Dataset
Reads CSV file and saves as bronze table
"""

from pyspark.sql import SparkSession
from brazilian_data_er.utils.spark_utils import read_csv_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_bronze_order_reviews(spark: SparkSession) -> None:
    """
    Process order reviews dataset: Read CSV and save as bronze table.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("bronze.order_reviews", config)
    dataset_name = "order_reviews"
    
    logger.info(f"Starting processing of {dataset_name} dataset")
    
    # Read CSV file
    bronze_df_order_reviews = read_csv_from_config(spark, dataset_name, config)
    log_dataframe_info(bronze_df_order_reviews, logger, config)
    
    # Save as table
    save_as_table_from_config(bronze_df_order_reviews, dataset_name, mode="overwrite", config=config)
    
    logger.info(f"Successfully processed {dataset_name} dataset")


def main() -> None:
    """Entry point for the order reviews bronze processing script."""
    spark = SparkSession.builder \
        .appName("BronzeOrderReviews") \
        .getOrCreate()
    
    try:
        process_bronze_order_reviews(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

