"""
Bronze Layer: Product Category Translation Dataset
Reads CSV file and saves as bronze table
"""

from pyspark.sql import SparkSession
from brazilian_data_er.utils.spark_utils import read_csv_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_bronze_product_category_translation(spark: SparkSession) -> None:
    """
    Process product category translation dataset: Read CSV and save as bronze table.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("bronze.product_category_translation", config)
    dataset_name = "product_category_translation"
    
    logger.info(f"Starting processing of {dataset_name} dataset")
    
    # Read CSV file
    bronze_df_product_category_translation = read_csv_from_config(spark, dataset_name, config)
    log_dataframe_info(bronze_df_product_category_translation, logger, config)
    
    # Save as table
    save_as_table_from_config(
        bronze_df_product_category_translation, 
        dataset_name, 
        mode="overwrite", 
        config=config
    )
    
    logger.info(f"Successfully processed {dataset_name} dataset")


def main() -> None:
    """Entry point for the product category translation bronze processing script."""
    spark = SparkSession.builder \
        .appName("BronzeProductCategoryTranslation") \
        .getOrCreate()
    
    try:
        process_bronze_product_category_translation(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

