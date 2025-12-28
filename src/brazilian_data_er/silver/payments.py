"""
Silver Layer: Payments Dataset
Clean and process payments data from bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_silver_payments(spark: SparkSession) -> None:
    """
    Process payments dataset: Clean and cast types.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("silver.payments", config)
    
    logger.info("Starting processing of silver payments dataset")
    
    # Read bronze table
    logger.debug("Reading bronze order_payments table")
    payments = read_table_from_config(spark, "order_payments", layer="bronze", config=config)
    
    # Process payments
    logger.debug("Processing payments: cleaning and casting types")
    silver_payments = (
        payments.select(
            "order_id",
            lower(trim(col("payment_type"))).alias("payment_type"),
            col("payment_value").cast("double"),
            "payment_installments"
        )
    )
    
    log_dataframe_info(silver_payments, logger, config)
    
    # Save as silver table
    logger.debug("Saving silver payments table")
    save_as_table_from_config(silver_payments, "payments", layer="silver", mode="overwrite", config=config)
    
    logger.info("Successfully processed silver payments dataset")


def main() -> None:
    """Entry point for the payments silver processing script."""
    spark = SparkSession.builder \
        .appName("SilverPayments") \
        .getOrCreate()
    
    try:
        process_silver_payments(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

