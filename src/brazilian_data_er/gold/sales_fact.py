"""
Gold Layer: Sales Fact Table
Create analytics-ready sales fact table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from brazilian_data_er.utils.spark_utils import read_table_from_config, save_as_table_from_config
from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info


def process_gold_sales_fact(spark: SparkSession) -> None:
    """
    Process sales fact: Join all relevant tables and create fact table.
    
    Args:
        spark: SparkSession object
    """
    config = load_config()
    logger = get_logger("gold.sales_fact", config)
    
    logger.info("Starting processing of gold sales_fact dataset")
    
    # Read silver tables
    logger.debug("Reading silver tables: orders, order_items, products, customers, sellers")
    silver_orders_df = read_table_from_config(spark, "orders", layer="silver", config=config).alias("orders")
    silver_order_items_df = read_table_from_config(spark, "order_items", layer="silver", config=config).alias("order_items")
    silver_products_df = read_table_from_config(spark, "products", layer="silver", config=config).alias("products")
    silver_customers_df = read_table_from_config(spark, "customers", layer="silver", config=config).alias("customers")
    silver_sellers_df = read_table_from_config(spark, "sellers", layer="silver", config=config).alias("sellers")
    
    # Create sales fact table
    logger.debug("Joining tables and creating sales fact")
    gold_sales_fact_df = (
        silver_orders_df
            .join(
                silver_order_items_df,
                col("orders.order_id") == col("order_items.order_id")
            )
            .join(
                silver_products_df,
                col("order_items.product_id") == col("products.product_id"),
                "left"
            )
            .join(
                silver_customers_df,
                col("orders.customer_id") == col("customers.customer_id"),
                "left"
            )
            .join(
                silver_sellers_df,
                col("order_items.seller_id") == col("sellers.seller_id"),
                "left"
            )
            .select(
                col("orders.order_id"),
                col("orders.order_created_ts").alias("order_timestamp"),
                col("orders.customer_id"),
                col("order_items.seller_id"),
                col("order_items.product_id"),
                col("products.category"),
                col("order_items.price"),
                col("order_items.freight_value"),
                (col("order_items.price") + col("order_items.freight_value")).alias("total_amount"),
                col("customers.city").alias("customer_city"),
                col("customers.state").alias("customer_state"),
                col("sellers.city").alias("seller_city"),
                col("sellers.state").alias("seller_state")
            )
    )
    
    log_dataframe_info(gold_sales_fact_df, logger, config)
    
    # Save as gold table
    logger.debug("Saving gold sales_fact table")
    save_as_table_from_config(gold_sales_fact_df, "sales_fact", layer="gold", mode="overwrite", config=config)
    
    logger.info("Successfully processed gold sales_fact dataset")


def main() -> None:
    """Entry point for the sales_fact gold processing script."""
    spark = SparkSession.builder \
        .appName("GoldSalesFact") \
        .getOrCreate()
    
    try:
        process_gold_sales_fact(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

