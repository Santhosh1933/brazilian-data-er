"""
Spark Utility Functions
Common functions for reading CSV files using PySpark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name
from typing import Dict, Any, Optional

from brazilian_data_er.utils.json_parser import load_config, get_config_value, get_csv_path, get_table_name


def read_csv_df(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
    config: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Read CSV file into a Spark DataFrame.
    
    Args:
        spark: SparkSession object
        path: Path to the CSV file
        header: Whether the CSV file has a header row (default: True)
        infer_schema: Whether to infer schema automatically (default: True)
        config: Optional configuration dictionary. If provided, will override
                default header and infer_schema values with config values.
        
    Returns:
        Spark DataFrame
    """
    if config is None:
        config = load_config()
    
    # Get read options from config if available
    read_options = get_config_value(config, "spark", "read_options", default={})
    
    # Use config values if provided, otherwise use function parameters
    header_val = read_options.get("header", header)
    infer_schema_val = read_options.get("inferSchema", infer_schema)
    
    df = spark.read.csv(
        path,
        header=header_val,
        inferSchema=infer_schema_val
    )
    
    return df


def read_csv_from_config(
    spark: SparkSession,
    dataset_name: str,
    config: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Read CSV file using dataset name from configuration.
    
    Args:
        spark: SparkSession object
        dataset_name: Name of the dataset (e.g., "customers", "orders")
        config: Optional configuration dictionary. If None, will load from default config.json
        
    Returns:
        Spark DataFrame
    """
    if config is None:
        config = load_config()
    
    csv_path = get_csv_path(config, dataset_name)
    return read_csv_df(spark, csv_path, config=config)


def save_as_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Save DataFrame as a table.
    
    Args:
        df: Spark DataFrame to save
        table_name: Full table name (e.g., "workspace.bronze.bronze_customers")
        mode: Write mode (default: "overwrite")
    """
    df.write.mode(mode).saveAsTable(table_name)


def read_table_from_config(
    spark: SparkSession,
    dataset_name: str,
    layer: str = "bronze",
    config: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Read table using dataset name from configuration.
    
    Args:
        spark: SparkSession object
        dataset_name: Name of the dataset (e.g., "customers", "orders")
        layer: Layer name - "bronze", "silver", or "gold" (default: "bronze")
        config: Optional configuration dictionary. If None, will load from default config.json
        
    Returns:
        Spark DataFrame
    """
    if config is None:
        config = load_config()
    
    table_name = get_table_name(config, dataset_name, layer=layer, include_schema=True)
    return spark.table(table_name)


def save_as_table_from_config(
    df: DataFrame,
    dataset_name: str,
    layer: str = "bronze",
    mode: str = "overwrite",
    config: Optional[Dict[str, Any]] = None
) -> None:
    """
    Save DataFrame as a table using dataset name from configuration.
    
    Args:
        df: Spark DataFrame to save
        dataset_name: Name of the dataset (e.g., "customers", "orders")
        layer: Layer name - "bronze", "silver", or "gold" (default: "bronze")
        mode: Write mode (default: "overwrite")
        config: Optional configuration dictionary. If None, will load from default config.json
    """
    if config is None:
        config = load_config()
    
    table_name = get_table_name(config, dataset_name, layer=layer, include_schema=True)
    save_as_table(df, table_name, mode)

