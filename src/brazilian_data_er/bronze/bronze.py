"""Configurable ingestion of all datasets into the bronze layer."""

import json
import os
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from brazilian_data_er.utils.json_parser import load_config
from brazilian_data_er.utils.logger import get_logger, log_dataframe_info
from brazilian_data_er.utils.spark_utils import read_csv_from_config, save_as_table_from_config


def _load_bronze_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load bronze configuration from the Databricks volume or repository."""
    if config_path is None:
        volume_config_path = "/Volumes/workspace/bronze/bronze_volume/brazilian-ecommerce/config/bronze.json"
        project_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../..", "config", "bronze.json")
        )
        config_path = (
            volume_config_path
            if os.path.exists(volume_config_path)
            else project_config_path
        )

    with open(config_path, "r") as config_file:
        return json.load(config_file)


def process_bronze(spark: SparkSession, bronze_config_path: Optional[str] = None) -> None:
    """Read each configured CSV and save it as its configured bronze table."""
    config = load_config()
    bronze_config = _load_bronze_config(bronze_config_path)
    logger = get_logger("bronze", config)

    for dataset_name in bronze_config["datasets"]:
        logger.info(f"Starting processing of {dataset_name} dataset")
        dataframe = read_csv_from_config(spark, dataset_name, config)
        log_dataframe_info(dataframe, logger, config)
        save_as_table_from_config(
            dataframe,
            dataset_name,
            mode=bronze_config.get("write_mode", "overwrite"),
            config=config,
        )
        logger.info(f"Successfully processed {dataset_name} dataset")


def main() -> None:
    """Entry point for configurable bronze processing."""
    bronze_config = _load_bronze_config()
    spark = SparkSession.builder.appName(bronze_config.get("app_name", "BronzeIngestion")).getOrCreate()

    try:
        process_bronze(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()