"""
Logger Utility
Centralized logging configuration for the Brazilian E-commerce Data Pipeline
"""

import logging
import sys
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame

from brazilian_data_er.utils.json_parser import load_config, get_config_value


def setup_logger(name: str = "brazilian_data_er", config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Setup and configure logger with settings from config file.
    
    Args:
        name: Logger name (default: "brazilian_data_er")
        config: Optional configuration dictionary. If None, will load from default config.json
        
    Returns:
        Configured logger instance
    """
    if config is None:
        config = load_config()
    
    # Get logger configuration
    log_level = get_config_value(config, "logging", "level", default="INFO").upper()
    log_format = get_config_value(
        config, 
        "logging", 
        "format", 
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def log_dataframe_info(df: DataFrame, logger: logging.Logger, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Log DataFrame information if debug mode is enabled.
    
    Args:
        df: Spark DataFrame to log information about
        logger: Logger instance
        config: Optional configuration dictionary. If None, will load from default config.json
    """
    if config is None:
        config = load_config()
    
    # Check if debug mode is enabled
    log_level = get_config_value(config, "logging", "level", default="INFO").upper()
    enable_debug = get_config_value(config, "logging", "enable_debug_info", default=False)
    
    if log_level == "DEBUG" or enable_debug:
        try:
            # Get column names
            columns = df.columns
            logger.debug(f"DataFrame columns ({len(columns)}): {', '.join(columns)}")
            
            # Count rows
            logger.debug("Counting DataFrame rows...")
            row_count = df.count()
            logger.debug(f"DataFrame row count: {row_count:,}")
            
            # Show top 5 rows (df.show() prints to stdout, so we log a message and call show)
            logger.debug("DataFrame top 5 rows:")
            df.show(5, truncate=False)
            
        except Exception as e:
            logger.warning(f"Failed to log DataFrame debug info: {str(e)}")


def get_logger(name: str = "brazilian_data_er", config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Get a logger instance with proper configuration.
    
    Args:
        name: Logger name (default: "brazilian_data_er")
        config: Optional configuration dictionary. If None, will load from default config.json
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only setup if not already configured
    if not logger.handlers:
        logger = setup_logger(name, config)
    
    return logger

