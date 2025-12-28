"""
JSON Parser Utility
Common utility functions for reading and parsing JSON configuration files
"""

import json
import os
from typing import Dict, Any, Optional


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load and parse JSON configuration file.
    
    Args:
        config_path: Path to the JSON configuration file. If None, uses default path:
                     /Volumes/workspace/bronze/bronze_volume/brazilian-ecommerce/config.json
        
    Returns:
        Dictionary containing parsed JSON configuration
        
    Raises:
        FileNotFoundError: If the config file doesn't exist
        json.JSONDecodeError: If the JSON file is malformed
    """
    # Default config path in Databricks volume - allows config changes without rebuilding
    default_config_path = "/Volumes/workspace/bronze/bronze_volume/brazilian-ecommerce/config.json"
    
    # Use default volume path if not provided
    if config_path is None:
        config_path = default_config_path
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Default configuration file not found: {config_path}"
            )
    
    # If config_path is relative, try to find it relative to project root
    elif not os.path.isabs(config_path):
        # Try current directory first
        if not os.path.exists(config_path):
            # Try project root (go up from src/brazilian_data_er/utils)
            # __file__ is at src/brazilian_data_er/utils/json_parser.py
            # Project root is 4 levels up (utils -> brazilian_data_er -> src -> project_root)
            current_file = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file))))
            project_config = os.path.join(project_root, config_path)
            if os.path.exists(project_config):
                config_path = project_config
            else:
                # Try default volume path as fallback
                if os.path.exists(default_config_path):
                    config_path = default_config_path
                else:
                    raise FileNotFoundError(
                        f"Configuration file not found: {config_path} "
                        f"(also checked: {project_config}, {default_config_path})"
                    )
    else:
        # Absolute path provided - check if it exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config


def get_config_value(config: Dict[str, Any], *keys: str, default: Optional[Any] = None) -> Any:
    """
    Get a nested value from configuration dictionary using dot notation.
    
    Args:
        config: Configuration dictionary
        *keys: Variable number of keys to traverse the nested dictionary
        default: Default value to return if key path doesn't exist
        
    Returns:
        Value at the specified key path, or default if not found
        
    Example:
        get_config_value(config, "paths", "csv_base_path")
    """
    current = config
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def get_csv_path(config: Dict[str, Any], dataset_name: str) -> str:
    """
    Get the full CSV file path for a dataset.
    
    Args:
        config: Configuration dictionary
        dataset_name: Name of the dataset (e.g., "customers", "orders")
        
    Returns:
        Full path to the CSV file
        
    Raises:
        KeyError: If dataset_name is not found in config
    """
    base_path = get_config_value(config, "paths", "csv_base_path", default="")
    csv_files = get_config_value(config, "paths", "csv_files", default={})
    
    if dataset_name not in csv_files:
        raise KeyError(f"Dataset '{dataset_name}' not found in configuration")
    
    filename = csv_files[dataset_name]
    
    # Handle both dbfs: and regular paths
    if base_path.startswith("dbfs:") or base_path.startswith("/Volumes"):
        return f"{base_path}/{filename}" if not base_path.endswith("/") else f"{base_path}{filename}"
    else:
        return os.path.join(base_path, filename)


def get_table_name(
    config: Dict[str, Any], 
    dataset_name: str, 
    layer: str = "bronze",
    include_schema: bool = True
) -> str:
    """
    Get the full table name (with schema) for a dataset.
    
    Args:
        config: Configuration dictionary
        dataset_name: Name of the dataset (e.g., "customers", "orders")
        layer: Layer name - "bronze", "silver", or "gold" (default: "bronze")
        include_schema: Whether to include schema prefix (default: True)
        
    Returns:
        Full table name (e.g., "workspace.bronze.bronze_customers")
        
    Raises:
        KeyError: If dataset_name or layer is not found in config
    """
    layer_config = get_config_value(config, "tables", layer, default={})
    
    if not layer_config:
        raise KeyError(f"Layer '{layer}' not found in configuration")
    
    schema = layer_config.get("schema", "")
    table_names = layer_config.get("names", {})
    
    if dataset_name not in table_names:
        raise KeyError(f"Dataset '{dataset_name}' not found in configuration for layer '{layer}'")
    
    table_name = table_names[dataset_name]
    
    if include_schema and schema:
        return f"{schema}.{table_name}"
    else:
        return table_name

