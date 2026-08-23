"""Failure monitoring and AutoHeal notification for Databricks tasks."""

from __future__ import annotations

import functools
import json
import logging
import socket
import traceback
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from pyspark.sql import SparkSession

F = TypeVar("F", bound=Callable[..., Any])
DEFAULT_FAILURE_LOG_TABLE = "`brazilian-e-commerce`.bronze.task_failure_logs"
DEFAULT_REPOSITORY_URL = "https://github.com/Santhosh1933/brazilian-data-etl-pipeline.git"
DEFAULT_WEBHOOK_URL = "https://6bea-27-5-229-236.ngrok-free.app/webhook/pipeline-failure"
logger = logging.getLogger("brazilian_data_er.self_healing")


def _spark_conf(spark: Optional[SparkSession], key: str, default: str) -> str:
    """Read optional Databricks metadata without requiring JVM access."""
    if spark is None:
        return default
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


def build_failure_payload(task_key: str, error: BaseException) -> Dict[str, Any]:
    """Build a failure payload from Databricks and local execution metadata."""
    spark = SparkSession.getActiveSession()
    try:
        spark_version = spark.version if spark else "local"
    except Exception:
        spark_version = "unknown"
    return {
        "failure_event_id": str(uuid.uuid4()),
        "run_id": _spark_conf(spark, "spark.databricks.job.runId", "local-demo-run"),
        "job_id": _spark_conf(spark, "spark.databricks.job.id", "brazilian-self-healing-demo"),
        "task_key": task_key,
        "error_type": error.__class__.__name__,
        "error_message": str(error),
        "stack_trace": traceback.format_exc(),
        "commit_sha": "",
        "repository_url": DEFAULT_REPOSITORY_URL,
        "cluster_id": _spark_conf(spark, "spark.databricks.clusterUsageTags.clusterId", socket.gethostname()),
        "spark_version": spark_version,
        "event_time": datetime.now(timezone.utc),
    }


def ensure_failure_log_table(spark: Optional[SparkSession]) -> bool:
    """Create the Delta failure-log table with CDF when it does not exist."""
    if spark is None:
        return False
    table_name = DEFAULT_FAILURE_LOG_TABLE
    try:
        if not spark.catalog.tableExists(table_name):
            spark.sql(
                "CREATE SCHEMA IF NOT EXISTS `brazilian-e-commerce`.bronze"
            )
            spark.sql(
                f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    failure_event_id STRING,
                    run_id STRING,
                    job_id STRING,
                    task_key STRING,
                    error_type STRING,
                    error_message STRING,
                    stack_trace STRING,
                    commit_sha STRING,
                    repository_url STRING,
                    cluster_id STRING,
                    spark_version STRING,
                    event_time TIMESTAMP,
                    capture_mode STRING,
                    triage_status STRING
                ) USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""
            )
        else:
            columns = {field.name for field in spark.table(table_name).schema.fields}
            if "repository_url" not in columns:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS (repository_url STRING)"
                )
        return True
    except Exception:
        return False


def write_failure_log(spark: Optional[SparkSession], payload: Dict[str, Any]) -> bool:
    """Append a failure event to the Delta log table when Databricks is available."""
    if spark is None or not ensure_failure_log_table(spark):
        return False
    try:
        event = {
            **payload,
            "capture_mode": "monitor_task",
            "triage_status": "received",
        }
        spark.createDataFrame([event]).write.mode("append").saveAsTable(DEFAULT_FAILURE_LOG_TABLE)
        return True
    except Exception:
        return False


def notify_failure(payload: Dict[str, Any], timeout_seconds: float = 5.0) -> bool:
    """POST a failure event to AutoHeal without masking pipeline failures."""
    endpoint = DEFAULT_WEBHOOK_URL
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, default=str).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            delivered = 200 <= response.status < 300
            logger.info(
                "AutoHeal webhook delivery status=%s endpoint=%s http_status=%s",
                "delivered" if delivered else "rejected",
                endpoint,
                response.status,
            )
            return delivered
    except urllib.error.HTTPError as error:
        logger.error(
            "AutoHeal webhook rejected event endpoint=%s http_status=%s",
            endpoint,
            error.code,
        )
        return False
    except (OSError, urllib.error.URLError) as error:
        logger.error(
            "AutoHeal webhook unreachable endpoint=%s error=%s",
            endpoint,
            error,
        )
        return False


def monitor_task(task_key: Optional[str] = None) -> Callable[[F], F]:
    """Decorate a task to persist and notify failures before re-raising them."""
    def decorator(function: F) -> F:
        @functools.wraps(function)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            resolved_task_key = task_key or function.__name__
            try:
                return function(*args, **kwargs)
            except Exception as error:
                payload = build_failure_payload(resolved_task_key, error)
                log_written = write_failure_log(SparkSession.getActiveSession(), payload)
                webhook_delivered = notify_failure(payload)
                logger.info(
                    "AutoHeal failure capture task=%s table_written=%s webhook_delivered=%s",
                    resolved_task_key,
                    log_written,
                    webhook_delivered,
                )
                raise

        return cast(F, wrapped)

    return decorator
