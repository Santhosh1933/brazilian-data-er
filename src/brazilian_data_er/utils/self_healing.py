"""Failure monitoring and AutoHeal notification for Databricks tasks."""

from __future__ import annotations

import functools
import json
import os
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


def _failure_log_table() -> str:
    """Return the configured fully qualified Delta failure-log table."""
    return os.getenv("AUTOHEAL_FAILURE_LOG_TABLE", DEFAULT_FAILURE_LOG_TABLE)


def _env_bool(name: str, default: bool = True) -> bool:
    """Read a boolean environment variable."""
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def build_failure_payload(task_key: str, error: BaseException) -> Dict[str, Any]:
    """Build a failure payload from Databricks and local execution metadata."""
    spark = SparkSession.getActiveSession()
    spark_context = spark.sparkContext if spark else None
    return {
        "failure_event_id": str(uuid.uuid4()),
        "run_id": os.getenv("DATABRICKS_RUN_ID", os.getenv("AUTOHEAL_RUN_ID", "local-demo-run")),
        "job_id": os.getenv("DATABRICKS_JOB_ID", os.getenv("AUTOHEAL_JOB_ID", "brazilian-self-healing-demo")),
        "task_key": task_key,
        "error_type": error.__class__.__name__,
        "error_message": str(error),
        "stack_trace": traceback.format_exc(),
        "commit_sha": os.getenv("GIT_COMMIT_SHA", "local-demo-commit"),
        "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", socket.gethostname()),
        "spark_version": spark_context.version if spark_context else "local",
        "event_time": datetime.now(timezone.utc),
    }


def ensure_failure_log_table(spark: Optional[SparkSession]) -> bool:
    """Create the Delta failure-log table with CDF when it does not exist."""
    if spark is None:
        return False
    table_name = _failure_log_table()
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
                    cluster_id STRING,
                    spark_version STRING,
                    event_time TIMESTAMP,
                    capture_mode STRING,
                    triage_status STRING
                ) USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""
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
        spark.createDataFrame([event]).write.mode("append").saveAsTable(_failure_log_table())
        return True
    except Exception:
        return False


def notify_failure(payload: Dict[str, Any], timeout_seconds: float = 5.0) -> bool:
    """POST a failure event to AutoHeal without masking pipeline failures."""
    if not _env_bool("AUTOHEAL_NOTIFY_ENABLED", True):
        return False
    endpoint = os.getenv(
        "AUTOHEAL_WEBHOOK_URL",
        "http://127.0.0.1:8000/webhook/pipeline-failure",
    )
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, default=str).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return 200 <= response.status < 300
    except (OSError, urllib.error.URLError):
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
                write_failure_log(SparkSession.getActiveSession(), payload)
                notify_failure(payload)
                raise

        return cast(F, wrapped)

    return decorator
