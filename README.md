# Brazilian Data ETL with AutoHeal Demo

This repository contains the Brazilian e-commerce Bronze ingestion job plus a focused PySpark Silver test path for the AutoHeal service.

## Databricks Locations

Bronze data and configuration are stored in these Databricks locations:

```text
`brazilian-e-commerce`.bronze.olist_customers_dataset
/Volumes/brazilian-e-commerce/bronze/raw_data/assets/csv/olist_customers_dataset.csv
/Volumes/brazilian-e-commerce/bronze/raw_data/builds/brazilian_data_er-0.1.2-py3-none-any.whl
/Volumes/brazilian-e-commerce/bronze/raw_data/configs/bronze.json
/Volumes/brazilian-e-commerce/bronze/raw_data/configs/config.json
```

Other Bronze datasets use the same table and CSV naming convention.

## AutoHeal Demo Tasks

The demo adds three declarative Silver tasks:

```text
silver_demo_customers -> silver_demo_orders -> silver_demo_quality_check
```

- `silver_demo_customers` creates and writes a small dummy customer DataFrame to `/Volumes/brazilian-e-commerce/bronze/raw_data/demo/silver/customers`.
- `silver_demo_orders` creates and writes a small dummy order DataFrame to `/Volumes/brazilian-e-commerce/bronze/raw_data/demo/silver/orders`.
- `silver_demo_quality_check` intentionally selects `missing_business_column` and raises a deterministic Spark `AnalysisException`.

The failing tasks use `@monitor_task` from `src/brazilian_data_er/utils/self_healing.py`.

On failure the decorator:

1. Captures the Databricks run, job, task, cluster, Spark error, and traceback metadata.
2. Checks whether `` `brazilian-e-commerce`.bronze.task_failure_logs `` exists.
3. Creates the Delta table with Change Data Feed enabled when it is missing.
4. Appends the event when the Databricks catalog is available.
5. Sends the event to the project AutoHeal webhook.
6. Re-raises the original Spark exception.

The ETL notifier requires no `.env` file or manually supplied run, job, or commit variables. It uses fixed project defaults and reads run, job, and cluster IDs from Spark configuration when Databricks exposes them. Branch selection, classification, RCA, validation, and PR creation remain responsibilities of the AutoHeal service.

## Local Demo

Start the AutoHeal service from the self-healing service repository, then build its validator image:

```bash
docker build -t autoheal-pyspark-validator:local /home/santhosh/project/self-healing-pipeline/sandbox
```

Run the ETL demo from this repository with the PySpark image. Set `DEMO_OUTPUT_PATH` to a local path when running outside Databricks; the default is the Databricks Volume path:

```bash
SPARK_LOCAL_HOSTNAME=localhost \
docker run --rm --network=host --read-only \
  --tmpfs '/tmp:rw,exec,nosuid,size=1g' \
  -v "$PWD:/workspace:ro" \
  -e PYTHONPATH=/workspace/src \
  -e SPARK_LOCAL_HOSTNAME \
  --entrypoint python autoheal-pyspark-validator:local -u \
  -m brazilian_data_er.silver.demo_tasks --task all
```

The command should exit with code `1` after reporting `UNRESOLVED_COLUMN` for `missing_business_column`. The webhook should receive HTTP `202` and the original Spark failure remains visible.

## Databricks Jobs

`databricks.yml` contains:

- `brazilian_ingestion`: the existing Bronze ingestion job using the Volume configuration and wheel path.
- `brazilian_self_healing_demo`: the three-task dummy Silver failure test.

The GitHub Actions workflow builds and uploads the wheel, uploads the two Bronze configuration files, validates/deploys the demo bundle, and retains the existing Bronze job deployment. The intentionally failing demo runs only when the workflow is manually dispatched with `run_demo=true`.

A Databricks cluster cannot reach a developer laptop through `127.0.0.1`. The current demo tunnel endpoint is `https://6bea-27-5-229-236.ngrok-free.app/webhook/pipeline-failure`; set `AUTOHEAL_WEBHOOK_URL` to a new reachable HTTPS service or tunnel URL when the tunnel changes.

## Build and Deploy

```bash
python -m pip install --upgrade pip build
```

```bash
python -m build
databricks bundle validate
databricks bundle deploy
```

The workflow requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` GitHub secrets. Use the manual `run_demo` input only when you want the expected failing test job to execute.
