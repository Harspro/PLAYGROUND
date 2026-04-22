---
 
# Dataproc Task Executor

## Overview
  The DataprocTaskExecutor dynamically creates Apache Airflow DAGs that submit Dataproc Serverless Spark batches using configuration provided in a YAML file. It reads environment-specific values from Airflow variables/files and builds the Dataproc batch (main class, jars, files, args, labels, properties) accordingly.

## Features

  - Dynamic DAG creation from YAML configuration
  - Dataproc Serverless Spark batch submission
  - Optional Spark args (dict or list format)
  - Environment placeholder replacement for `{env}` in config values
  - Automatic environment-specific network configuration (subnetwork URI, service account, network tags)
  - DAG-safe context (operators are attached within `with dag:` block)
  - Start and end EmptyOperator tasks for better DAG visualization
  - Sequential task dependencies
  - Batch ID includes DAG ID and timestamp, with `_` replaced by `-`

## Requirements

  Python: 3.8+
  Apache Airflow: 2.x
  Dependencies:
  - pendulum (timezone handling)
  - pyyaml (YAML parsing)
  - apache-airflow-providers-google (Dataproc operators)
  Custom modules: util.constants, util.miscutils, util.deploy_utils

## Configuration Directory

  Create the directory `config/dataproc_task_executor_configs` under `DAGS_FOLDER` and place your `dataproc_task_executor_config.yaml` there.

## YAML Configuration

  Minimal example:
  ```yaml
  create_dataproc_serverless_cluster_job:
    default_args:
      owner: "sharing-reusable-components"
      capability: "multiple areas"
      severity: "P3"
      retries: 3
      retry_delay: 300
    dag:
      dagrun_timeout: 120
      start_date: "2025-01-01"
      catchup: false
      description: "Example Dataproc Serverless job"
      tags: ["team-defenders-alerts"]
    spark_job:
      jar_file_uris:
        - gs://pcb-{env}-staging-artifacts/jarfiles/your-app.jar
      main_class: com.example.YourMain
      file_uris:
        - gs://pcb-{env}-staging-artifacts/resources/your-app/application.yaml
      # args is optional; can be a list or a dict
      # properties is optional; Spark configuration properties
      # args:
      #   key1: value1
      #   key2: value2
  ```

  **Required Fields:**
  - `spark_job.jar_file_uris`: List of JAR file URIs in GCS
  - `spark_job.main_class`: Main class for the Spark application

  **Optional Fields:**
  - `spark_job.file_uris`: List of additional file URIs (config files, schemas, etc.)
  - `spark_job.args`: Arguments passed to the Spark application (see below)
  - `spark_job.properties`: Spark configuration properties (dict)

  **Configuration Notes:**
  - `{env}` placeholder is automatically replaced with the current deployment environment (dev, uat, prod).
  - `spark_job.args` is optional. Supported forms:
    - **List**: `["k=v", "k2=v2"]` - passed through as-is
    - **Dict**: `{k: v}` - automatically converted to `["k=v", ...]` format
    - **Omitted**: An empty list is sent to Dataproc
  - If your application requires a config file as the first positional argument, include it in `spark_job.args` as the first list item.

  **Example with args as dict (converted to key=value list):**
  ```yaml
  create_dataproc_serverless_cluster_job:
    spark_job:
      jar_file_uris:
        - gs://pcb-{env}-staging-artifacts/jarfiles/your-app.jar
      main_class: com.example.YourMain
      args:
        app.mode: batch
        app.flag: true
  ```

  **Example with args as list (positional first):**
  ```yaml
  create_dataproc_serverless_cluster_job:
    spark_job:
      jar_file_uris:
        - gs://pcb-{env}-staging-artifacts/jarfiles/your-app.jar
      main_class: com.example.YourMain
      args:
        - application.yaml
        - app.mode=batch
  ```

## Launcher

  The launcher file `dags/dataproc_task_executor/dataproc_task_executor_launcher.py` is already present and creates DAGs from the YAML configuration:

  ```python
  from airflow import settings
  from dataproc_task_executor.dataproc_task_executor_base import DataprocTaskExecutor

  globals().update(
      DataprocTaskExecutor(
          "dataproc_task_executor_config.yaml",
          f"{settings.DAGS_FOLDER}/config/dataproc_task_executor_configs"
      ).create_dags()
  )
  ```

## Runtime Details

  - DAG structure: Each DAG includes start and end EmptyOperator tasks, with the Dataproc batch task in between.
  - Tasks are constructed inside the DAG context using a `with self.dag:` block so they appear in the Airflow UI.
  - Sequential dependencies: Tasks are linked sequentially (start → dataproc_batch → end).
  - `batch_id` is generated as:
    `"{{ dag.dag_id.replace('_', '-') }}-{YYYYMMDDHHMMSSffffff}`
  - Network configuration: Subnetwork URI, service account, and network tags are automatically configured based on the deployment environment using `util.miscutils.get_serverless_cluster_config()`:
    - Non-prod environments (dev, uat): Uses Shared VPC subnetwork with `{env}` placeholder replaced
    - Prod environment: Uses dedicated prod subnetwork URI
    - Service account format: `dataproc@pcb-{env}-processing.iam.gserviceaccount.com`
    - Network tags: Includes the configured network tag plus "processing"

## Error Handling

  - Raises `AirflowFailException` for invalid YAML structures (e.g., missing `spark_job`, missing `jar_file_uris`/`main_class`, invalid `args` type).
  - Logs batch configuration for troubleshooting via standard Airflow logging.

## Tips

  - If your Spark app expects positional args (e.g., config file first), pass `spark_job.args` as a list in the correct order.
  - Keep any required configuration file in `file_uris` and refer to it via args if your app requires it.
  - Network configuration (subnetwork, service account, tags) is automatically handled based on the deployment environment - no manual configuration needed.
  - The DAG will include start and end tasks for better visualization in the Airflow UI.

## DAG Structure

  Each generated DAG follows this structure:
  ```
  start (EmptyOperator)
    ↓
  create_dataproc_serverless_cluster (DataprocCreateBatchOperator)
    ↓
  end (EmptyOperator)
  ```

**Author:**  
Sharing Reusable Components Team
---