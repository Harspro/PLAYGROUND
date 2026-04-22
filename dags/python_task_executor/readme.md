---

# Python Task Executor

## Overview
  The PythonTaskExecutor is a Python class designed to dynamically create Apache Airflow DAGs for executing Python callables based on configurations specified in a YAML file. It leverages Airflow's PythonOperator to execute Python functions and supports sequential task dependencies. The class is particularly useful for automating workflows that involve executing Python scripts in a controlled, scheduled manner.

## Features

  Dynamic DAG Creation: Generates Airflow DAGs based on YAML configurations.
  Python Callable Execution: Executes Python functions specified in the configuration using importlib.
  Sequential Task Dependencies: Supports sequential execution of tasks within a DAG.
  Configurable Default Arguments: Allows customization of DAG default arguments via YAML.
  Error Handling: Raises AirflowFailException for invalid configurations or missing Python callables.
  Environment-Specific Configurations: Reads environment-specific settings using utility functions.

## Requirements

  Python: 3.8+
  Apache Airflow: 2.x
  Dependencies:
  - pendulum for timezone handling
  - pyyaml for YAML file parsing
  Custom utility modules: util.constants, util.logging_utils, util.miscutils


Configure Airflow:
  - Ensure the Airflow environment is set up with a DAGS_FOLDER defined.
  - Place the python_task_executor module in the Airflow DAGS_FOLDER.


Create Configuration Directory:
  - Create a directory config/python_task_executor_configs under DAGS_FOLDER.
  - Place the python_task_executor_config.yaml file in this directory.



## Usage

  Define YAML Configuration:Create a python_task_executor_config.yaml file in the configuration directory. Example:
  pcma_cdic_0130_file_consolidation_job:
    default_args:
      owner: "team-defenders-alerts"
      capability: "risk-management"
      sub_capability: "deposit-insurance-cdic"
      severity: "P2"
      business_impact: "Compliance"
      customer_impact: "No Impact"
      retries: 3
      retry_delay: 300
    dag:
      dagrun_timeout: 120
      start_date: "2025-01-01"
      catchup: false
      description: "PCMA CDIC pcb-{env}-curated.cots_cdic.PCMA_CDIC_0130 file consolidation."
      tags: [ "team-defenders-alerts" ]
    tasks:
      - task_id: "file_0130_consolidation"
        python_callable:
          module: util.miscutils
          method: compose_infinite_files_into_one
        args:
          source_config: {"bucket": "pcb-{env}-staging-extract", "folder_prefix": "cdic/pcma/extract", "file_prefix": "cdic-0130-"}
          dest_config: {"bucket": "cdic-outbound-pcb{env_suffix}", "folder_prefix": "pcma", "file_name": "PCBA{{ dag_run.conf['date_prefix'] }}01302310002.txt"}
          win_compatible: true
          replace_empty_with_blank: true


  Launch the DAGs:Place the launcher script in the Airflow DAGS_FOLDER:
    from airflow import settings
    from python_task_executor.python_task_executor_base import PythonTaskExecutor

    globals().update(
        PythonTaskExecutor(
            "python_task_executor_config.yaml",
            f"{settings.DAGS_FOLDER}/config/python_task_executor_configs"
        ).create_dags()
    )

    Airflow will automatically detect and schedule the DAGs based on the configuration.

  Monitor DAGs:

    - Use the Airflow UI to monitor the DAGs and their tasks.
    - Check logs for any errors related to module imports or task execution.



## Configuration Details

  default_args: Overrides for Airflow DAG default arguments (e.g., owner, retries, on_failure_callback).
  dag:
  dagrun_timeout: Maximum runtime for the DAG (in minutes).
  schedule_interval: Cron expression or Airflow schedule for DAG execution.
  start_date: Start date for the DAG.
  catchup: Whether to backfill missed DAG runs (default: false).
  description: Description of the DAG.
  tags: Tags for categorizing the DAG in Airflow UI.


  tasks:
  task_id: Unique identifier for the task.
  python_callable: Specifies the Python module and method to execute.
  args: Arguments to pass to the Python callable.



## Error Handling

  The class raises AirflowFailException if:
    - The configuration file cannot be read or parsed.
    - The specified Python module or method is invalid.
    - No Python callable is provided in the task configuration.



## Logging

  - Uses the logging module with a logger named __name__.
  - Logs method import attempts and completions.

## Example Workflow
  The provided YAML configuration creates a DAG named pcma_cdic_0130_file_consolidation_job that:

  - Starts with an EmptyOperator (start_task).
  - Executes the compose_infinite_files_into_one function from util.miscutils to      consolidate files.
  - Ends with an EmptyOperator (end_task).

## Notes

  - Ensure the specified Python modules and methods are available in the Airflow environment.
  - The configuration directory path can be customized by passing config_dir to the PythonTaskExecutor constructor.
  - The class assumes the presence of a GCP_CONFIG variable or file for environment-specific settings.

**Author:**  
Sarik Bansal
---