---
# Postgres Query Executor

## Overview
```
    The PostgresQueryExecutor is a Python class designed to dynamically create Apache Airflow DAGs for executing SQL queries in a Postgres database. It uses Airflow's PythonOperator and a custom PcfPrePostEtlOperator to manage database connections and query execution. The class is configured via a YAML file and supports environment-specific database connections.
```

## Features

    Dynamic DAG Creation: Generates Airflow DAGs based on YAML configurations.
    Postgres Query Execution: Executes SQL queries in a Postgres database using PcfPrePostEtlOperator.
    Airflow Connection Management: Dynamically creates Airflow connections for Postgres databases.
    Environment-Specific Configurations: Supports dev, uat, and prod environments via YAML.
    Pause/Unpause Support: Configurable DAG pause settings based on deployment configurations.
    Error Handling: Raises AirflowFailException for invalid configurations or missing queries.

## Requirements

    Python: 3.8+
    Apache Airflow: 2.x
    Dependencies:
        - pendulum for timezone handling
        - pyyaml for YAML file parsing
    Custom utility modules: util.constants, util.deploy_utils, util.logging_utils, util.miscutils
    Custom operator: pcf_operators.bigquery_operators.bigquery_to_postgres.PcfPrePostEtlOperator



## Setup

    Install Airflow: pip install apache-airflow


    Install Additional Dependencies: pip install pendulum pyyaml


    Configure Airflow:
        - Ensure the Airflow environment is set up with a DAGS_FOLDER defined.
        - Place the postgres_query_executor module in the Airflow DAGS_FOLDER.


    Create Configuration Directory:
        - Create a directory config/postgres_query_executor_configs under DAGS_FOLDER.
        - Place the postgres_query_executor_config.yaml file in this directory.


    SQL Query Files:
        - Place SQL query files in the appropriate directory (e.g., bi_query_reverse_etl_postgres/pg_scripts/).



## Usage

    Define YAML Configuration:Create a postgres_query_executor_config.yaml file in the configuration directory. Example:
    reverse_etl_processing_create_views_postgres:
    default_args:
        owner: "team-defenders"
        capability: "Risk Management"
        sub_capability: "BiQuery Reverse ETL"
        severity: "P4"
        business_impact: "Reverse ETL views creation in postgres"
        customer_impact: "None"
    postgres:
        postgres_conn_id: reverse_etl_processing_create_views_postgres_connection
        pg_query_file: bi_query_reverse_etl_postgres/pg_scripts/bi_query_create_views.sql
    airflow_connection_config:
        conn_id: reverse_etl_processing_create_views_postgres_connection
        conn_type: postgres
        description: "BiQuery Reverse ETL views creation Postgres Connection"
        dev:
        host: pcbdvs-pg-piquery.nonprod.pcfcloud.io
        login: piquery_owner
        vault_password_secret_path: "/dev-secret/data/ffm/dataproc/gcpDev002/bigquerytopostgres-spark-batch"
        vault_password_secret_key: piquery.postgres.password
        schema: piquery
        port: 5432
        uat:
        host: pcbdvs-pg-piquery-uat.nonprod.pcfcloud.io
        login: piquery_owner
        vault_password_secret_path: "/uat-secret/data/ffm/dataproc/gcpStg001/bigquerytopostgres-spark-batch"
        vault_password_secret_key: piquery.postgres.password
        schema: piquery
        port: 5432
        prod:
        host: pcbprs-pg-piquery.prod.pcfcloud.io
        login: piquery_owner
        vault_password_secret_path: "/secret/data/biquery/bigquerytopostgres-spark-batch"
        vault_password_secret_key: piquery.postgres.password
        schema: piquery
        port: 5432
    dag:
        dagrun_timeout: 120
        tags: ["BiQueryReverseETL"]


    Launch the DAGs:Place the launcher script in the Airflow DAGS_FOLDER:
        ```
            from airflow import settings
            from postgres_query_executor.postgres_query_executor_base import PostgresQueryExecutor

            globals().update(
                PostgresQueryExecutor(
                    'postgres_query_executor_config.yaml',
                    f'{settings.DAGS_FOLDER}/config/postgres_query_executor_configs'
                ).create_dags()
            )
        ```

    Airflow will automatically detect and schedule the DAGs based on the configuration.

    Monitor DAGs:

        - Use the Airflow UI to monitor the DAGs and their tasks.
        - Check logs for any errors related to connection creation or query execution.



## Configuration Details

    default_args: Overrides for Airflow DAG default arguments (e.g., owner, on_failure_callback).
    postgres:
    postgres_conn_id: Connection ID for the Postgres database.
    pg_query: Direct SQL query (optional).
    pg_query_file: Path to the SQL query file (relative to DAGS_FOLDER).


    airflow_connection_config:
    conn_id: Airflow connection ID.
    conn_type: Type of connection (e.g., postgres).
    description: Description of the connection.
    Environment-specific settings (dev, uat, prod):
    host: Database host.
    login: Database login username.
    vault_password_secret_path: Path to the secret in the vault.
    vault_password_secret_key: Key for the password in the vault.
    schema: Database schema.
    port: Database port.




    dag:
    dagrun_timeout: Maximum runtime for the DAG (in minutes).
    schedule_interval: Cron expression or Airflow schedule for DAG execution.
    start_date: Start date for the DAG.
    catchup: Whether to backfill missed DAG runs (default: false).
    tags: Tags for categorizing the DAG in Airflow UI.
    read_pause_deploy_config: Whether to read pause/unpause settings from deployment configuration.



## Error Handling

    The class raises AirflowFailException if:
        - The configuration file cannot be read or parsed.
        - No SQL query or query file is provided.
        - The Airflow connection configuration is invalid.



## Logging

    - Uses the logging module with a logger named __name__.
    - Logs connection creation and query execution details.

## Example Workflow
    The provided YAML configuration creates a DAG named reverse_etl_processing_create_views_postgres that:

        - Starts with an EmptyOperator (start_task).
        - Creates an Airflow connection for the Postgres database using     create_airflow_connection.
        - Executes an SQL query from bi_query_create_views.sql using PcfPrePostEtlOperator.
        - Ends with an EmptyOperator (end_task).

## Notes

    - Ensure the SQL query file exists in the specified path relative to DAGS_FOLDER.
    - The configuration directory path can be customized by passing config_dir to the PostgresQueryExecutor constructor.
    - The class assumes the presence of a GCP_CONFIG variable or file for environment-specific settings.
    - The PcfPrePostEtlOperator must be available in the Airflow environment.


---