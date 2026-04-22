from __future__ import annotations

import util.constants as consts
from airflow.models.dag import DAG, settings
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from datetime import datetime, timedelta

from util.miscutils import (
    read_variable_or_file,
    read_file_env
)

gcp_config = read_variable_or_file('gcp_config')
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
sql_file_name = f"{settings.DAGS_FOLDER}/money_movement/batch_tables/batch_run_log/sql/01_create_batch_run_log_table.sql"
sql = read_file_env(sql_file_name, deploy_env)
default_args = {
    "owner": "team-money-movement-eng",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    'capability': 'Payments',
    'severity': 'P3',
    'sub_capability': 'EMT',
    'business_impact': 'Batch run log table data not updated for logging',
    'customer_impact': 'This is not customer facing and used for batch audit'
}

with DAG(
        description="DAG to create batch run log table",
        dag_id="batch_run_log_table",
        schedule="@once",
        start_date=datetime(2024, 5, 10),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=["gold-alchemists"],
        is_paused_upon_creation=True
) as dag:
    create_or_update_table = BigQueryInsertJobOperator(
        task_id="create_batch_run_log_table",
        project_id=f"pcb-{deploy_env}-landing",
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=gcp_config.get(consts.BQ_QUERY_LOCATION)
    )
