from __future__ import annotations

import util.constants as consts
from airflow.models.dag import DAG, settings
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from util.miscutils import (
    read_variable_or_file,
    read_file_env
)

gcp_config = read_variable_or_file('gcp_config')
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
sql_file_name = f"{settings.DAGS_FOLDER}/scms_card_embossing/audit_log_table/sql/create_audit_log_table.sql"
sql = read_file_env(sql_file_name, deploy_env)
default_args = {
    "owner": "team-ogres-alerts",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    'capability': 'card embossing',
    'severity': 'P3',
    'sub_capability': 'SCMS',
    'business_impact': 'audit log table data not updated for logging',
    'customer_impact': 'audit log table data not updated for logging'
}

with DAG(
        description="DAG to create batch run log table",
        dag_id="card_embossing_audit_log_table",
        schedule="@once",
        start_date=datetime(2024, 5, 10),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        is_paused_upon_creation=True
) as dag:
    start = EmptyOperator(task_id='Start')
    end = EmptyOperator(task_id='End')
    create_or_update_table = BigQueryInsertJobOperator(
        task_id="create_audit_log_table",
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
    start >> create_or_update_table >> end
