import json
import logging
from copy import deepcopy
from datetime import datetime
from typing import List
import pendulum

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import run_bq_query, create_backup_table
from util.miscutils import save_job_to_control_table
from dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Data Patch on BigQuery',
    'severity': 'P3',
    'sub_capability': 'BigQuery',
    'business_impact': 'N/A',
    "retries": 0
}


def get_string_columns(table_ref: str) -> List[str]:
    logger.info(f"Getting string columns for table: {table_ref}")

    bq_client = bigquery.Client()
    table = bq_client.get_table(table_ref)
    string_columns = []

    for field in table.schema:
        if field.field_type == 'STRING':
            string_columns.append(field.name)

    logger.info(f"""Found {len(string_columns)} string columns in {table_ref}
                    String columns: {string_columns}""")

    return string_columns


def build_update_query(table_ref: str, string_columns: List[str]) -> str:
    if not string_columns:
        error_msg = f"No string columns found for table {table_ref}. Cannot proceed with empty string to NULL conversion."
        logger.error(error_msg)
        raise AirflowFailException(error_msg)

    logger.info(f"""Building UPDATE query for table: {table_ref}
                    Processing {len(string_columns)} string columns""")

    set_clauses = []
    for column in string_columns:
        set_clause = f"{column} = NULLIF(TRIM({column}), '')"
        set_clauses.append(set_clause)

    set_clause = ",\n  ".join(set_clauses)

    update_query = f"""
    UPDATE `{table_ref}`
    SET
      {set_clause}
      WHERE 1=1;
    """

    logger.info(f"""Built update query for table {table_ref} with {len(string_columns)} string columns
                    UPDATE query:
                    {update_query}""")

    return update_query


class EmptyStringToNullPatcher(BaseDagBuilder):
    def create_table_backup(self, table_ref: str) -> str:
        project_id, dataset_id, table_id = table_ref.split('.')

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_table_id = f"{table_id}_backup_{timestamp}"
        backup_table_ref = f"{project_id}.{dataset_id}.{backup_table_id}"

        create_backup_table(backup_table_ref, table_ref)

        logger.info(f"Backup created successfully: {backup_table_ref}")
        return backup_table_ref

    def execute_update_query(self, table_ref: str) -> str:
        try:
            logger.info(f"""Starting update process for table: {table_ref}
                            Step 1: Fetching String data_type columns...""")
            string_columns = get_string_columns(table_ref)

            logger.info("Step 2: Building UPDATE Query...")
            update_query = build_update_query(table_ref, string_columns)

            logger.info(f"""Step 3: Executing UPDATE Query...
                            Number of columns to update: {len(string_columns)}""")

            job_id = run_bq_query(update_query)

            logger.info(f"""Update completed successfully!
                            Job_Id: {job_id}
                            Updated {len(string_columns)} string columns""")

            return f"Update completed for {table_ref}"

        except Exception as e:
            raise AirflowFailException(f"Failed to execute update query: {str(e)}")

    def build_control_record_saving_job(self, table_ref: str, **context):
        backup_table_ref = context['task_instance'].xcom_pull(task_ids='create_table_backup')

        job_params_str = json.dumps({
            'source_table_ref': table_ref,
            'backup_table_ref': backup_table_ref
        })

        save_job_to_control_table(job_params_str, **context)

    def build(self, dag_id: str, config: dict) -> DAG:
        table_ref = config.get(consts.TABLE_REF)
        schedule = config.get("dag_schedule", None)
        tags = config.get("tags", [])

        if not table_ref:
            raise AirflowFailException(f"table_ref is required in dag_config for {dag_id}")

        logger.info(f"Creating DAG for {dag_id} with table: {table_ref}")

        default_args = deepcopy(DAG_DEFAULT_ARGS)
        default_args.update(config.get(consts.DEFAULT_ARGS, {}))

        with DAG(
                dag_id=dag_id,
                default_args=default_args,
                schedule=schedule,
                start_date=pendulum.datetime(2025, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
                catchup=False,
                max_active_runs=1,
                is_paused_upon_creation=True,
                tags=tags
        ) as dag:

            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            create_backup_task = PythonOperator(
                task_id='create_table_backup',
                python_callable=self.create_table_backup,
                op_kwargs={'table_ref': table_ref}
            )

            execute_update_task = PythonOperator(
                task_id='execute_update_query',
                python_callable=self.execute_update_query,
                op_kwargs={'table_ref': table_ref}
            )

            control_table_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'table_ref': table_ref}
            )

            start_point >> create_backup_task >> execute_update_task >> control_table_task >> end_point

        return dag


globals().update(DAGFactory().create_dynamic_dags(EmptyStringToNullPatcher, 'empty_string_null_data_patch_config.yaml', f'{settings.DAGS_FOLDER}/bq_data_patch/config'))
