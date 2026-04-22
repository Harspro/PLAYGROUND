import json
import logging
from copy import deepcopy

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import build_back_up_job, check_bq_table_exists
from util.miscutils import save_job_to_control_table
from dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder

logger = logging.getLogger(__name__)

DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    'capability': 'Account Management - Digital Adoption',
    'severity': 'P3',
    'sub_capability': 'NA',
    'business_impact': 'cleaning up Dag  for account master monthly file data',
    'customer_impact': 'Delay in loading business data',
    "retries": 0
}


class BigQueryBulkDataCleanupBuilder(BaseDagBuilder):

    def remove_records(self, table_ref: str, filter_condition: str):
        delete_stmt = f'DELETE FROM `{table_ref}` WHERE {filter_condition};'
        count_stmt = f'SELECT COUNT(*) AS REC_COUNT FROM `{table_ref}` WHERE {filter_condition};'
        client = bigquery.Client()

        logger.info(f'Fetching no. of records for clean-up using SQL: {count_stmt}')
        count_query_result = client.query(count_stmt).result()
        rec_count = next(count_query_result).get('REC_COUNT')
        logger.info(f"""Number of records to be deleted: {rec_count}
                        Executing cleanup using SQL: {delete_stmt}""")
        client.query(delete_stmt).result()

        logger.info(f'Successfully deleted {rec_count} records from {table_ref}')

    def check_table_and_create_backup(self, table_ref: str):
        logger.info(f'Checking if table exists: {table_ref}')
        exists = check_bq_table_exists(table_ref)
        if not exists:
            raise AirflowFailException(f'Table {table_ref} does not exist')
        logger.info(f"""Table {table_ref} exists
                         Creating backup for table: {table_ref}""")
        build_back_up_job(table_ref, backup_enabled=True)
        logger.info(f'Backup created successfully for {table_ref}')

        return exists

    def build_control_record_saving_job(self, filter_condition: str, **context):
        job_params_str = json.dumps({
            'operation': 'bigquery_bulk_data_cleanup',
            'filter': filter_condition
        })
        save_job_to_control_table(job_params_str, **context)

    def build(self, dag_id: str, config: dict) -> DAG:

        filter_condition = config.get('filter')
        bigquery_config = config.get('bigquery', {})

        project_id = bigquery_config.get('project_id')
        dataset_id = bigquery_config.get('dataset_id')
        table_ids = bigquery_config.get('table_id', [])

        if not project_id or not dataset_id or not table_ids:
            raise AirflowFailException(f'Missing required fields in config for {dag_id}')

        if not filter_condition:
            raise AirflowFailException(f'Filter Condition is required in config for {dag_id}')

        logger.info(f"Creating DAG for {dag_id} with {len(table_ids)} tables")

        default_args = deepcopy(DAG_DEFAULT_ARGS)
        default_args.update(config.get(consts.DEFAULT_ARGS, {}))

        with DAG(
                dag_id=dag_id,
                default_args=default_args,
                schedule=None,
                start_date=pendulum.datetime(2025, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
                catchup=False,
                max_active_runs=1,
                is_paused_upon_creation=True,
                max_active_tasks=10,
                tags=config.get("tags", [])
        ) as dag:

            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            # Create TaskGroups for each table
            table_task_groups = []

            for table_id in table_ids:
                table_ref = f"{project_id}.{dataset_id}.{table_id}"

                with TaskGroup(group_id=f"process_{table_id.lower()}", dag=dag) as table_group:

                    create_backup_task = PythonOperator(
                        task_id='create_backup_table',
                        python_callable=self.check_table_and_create_backup,
                        op_kwargs={
                            'table_ref': table_ref
                        }
                    )

                    data_cleanup_task = PythonOperator(
                        task_id='data_cleanup',
                        python_callable=self.remove_records,
                        op_kwargs={
                            'table_ref': table_ref,
                            'filter_condition': filter_condition
                        }
                    )

                    create_backup_task >> data_cleanup_task

                table_task_groups.append(table_group)

            control_table_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={
                    'filter_condition': filter_condition
                }
            )

            start_point >> table_task_groups >> control_table_task >> end_point

        return dag


globals().update(DAGFactory().create_dynamic_dags(BigQueryBulkDataCleanupBuilder, 'bigquery_bulk_data_cleanup_config.yaml', f'{settings.DAGS_FOLDER}/config'))
