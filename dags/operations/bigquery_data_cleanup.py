import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from google.cloud import bigquery

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)
from util.bq_utils import build_back_up_job
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryDataCleanUp:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_data_cleanup_config.yaml'

        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

        self.default_args = {
            'owner': 'team-centaurs',
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Terminus Data Platform',
            'business_impact': 'NA',
            'customer_impact': 'NA',
            'depends_on_past': False,
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "execution_timeout": timedelta(minutes=10),
            "retries": 1,
            'retry_delay': timedelta(minutes=3)
        }

    def remove_records(self, table_ref: str, filter: str):
        delete_stmt = f'DELETE FROM `{table_ref}` WHERE {filter};'
        count_stmt = f'SELECT COUNT(*) AS REC_COUNT FROM `{table_ref}` WHERE {filter};'
        client = bigquery.Client()
        logger.info(f'Fetching no. of records for clean-up using SQL: {count_stmt}')
        count_query_result = client.query(count_stmt).result()
        rec_count = next(count_query_result).get('REC_COUNT')
        logger.info(f'Number of records to be deleted : {rec_count}')

        logger.info(f'doing clean up using SQL: {delete_stmt}')
        client.query(delete_stmt).result()

    def validate_filter(self, yaml_filter: str = None, **context):
        """Validate filter conditions from YAML and DAG parameters."""
        task_instance = context['task_instance']
        dag_run = context['dag_run']

        # Get filter from DAG parameters (if provided at runtime)
        param_filter = dag_run.conf.get('filter') if dag_run.conf else None

        # Validation logic
        if yaml_filter:
            if param_filter:
                if yaml_filter == param_filter:
                    # Both are provided and equal; use yaml_filter as required
                    filter_to_use = yaml_filter
                else:
                    # Both are provided and are different; use param_filter
                    filter_to_use = param_filter
            else:
                # Only yaml_filter is provided; use it
                filter_to_use = yaml_filter
        elif param_filter:
            # Only param_filter is provided; use it
            filter_to_use = param_filter
        else:
            # Neither filter is provided; raise an exception
            raise AirflowException("No filter provided in YAML or DAG parameters.")

        if filter_to_use.isspace() or len(str(filter_to_use)) == 0:
            raise AirflowException('filter is not set.')

        logging.info(f'Filter to be used for cleanup : {filter_to_use}')

        # Push the validated filter to XCom
        task_instance.xcom_push(key='filter_to_use', value=filter_to_use)

    def build_dag(self, dag_id: str, table_ref: str, yaml_filter: str, backup_enabled: bool = False) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.datetime(2024, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5,
            params={
                "filter": ""
            }
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            if not table_ref or table_ref.isspace():
                raise AirflowException('table_ref is not set.')

            table = bigquery.Table(table_ref)

            # Task to check if the target table exists in BigQuery
            check_table = BigQueryTableExistenceSensor(
                task_id=f'check_table_{table.table_id}',
                project_id=table.project,
                dataset_id=table.dataset_id,
                table_id=table.table_id,
                timeout=300,
                mode='reschedule'
            )

            # Task to validate filter from YAML and DAG params
            validate_filter_task = PythonOperator(
                task_id='validate_filter',
                python_callable=self.validate_filter,
                op_kwargs={
                    'yaml_filter': yaml_filter
                }
            )

            back_up_original_table = PythonOperator(
                task_id=f"back_up_{table.table_id}",
                python_callable=build_back_up_job,
                op_kwargs={
                    'table_ref': table_ref,
                    'backup_enabled': backup_enabled
                }
            )

            cleanup_task = PythonOperator(
                task_id=f"cleanup_{table.table_id}",
                python_callable=self.remove_records,
                op_kwargs={
                    'table_ref': table_ref,
                    'filter': "{{ task_instance.xcom_pull(task_ids='validate_filter', key='filter_to_use') }}"
                }
            )

            start_point >> check_table >> validate_filter_task >> back_up_original_table >> cleanup_task >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                backup_enabled = False
                if (consts.BACKUP_ENABLED in config) and str(config.get(consts.BACKUP_ENABLED, '')).lower() == 'true':
                    backup_enabled = True
                dags[job_id] = self.build_dag(job_id, config.get(consts.TABLE_REF), config.get(consts.FILTER), backup_enabled)

        return dags


globals().update(BigQueryDataCleanUp().create_dags())
