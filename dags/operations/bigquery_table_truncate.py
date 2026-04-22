import logging
from datetime import timedelta

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import build_back_up_job
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryTableTruncate:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_table_truncate_config.yaml'

        self.tables_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

        self.default_args = {
            'owner': None,
            'capability': 'TBD',
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

    def truncate_table(self, table_ref: str):
        truncate_stmt = f'TRUNCATE TABLE `{table_ref}`;'

        logger.info(f'Truncating table {table_ref} using SQL: {truncate_stmt}')
        bigquery.Client().query(truncate_stmt).result()

    def truncate_tables(self, table_configs):
        task_groups = []
        table_list = []
        backup_enabled = False
        if isinstance(table_configs, dict):
            if (consts.BACKUP_ENABLED in table_configs) and str(table_configs.get(consts.BACKUP_ENABLED, '')).lower() == 'true':
                backup_enabled = True
            table_list = table_configs.get(consts.TABLES)
        elif isinstance(table_configs, list):
            table_list = table_configs
        for table_ref in table_list:
            table = bigquery.Table(table_ref)
            with TaskGroup(group_id=table.table_id) as tgrp:
                check_table = BigQueryTableExistenceSensor(
                    task_id=f'check_table_{table.table_id}',
                    project_id=table.project,
                    dataset_id=table.dataset_id,
                    table_id=table.table_id,
                    timeout=300,
                    mode='reschedule'
                )

                back_up_original_table = PythonOperator(
                    task_id=f'back_up_{table.table_id}',
                    python_callable=build_back_up_job,
                    op_kwargs={'table_ref': table_ref, 'backup_enabled': backup_enabled}
                )

                truncate_table = PythonOperator(
                    task_id=f"truncate_table_{table.table_id}",
                    python_callable=self.truncate_table,
                    op_kwargs={'table_ref': table_ref}
                )
                check_table >> back_up_original_table >> truncate_table
                task_groups.append(tgrp)

        return task_groups

    def build_dag(self, dag_id: str, table_configs: dict) -> DAG:
        self.default_args['owner'] = table_configs.get(consts.DAG_OWNER)
        tags = table_configs.get(consts.TAGS)
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.datetime(2024, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            tags=tags,
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            if not table_configs.get(consts.TABLES):
                raise AirflowException('table_list is not provided.')
            task_groups = self.truncate_tables(table_configs)

            start_point >> task_groups >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.tables_config:
            for job_id, table_configs in self.tables_config.items():
                dags[job_id] = self.build_dag(job_id, table_configs)

        return dags


globals().update(BigQueryTableTruncate().create_dags())
