import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from google.cloud import bigquery

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryTableRestore:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_table_restore_config.yaml'

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

    def table_restore_job(self, table_ref: str):
        backup_table_ref = table_ref + consts.BACK_UP_STR
        table_restore_ddl = f"""
                CREATE OR REPLACE TABLE
                    `{table_ref}`
                CLONE
                    {backup_table_ref};
        """

        logger.info(table_restore_ddl)
        bigquery.Client().query(table_restore_ddl).result()
        logger.info(f'Finished restoring {table_ref} from ')

    def restore_tables(self, table_configs: dict):
        task_groups = []
        table_list = table_configs.get(consts.TABLES)
        for table_ref in table_list:
            backup_table_ref = str(table_ref) + consts.BACK_UP_STR
            backup_table = bigquery.Table(backup_table_ref)
            table = bigquery.Table(table_ref)
            with TaskGroup(group_id=table.table_id) as tgrp:
                check_table = BigQueryTableExistenceSensor(
                    task_id=f'check_table_{backup_table.table_id}',
                    project_id=backup_table.project,
                    dataset_id=backup_table.dataset_id,
                    table_id=backup_table.table_id,
                    timeout=300,
                    mode='reschedule'
                )

                restore_table = PythonOperator(
                    task_id=f"restore_table_{table.table_id}",
                    python_callable=self.table_restore_job,
                    op_kwargs={'table_ref': table_ref, 'backup_table_ref': backup_table_ref}
                )
                check_table >> restore_table
                task_groups.append(tgrp)

        return task_groups

    def build_dag(self, dag_id: str, table_configs: dict) -> DAG:
        self.default_args['owner'] = table_configs.get(consts.DAG_OWNER)
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.datetime(2024, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            if not table_configs[consts.TABLES]:
                raise AirflowException('table_list is not provided.')
            task_groups = self.restore_tables(table_configs)

            start_point >> task_groups >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.tables_config:
            for job_id, table_configs in self.tables_config.items():
                dags[job_id] = self.build_dag(job_id, table_configs)

        return dags


globals().update(BigQueryTableRestore().create_dags())
