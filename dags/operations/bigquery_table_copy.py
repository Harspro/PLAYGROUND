import logging
import pendulum
from copy import deepcopy
from typing import Final
from datetime import timedelta
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from google.cloud.bigquery.table import TableReference

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)

from util.bq_utils import copy_table
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

SOURCE_TABLE_REF: Final = 'source_table_ref'
TARGET_TABLE_REF: Final = 'target_table_ref'

"""
This is a tool dag that makes copies of bigquery tables.
"""


class BigQueryTableCopy:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_table_copy_config.yaml'

        self.table_copy_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
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

    def create_table_copying_tasks(self, table_configs: list):
        task_groups = []
        for table_config in table_configs:
            source_table_ref = table_config.get(SOURCE_TABLE_REF)
            target_table_ref = table_config.get(TARGET_TABLE_REF)
            source_table = TableReference.from_string(source_table_ref)
            target_table = TableReference.from_string(target_table_ref)
            with TaskGroup(group_id=source_table.table_id) as tgrp:
                table_checking_task = BigQueryTableExistenceSensor(
                    task_id=f'check_table_{source_table.table_id}',
                    project_id=source_table.project,
                    dataset_id=source_table.dataset_id,
                    table_id=source_table.table_id,
                    timeout=300,
                    mode='reschedule'
                )

                table_copying_task = PythonOperator(
                    task_id=f"create_table_{target_table.table_id}",
                    python_callable=copy_table,
                    op_kwargs={'source_table_ref': source_table_ref,
                               'target_table_ref': target_table_ref}
                )
                table_checking_task >> table_copying_task
                task_groups.append(tgrp)

        return task_groups

    def build_dag(self, dag_id: str, dag_config: dict) -> DAG:
        default_args = deepcopy(self.default_args)
        default_args['owner'] = dag_config.get(consts.DEFAULT_ARGS, {}).get(consts.DAG_OWNER)
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,
            start_date=pendulum.datetime(2025, 5, 1, tz=consts.TORONTO_TIMEZONE_ID),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            table_copying_tasks = self.create_table_copying_tasks(dag_config.get(consts.TABLES))

            start_point >> table_copying_tasks >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.table_copy_config:
            for job_id, dag_config in self.table_copy_config.items():
                dags[job_id] = self.build_dag(job_id, dag_config)

        return dags


globals().update(BigQueryTableCopy().create_dags())
