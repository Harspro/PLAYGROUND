from datetime import timedelta
import logging
import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryTableRenameTool:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_table_rename_config.yaml'

        self.dag_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
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

    def rename_table(self, table_config: dict):
        project_id = table_config.get(consts.PROJECT_ID)
        dataset_id = table_config.get(consts.DATASET_ID)
        from_name = table_config.get(consts.FROM_NAME)
        to_name = table_config.get(consts.TO_NAME)

        from_table_ref = f'{project_id}.{dataset_id}.{from_name}'

        rename_stmt = f"""
               ALTER TABLE {from_table_ref}
               RENAME TO {to_name};
           """

        logger.info(rename_stmt)
        bigquery.Client().query(rename_stmt).result()

    def build_dag(self, dag_id: str, table_config: dict) -> DAG:
        logger.info(f'table_config : {table_config}')
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

            rename_table_task = PythonOperator(
                task_id='rename_table',
                python_callable=self.rename_table,
                op_kwargs={'table_config': table_config}
            )

            start_point >> rename_table_task >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.dag_config:
            for job_id, table_config in self.dag_config.items():
                dags[job_id] = self.build_dag(job_id, table_config)

        return dags


globals().update(BigQueryTableRenameTool().create_dags())
