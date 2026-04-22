import logging
import pendulum

import util.constants as consts

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from copy import deepcopy
from datetime import timedelta, datetime
from typing import Final

from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_file_env, read_variable_or_file, read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args as needed through configuration
placed in the bigquery_table_insert_config.yaml file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False
}


class BigQueryTableInsert:
    """
    BigQueryTableInsert class, creates Airflow DAGs based on configuration to insert data into a bigquery table
    originating from a separate table.

     Methods:
        create_dag(): Defines the structure for the dynamically created DAGs.
        create_dags(): Runs through provided configuration in bigquery_table_insert_config.yaml and dynamically
                       creates DAGs based on them.
    """
    def __init__(self, config_filename: str, config_dir: str = None):
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        if self.job_config is None:
            self.job_config = {}

        self.processing_zone_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
        Defines structure for the dynamically created DAGs, made of 3 tasks:
        1. Empty Operator start point.
        2. Bigquery Insert Job Operator, inserts data into bigquery table based on provided query.
        3. Empty Operator end point.

        Args:
            dag_id (str): unique identifier to be used.
            config (dict): job configuration provided.
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description="DAG to utilize the BigQueryInsertJobOperator for inserting data into bigquery table based on "
                        "provided query.",
            render_template_as_native_obj=True,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            is_paused_upon_creation=True
        )

        with dag:
            if config[consts.DAG].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)
            end = EmptyOperator(task_id=consts.END_TASK_ID)

            project_id = config[consts.BIGQUERY].get(consts.PROJECT_ID)
            dataset_id = config[consts.BIGQUERY].get(consts.DATASET_ID)
            table_name = config[consts.BIGQUERY].get(consts.TABLE_NAME)
            location = config[consts.BIGQUERY].get(consts.LOCATION)
            use_legacy_sql = config[consts.BIGQUERY].get(consts.USE_LEGACY_SQL, False)

            if config[consts.BIGQUERY].get(consts.INSERT_QUERY):
                insert_query = config[consts.BIGQUERY].get(consts.INSERT_QUERY)
            elif config[consts.BIGQUERY].get(consts.QUERY_FILE):
                query_sql_file = f"{settings.DAGS_FOLDER}/{config[consts.BIGQUERY].get(consts.QUERY_FILE)}"
                insert_query = read_file_env(query_sql_file, self.deploy_env)
            else:
                raise AirflowFailException("Please provide query, none found.")

            insert_bigquery_table = BigQueryInsertJobOperator(
                task_id=consts.INSERT_BIGQUERY_TABLE_TASK_ID,
                configuration={
                    consts.QUERY: {
                        consts.QUERY: insert_query,
                        "useLegacySql": use_legacy_sql,
                        "destinationTable": {
                            "projectId": project_id,
                            "datasetId": dataset_id,
                            "tableId": table_name,
                        },
                        "writeDisposition": "WRITE_APPEND",
                    }
                },
                location=location,
            )

            start >> insert_bigquery_table >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        if self.job_config:
            dags = {}

            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

            return dags
        else:
            return {}


globals().update(BigQueryTableInsert('bigquery_table_insert_config.yaml').create_dags())
