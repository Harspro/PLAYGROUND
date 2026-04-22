import logging
import pendulum
from datetime import timedelta
from typing import List
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import convert_timestamp_to_datetime
from util.miscutils import read_variable_or_file, read_yamlfile_env, read_file_env
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryCDCViewCreator:
    """
    A class to manage the creation of CDC (Change Data Capture) views in BigQuery.

    This class provides methods to generate SQL queries and create views dynamically in BigQuery.
    It also constructs DAGs for each view configuration,
    checking table existence, and creating views.

    Attributes:
        gcp_config (dict): Configuration for GCP.
        deploy_env (str): Deployment environment.
        views_config (dict): Configuration for views to be created in BigQuery.
        view_files_dir (str): Directory containing SQL files for view creation.
        local_tz (pendulum.timezone): Timezone for scheduling the DAGs.
        default_args (dict): Default arguments for DAGs.
    """

    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_cdc_view_creator_config.yaml'

        self.views_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        self.view_files_dir = f'{config_dir}/bigquery_view_sql'
        self.dag_owner = None

        self.default_args = {
            'owner': None,
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            'depends_on_past': True,  # This ensures the current DAG run waits for the previous to complete successfully
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "execution_timeout": timedelta(minutes=10),
            "retries": 3,
            'retry_delay': timedelta(hours=1)
        }

    def generate_sql(self, bigquery_client, table_ref: str, uid_column: str) -> str:
        """
        Generates an optimized SQL query for BigQuery to create a CDC (Change Data Capture) view.

        This method generates SQL by:
        1. Retrieving and converting timestamp columns using `convert_timestamp_to_datetime`.
        2. Filtering out unnecessary columns.
        3. Constructing a SQL query to select the most recent records using ROW_NUMBER(),
           partitioned by `uid_column`, and excluding deleted records (where `optype` is not 'D').

        Args:
            bigquery_client (bigquery.Client): BigQuery client to interact with the service.
            table_ref (str): The full reference to the table (project.dataset.table).
            uid_column (str): The column used to identify unique records.

        Returns:
            str: The constructed SQL query.
        """
        # Step 1: Convert timestamp columns using helper function and BigQuery client.
        columns = convert_timestamp_to_datetime(bigquery_client, table_ref, column_mappings=None, tz='ODS')
        logger.info(f'Converted columns: {columns}')

        # Step 2: Filter out unnecessary columns.
        excluded_columns = {'optype', 'position', 'timestamp', 'currenttimestamp', 'REC_RANK'}
        columns = [col.strip() for col in columns.split(',') if col.strip().lower() not in excluded_columns]

        # Step 3: Join remaining columns into a single comma-separated string.
        columns_str = consts.COMMA_SPACE.join(columns)
        logger.info(f'Final selected columns: {columns_str}')

        # Step 4: Construct SQL query.
        sql = f"""
            SELECT {columns_str}
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {uid_column} ORDER BY currenttimestamp DESC) AS REC_RANK
                FROM `{table_ref}`
            )
            WHERE REC_RANK = 1 AND optype <> 'D'
        """
        return sql

    def create_view(self, table_ref: str, view_ref: str, uid_column: str, view_sql: str = None, view_file: str = None):
        """
        Creates a BigQuery view or updates an existing one using either a provided SQL or generated SQL.

        Args:
            table_ref (str): The reference to the table (project.dataset.table).
            view_ref (str): The reference to the view (project.dataset.view).
            uid_column (str): The column used to partition records.
            view_sql (str, optional): Predefined SQL for the view. Defaults to None.
            view_file (str, optional): Filename containing SQL for the view. Defaults to None.
        """
        dataset_table_id = table_ref.split('.', 1)[1]
        table_id = dataset_table_id.split('.', 1)[1].lower()

        bq_client = bigquery.Client()
        if view_file:
            view_sql = read_file_env(f'{self.view_files_dir}/{view_file}', self.deploy_env)

        view_query = view_sql or self.generate_sql(bq_client, table_ref, uid_column)

        # DDL updates the view schema.
        view_ddl = f"""
            CREATE OR REPLACE VIEW `{view_ref}`
            OPTIONS (
              labels= [('table_ref','{table_id}')]
            )
            AS
                {view_query}
        """
        logger.info(f'Executing View DDL: {view_ddl}')
        query_job = bq_client.query(view_ddl)
        query_job.result()

    def create_views(self, view_configs: list):
        """
        Creates views based on the provided configuration.

        Args:
            view_configs (List[dict]): List of configurations for views to be created.
        """
        task_groups = []

        for view_config in view_configs['tables']:
            table_ref = view_config.get('table_ref')
            view_ref = view_config.get('view_ref')
            uid_column = view_config.get('uid_column')

            view = bigquery.Table(view_ref)
            table = bigquery.Table(table_ref)
            view_sql = view_config.get(consts.VIEW_SQL)
            view_file = view_config.get(consts.VIEW_FILE)

            with TaskGroup(group_id=view.table_id) as tgrp:
                check_table = BigQueryTableExistenceSensor(
                    task_id='check_table',
                    project_id=table.project,
                    dataset_id=table.dataset_id,
                    table_id=table.table_id,
                    timeout=300,
                    mode='reschedule'
                )

                create_view = PythonOperator(
                    task_id="create_view",
                    python_callable=self.create_view,
                    op_kwargs={'table_ref': table_ref,
                               'view_ref': view_ref,
                               'uid_column': uid_column,
                               'view_sql': view_sql,
                               'view_file': view_file
                               }
                )

                check_table >> create_view

                task_groups.append(tgrp)

        return task_groups

    def build_dag(self, dag_id: str, view_configs: list) -> DAG:
        # logger.info(f'view_configs : {view_configs}')
        self.default_args['owner'] = view_configs['dag_owner']
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.datetime(2024, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5,
            tags=view_configs.get(consts.TAGS, [])
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            task_groups = self.create_views(view_configs)

            start_point >> task_groups >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.views_config:
            logger.info('view config is empty.')
            return dags

        for job_id, view_configs in self.views_config.items():
            dags[job_id] = self.build_dag(job_id, view_configs)

        return dags


globals().update(BigQueryCDCViewCreator().create_dags())
