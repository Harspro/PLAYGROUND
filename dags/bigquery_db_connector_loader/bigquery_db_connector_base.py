"""
Bigquery to DB connector class.

Author: Sharif Mansour
"""
import logging

from copy import deepcopy
from datetime import timedelta, datetime
from typing import Final
from google.cloud import bigquery
import pendulum
import util.constants as consts

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pcf_operators.bigquery_operators.bigquery_to_postgres import PcfBigQueryToPostgresOperator

from util.bq_utils import run_bq_query, get_table_columns
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag, pause_unpause_dags
from util.miscutils import create_airflow_connection, read_file_env, read_variable_or_file, \
    read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args
as needed through configuration placed in the "".yaml file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


class BigQueryDbConnector:
    """
    BigQueryDbConnector class, creates Airflow DAGs that query BigQuery tables and writes
    the output to a DB outside of Terminus. Currently supports writing only to Postgres DB's.

    Methods -
        create_dag(): Defines the structure for the dynamically created DAGs.
        create_dags(): Runs through provided configuration in bigquery_db_connector_config.yaml
                    and dynamically creates DAGs based on them.
    """

    def __init__(self, config_filename: str, config_dir: str = None):
        """
        Constructor: sets up gcp/job configuration for class.

        :param config_filename: configuration file name.
        :type config_filename: str

        :param config_dir: configuration directory name, if none is provided will utilize
            the settings.DAGS_FOLDER.
        :type config_dir: str
        """

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/bigquery_db_connector_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    @staticmethod
    def run_bigquery_staging_query(query: str = None, project_id: str = None, dataset_id: str = None,
                                   table_id: str = None, source_project_id: str = None, source_dataset_id: str = None,
                                   query_file: str = None, deploy_env: str = None, replacements: dict = None,
                                   env_specific_replacements: bool = False, target_service_account: str = None,
                                   target_project: str = None):
        """
        Runs bigquery staging query to create staging table.

        :param query: Staging Query.
        :type query: str

        :param project_id: BigQuery project ID.
        :type project_id: str

        :param dataset_id: BigQuery dataset ID.
        :type dataset_id: str

        :param table_id: BigQuery table ID.
        :type table_id: str

        :param source_project_id: BigQuery table ID.
        :type source_project_id: str

        :param source_dataset_id: BigQuery dataset ID.
        :type source_dataset_id: str

        :param query_file: File to read query from.
        :type query_file: str

        :param deploy_env: Deployment environment.
        :type deploy_env: str

        :param replacements: Replacements dictionary for query.
        :type replacements: dict

        :param env_specific_replacements: Flag for environment specific replacements
        :type env_specific_replacements: bool
        """
        if query_file:
            query = read_file_env(query_file, deploy_env)

        if query:
            if replacements:
                logger.info(f'replacements : {replacements}')
                # Initialize final replacements with common replacements
                final_replacements = deepcopy(replacements.get('common', {}))
                # Merge in environment-specific replacements if flag is true and such replacements exist
                if env_specific_replacements:
                    env_replacements = replacements.get(deploy_env, {})
                    final_replacements.update(env_replacements)
                # If no env_specific_replacements, use replacements directly
                if not env_specific_replacements:
                    final_replacements.update(replacements)

                # Apply replacements to query
                logger.info(f'final_replacements : {final_replacements}')
                for placeholder, value in final_replacements.items():
                    query = query.replace(placeholder, value)

            logger.info("Running query provided to create staging table in GCP processing zone.")
            if source_project_id and source_dataset_id:
                logger.info(f"SOURCE_TABLE_ID: {source_project_id}.{source_dataset_id}.{table_id}")
                bq_client = bigquery.Client()
                table_columns_id = get_table_columns(bq_client, f"{source_project_id}.{source_dataset_id}.{table_id}")
                logger.info(f"COLUMNS : {table_columns_id.get(consts.COLUMNS)}")

                run_bq_query(
                    query.replace(f"{{{consts.STAGING_TABLE_ID}}}", f"{project_id}.{dataset_id}.{table_id}")
                    .replace(f"{{{consts.SOURCE_TABLE_ID}}}", f"{source_project_id}.{source_dataset_id}.{table_id}")
                    .replace(f"{{{consts.COLUMNS}}}", f"{table_columns_id.get(consts.COLUMNS)}"),
                    target_service_account,
                    target_project
                )
            else:
                run_bq_query(
                    query.replace(f"{{{consts.STAGING_TABLE_ID}}}", f"{project_id}.{dataset_id}.{table_id}"),
                    target_service_account,
                    target_project
                )
            logger.info(f"Query : {query}")
            logger.info("Query executed in GCP BigQuery.")
        else:
            logger.info("No query was provided.")

    @staticmethod
    def create_success_callback(pause_dags_at_end: list, unpause_dags_at_end: list):
        """
        Creates a success callback function with pre-evaluated configuration.

        :param pause_dags_at_end: List of DAG IDs to pause at end
        :param unpause_dags_at_end: List of DAG IDs to unpause at end
        :return: Callback function
        """
        def on_success_callback(context):
            """
            Callback function that executes after DAG completes successfully.
            Handles pausing IDL DAGs and unpausing daily DAGs.
            """
            dag_id = context['dag'].dag_id
            logger.info(f"DAG {dag_id} completed successfully. Executing pause/unpause operations.")

            # Unpause daily DAGs first
            if unpause_dags_at_end:
                logger.info(f"Unpausing daily DAGs: {unpause_dags_at_end}")
                pause_unpause_dags(unpause_dags_at_end, False)
            # Then pause IDL DAGs (including self)
            if pause_dags_at_end:
                logger.info(f"Pausing IDL DAGs: {pause_dags_at_end}")
                pause_unpause_dags(pause_dags_at_end, True)

        return on_success_callback

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
        Defines structure for the dynamically created DAGs, made of 5 tasks:
        1. Empty Operator, start point.
        2. Python Operator, executes query on source table and saves result to temporary table.
        3. Python Operator, executes creation of Airflow Connections.
        4. BigQuery To Postgres Operator, inserts contents of temporary table into target table.
        5. Empty Operator, end point.

        :param dag_id: cluster name based on DAG id.
        :type dag_id: str

        :param config: job configuration provided.
        :type config: dict

        :return: Created dag.
        :rtype: DAG
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(consts.DAGRUN_TIMEOUT) else 30

        # DAG pause/unpause configuration
        dag_pause_unpause_config = config.get(consts.DAG_PAUSE_UNPAUSE_CONFIG, {})
        self.pause_dags_at_end = dag_pause_unpause_config.get(consts.PAUSE_DAGS_AT_END, [])
        self.unpause_dags_at_end = dag_pause_unpause_config.get(consts.UNPAUSE_DAGS_AT_END, [])

        # Set up success callback if end pause/unpause is configured
        success_callback = None
        if self.pause_dags_at_end or self.unpause_dags_at_end:
            success_callback = self.create_success_callback(
                self.pause_dags_at_end,
                self.unpause_dags_at_end
            )

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description="DAG to utilize the BigQueryDbConnector class. This class facilitates the "
                        "ingestion of data from BigQuery into DB's.",
            render_template_as_native_obj=True,
            schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
            start_date=datetime(2024, 8, 1, tzinfo=self.local_tz),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=dag_timeout),
            is_paused_upon_creation=True,
            tags=config[consts.DAG].get(consts.TAGS),
            on_success_callback=success_callback
        )
        with dag:
            if config[consts.DAG].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # Empty Operator, used to indicate starting and ending tasks.
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            # DAG pause/unpause configuration for start tasks only
            # End tasks are handled by on_success_callback
            pause_dags_at_start = dag_pause_unpause_config.get(consts.PAUSE_DAGS_AT_START, [])
            unpause_dags_at_start = dag_pause_unpause_config.get(consts.UNPAUSE_DAGS_AT_START, [])

            env = self.deploy_env
            dataset_id = config[consts.BIGQUERY].get(consts.DATASET_ID)
            source_dataset_id = config[consts.BIGQUERY].get(consts.SOURCE_DATASET_ID)
            source_project_id = config[consts.BIGQUERY].get(consts.SOURCE_PROJECT_ID)
            env_specific_replacements = config.get(consts.ENV_SPECIFIC_REPLACEMENTS)
            replacements_dict = config.get(consts.REPLACEMENTS, {})
            table_id = config[consts.BIGQUERY].get(consts.TABLE_ID).replace(
                consts.CURRENT_DATE_PLACEHOLDER,
                datetime.now(tz=self.local_tz).strftime('%Y_%m_%d')
            )

            if config[consts.BIGQUERY].get(consts.QUERY):
                bq_pre_etl_query = config[consts.BIGQUERY].get(consts.QUERY)
            elif config[consts.BIGQUERY].get(consts.QUERY_FILE):
                bq_pre_etl_file = f"{settings.DAGS_FOLDER}/{config[consts.BIGQUERY].get(consts.QUERY_FILE)}"
                bq_pre_etl_query = read_file_env(bq_pre_etl_file, env)
            else:
                raise AirflowFailException("Please provide query, none found.")

            # DAG Pause/Unpause Tasks at Start
            start_tasks = []

            # Pause task at start
            if pause_dags_at_start:
                pause_dags_start_task = PythonOperator(
                    task_id=consts.PAUSE_DAGS_AT_START_TASK_ID,
                    python_callable=pause_unpause_dags,
                    op_kwargs={
                        'dag_ids': pause_dags_at_start,
                        'is_paused': True
                    }
                )
                start_tasks.append(pause_dags_start_task)

            # Unpause task at start
            if unpause_dags_at_start:
                unpause_dags_start_task = PythonOperator(
                    task_id=consts.UNPAUSE_DAGS_AT_START_TASK_ID,
                    python_callable=pause_unpause_dags,
                    op_kwargs={
                        'dag_ids': unpause_dags_at_start,
                        'is_paused': False
                    }
                )
                start_tasks.append(unpause_dags_start_task)

            # BigQuery Execute Query Task, used to query source table and create temporary staging BigQuery table.
            bigquery_execute_query_pre_etl_task = PythonOperator(
                task_id=consts.QUERY_BIGQUERY_TABLE_PRE_ETL_TASK_ID,
                python_callable=self.run_bigquery_staging_query,
                op_kwargs={
                    consts.QUERY: bq_pre_etl_query,
                    consts.PROJECT_ID: config[consts.BIGQUERY].get(consts.PROJECT_ID),
                    consts.DATASET_ID: dataset_id,
                    consts.TABLE_ID: table_id,
                    consts.SOURCE_PROJECT_ID: source_project_id,
                    consts.SOURCE_DATASET_ID: source_dataset_id,
                    consts.DEPLOY_ENV: env,
                    consts.REPLACEMENTS: replacements_dict,
                    consts.ENV_SPECIFIC_REPLACEMENTS: env_specific_replacements
                }
            )

            if config.get(consts.AIRFLOW_CONNECTION_CONFIG) and config.get(consts.POSTGRES):
                db_host = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.HOST)
                db_login = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.LOGIN)
                db_schema = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.SCHEMA)
                db_port = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.PORT)
                secret_path = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.VAULT_PASSWORD_SECRET_PATH)
                secret_key = config[consts.AIRFLOW_CONNECTION_CONFIG][env].get(consts.VAULT_PASSWORD_SECRET_KEY)

                # Create Airflow Connection Task, creates Airflow connection used when writing to DB.
                create_airflow_connection_task = PythonOperator(
                    task_id=consts.CREATE_AIRFLOW_CONNECTION,
                    python_callable=create_airflow_connection,
                    op_kwargs={
                        consts.CONN_ID: config[consts.AIRFLOW_CONNECTION_CONFIG].get(consts.CONN_ID),
                        consts.CONN_TYPE: config[consts.AIRFLOW_CONNECTION_CONFIG].get(consts.CONN_TYPE),
                        consts.DESCRIPTION: config[consts.AIRFLOW_CONNECTION_CONFIG].get(consts.DESCRIPTION),
                        consts.HOST: db_host,
                        consts.LOGIN: db_login,
                        consts.VAULT_PASSWORD_SECRET_PATH: secret_path,
                        consts.VAULT_PASSWORD_SECRET_KEY: secret_key,
                        consts.SCHEMA: db_schema,
                        consts.PORT: db_port
                    }
                )

                bq_post_etl_query = None
                if config[consts.BIGQUERY].get(consts.QUERY_FILE_POST_ETL):
                    bq_post_etl_file = f"{settings.DAGS_FOLDER}/{config[consts.BIGQUERY].get(consts.QUERY_FILE_POST_ETL)}"
                    bq_post_etl_query = read_file_env(bq_post_etl_file, env)

                # BigQuery To Postgres Task, used to write records from BigQuery into Postgres.
                bigquery_to_postgres_task = PcfBigQueryToPostgresOperator(
                    task_id=consts.BIGQUERY_TO_POSTGRES_TASK_ID,
                    dataset_table=f"{dataset_id}.{table_id}",
                    postgres_conn_id=config[consts.POSTGRES].get(consts.POSTGRES_CONN_ID),
                    target_table_name=config[consts.POSTGRES].get(consts.TARGET_TABLE_ID),
                    bigquery_batch_size=config[consts.BIGQUERY].get(consts.BATCH_SIZE),
                    sql_batch_size=config[consts.POSTGRES].get(consts.BATCH_SIZE),
                    selected_fields=config[consts.POSTGRES].get(consts.SELECT_FIELDS),
                    update_fields=config[consts.POSTGRES].get(consts.UPDATE_FIELDS),
                    conflict_fields=config[consts.POSTGRES].get(consts.CONFLICT_FIELDS),
                    append_fields=config[consts.POSTGRES].get(consts.APPEND_FIELDS),
                    execution_method=config[consts.POSTGRES].get(consts.EXECUTION_METHOD)
                )

                bigquery_execute_query_post_etl_task = PythonOperator(
                    task_id=consts.QUERY_BIGQUERY_TABLE_POST_ETL_TASK_ID,
                    python_callable=self.run_bigquery_staging_query,
                    op_kwargs={
                        consts.QUERY: bq_post_etl_query,
                        consts.DEPLOY_ENV: env,
                        consts.REPLACEMENTS: replacements_dict,
                        consts.ENV_SPECIFIC_REPLACEMENTS: env_specific_replacements
                    }
                )

                # Build task dependencies with proper start/end points
                if start_tasks:
                    # Start point -> pause/unpause tasks -> main ETL
                    if len(start_tasks) > 1:
                        # Multiple start tasks - run in parallel
                        start_point >> start_tasks
                        start_tasks >> bigquery_execute_query_pre_etl_task
                    else:
                        # Single start task
                        start_point >> start_tasks[0] >> bigquery_execute_query_pre_etl_task
                else:
                    # No start pause/unpause tasks - direct connection
                    start_point >> bigquery_execute_query_pre_etl_task

                # Main ETL flow
                bigquery_execute_query_pre_etl_task >> create_airflow_connection_task \
                    >> bigquery_to_postgres_task >> bigquery_execute_query_post_etl_task

                # End connection - always use end_point for consistency with other DAGs
                # on_success_callback handles additional pause/unpause operations after DAG completion
                bigquery_execute_query_post_etl_task >> end_point
            else:
                # Build task dependencies for simple case (no postgres config) with proper start/end points
                if start_tasks:
                    # Start point -> pause/unpause tasks -> main BigQuery task
                    if len(start_tasks) > 1:
                        # Multiple start tasks - run in parallel
                        start_point >> start_tasks
                        start_tasks >> bigquery_execute_query_pre_etl_task
                    else:
                        # Single start task
                        start_point >> start_tasks[0] >> bigquery_execute_query_pre_etl_task
                else:
                    # No start pause/unpause tasks - direct connection
                    start_point >> bigquery_execute_query_pre_etl_task

                # End connection - always use end_point for consistency with other DAGs
                # on_success_callback handles additional pause/unpause operations after DAG completion
                bigquery_execute_query_pre_etl_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        """
        Creates a dag for each provided configuration in the bigquery_db_connector_config.yaml file.

        :return: created dags.
        :rtype: dict
        """

        if self.job_config:
            dags = {}

            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

            return dags
        else:
            return {}
