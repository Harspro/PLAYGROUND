import enum
import logging
from copy import deepcopy
from datetime import timedelta, datetime
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import (
    apply_row_transformation,
    create_external_table,
    apply_column_transformation,
    apply_join_transformation,
    apply_timestamp_transformation,
    parse_join_config,
    schema_preserving_load
)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag,
    split_table_name
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class LoadingFrequency(enum.Enum):
    Onetime = 1
    Daily = 2


BIGQUERY_LOADER: Final = 'bigquery_loader'

INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'team-centaurs',
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


class BigQueryLoader:
    def __init__(self, config_filename, loading_frequency: LoadingFrequency, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.loading_frequency = loading_frequency

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.config_file_path = f'{config_dir}/{config_filename}'
        self.job_config = read_yamlfile_env(self.config_file_path, self.deploy_env)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

        self.local_tz = pendulum.timezone('America/Toronto')

    def extract_tables_info(self, config: dict) -> list:
        tables_info = []

        if not isinstance(config, dict) or consts.BIGQUERY not in config:
            return tables_info

        bq_config = config[consts.BIGQUERY]
        dataset_id = bq_config.get(consts.DATASET_ID)
        table_name = bq_config.get(consts.TABLE_NAME)
        project_id = bq_config.get(consts.PROJECT_ID)

        if dataset_id and table_name:
            tables_info = [{
                'project_id': project_id,
                'dataset_id': dataset_id,
                'table_name': table_name
            }]

        return tables_info

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        logger.info(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))

        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)
        join_spec = transformation_config.get(consts.JOIN_SPECIFICATION)
        filter_rows = transformation_config.get(consts.FILTER_ROWS)

        if join_spec:
            join_parse_spec = parse_join_config(bigquery_client, join_spec, transformed_data.get(consts.ID),
                                                add_output_prefix=False)
            join_transform_view = apply_join_transformation(bigquery_client, transformed_data, join_parse_spec)
            transformed_data = join_transform_view
        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view

        if filter_rows:
            row_transform_view = apply_row_transformation(bigquery_client, transformed_data, filter_rows)
            transformed_data = row_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        return transformed_data

    @staticmethod
    def get_schedule_interval(config: dict):
        scheduling_interval = config.get(consts.DAG, {}).get(consts.SCHEDULE_INTERVAL, None)
        return scheduling_interval

    def build_fetching_job(self, cluster_name: str, config: dict):
        properties = config.get(consts.PROPERTIES) or consts.DEFAULT_SPARK_SETTINGS
        gcs_config = config[consts.GCS]
        schema = split_table_name(config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME), True)
        staging_folder = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{schema}/"
        if self.loading_frequency is LoadingFrequency.Daily:
            staging_folder = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{schema}/{datetime.today().strftime('%Y%m%d')}/"

        arg_list = [config[consts.APPLICATION_CONFIG], staging_folder]

        if consts.ARGS in config:
            for k, v in config[consts.ARGS].items():
                arg_list.append(f'{k}={v}')

        if not any(consts.DB_READ_PARALLELISM in arg for arg in arg_list):
            arg_list.append(consts.DB_READ_PARALLELISM + '=1')

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)
        return {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: config[consts.MAIN_CLASS],
                consts.FILE_URIS: config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: properties
            }
        }

    def build_loading_job(self, **context):
        config = context[consts.CONFIG]
        frequency = context[consts.LOADING_FREQUENCY]
        gcs_config = config[consts.GCS]
        bigquery_config = config[consts.BIGQUERY]
        bq_table_name = bigquery_config[consts.TABLE_NAME]
        schema, source_table_name = split_table_name(config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME))
        parquet_files_path = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{schema}/{source_table_name}/*.parquet"
        if frequency is LoadingFrequency.Daily:
            parquet_files_path = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{schema}/{datetime.today().strftime('%Y%m%d')}/{source_table_name}/*.parquet"

        logging.info(f"Starting import of data for table {bq_table_name} from {parquet_files_path}")

        bq_proc_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]

        bq_table_name = bigquery_config[consts.TABLE_NAME]
        bq_ext_table_id = f"{bq_proc_project_name}.{bigquery_config[consts.DATASET_ID]}.{bq_table_name}_DBEXT"
        project_id = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)

        bq_table_id = f"{project_id}.{bigquery_config[consts.DATASET_ID]}.{bq_table_name}"

        additional_columns = bigquery_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = bigquery_config.get(consts.DROP_COLUMNS)

        bq_client = bigquery.Client()

        transformation_config = {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
        }

        staged_view = self.apply_transformations(bq_client, transformation_config)

        # Extract partition and clustering config if present
        partition_field = bigquery_config.get('partition_field')
        clustering_fields = bigquery_config.get('clustering_fields')

        partition_str = f"PARTITION BY {partition_field}" if partition_field else ""
        clustering_str = f"CLUSTER BY {', '.join(clustering_fields)}" if clustering_fields else ""

        create_ddl = f"""
                    CREATE OR REPLACE TABLE `{bq_table_id}`
                    {partition_str}
                    {clustering_str}
                    AS
                    SELECT {staged_view.get('columns')}
                    FROM `{staged_view.get('id')}`
                """

        # Build INSERT statement
        insert_stmt = f"""
                    INSERT INTO `{bq_table_id}`
                    SELECT {staged_view.get('columns')}
                    FROM `{staged_view.get('id')}`
                """
        # The util function will choose TRUNCATE + INSERT or CREATE OR REPLACE based on table existence
        schema_preserving_load(create_ddl, insert_stmt, bq_table_id, bq_client=bq_client)

        target_table = bq_client.get_table(bq_table_id)
        logging.info(
            f"Finished import of data for table {bq_table_id} from {parquet_files_path}. Imported {target_table.num_rows} rows.")

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule_interval(config),
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            is_paused_upon_creation=True,
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            tags=config.get(consts.TAGS, [])
        )

        with dag:
            if config.get('dag') and config.get('dag').get('read_pause_deploy_config'):
                is_paused = read_pause_unpause_setting(BIGQUERY_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            is_sqlserver = config.get(consts.IS_SQLSERVER, None)
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                              job_size, is_sqlserver),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=cluster_name
            )

            fetching_task = DataprocSubmitJobOperator(
                task_id="fetch_data_task",
                job=self.build_fetching_job(cluster_name, config),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
            )

            loading_task = PythonOperator(
                task_id="load_data_task",
                python_callable=self.build_loading_job,
                op_kwargs={consts.CONFIG: config, consts.LOADING_FREQUENCY: self.loading_frequency},
            )

            start_point >> cluster_creating_task >> fetching_task >> loading_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
