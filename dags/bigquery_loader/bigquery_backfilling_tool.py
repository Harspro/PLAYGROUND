import logging
from datetime import timedelta
from copy import deepcopy
import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import (
    table_exists,
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation
)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class BigQueryBackfillingTool:
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

        self.default_args = {
            "owner": "team-centaurs",
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 10,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True
        }

        self.local_tz = pendulum.timezone('America/Toronto')

    def build_fetching_job(self, staging_folder: str, cluster_name: str, config: dict):
        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]
        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")
        spark_config = self.dbconfig[db_name][consts.FETCH_SPARK_JOB].copy()

        arg_list = [spark_config[consts.APPLICATION_CONFIG], staging_folder]

        for k, v in config[consts.FETCH_SPARK_JOB][consts.ARGS].items():
            arg_list.append(f'{k}={v}')

        return {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: consts.DEFAULT_SPARK_SETTINGS
            }
        }

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        print(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))

        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)

        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_loading_job(self, config: dict, staging_folder: str):
        bigquery_config = config[consts.BIGQUERY]
        bq_table_name = bigquery_config[consts.TABLE_NAME]

        parquet_files_path = f"{staging_folder}/*.parquet"
        logger.info(f"Starting import of data for table {bq_table_name} from {parquet_files_path}")

        processing_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]

        bq_table_name = bigquery_config[consts.TABLE_NAME]
        bq_ext_table_id = f"{processing_project_id}.{bigquery_config[consts.DATASET_ID]}.{bq_table_name}_BFEXT"
        landing_project_id = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config.get(
            consts.LANDING_ZONE_PROJECT_ID)

        bq_landing_table_id = f"{landing_project_id}.{bigquery_config[consts.DATASET_ID]}.{bq_table_name}"

        additional_columns = bigquery_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = bigquery_config.get(consts.DROP_COLUMNS)

        transformation_config = {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_landing_table_id
            }
        }

        bq_client = bigquery.Client()
        staged_view = self.apply_transformations(bq_client, transformation_config)

        if bigquery_config.get(consts.OVERWRITE_BIGQUERY_TABLE):
            loading_stmt = f"""
            CREATE OR REPLACE TABLE
                 `{bq_landing_table_id}`
            AS
                SELECT {staged_view.get('columns')}
                FROM `{staged_view.get('id')}`;
            """
        else:
            loading_stmt = f"""
                INSERT INTO `{bq_landing_table_id}`
                SELECT {staged_view.get('columns')}
                FROM `{staged_view.get('id')}`;
            """

        logger.info(loading_stmt)
        query_job = bq_client.query(loading_stmt)
        query_job.result()

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        # Use config default_args if defined, otherwise use class default_args
        dag_default_args = deepcopy(self.default_args)
        if 'default_args' in config:
            # Only update keys that already exist in the default args
            dag_default_args.update(config['default_args'])

        dag = DAG(
            dag_id=dag_id,
            default_args=dag_default_args,
            schedule=None,
            start_date=pendulum.today('America/Toronto').add(days=-1),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                              job_size),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=cluster_name
            )

            staging_folder = f"gs://pcb-{self.deploy_env}-staging-extract/backfill/{config[consts.BIGQUERY].get(consts.TABLE_NAME)}"

            fetching_task = DataprocSubmitJobOperator(
                task_id='fetch_data',
                job=self.build_fetching_job(staging_folder, cluster_name, config),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
            )

            loading_task = PythonOperator(
                task_id='load_data_into_bq',
                python_callable=self.build_loading_job,
                op_kwargs={consts.CONFIG: config, 'staging_folder': staging_folder}
            )

            start_point >> cluster_creating_task >> fetching_task >> loading_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags


globals().update(BigQueryBackfillingTool(config_filename='bigquery_backfilling_config.yaml').create_dags())
