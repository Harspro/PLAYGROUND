import json
import pendulum
from copy import deepcopy
from abc import ABC, abstractmethod
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import timedelta, date
from google.cloud import bigquery
from typing import Final, Union

import util.constants as consts
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation
)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.gcs_utils import list_blobs_with_prefix
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    save_job_to_control_table,
    get_cluster_name_for_dag,
    get_cluster_config_by_job_size
)
from dag_factory.terminus_dag_factory import add_tags

TSYS_FILE_LOADER: Final = 'tsys_file_loader'

INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Data Movement',
    'business_impact': 'N/A',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


class TsysFileLoader(ABC):
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

        self.local_tz = pendulum.timezone('America/Toronto')

    def extract_tables_info(self, dag_config: dict) -> list:
        tables_info = []

        if not isinstance(dag_config, dict) or consts.BIGQUERY not in dag_config:
            return tables_info

        bq_config = dag_config[consts.BIGQUERY]
        dataset_id = bq_config.get(consts.DATASET_ID)
        project_id = bq_config.get(consts.PROJECT_ID)

        if not dataset_id:
            return tables_info

        # TSYS loaders primarily use segment-based multi-table pattern
        tables_config = bq_config.get(consts.TABLES)
        if isinstance(tables_config, dict):
            for segment, table_config in tables_config.items():
                if isinstance(table_config, dict):
                    segment_table_name = table_config.get(consts.TABLE_NAME)
                    if segment_table_name:
                        tables_info.append({
                            'project_id': project_id,
                            'dataset_id': dataset_id,
                            'table_name': segment_table_name
                        })

        # Include parent_table if present (e.g., BATCH_AUTHORIZATION_LOG)
        parent_table = bq_config.get(consts.PARENT_TABLE)
        if parent_table:
            tables_info.append({
                'project_id': project_id,
                'dataset_id': dataset_id,
                'table_name': parent_table
            })

        # Fallback: handle rare single-table TSYS configs
        if not tables_info:
            table_name = bq_config.get(consts.TABLE_NAME)
            if table_name:
                tables_info.append({
                    'project_id': project_id,
                    'dataset_id': dataset_id,
                    'table_name': table_name
                })

        return tables_info

    def is_trailer(self, segment_name: str):
        return consts.TRAILER_SEGMENT_NAME == segment_name

    def get_body_segment_list(self, dag_config: dict):
        spark_config = dag_config.get(consts.SPARK)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)

        return [segment_name for segment_name in segment_configs if segment_name != consts.TRAILER_SEGMENT_NAME]

    def get_input_file_path(self, dag_config: dict):
        return f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{{{{ dag_run.conf['name']}}}}"

    def get_output_dir_path(self, dag_config: dict):
        return f"gs://{dag_config[consts.PROCESSING_BUCKET_EXTRACT]}/{{{{ dag_run.conf['name'] }}}}_extract"

    def verify_file_exists(self, dag_config: dict):
        if dag_config.get(consts.SCHEDULE_INTERVAL, None) is None:
            return
        source_bucket = f"{dag_config.get(consts.SOURCE_BUCKET)}{self.gcp_config.get(consts.DEPLOY_ENV_STORAGE_SUFFIX)}"
        file_prefix_search_str = f"{dag_config.get(consts.FOLDER_NAME)}/" \
                                 f"{dag_config.get(consts.FILE_PREFIX)}_{date.today().strftime('%Y%m%d')}"
        items = list_blobs_with_prefix(bucket_name=source_bucket,
                                       prefix=file_prefix_search_str)
        if len(items) < 1:
            raise AirflowException("Today's daily file could not be found.")
        elif len(items) > 1:
            raise AirflowException("Multiple files detected with today's date.")

    def build_file_staging_task(self, dag_config: dict):
        file_prefix = dag_config.get(consts.FILE_PREFIX)
        return GCSToGCSOperator(
            task_id=f"staging_{file_prefix}_file",
            gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
            source_bucket="{{ dag_run.conf['bucket'] }}",
            source_object="{{ dag_run.conf['name'] }}",
            destination_bucket=dag_config.get(consts.PROCESSING_BUCKET),
            destination_object="{{ dag_run.conf['name']}}"
        )

    def build_cluster_creating_task(self, cluster_name: str, job_size: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=cluster_name
        )

    def build_filtering_task(self, cluster_name: str, dag_config: dict):
        spark_config = dag_config.get(consts.SPARK)
        filtering_job_args = spark_config.get(consts.FILTERING_JOB_ARGS)
        filtering_job_args['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)

        arglist = []
        for k, v in filtering_job_args.items():
            arglist.append(f'{k}={v}')

        arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arglist)

        spark_filter_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.ARGS: arglist
            }
        }

        return DataprocSubmitJobOperator(
            task_id="filter_file",
            job=spark_filter_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    @abstractmethod
    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        pass

    def build_control_record_saving_job(self, file_name: str, output_dir: str, **context):
        job_params_str = json.dumps({
            'source_filename': file_name,
            'extract_path': output_dir
        })
        save_job_to_control_table(job_params_str, **context)

    @abstractmethod
    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        pass

    def build_segment_parsing_task(self, cluster_name: str, spark_config: dict, parsing_job_config: dict,
                                   segment_config: dict):
        segment_job_config = parsing_job_config
        # add segment args
        segment_job_config.update(segment_config)

        arglist = []
        for k, v in segment_job_config.items():
            arglist.append(f'{k}={v}')

        arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arglist)
        segment_parsing_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.ARGS: arglist
            }
        }

        return DataprocSubmitJobOperator(
            task_id="parse_file",
            job=segment_parsing_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def build_transformation_config(self, bigquery_config: dict, output_dir: str, segment_name: str):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)

        parquet_files_path = f"{output_dir}/{segment_name}/*.parquet"

        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        bq_table_name = table_config[consts.TABLE_NAME]

        bq_ext_table_id = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_dataset_name}.{bq_table_name}"

        additional_columns = table_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = table_config.get(consts.DROP_COLUMNS) or []

        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_table_id
            }
        }

    def build_segment_loading_job(self, bigquery_config: dict,
                                  transformation_config: dict):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)

        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)

        if data_load_type == consts.APPEND_ONLY:
            loading_sql = f"""
                CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)}
                    LIMIT 0;

                INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)};
            """
        else:
            loading_sql = f"""
                CREATE TABLE
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)};
            """

        print(loading_sql)
        query_job = bigquery_client.query(loading_sql)
        query_job.result()

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                       'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config}
        )

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

        return transformed_data

    @abstractmethod
    def build_segment_task_group(self, cluster_name: str, dag_config: dict, segment_name: str):
        pass

    def build_body_task_groups(self, cluster_name: str, dag_config: dict, body_segments: Union[list, set]):
        task_groups = []

        for segment_name in body_segments:
            task_groups.append(self.build_segment_task_group(cluster_name, dag_config, segment_name))

        return task_groups

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:

        self.default_args.update(dag_config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.today('America/Toronto').add(days=-1),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            tags=dag_config.get(consts.TAGS, [])
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TSYS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)
            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            file_staging_task = self.build_file_staging_task(dag_config)

            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)

            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            trailer_segment_task = self.build_segment_task_group(cluster_name, dag_config,
                                                                 consts.TRAILER_SEGMENT_NAME)
            body_segment_tasks = self.build_body_task_groups(cluster_name, dag_config,
                                                             self.get_body_segment_list(dag_config))

            postprocess_task = self.build_postprocessing_task_group(dag_id, dag_config)

            done = EmptyOperator(task_id='Done')

            (file_staging_task >> cluster_creating_task >> preprocess_task >> trailer_segment_task
             >> body_segment_tasks >> postprocess_task >> done)

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config is not None:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags
