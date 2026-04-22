import json
import logging
from datetime import timedelta, datetime

from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from tsys_processing.tsys_file_loader_base import TSYS_FILE_LOADER
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists,
    apply_join_transformation,
    parse_join_config)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.gcs_utils import list_blobs_with_prefix
from util.miscutils import (
    get_cluster_name_for_dag,
    get_previous_month_details)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class TsysAccMasterFullFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.ephemeral_cluster_name = "acc-master-mdpc-ephemeral"

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        logger.info(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))
        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view
        join_spec = transformation_config.get(consts.JOIN_SPECIFICATION)
        if join_spec:
            join_parse_spec = parse_join_config(bigquery_client, join_spec,
                                                transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID),
                                                join_type=consts.LEFT_JOIN)
            join_transform_view = apply_join_transformation(bigquery_client, transformed_data, join_parse_spec)
            transformed_data = join_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        return self.build_filtering_task(cluster_name, dag_config)

    def validate_row_count(self, bigquery_client, bq_ext_table_id: str, segment_name: str,
                           record_count_column: str, context, tsys_count: str, is_redefine=False):
        if self.is_trailer(segment_name):
            logger.info("########## inside if condition for trailer #######")
            query = f"SELECT * FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe().to_json()
            context['ti'].xcom_push(key=f"{consts.RECORD_COUNT}", value=f'{results}')
        elif is_redefine:
            logger.info("########## Redefines : %s segment record count validation skipped #######", segment_name)
        elif tsys_count > 0:
            logger.info("########## inside else condition for other than trailer #######")
            query = f"SELECT COUNT(1) AS {record_count_column} FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe()
            num = results[record_count_column].values[0]
            if int(tsys_count) != int(num):
                raise AirflowException(f"tsys count {tsys_count} does not match bigquery count {num}")

    def get_tsys_count(self, context, segment_name: str, record_count_column):
        tsys_count = 1
        if not self.is_trailer(segment_name):
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
        return tsys_count

    def validate_rec_count_job(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str, **context):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if record_count_column or self.is_trailer(segment_name):
            tsys_count = self.get_tsys_count(context, segment_name, record_count_column)
            self.validate_row_count(bigquery_client, bq_ext_table_id, segment_name, record_count_column, context,
                                    tsys_count, table_config.get(consts.IS_REDEFINE))

    def validate_rec_count_task(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str):
        return PythonOperator(
            task_id="rec_count_validate",
            python_callable=self.validate_rec_count_job,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'bq_ext_table_id': bq_ext_table_id,
                'segment_name': segment_name}
        )

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict, segment_name: str):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)

        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)
        if self.is_trailer(segment_name) and table_exists(bigquery_client, transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)):
            self.duplicate_trailer_check(bigquery_config, transformation_config, transformed_view, segment_name)
        if data_load_type == consts.APPEND_ONLY:
            loading_sql = f"""
                   CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                    AS
                       SELECT {transformed_view.get(consts.COLUMNS)}
                       FROM {transformed_view.get(consts.ID)}
                       LIMIT 0;

                   INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                   SELECT {transformed_view.get(consts.COLUMNS)}
                   FROM {transformed_view.get(consts.ID)};
               """
        logger.info(loading_sql)
        query_job = bigquery_client.query(loading_sql)
        query_job.result()

    def duplicate_trailer_check(self, bigquery_config: dict, transformation_config: dict, transformed_view: dict,
                                segment_name: str):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        duplicate_check_cols = table_config.get(consts.DUPLICATE_CHECK_COLS) or []
        if duplicate_check_cols:
            join_clause = " and ".join(map(lambda x: f't.{x} = s.{x}', duplicate_check_cols))
            duplicate_check_cols_str = ','.join([f's.{x}' for x in duplicate_check_cols])
            query = f""" select {duplicate_check_cols_str} from {transformed_view.get(consts.ID)} s join
                                 {transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)} t
                                 ON  {join_clause} limit 1
                           """
            results = bigquery_client.query(query).result().to_dataframe()
            res = results.to_string(index=False)
            if not results.empty:
                raise AirflowFailException(
                    f"Duplicate entry with these details {res} exists for {segment_name} in "
                    f"source : '{transformed_view.get(consts.ID)}' ")

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict, segment_name: str):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config,
                       'segment_name': segment_name}
        )

    def get_execution_date(self, dag_id):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        return var_dict.get('execution_date', {}) if var_dict else None

    def get_latest_file(self, dag_id: str, dag_config: dict, **context):
        source_bucket = f"{dag_config.get(consts.SOURCE_BUCKET)}{self.gcp_config.get(consts.DEPLOY_ENV_STORAGE_SUFFIX)}"
        logger.info("source-bucket:", source_bucket)
        prefix_date = self.get_execution_date(dag_id) or datetime.now(self.local_tz).strftime("%Y-%m-%d")

        prefix = get_previous_month_details(prefix_date, dag_config)
        logger.info("prefix:", prefix)
        filename = list_blobs_with_prefix(source_bucket, prefix, None)
        filename.sort(reverse=True)
        logger.info(filename)
        sourcefile = filename[0]
        logger.info(sourcefile)
        context['ti'].xcom_push(key='filename', value=f'{sourcefile}')

    def get_latest_file_job(self, dag_id: str, dag_config: dict):
        return PythonOperator(
            task_id="get_latest_file",
            python_callable=self.get_latest_file,
            op_kwargs={
                'dag_id': dag_id,
                'dag_config': dag_config}
        )

    def build_file_staging_task(self, dag_id: str, dag_config: dict):
        with TaskGroup(group_id='FileStaging') as FileStaging:
            get_file_task = self.get_latest_file_job(dag_id, dag_config)
            file_moving_task = self.build_file_moving_task(dag_config)

            get_file_task >> file_moving_task

        return FileStaging

    def build_file_moving_task(self, dag_config: dict):
        file_prefix = dag_config.get(consts.FILE_PREFIX)
        destination_file = f"{consts.STAGING_FOLDER_NAME}/{{{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename') }}}}"
        source_bucket = f"{dag_config.get(consts.SOURCE_BUCKET)}{self.gcp_config.get(consts.DEPLOY_ENV_STORAGE_SUFFIX)}"
        return GCSToGCSOperator(
            task_id=f"staging_{file_prefix}_file",
            gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
            source_bucket=source_bucket,
            source_object="{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename') }}",
            destination_bucket=dag_config.get(consts.PROCESSING_BUCKET),
            destination_object=destination_file
        )

    def check_record_count(self, bigquery_config: dict, segment_name: str, **context):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if not self.is_trailer(segment_name) and record_count_column:
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
            if tsys_count > 0:
                return f'{segment_name}.parse_file'
            else:
                return f'{segment_name}.rec_count_validate'
        else:
            return f'{segment_name}.parse_file'

    def build_record_count_check(self, bigquery_config: dict, segment_name: str):
        return BranchPythonOperator(
            task_id="record_count_check",
            python_callable=self.check_record_count,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'segment_name': segment_name}
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
        join_spec = table_config.get(consts.JOIN) or []
        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_table_id
            },
            consts.JOIN_SPECIFICATION: join_spec,
        }

    def get_input_file_path(self, dag_config: dict):
        return f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{consts.STAGING_FOLDER_NAME}/{{{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename')}}}}"

    def get_output_dir_path(self, dag_config: dict):
        return f"gs://{dag_config[consts.PROCESSING_BUCKET_EXTRACT]}/{consts.STAGING_FOLDER_NAME}/{{{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename')}}}}" + '_extract'

    def build_segment_task_group(self, cluster_name: str, dag_config: dict, segment_name: str):
        spark_config = dag_config.get(consts.SPARK)
        parsing_job_config = spark_config.get(consts.PARSING_JOB_ARGS).copy()
        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)
        segment_config = segment_configs.get(segment_name)

        bigquery_config = dag_config.get(consts.BIGQUERY)

        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        output_dir = self.get_output_dir_path(dag_config)
        parsing_job_config['pcb.tsys.processor.output.path'] = f"{output_dir}/{segment_name}"

        with TaskGroup(group_id=segment_name) as segment_task_group:
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config,
                                                           segment_config)
            transformation_config = self.build_transformation_config(bigquery_config, output_dir, segment_name)
            loading_task = self.build_segment_loading_task(bigquery_config, transformation_config, segment_name)
            validation_task = self.validate_rec_count_task(bigquery_config,
                                                           transformation_config.get(
                                                               consts.EXTERNAL_TABLE_ID),
                                                           segment_name)
            check_record_count_task = self.build_record_count_check(bigquery_config, segment_name)

            check_record_count_task >> parsing_task >> loading_task >> validation_task
            check_record_count_task >> validation_task
        return segment_task_group

    def get_body_segment_list(self, dag_config: dict):
        spark_config = dag_config.get(consts.SPARK)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)

        return [segment_name for segment_name in segment_configs if
                (segment_name != consts.TRAILER_SEGMENT_NAME and segment_name != 'AM0A')]

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        outputpath = f"gs://{dag_config.get('processing_bucket_extract')}/{consts.STAGING_FOLDER_NAME}/{{{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename') }}}}" + '_extract'
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'file_name': "{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename')}}",
                       'output_dir': outputpath,
                       'tables_info': self.extract_tables_info(dag_config)}
        )

    def get_schedule_interval(self, dag_id: str):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        if var_dict:
            scheduling_interval = var_dict.get('scheduling_interval')
        else:
            scheduling_interval = '00 08 1 * *'
        return scheduling_interval

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule_interval(dag_id),
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TSYS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            cluster_name = get_cluster_name_for_dag(dag_id)
            job_size = dag_config[consts.SPARK].get(consts.DATAPROC_JOB_SIZE)

            file_staging_task = self.build_file_staging_task(dag_id, dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            trailer_segment_task = self.build_segment_task_group(cluster_name, dag_config,
                                                                 'TRLR')
            amoa_tasks = self.build_segment_task_group(cluster_name, dag_config,
                                                       'AM0A')
            body_segment_tasks = self.build_body_task_groups(cluster_name, dag_config,
                                                             self.get_body_segment_list(dag_config))
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')
            start >> file_staging_task >> cluster_creating_task >> preprocess_task >> trailer_segment_task >> amoa_tasks >> body_segment_tasks >> control_table_task >> end

        return add_tags(dag)
