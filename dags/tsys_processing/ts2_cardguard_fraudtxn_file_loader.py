import io
import json
import logging
from copy import deepcopy
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from zipfile import (
    ZipFile,
    is_zipfile,
    BadZipFile
)

from airflow.utils.task_group import TaskGroup

import util.constants as consts
from typing import Union

from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.gcs_utils import (
    read_file_bytes,
    write_file_bytes
)
from util.miscutils import (
    get_cluster_name_for_dag
)

from tsys_processing.generic_file_loader import GenericFileLoader
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class FraudTxnFileLoader(GenericFileLoader):

    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            cluster_name = get_cluster_name_for_dag(dag_id)
            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)

            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')

            file_staging_task = self.build_file_staging_task(dag_config)
            unzip_extract_task = self.build_unzip_extract_task(dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)

            start >> file_staging_task >> unzip_extract_task >> cluster_creating_task

            leading_task = cluster_creating_task
            group_end_tasks = []
            file_processing_groups = dag_config.get(consts.FILE_GROUPS)
            for file_processing_config in file_processing_groups:
                group_name = file_processing_config.get(consts.GROUP_NAME)
                group_start = EmptyOperator(task_id=f'{group_name}_start')
                group_end = EmptyOperator(task_id=f'{group_name}_end')
                leading_task >> group_start

                file_config = deepcopy(file_processing_config)
                file_config[consts.PROCESSING_BUCKET] = dag_config.get(consts.PROCESSING_BUCKET)
                file_config[consts.PROCESSING_BUCKET_EXTRACT] = dag_config.get(consts.PROCESSING_BUCKET_EXTRACT)

                file_filter_group_tasks = self.build_preprocessing_task_group(dag_id, cluster_name, file_config)
                group_start >> file_filter_group_tasks >> group_end
                group_end_tasks.append(group_end)

            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)

            for group_end_task in group_end_tasks:
                group_end_task >> control_table_task >> end

        return add_tags(dag)

    def extract_and_upload_zip(self, file_path: str, bucket_name: str):
        zip_bytes = io.BytesIO(read_file_bytes(bucket_name, file_path))

        try:
            if is_zipfile(zip_bytes):
                with ZipFile(zip_bytes, 'r') as zip_ref:
                    for content_filename in zip_ref.namelist():
                        content_file = zip_ref.read(content_filename)
                        write_file_bytes(bucket_name, file_path, content_filename, content_file)
            else:
                raise BadZipFile()
        except BadZipFile:
            logger.error(f"{file_path} is not a valid .zip file")
            raise

    def build_unzip_extract_task(self, dag_config: dict):
        return PythonOperator(
            task_id="unzip_and_extract",
            python_callable=self.extract_and_upload_zip,
            op_kwargs={'file_path': "{{ dag_run.conf['name']}}",
                       'bucket_name': dag_config.get(consts.PROCESSING_BUCKET)}
        )

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, file_config: dict):
        file_name = file_config.get(consts.FILE_NAME)
        with TaskGroup(group_id=file_name) as file_name_group_task:
            preprocess_task = self.build_filtering_task(cluster_name, file_config)

            leading_task = preprocess_task

            segment_dependency_groups = file_config.get(consts.SPARK).get(consts.SEGMENT_DEPENDENCY_GROUPS)

            for segment_dependency_group in segment_dependency_groups:
                group_name = segment_dependency_group.get(consts.GROUP_NAME)
                group_start = EmptyOperator(task_id=f'{group_name}_start')
                group_end = EmptyOperator(task_id=f'{group_name}_end')
                leading_task >> group_start
                leading_task = group_end
                group_segments_str = segment_dependency_group.get(consts.SEGMENTS)
                group_segment_list = set([segment.strip() for segment in group_segments_str.split(consts.COMMA)])

                group_tasks = self.build_body_task_groups(cluster_name, file_config, group_segment_list)

                group_start >> group_tasks >> group_end

        return file_name_group_task

    def build_body_task_groups(self, cluster_name: str, file_config: dict, body_segments: Union[list, set]):
        task_groups = []

        for segment_name in body_segments:
            task_groups.append(
                self.build_segment_task_group(cluster_name, file_config, segment_name))

        return task_groups

    def build_segment_task_group(self, cluster_name: str, file_config: dict, segment_name: str):
        spark_config = file_config.get(consts.SPARK)
        parsing_job_config = deepcopy(spark_config.get(consts.PARSING_JOB_ARGS))
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)
        segment_config = segment_configs.get(segment_name)
        file_name = file_config.get(consts.FILE_NAME)

        bigquery_config = file_config.get(consts.BIGQUERY)

        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(file_config)
        output_dir = self.get_output_dir_path(file_config)
        parsing_job_config['pcb.tsys.processor.output.path'] = f"{output_dir}/{segment_name}"

        with TaskGroup(group_id=segment_name) as segment_task_group:
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config,
                                                           segment_config)
            transformation_config = self.build_fraudtxn_transformation_config(bigquery_config, file_config, output_dir, segment_name)

            loading_task = self.build_segment_loading_task(bigquery_config, transformation_config, segment_name)
            validation_task = self.validate_rec_count_task(bigquery_config,
                                                           transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                           segment_name, file_name)
            check_record_count_task = self.build_fraudtxn_record_count_check(bigquery_config, segment_name, file_name)

            check_record_count_task >> parsing_task >> loading_task >> validation_task
            check_record_count_task >> validation_task

        return segment_task_group

    def build_fraudtxn_transformation_config(self, bigquery_config: dict, file_config: dict, output_dir: str, segment_name: str):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)

        parquet_files_path = f"{output_dir}/{segment_name}/*.parquet"

        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        bq_table_name = table_config[consts.TABLE_NAME]
        file_name = file_config[consts.FILE_NAME]

        bq_ext_table_id = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{file_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
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

    def check_fraudtxn_record_count(self, bigquery_config: dict, segment_name: str, file_name: str, **context):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if not self.is_trailer(segment_name) and record_count_column:
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{file_name}.{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
            if int(tsys_count) > 0:
                return f'{file_name}.{segment_name}.parse_file'
            else:
                return f'{file_name}.{segment_name}.rec_count_validate'
        else:
            return f'{file_name}.{segment_name}.parse_file'

    def build_fraudtxn_record_count_check(self, bigquery_config: dict, segment_name: str, file_name: str):
        return BranchPythonOperator(
            task_id="record_count_check",
            python_callable=self.check_fraudtxn_record_count,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'segment_name': segment_name,
                'file_name': file_name}
        )

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                       'output_dir': self.get_post_processing_output_dir_path(dag_config),
                       'tables_info': self.extract_tables_info(dag_config)}
        )

    def get_input_file_path(self, file_config: dict):
        return f"gs://{file_config[consts.PROCESSING_BUCKET]}/{{{{ dag_run.conf['name']}}}}/" \
               f"{file_config[consts.FILE_NAME]}"

    def get_output_dir_path(self, file_config: dict):
        return f"gs://{file_config[consts.PROCESSING_BUCKET_EXTRACT]}/{{{{ dag_run.conf['name'] }}}}_extract/" \
               f"{file_config[consts.FILE_NAME]}"

    def get_post_processing_output_dir_path(self, dag_config: dict):
        return f"gs://{dag_config[consts.PROCESSING_BUCKET_EXTRACT]}/{{{{ dag_run.conf['name'] }}}}_extract"
