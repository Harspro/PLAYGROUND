import json
from copy import deepcopy
import logging
import pendulum
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage

import util.constants as consts
from datetime import timedelta, datetime
from etl_framework.etl_dag_base import ETLDagBase
from util.miscutils import read_variable_or_file, read_env_filepattern, save_job_to_control_table
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)
LOCAL_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=LOCAL_TZ)
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"


def get_output_folder_path(config: dict):
    return f"gs://{config[consts.PROCESSING_BUCKET]}/{config.get('PROCESSING_OUTPUT_DIR')}"


def get_interim_folder_path(config: dict):
    return f"gs://{config[consts.PROCESSING_BUCKET]}/{config.get('PROCESSING_INTERIM_DIR')}"


class EstmtAvailGenerator(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)
        self.default_args = deepcopy(dag_default_args)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

    def get_output_file_name(self, config: dict, **context):
        output_file_config = config.get('output_file')
        output_file_extension = read_env_filepattern(output_file_config.get('filename_extension'), self.deploy_env)
        output_file_name = f"{output_file_config.get('output_filename')}_{JOB_DATE_TIME}.{output_file_extension}"

        context['task_instance'].xcom_push(key='output_file_name', value=output_file_name)
        logger.info(f"Output file name for this execution is: {output_file_name}")

    def build_control_record_saving_job(self, file_name: str, output_dir: str, status: str, **context):
        job_params_str = json.dumps({'source_filename': file_name,
                                     'extract_path': output_dir,
                                     'status': context.get('status'),
                                     'input_load_date': context.get('input_load_date')}
                                    )
        save_job_to_control_table(job_params_str, **context)

    def export_bq_data_to_file(self, config: dict, file_name: str, **context):
        client = bigquery.Client()
        storage_client = storage.Client()
        output_file_config = config.get('output_file')
        column_name = output_file_config.get('column')
        table_detail = output_file_config.get('table_detail')
        output_folder = get_output_folder_path(config)

        bucket_name = output_folder.split('/')[2]
        object_path = "/".join(output_folder.split('/')[3:])
        file_path = f"{object_path}/{file_name}" if object_path else file_name

        logger.info(f"Bucket Name: {bucket_name} for output file")
        logger.info(f"File Path in GCS: {file_path}")

        bucket = storage_client.lookup_bucket(bucket_name)
        if bucket is None:
            logger.info(f"Bucket {bucket_name} does not exist. Creating...")
            bucket = storage_client.create_bucket(bucket_name,
                                                  location="northamerica-northeast1")
            logger.info(f"Bucket {bucket_name} created successfully.")

        table_order = ["HEDR", "DETL", "TRLR"]
        blob = bucket.blob(file_path)

        with blob.open('w') as file_stream:
            total_records = 0
            for segment in table_order:
                table = table_detail.get(segment)
                if table:
                    query = f"""
                                SELECT {column_name}
                                FROM {table}
                             """
                    logger.info(f"Querying {segment} table: {table}")

                    query_job = client.query(query)
                    results = query_job.result()

                    segment_records = 0
                    null_count = 0

                    for row in results:
                        value = getattr(row, column_name)

                        if value is None:
                            value = ''
                            null_count += 1
                        else:
                            value = str(value)

                        file_stream.write(value + '\n')
                        segment_records += 1
                        total_records += 1

                        if segment_records % 1000 == 0:
                            logger.info(f"Processed {segment_records} records from {segment}...")

                    logger.info(f"Completed {segment}: {segment_records} records, {null_count} null values")

        logger.info(f"Successfully wrote {total_records} total records to {file_path}")

    def preprocessing_job(self, config: dict, upstream_task: list):
        preprocessing_task = PythonOperator(
            task_id="get_output_file_name",
            python_callable=self.get_output_file_name,
            op_kwargs={consts.CONFIG: config}
        )
        upstream_task.append(preprocessing_task)

        return upstream_task

    def postprocessing_job(self, config: dict, upstream_task: list):
        activity_var = config.get('activity_name', '')
        task_grp_id = f'{activity_var}_post_trans_job'

        with (TaskGroup(group_id=task_grp_id)):
            create_output_file_task = PythonOperator(
                task_id='creating_output_file',
                python_callable=self.export_bq_data_to_file,
                op_kwargs={'config': config,
                           'file_name': "{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}"
                           }
            )
            upstream_task.append(create_output_file_task)

            move_output_file_to_landing_task = GCSToGCSOperator(
                task_id='move_file_to_outbound_landing',
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                source_bucket=config.get(consts.STAGING_BUCKET),
                source_object=f"{config.get('PROCESSING_OUTPUT_DIR')}/{{{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}}}",
                destination_bucket=config.get(consts.DESTINATION_BUCKET),
                destination_object=f"{config.get('destination_folder')}/{{{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}}}"
            )
            upstream_task.append(move_output_file_to_landing_task)

            move_output_file_to_landing_task_2 = GCSToGCSOperator(
                task_id='move_file_to_outbound_landing_2',
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                source_bucket=config.get(consts.STAGING_BUCKET),
                source_object=f"{config.get('PROCESSING_OUTPUT_DIR')}/{{{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}}}",
                destination_bucket=config.get('mft_outbound_bucket'),
                destination_object=f"{config.get('estmt_outbound_folder')}/{{{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}}}"
            )
            upstream_task.append(move_output_file_to_landing_task_2)

            control_record_saving_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'file_name': "{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}",
                           'output_dir': f"gs://{config.get(consts.DESTINATION_BUCKET)}",
                           'status': "COMPLETED"}
            )
            upstream_task.append(control_record_saving_task)

        return upstream_task
