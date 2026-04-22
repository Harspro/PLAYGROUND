import re
import json
import logging
from typing import Final
from dataclasses import asdict
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator,
)
from tsys_processing.generic_file_loader import GenericFileLoader
from copy import deepcopy
from datetime import datetime
import pytz
import util.constants as consts
from util.logging_utils import build_spark_logging_info
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from google.cloud import bigquery
from util.miscutils import (
    read_variable_or_file,
    save_job_to_control_table,
    get_cluster_name_for_dag
)
from util.gcs_utils import (
    get_bucket_project_id
)
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists
import data_transfer.util.constants as dt_consts
from dag_factory.terminus_dag_factory import add_tags
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


class PCDownloadFileLoader(GenericFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.env_storage_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.audit_util = AuditUtil('post_processing.prepare_decoding_config', 'audit_record')

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        cluster_name = get_cluster_name_for_dag(dag_id)
        with TaskGroup(group_id="post_processing") as postprocessing:
            get_file_create_dt_task = PythonOperator(
                task_id='get_file_create_dt_task',
                trigger_rule='none_failed',
                python_callable=self.get_file_create_dt,
                op_kwargs={'dag_id': dag_id,
                           'dag_config': dag_config}
            )
            prepare_decoding_config_task = PythonOperator(
                task_id='prepare_decoding_config',
                python_callable=self.prepare_decoding_config,
                op_kwargs={
                    "dag_id": dag_id,
                    "file_name": "{{ dag_run.conf['name']}}",
                    "dag_config": dag_config,
                    "cluster_name": cluster_name,
                },
                on_failure_callback=self.audit_util.record_request_failure
            )
            decode_task = DataprocSubmitJobOperator(
                task_id="decode_task",
                job="{{ ti.xcom_pull(task_ids='post_processing.prepare_decoding_config', key='decoding_job_config') }}",
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(
                    consts.PROCESSING_ZONE_CONNECTION_ID
                ),
            )
            move_data_to_landing_zone = GCSToGCSOperator(
                task_id="move_data_to_landing_zone",
                gcp_conn_id=self.gcp_config[consts.PROCESSING_ZONE_CONNECTION_ID],
                source_bucket="{{ ti.xcom_pull(task_ids='post_processing.prepare_decoding_config', key='processing_extract_bucket') }}",
                source_object="{{ ti.xcom_pull(task_ids='post_processing.prepare_decoding_config', key='destination_path') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='post_processing.prepare_decoding_config', key='destination_bucket') }}",
                destination_object="{{ ti.xcom_pull(task_ids='post_processing.prepare_decoding_config', key='destination_path') }}",
                on_failure_callback=self.audit_util.record_request_failure,
                on_success_callback=self.audit_util.record_request_success
            )
            build_control_record_saving_job = PythonOperator(
                task_id="save_job_to_control_table",
                trigger_rule="all_success",
                python_callable=self.build_control_record_saving_job,
                op_kwargs={
                    "file_name": "{{ dag_run.conf['name']}}",
                    "output_dir": self.get_output_dir_path(dag_config),
                    "tables_info": self.extract_tables_info(dag_config),
                },
            )
            (get_file_create_dt_task >> prepare_decoding_config_task >> decode_task >> move_data_to_landing_zone >> build_control_record_saving_job)

        return postprocessing

    def prepare_decoding_config(self, dag_id: str, file_name: str, dag_config: dict, cluster_name, **context):
        create_data_transfer_audit_table_if_not_exists()
        if file_name is None:
            raise ValueError(
                "The 'file_name' provided in DAG run config is None. Cannot proceed with renaming."
            )
        file_create_dt = context['ti'].xcom_pull(task_ids='post_processing.get_file_create_dt_task', key='file_create_dt')
        spark_config = dag_config.get(consts.SPARK)
        destination_filename_prefix = dag_config.get(consts.OUTPUTFILE_FILE_PREFIX)
        decoding_job_args = deepcopy(spark_config.get(consts.DECODING_JOB_ARGS))
        decoding_job_args["pcb.tsys.processor.datafile.path"] = f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{file_name}"
        source_path = decoding_job_args["pcb.tsys.processor.datafile.path"]
        logger.info(f'source_path is {source_path}')
        logger.info(f'file_create_dt is {file_create_dt}')
        match = re.search(r'_(\d{14})\.', source_path)
        if match:
            datetimepart = match.group(1)
            time_part = datetimepart[-6:]
            logger.info(f"Extracted time_part: {time_part}")
        dt = datetime.strptime(file_create_dt, "%Y-%m-%d")
        date_part = dt.strftime("%Y%m%d")
        timestamp = f"{date_part}{time_part}"
        destination_filename = f"{destination_filename_prefix}_{timestamp}"
        file_suffix = "prod" if self.deploy_env == "prod" else "uatv"
        destination_filename = f"{destination_filename}.{file_suffix}"
        logger.info(f'destination_filename is {destination_filename}')
        processing_extract_bucket = dag_config.get(consts.PROCESSING_BUCKET_EXTRACT)
        processing_extract_path = f"gs://{processing_extract_bucket}/{destination_filename_prefix}/{destination_filename}"
        destination_bucket = f"{dag_config.get(consts.DESTINATION_BUCKET)}{self.gcp_config.get(consts.DEPLOY_ENV_STORAGE_SUFFIX)}"
        destination_path = f"{destination_filename_prefix}/{destination_filename}"
        decoding_job_args["pcb.tsys.processor.output.path"] = processing_extract_path

        arglist = []
        for k, v in decoding_job_args.items():
            arglist.append(f"{k}={v}")

        arglist = build_spark_logging_info(
            dag_id="{{dag.dag_id}}",
            default_args=self.default_args,
            arg_list=arglist,
        )

        decoding_job_config = {
            consts.REFERENCE: {
                consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)
            },
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.ARGS: arglist,
            },
        }
        source_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        source_dag_id = context.get('dag').dag_id
        source_dag_run_id = context.get('run_id')
        source_type = 'file'
        destination_project_id = get_bucket_project_id(destination_bucket)
        audit_record_values = f"""
                               '{source_project_id}',
                               '{source_dag_id}',
                               '{source_dag_run_id}',
                               '{source_type}',
                               '{processing_extract_path}',
                               '{destination_filename}',
                               '{destination_project_id}',
                               'gs://{destination_bucket}/{destination_filename}',
                               '{source_dag_id}',
                               '{source_dag_run_id}',
                               '{dt_consts.DataTransferExecutionType.VENDOR_DATA_OUTBOUND.value}',
                               '{context.get('dag_run').run_type}',
                               '{destination_project_id}',
                               CURRENT_TIMESTAMP()
                           """
        audit_record = AuditRecord(consts.DOMAIN_TECHNICAL_DATASET_ID, dt_consts.DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
        self.audit_util.record_request_received(audit_record)
        context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
        context["ti"].xcom_push(key="decoding_job_config", value=decoding_job_config)
        context["ti"].xcom_push(key="source_path", value=source_path)
        context["ti"].xcom_push(key="processing_extract_path", value=processing_extract_path)
        context["ti"].xcom_push(key="destination_bucket", value=destination_bucket)
        context["ti"].xcom_push(key="processing_extract_bucket", value=processing_extract_bucket)
        context["ti"].xcom_push(key="destination_path", value=destination_path)
        return decoding_job_config

    def build_control_record_saving_job(self, file_name: str, output_dir: str, **context):
        job_params_str = json.dumps({
            'source_filename': file_name,
            'extract_path': output_dir,
            'processing_extract_bucket': context['ti'].xcom_pull(task_ids='post_processing.prepare_decoding_config', key='processing_extract_bucket'),
            'destination_bucket': context['ti'].xcom_pull(task_ids='post_processing.prepare_decoding_config', key='destination_bucket'),
            'destination_path': context['ti'].xcom_pull(task_ids='post_processing.prepare_decoding_config', key='destination_path'),
        })
        save_job_to_control_table(job_params_str, **context)

    def get_file_create_dt(self, dag_id: str, dag_config: dict, **context):
        bigquery_client = bigquery.Client()
        bigquery_config = dag_config.get(consts.BIGQUERY)
        bq_processing_project_name = bigquery_config.get(consts.PROJECT_ID)
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        file_date_column = dag_config.get("file_date_column")
        bq_table = bigquery_config.get("tables").get("TRLR").get("table_name")
        create_date_sql = f"""
            SELECT MAX({file_date_column}) AS {file_date_column}
            FROM {bq_processing_project_name}.{bq_dataset_name}.{bq_table}
        """
        create_dt_result = bigquery_client.query(create_date_sql).result().to_dataframe()
        file_create_dt: Final = create_dt_result[file_date_column].values[0]
        if file_create_dt:
            logging.info(f"File create date for this execution is {file_create_dt}")
            context['ti'].xcom_push(key='file_create_dt', value=str(file_create_dt))
        else:
            raise AirflowException(f"file_create_dt is not found for {dag_id}")


globals().update(PCDownloadFileLoader('ts2_pcdownload_ingestion_config.yaml').create_dags())  # pragma: no cover
