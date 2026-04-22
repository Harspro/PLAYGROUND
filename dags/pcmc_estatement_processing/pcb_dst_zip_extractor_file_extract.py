import json
import logging
import pendulum
from airflow.exceptions import AirflowFailException
import tempfile
from google.cloud import storage
import util.constants as consts
from airflow import settings
from airflow.models import Variable
import os
from typing import Final
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.task_group import TaskGroup

from bq_to_oracle_loader.bq_to_oracle_base import UPDATE_JAR_URI, UPDATER_MAIN_CLASS
from util import logging_utils
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix,
    read_env_filepattern,
    get_cluster_config_by_job_size,
    save_job_to_control_table)
from util.gcs_utils import delete_blobs, delete_folder
import util.constants as const
from zipfile import ZipFile, ZIP_DEFLATED
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']
LOCAL_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=LOCAL_TZ)
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"
OUTPUT_FILE_CREATION: Final = 'output_file_creation'
PROCESSING_OUTPUT_DIR: Final = 'processing_output_dir'
PROCESSING_INTERIM_DIR: Final = 'processing_interim_dir'
DAG_RUN_PLACEHOLDER: Final = 'dag_run'
CYCLE_LIST_PLACEHOLDER: Final = 'cycle_list'
FILE_CREATE_DATE_PLACEHOLDER: Final = 'file_create_dt'
CYCLE_NUM_PLACEHOLDER: Final = '{cycle}'
FILE_COUNT_CHECK: Final = 'file_count_check'
VALIDATION: Final = 'validation'
DESTINATION_OUTPUT_DIR: Final = 'destination_output_dir'


def get_output_folder_path(dag_config: dict):
    return f"{dag_config.get(PROCESSING_OUTPUT_DIR)}"


def get_output_file_name_env_based(dag_config: dict, deploy_env, cycle_no, dag_id):
    var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
    if var_dict.get('output_file_name'):
        output_file_name = var_dict.get('output_file_name')
    else:
        output_file_name = dag_config.get('output_file_name').get(deploy_env)
    output_file_extension = dag_config.get('output_file_extension').get(deploy_env)
    output_file_name = f"{output_file_name}_{JOB_DATE_TIME}.{output_file_extension}"
    output_file_name = output_file_name.replace(CYCLE_NUM_PLACEHOLDER, str(cycle_no).zfill(2))
    if dag_config.get('output_file_env_vars'):
        env_var_val = dag_config.get('output_file_env_vars')
        output_file_name = output_file_name.replace('{env_var}', env_var_val.get(deploy_env))
    output_file_path = f"{dag_config.get('processing_output_dir')}/{output_file_name}"
    return output_file_path


def get_interim_folder_path(dag_config: dict):
    return f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{dag_config.get(PROCESSING_INTERIM_DIR)}"


# to be deleted
def zip_and_move_files_for_one_cycle(dag_config, bucket_name, source_folder_prefix, destination_zip_blob_path):
    dest_folder = get_output_folder_path(dag_config)
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=source_folder_prefix))
    destination_zip_blob_name = destination_zip_blob_path.split("/")[-1]

    with tempfile.TemporaryDirectory() as temp_dir:
        for blob in blobs:
            local_file_path = os.path.join(temp_dir, blob.name.split("/")[-1])
            blob.download_to_filename(local_file_path)

        temp_zip_path = os.path.join(temp_dir, destination_zip_blob_name)

        with ZipFile(temp_zip_path, 'w', compression=ZIP_DEFLATED, compresslevel=9) as zipf:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file == destination_zip_blob_name:
                        continue
                    zipf.write(os.path.join(root, file), arcname=file)

        bucket = client.bucket(bucket_name)
        dest_blob = bucket.blob(f"{dest_folder}/{destination_zip_blob_name}")
        dest_blob.upload_from_filename(temp_zip_path)

    logger.info(f"Created zip file {destination_zip_blob_name} containing all files from folder '{source_folder_prefix}'")


def delete_existing_blob(dag_config, bucket_name, destination_zip_blob_path):
    dest_folder = get_output_folder_path(dag_config)
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=dest_folder))
    destination_zip_blob_name = destination_zip_blob_path.split("/")[-1]
    logger.info(f"Checking for zip file : {destination_zip_blob_name}")
    if destination_zip_blob_name in blobs:
        logger.info(f"Deleting zip file : {destination_zip_blob_name}")
        delete_blobs(bucket_name, destination_zip_blob_path)
        logger.info(f"Deleted zip file : {destination_zip_blob_name}")


class BroadridgeSTMTOutputFileCreation():

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.deploy_env_suffix = self.gcp_config['deploy_env_storage_suffix']
        self.job_config = read_yamlfile_env_suffix(f'{config_dir}/{config_filename}', self.deploy_env,
                                                   self.deploy_env_suffix)
        self.default_args = {
            "owner": "TBD",
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True
        }

        self.local_tz = LOCAL_TZ

    def fetch_parent_dag_details(self, dag_id: str, cycle_list, file_create_dt, **context):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        if var_dict.get(FILE_CREATE_DATE_PLACEHOLDER) and var_dict.get(CYCLE_LIST_PLACEHOLDER):
            file_create_dt = var_dict.get(FILE_CREATE_DATE_PLACEHOLDER, None)
            cycle_list = var_dict.get(CYCLE_LIST_PLACEHOLDER, None)
        else:
            file_create_dt = file_create_dt
            cycle_list = [int(num.strip()) for num in cycle_list.strip('[]').split(',')]

        logger.info(
            f"Details of the DAG - file create date: {file_create_dt}, cycle list: {cycle_list}")
        if file_create_dt is None or cycle_list is None:
            raise AirflowFailException("Required details of the DAG are missing")
        else:
            context['ti'].xcom_push(key=FILE_CREATE_DATE_PLACEHOLDER, value=file_create_dt)
            context['ti'].xcom_push(key=CYCLE_LIST_PLACEHOLDER, value=cycle_list)

    def fetch_parent_dag_details_task(self, dag_id: str):
        return PythonOperator(
            task_id='fetch_parent_dag_details',
            python_callable=self.fetch_parent_dag_details,
            op_kwargs={
                'dag_id': dag_id,
                'cycle_list': "{{ dag_run.conf['cycle_list'] | default([])}}",
                'file_create_dt': "{{ dag_run.conf['file_create_dt'] | default('')}}"
            }
        )

    def validate_file_count(self, dag_config: dict, cycle_list, file_count_check, **context):
        logger.info("Starting file count check in")

        client = storage.Client()

        bucket_path = dag_config.get(const.PROCESSING_BUCKET)
        bucket = client.get_bucket(bucket_path)
        folder_path_gen = dag_config.get(PROCESSING_INTERIM_DIR)
        cycle_list = [int(num.strip()) for num in cycle_list.strip('[]').split(',')]

        for cycle in cycle_list:
            file_count = 0
            folder_path = folder_path_gen.replace(CYCLE_NUM_PLACEHOLDER, str(cycle))
            if not folder_path.endswith("/"):
                folder_path += "/"
            blobs = bucket.list_blobs(prefix=folder_path)
            for blob in blobs:
                logger.info(f"Blobs to be processed: {blob.name}")
                file_count = file_count + 1
            logger.info(f"File count for {folder_path} is {file_count}")
            if file_count == file_count_check:
                pass
            else:
                raise AirflowFailException(f"File count for {folder_path} is {file_count} but count should be {file_count_check}")

    def build_pre_validation_task(self, dag_config, dag_id, **context):
        validation_config = dag_config.get(VALIDATION)
        file_count_check = validation_config.get(FILE_COUNT_CHECK)
        if validation_config:
            return PythonOperator(
                task_id="validate_file_count",
                python_callable=self.validate_file_count,
                op_kwargs={'dag_config': dag_config,
                           'cycle_list': "{{ ti.xcom_pull(task_ids='fetch_parent_dag_details', key='cycle_list') }}",
                           'file_count_check': file_count_check
                           }
            )
        else:
            return DummyOperator(task_id="skip_pre_validation")

    def build_zip_and_move_files_task(self, cluster_name: str, dag_config: dict, **context):
        arglist = []
        spark_config = dag_config.get(consts.SPARK)
        cycle_list = "{{ ti.xcom_pull(task_ids='fetch_parent_dag_details', key='cycle_list') }}"
        cycle_list_array = [str(num.strip()) for num in cycle_list.strip('[]').split(',')]
        cycle_list = ",".join(cycle_list_array)
        logger.info(f"Cycle list to be sent to Spark : {cycle_list}")
        output_file_env_vars = dag_config.get('output_file_env_vars').get(self.deploy_env)
        output_file_env_extension = dag_config.get('output_file_extension').get(self.deploy_env)
        zip_pattern_var = dag_config.get('output_file_name').get(self.deploy_env)
        zip_pattern = zip_pattern_var.replace('{env_var}', output_file_env_vars)
        zip_pattern = zip_pattern + "." + output_file_env_extension
        arglist.append(dag_config.get("processing_bucket"))
        arglist.append(dag_config.get("processing_interim_dir"))
        arglist.append(dag_config.get("processing_output_dir"))
        arglist.append(zip_pattern)
        arglist.append(cycle_list)

        arglist = logging_utils.build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args,
                                                         arg_list=arglist)

        logger.info(f"Final Spark job arguments: {arglist}")
        spark_zip_gcp_folder_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[UPDATE_JAR_URI],
                consts.MAIN_CLASS: spark_config[UPDATER_MAIN_CLASS],
                consts.ARGS: arglist
            }
        }
        return DataprocSubmitJobOperator(
            task_id="spark_zip_gcp_folder_job",
            job=spark_zip_gcp_folder_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def move_output_file_to_landing(self, dag_config, **context):
        move_output_files_to_landing_job = GCSToGCSOperator(
            task_id='moving_output_files_to_landing',
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            source_bucket=dag_config.get(consts.PROCESSING_BUCKET),
            source_object=dag_config.get(PROCESSING_OUTPUT_DIR),
            destination_bucket=dag_config.get(consts.DESTINATION_BUCKET),
            destination_object=dag_config.get(DESTINATION_OUTPUT_DIR),
            match_glob=dag_config.get(PROCESSING_OUTPUT_DIR) + "/**",
            move_object=True
        )
        return move_output_files_to_landing_job

    def build_control_record(self, output_dir: str, file_create_dt, cycle_list, status, **context):
        job_params_str = json.dumps({
            'extract_path': output_dir,
            'status': status,
            'file_create_dt': file_create_dt,
            'cycle_list': cycle_list
        })
        save_job_to_control_table(job_params_str, **context)

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        with TaskGroup(group_id='postprocessing') as postprocessing:
            build_control_record_saving_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record,
                op_kwargs={
                    'output_dir': f"gs://{dag_config.get(consts.DESTINATION_BUCKET)}",
                    'file_create_dt': "{{ ti.xcom_pull(task_ids='fetch_parent_dag_details', key='file_create_dt') }}",
                    'cycle_list': "{{ ti.xcom_pull(task_ids='fetch_parent_dag_details', key='cycle_list') }}",
                    'status': "COMPLETED"
                }
            )
            build_control_record_saving_task

        return postprocessing

    def cleanup_task(self, dag_config: dict, dag_id, **context):

        cleanup_task = PythonOperator(
            task_id='cleanup_task',
            python_callable=self.cleanup_intermediate_folders,
            op_kwargs={
                'dag_config': dag_config,
                'cycle_list': "{{ ti.xcom_pull(task_ids='fetch_parent_dag_details', key='cycle_list') }}",
                'dag_id': dag_id
            }
        )
        return cleanup_task

    def cleanup_intermediate_folders(self, dag_config: dict, cycle_list, dag_id, **context):
        logger.info(f"cycle_list for cleanup - {cycle_list}")
        cycle_list = [int(num.strip()) for num in cycle_list.strip('[]').split(',')]
        for cycle in cycle_list:
            logger.info(f"Starting cleanup for cycle - {cycle}")
            source_folder_prefix = dag_config.get(PROCESSING_INTERIM_DIR).replace(CYCLE_NUM_PLACEHOLDER, str(cycle))
            if not source_folder_prefix.endswith("/"):
                source_folder_prefix += "/"
            delete_folder(dag_config.get(const.PROCESSING_BUCKET), source_folder_prefix)
        logger.info(f"Cleanup completed for cycle - {cycle_list}")

    def build_cluster_creating_task(self, job_size: str, cluster_name: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=cluster_name
        )

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        """
        Main DAG which is triggered to extract output file
        """
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            start_date=datetime(2025, 4, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            schedule=None,
            is_paused_upon_creation=True
        )
        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(OUTPUT_FILE_CREATION, self.deploy_env)
                pause_unpause_dag(dag, is_paused)
            start = DummyOperator(task_id='Start')
            fetch_parent_dag_details = self.fetch_parent_dag_details_task(dag_id)
            pre_validation_job = self.build_pre_validation_task(dag_config, dag_id)
            cluster_name = dag_config.get('cluster_name')
            create_cluster_task = self.build_cluster_creating_task(
                job_size=dag_config.get('job_size'),
                cluster_name=cluster_name
            )
            zip_gcs_folder_to_another_task = self.build_zip_and_move_files_task(cluster_name, dag_config)
            move_output_file_to_landing = self.move_output_file_to_landing(dag_config)
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            cleanup = self.cleanup_task(dag_config, dag_id)
            end = DummyOperator(task_id='End')

            start >> fetch_parent_dag_details >> pre_validation_job >> create_cluster_task >> zip_gcs_folder_to_another_task >> move_output_file_to_landing >> control_table_task >> cleanup >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        if self.job_config:
            dags = {}

            for job_id, config in self.job_config.items():
                self.default_args.update(config.get(consts.DEFAULT_ARGS))
                dags[job_id] = self.create_dag(job_id, config)
                dags[job_id].tags = config.get(consts.TAGS)

            return dags
        else:
            return {}
