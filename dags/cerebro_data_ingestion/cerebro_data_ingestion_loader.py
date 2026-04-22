import json
import logging
from typing import Final

import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from data_transfer.util.utils import write_marker_file_to_business_bucket
from util.miscutils import (read_variable_or_file, read_yamlfile_env_suffix)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
DAG_ID: Final[str] = "pcb-cerebro-data-ingestion"
GS_TAG: Final[str] = "team-growth-and-sales"
NP_SUFFIX: Final[str] = "-np"
PROD_ENV: Final[str] = "prod"
CR_ARCHIVE_BUCKET: Final[str] = "creditrisk_processed_bucket"
CR_ARCHIVE_PATH: Final[str] = "creditrisk_processed_path"
DESTINATION_BUCKET_NP: Final[str] = "destination_bucket_np"
DESTINATION_PATH_NP: Final[str] = "destination_path_np"
ARCHIVE_BUCKET_PROD: Final[str] = "archive_bucket_prod"


def read_json_schema(file_path):
    with open(file_path, 'r') as f:
        schema = json.load(f)
    return schema


class CerebroDataIngestionLoader:

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/cerebro_data_ingestion/'
        self.job_config = read_yamlfile_env_suffix(f'{config_dir}/{config_filename}', self.deploy_env,
                                                   env_suffix=NP_SUFFIX if self.deploy_env != PROD_ENV else '')
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.cerebro_schema = f"{settings.DAGS_FOLDER}/cerebro_data_ingestion/CEREBRO_DATA_TABLE_SCHEMA.json"
        if self.deploy_env == PROD_ENV:
            self.cerebro_bucket = self.job_config[consts.DESTINATION_BUCKET]
            self.cerebro_destination = self.job_config[consts.DESTINATION_PATH]
            self.archive_bucket = self.job_config[ARCHIVE_BUCKET_PROD]
        else:
            self.cerebro_bucket = self.job_config[DESTINATION_BUCKET_NP]
            self.cerebro_destination = self.job_config[DESTINATION_PATH_NP]
            self.archive_bucket = self.job_config[consts.ARCHIVE_BUCKET]

    def prepare_config(self, **context):
        required_keys = ['source_bucket_name', 'source_file_path', 'marker_file_bucket', 'marker_file_prefix']
        conf = context['dag_run'].conf

        missing_keys = [key for key in required_keys if key not in conf]
        if missing_keys:
            raise AirflowException(f"Missing required configuration keys: {', '.join(missing_keys)}")

        context['ti'].xcom_push(key='marker_file_bucket', value=conf.get('marker_file_bucket'))
        context['ti'].xcom_push(key='marker_file_prefix', value=conf.get('marker_file_prefix'))
        context['ti'].xcom_push(key='source_bucket_name', value=conf.get('source_bucket_name'))
        context['ti'].xcom_push(key='source_file_path', value=conf.get('source_file_path'))
        context['ti'].xcom_push(key='source_file_name', value=conf.get('source_file_path').split("/")[-1])

    def create_dag(self, dag_id: str) -> DAG:

        dag = DAG(dag_id=dag_id,
                  schedule=None,
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  is_paused_upon_creation=True,
                  catchup=False,
                  default_args=self.job_config[consts.DEFAULT_ARGS],
                  tags=[GS_TAG])

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            prepare_config = PythonOperator(task_id='prepare_config', python_callable=self.prepare_config)

            creditrisk_to_bq = GCSToBigQueryOperator(
                task_id='creditrisk_to_bigquery',
                bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_objects="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_project_dataset_table=self.job_config[consts.LANDING_TABLE],
                schema_fields=read_json_schema(self.cerebro_schema),
                skip_leading_rows=1,
                write_disposition=consts.WRITE_APPEND,
                location=self.gcp_config.get(consts.BQ_QUERY_LOCATION)
            )

            send_to_cerebo = GCSToGCSOperator(
                task_id="send_to_cerebo",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket=self.cerebro_bucket,
                destination_object=f"{self.cerebro_destination}/{{{{ti.xcom_pull(task_ids='prepare_config', key='source_file_name')}}}}",
                move_object=False,
                exact_match=True
            )

            archive_to_pcb = GCSToGCSOperator(
                task_id="archive_to_pcb",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket=self.archive_bucket,
                destination_object=f"{self.job_config[consts.ARCHIVE_PATH]}/{{{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_name') }}}}",
                move_object=False,
                exact_match=True
            )

            archive_to_creditrisk = GCSToGCSOperator(
                task_id="archive_to_creditrisk",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket=self.job_config[CR_ARCHIVE_BUCKET],
                destination_object=f"{self.job_config[CR_ARCHIVE_PATH]}/{{{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_name') }}}}",
                move_object=True,
                exact_match=True
            )

            write_marker_file_to_business_bucket_task = PythonOperator(
                task_id="write_marker_file_to_business_bucket",
                python_callable=write_marker_file_to_business_bucket,
                op_kwargs={
                    'marker_file_bucket': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_bucket') }}",
                    'marker_file_prefix': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_prefix') }}",
                    'task_id': 'archive_to_creditrisk'
                },
                trigger_rule='none_skipped'
            )

            start_point >> prepare_config >> creditrisk_to_bq >> send_to_cerebo >> archive_to_pcb >> archive_to_creditrisk >> write_marker_file_to_business_bucket_task >> end_point

            return add_tags(dag)

    def create(self) -> dict:
        dags = {}
        dags[DAG_ID] = self.create_dag(DAG_ID)
        return dags


globals().update(CerebroDataIngestionLoader('cerebro_data_ingestion_config.yaml').create())
