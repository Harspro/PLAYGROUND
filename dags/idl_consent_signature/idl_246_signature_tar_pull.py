import logging
from typing import Final

import pendulum
from airflow import DAG, configuration as airflow_conf, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import util.constants as consts
from pcf_operators.sftp_operator.sftp_get_operator import PCBSFTPGetOperator
from util.miscutils import (read_variable_or_file, read_yamlfile_env)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
DAG_ID: Final[str] = "idl-246-consent-signature-tar-pull"
CONSENT_TAG: Final[str] = "consents"


class Idl246Signature:

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/idl_consent_signature/'
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def prepare_config(self, **context):
        conf = context['dag_run'].conf

        source_bucket = airflow_conf.conf.get('logging', 'remote_base_log_folder').split('/')[2]
        context['ti'].xcom_push(key='source_bucket', value=source_bucket)
        context['ti'].xcom_push(key='destination_bucket', value=conf.get('destination_bucket'))
        destination_path = conf.get('destination_path')
        destination_filename = destination_path.rsplit('/', 1)[-1]
        context['ti'].xcom_push(key='destination_path', value=destination_path)
        context['ti'].xcom_push(key='destination_filename', value=destination_filename)
        context['ti'].xcom_push(key='gcs_local_filepath', value=[f"data/{destination_filename}"])
        context['ti'].xcom_push(key='remote_filepath', value=conf.get('remote_filepath'))
        context['ti'].xcom_push(key='lookup_key', value=conf.get('lookup_key'))

    def create_dag(self, dag_id: str) -> DAG:
        dag = DAG(dag_id=dag_id,
                  schedule=None,
                  render_template_as_native_obj=True,
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  is_paused_upon_creation=True,
                  catchup=False,
                  default_args=self.job_config[consts.DEFAULT_ARGS],
                  tags=[CONSENT_TAG])

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)

            prepare_config_task = PythonOperator(
                task_id="prepare_config",
                python_callable=self.prepare_config
            )

            move_data_from_remote_server = PCBSFTPGetOperator(
                task_id="move_data_from_remote_server",
                local_filepath="/home/airflow/gcs/data/{{ ti.xcom_pull(task_ids='prepare_config', key='destination_filename') }}",
                remote_filepath="{{ ti.xcom_pull(task_ids='prepare_config', key='remote_filepath') }}",
                lookup_key="{{ ti.xcom_pull(task_ids='prepare_config', key='lookup_key') }}")

            move_tar_to_cnst_sign_bucket = GCSToGCSOperator(
                task_id="move_tar_to_cnst_sign_bucket",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket') }}",
                source_object="data/{{ ti.xcom_pull(task_ids='prepare_config', key='destination_filename') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_bucket') }}",
                destination_object="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_path') }}"
            )

            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            start_point >> prepare_config_task >> move_data_from_remote_server >> move_tar_to_cnst_sign_bucket >> end_point

            return add_tags(dag)

    def create(self) -> dict:
        dags = {DAG_ID: self.create_dag(DAG_ID)}
        return dags


globals().update(Idl246Signature('idl_consent_signature_config.yaml').create())
