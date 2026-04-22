import logging
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator

import util.constants as consts
from util.miscutils import (read_variable_or_file, read_yamlfile_env, get_cluster_config_by_job_size)
from airflow.operators.python import PythonOperator
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
DAG_ID: Final[str] = "idl-consent-signature"
CONSENT_TAG: Final[str] = "consents"


class IdlConsentSignature:

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/idl_consent_signature/'
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.ephemeral_cluster_name = self.job_config["cluster_name"]

    def check_idl_type(self, **kwargs):
        idl_type = kwargs['dag_run'].conf.get('IDL_TYPE', None)
        if not idl_type:
            raise ValueError("IDL_TYPE is not provided in the DAG configuration!")

    def build_spark_fetching_job(self):
        idl_type = "{{ dag_run.conf.get('IDL_TYPE') }}"
        return {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.ephemeral_cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: self.job_config[consts.SPARK_JOB][consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: self.job_config[consts.SPARK_JOB][consts.MAIN_CLASS],
                consts.FILE_URIS: self.job_config[consts.SPARK_JOB][consts.FILE_URIS],
                consts.PROPERTIES: self.job_config[consts.SPARK_JOB][consts.PROPERTIES],
                consts.ARGS: [
                    f'idl.type = {idl_type}'
                ]
            }
        }

    def create_dag(self, dag_id: str) -> DAG:
        dag = DAG(dag_id=dag_id,
                  schedule=None,
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  is_paused_upon_creation=True,
                  catchup=False,
                  default_args=self.job_config[consts.DEFAULT_ARGS],
                  tags=[CONSENT_TAG])

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)

            check_idl_type_task = PythonOperator(
                task_id='check_idl_type_task',
                python_callable=self.check_idl_type
            )

            create_cluster = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_cluster_config_by_job_size(self.deploy_env,
                                                              self.gcp_config.get(consts.NETWORK_TAG), "medium"),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=self.ephemeral_cluster_name,
            )

            idl_consent_signature = DataprocSubmitJobOperator(
                task_id="submit_idl_java_spark_job",
                job=self.build_spark_fetching_job(),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            )

            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            start_point >> check_idl_type_task >> create_cluster >> idl_consent_signature >> end_point

            return add_tags(dag)

    def create(self) -> dict:
        dags = {DAG_ID: self.create_dag(DAG_ID)}
        return dags


globals().update(IdlConsentSignature('idl_consent_signature_config.yaml').create())
