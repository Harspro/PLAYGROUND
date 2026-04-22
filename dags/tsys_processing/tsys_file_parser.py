from datetime import timedelta, datetime
from copy import deepcopy
import logging
import pendulum
from typing import Final
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_config_by_job_size
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args
as needed through configuration placed in the "tsys_file_parser_config.yaml" file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


class TsysFileParser:

    """
    TsysFileParser class creates Airflow DAGs for spinning up a DataProc cluster and executing a DataProc job
    to parse any TSYS file to parquet files in GCS bucket.

     Methods:
        build_filter_job(self, cluster_name: str, config: dict, datafilepath: str): Constructs details for dataproc job(file filtering).
        build_parse_job(self, cluster_name: str, config: dict, datafilepath: str, outputpath: str): Constructs details for dataproc job(file parsing).
        create_dag(): Defines the structure for the dynamically created DAGs.
        create_dags(): Runs through provided configuration in tsys_file_parser_config.yaml and dynamically
                       creates DAGs based on them.
    """

    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.network_tag = self.gcp_config[consts.NETWORK_TAG]
        self.processing_zone_connection_id = self.gcp_config[consts.PROCESSING_ZONE_CONNECTION_ID]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/tsys_processing'

        if config_filename is None:
            config_filename = 'tsys_file_parser_config.yaml'

        self.dag_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.dataproc_location = self.dataproc_config[consts.LOCATION]
        self.dataproc_project_id = self.dataproc_config[consts.PROJECT_ID]

    def build_filter_job(self, cluster_name: str, config: dict, datafilepath: str):
        arg_list = []

        for k, v in config[consts.SPARK_FILTER_JOB][consts.ARGS].items():
            arg_list.append(f'{k}={v}')

        arg_list.append(f'pcb.tsys.processor.datafile.path={datafilepath}')

        spark_filter_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_project_id},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: config[consts.SPARK_FILTER_JOB][consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: config[consts.SPARK_FILTER_JOB][consts.MAIN_CLASS],
                consts.ARGS: arg_list
            }
        }
        return spark_filter_job

    def build_parse_job(self, cluster_name: str, config: dict, datafilepath: str, outputpath: str):
        arg_list = []
        for k, v in config[consts.SPARK_PARSE_JOB][consts.ARGS].items():
            arg_list.append(f'{k}={v}')

        arg_list.append(f'pcb.tsys.processor.datafile.path={datafilepath}')
        arg_list.append(f'pcb.tsys.processor.output.path={outputpath}')

        spark_parse_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_project_id},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: config[consts.SPARK_PARSE_JOB][consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: config[consts.SPARK_PARSE_JOB][consts.MAIN_CLASS],
                consts.ARGS: arg_list
            }
        }

        return spark_parse_job

    def create_dag(self, dag_id: str, config: dict):
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            description=config[consts.DAG].get(consts.DESCRIPTION),
            default_args=self.default_args,
            dagrun_timeout=timedelta(minutes=30),
            is_paused_upon_creation=True,
            tags=config[consts.DAG].get(consts.TAGS)
        )

        with dag:

            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            job_size = config[consts.DAG].get(consts.DATAPROC_JOB_SIZE)
            cluster_name = config[consts.DAG].get(consts.DATAPROC_CLUSTER_NAME)
            processing_extract_bucket = config.get(consts.PROCESSING_BUCKET_EXTRACT)
            file_type = config.get(consts.FILE_TYPE)

            datafilepath = f"gs://{config.get(consts.PROCESSING_BUCKET)}/{{{{ dag_run.conf['name']}}}}"
            outputpath = f"gs://{config.get(consts.PROCESSING_BUCKET_EXTRACT)}/{{{{ dag_run.conf['name'] }}}}" + '_extract/' + f"{file_type}"

            move_file_to_processing_zone = GCSToGCSOperator(
                task_id="moving_file_to_processing_zone",
                gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
                source_bucket="{{ dag_run.conf['source_bucket'] }}",
                source_object="{{ dag_run.conf['name'] }}",
                destination_bucket=config.get(consts.PROCESSING_BUCKET),
                destination_object="{{ dag_run.conf['name']}}"
            )

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_project_id,
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.network_tag, job_size),
                region=self.dataproc_location,
                cluster_name=cluster_name
            )

            file_filter_task = DataprocSubmitJobOperator(
                task_id="dataproc_submit_file_filter_job",
                job=self.build_filter_job(cluster_name, config, datafilepath),
                region=self.dataproc_location,
                project_id=self.dataproc_project_id,
                gcp_conn_id=self.processing_zone_connection_id
            )

            file_parser_task = DataprocSubmitJobOperator(
                task_id='dataproc_submit_file_parser_task',
                job=self.build_parse_job(cluster_name, config, datafilepath, outputpath),
                region=self.dataproc_location,
                project_id=self.dataproc_project_id,
                gcp_conn_id=self.processing_zone_connection_id
            )

            if consts.KAFKA_WRITER in config:
                dag_trigger_task = TriggerDagRunOperator(
                    task_id=f"{consts.DAG_TRIGGER_TASK}_{file_type}",
                    trigger_dag_id=config[consts.KAFKA_WRITER].get(consts.DAG_ID),
                    conf={
                        consts.BUCKET: processing_extract_bucket,
                        consts.NAME: "{{ dag_run.conf['name'] }}",
                        consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
                        consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}",
                        consts.CLUSTER_NAME: cluster_name
                    },
                    wait_for_completion=config[consts.KAFKA_WRITER].get(consts.WAIT_FOR_COMPLETION),
                    poke_interval=30
                )

                start_point >> move_file_to_processing_zone >> cluster_creating_task >> file_filter_task >> file_parser_task >> dag_trigger_task >> end_point
            else:
                start_point >> move_file_to_processing_zone >> cluster_creating_task >> file_filter_task >> file_parser_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.dag_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags


globals().update(TsysFileParser().create_dags())
