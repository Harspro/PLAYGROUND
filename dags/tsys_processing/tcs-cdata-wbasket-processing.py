import logging
from abc import ABC
from datetime import timedelta, datetime
from typing import Final

import pendulum
from airflow import settings, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import util.constants as consts
from util.bq_utils import run_bq_query
from util.deploy_utils import (
    pause_unpause_dag,
    read_pause_unpause_setting,
    read_feature_flag
)
from util.miscutils import (
    get_cluster_name_for_dag,
    get_ephemeral_cluster_config,
    read_file_env,
    read_variable_or_file,
    read_yamlfile_env,
)
from dag_factory.terminus_dag_factory import add_tags

TCS_FILE_LOADER: Final = 'tcs_file_loader'


class TCSFileLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/tsys_processing/tcs-processing-config.yaml"
        self.job_config = read_yamlfile_env(f"{self.dag_config_fname}", self.deploy_env)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.default_args = {
            "owner": "team-money-movement-eng",
            "depends_on_past": False,
            "wait_for_downstream": False,
            "retries": 0,
            "retry_delay": timedelta(seconds=10),
            'capability': 'Payments',
            'severity': 'P3',
            'sub_capability': 'EMT',
            'business_impact': 'none',
            'customer_impact': 'None'
        }

    def drop_bq_stgn_tables(self, sql_file_path, **context):
        file_name = context[consts.DAG_RUN].conf.get(consts.FILE_NAME)
        logging.info(f'Running Sql file to delete the rows of current file {file_name}  from staging tables')
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env).replace("<<file_name>>", file_name)
        run_bq_query(sql)
        logging.info("Rows deleted from staging tables")

    def create_def_args(self, dag_config: dict):
        default_args = self.default_args.copy()
        default_args.update({"business_impact": f'{dag_config.get("business_impact")}',
                             "customer_impact": f'{dag_config.get("customer_impact")}',
                             "sub_capability": f'{dag_config.get("sub_capability")}',
                             "capability": f'{dag_config.get("capability")}'})
        return default_args

    def load_staging_table_to_bq_table(self, sql_file_path, **context):
        file_name = context[consts.DAG_RUN].conf.get(consts.FILE_NAME)
        logging.info(f'Running Sql file to load the rows of current file {file_name} from staging tables to TCS tables')
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env).replace("<<file_name>>", file_name)
        run_bq_query(sql)
        logging.info("Staging tables data loaded into the TCS tables")

    def get_ethoca_dispute_trigger(self, dag_id):
        if dag_id in [self.job_config.get(dag_id).get("cdata_dag")]:
            return f"trigger_{self.job_config.get(dag_id).get('trigger_dag_id')}"
        else:
            return "end"

    def check_tcsdispute_dag(self, dag_id):
        return BranchPythonOperator(
            task_id="tcs_dispute_dag_check",
            python_callable=self.get_ethoca_dispute_trigger,
            op_kwargs={"dag_id": dag_id}
        )

    def build_cluster_creating_task(self, dag_id: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_ephemeral_cluster_config(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG), self.job_config.get(dag_id).get("num_workers")),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=self.job_config.get(dag_id).get(consts.CLUSTER_NAME)
        )

    def copy_from_landing_to_processing_task(self, dag_id: str):
        file_name = "{{ dag_run.conf['name'] }}"
        return GCSToGCSOperator(
            task_id="copy_from_landing_to_processing",
            gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
            source_bucket="{{ dag_run.conf['bucket'] }}",
            source_object=file_name,
            destination_bucket=self.job_config.get(dag_id).get(consts.STAGING_FOLDER),
            destination_object=file_name,
        )

    def dataproc_spark_submit_task(self, dag_id: str):
        FOLDER_NAME = "{{ dag_run.conf['name'] }}"
        SPARK_JOB = {
            consts.REFERENCE: {consts.PROJECT_ID: self.gcp_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.CLUSTER_NAME)},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: self.job_config.get(dag_id).get(consts.JAR_FILE_URIS),
                consts.FILE_URIS: self.job_config.get(dag_id).get(consts.FILE_URIS),
                consts.MAIN_CLASS: self.job_config.get(dag_id).get(consts.MAIN_CLASS),
                consts.PROPERTIES: self.job_config.get(dag_id).get(consts.PROPERTIES),
                consts.ARGS: [f'gcs_staging_bucket_name={self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}',
                              f'data_file_name={FOLDER_NAME}',
                              f'tcs_file_type={self.job_config.get(dag_id).get("tcs_file_type")}']
            }
        }
        return DataprocSubmitJobOperator(
            task_id="dataproc_spark_submit_task",
            job=SPARK_JOB,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            region=self.dataproc_config.get(consts.LOCATION),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def create_dag(self, dag_id: str, dag_config: dict, default_args: dict) -> DAG:
        dag = DAG(
            description=self.job_config.get(dag_id).get("description"),
            tags=self.job_config.get(dag_id).get(consts.TAGS),
            max_active_runs=5,
            catchup=False,
            is_paused_upon_creation=True,
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TCS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)
            if read_feature_flag("feature.flag.dataproc.cluster.size.optimization.enabled", self.deploy_env):
                self.ephemeral_cluster_name = get_cluster_name_for_dag(dag_id)

            start = EmptyOperator(task_id=consts.START_TASK_ID)
            cluster_creating = self.build_cluster_creating_task(dag_id)
            copy_from_landing_to_processing = self.copy_from_landing_to_processing_task(dag_id)
            dataproc_submit_spark_job = self.dataproc_spark_submit_task(dag_id)
            load_file_stgn_table_rows_bq = PythonOperator(
                task_id="load_file_stgn_table_rows_to_bq",
                python_callable=self.load_staging_table_to_bq_table,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("sql_file_path")},
            )

            delete_processed_file_rows_from_stgn = PythonOperator(
                task_id="delete_processed_file_rows_from_stgn",
                python_callable=self.drop_bq_stgn_tables,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("drop_sql_file_path")},
            )
            check_tcsdispute_task = self.check_tcsdispute_dag(dag_id)
            trigger_dag_ethoca = TriggerDagRunOperator(
                task_id=f"trigger_{self.job_config.get(dag_id).get('trigger_dag_id')}",
                trigger_dag_id=self.job_config.get(dag_id).get('trigger_dag_id'),
                wait_for_completion=True,
                poke_interval=30,
            )
            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> cluster_creating >> copy_from_landing_to_processing \
                >> dataproc_submit_spark_job >> load_file_stgn_table_rows_bq \
                >> delete_processed_file_rows_from_stgn >> check_tcsdispute_task \
                >> trigger_dag_ethoca >> end
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for key, values in self.job_config.items():
            dags[key] = self.create_dag(key, values, self.create_def_args(values))

        return dags


globals().update(TCSFileLoader().create_dags())
