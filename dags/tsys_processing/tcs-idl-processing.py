import logging
from abc import ABC
from datetime import timedelta, datetime
from typing import Final, List

import pendulum
from airflow import settings, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator

import util.constants as consts
from util.bq_utils import run_bq_query
from util.deploy_utils import (
    pause_unpause_dag,
    read_pause_unpause_setting
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_file_env,
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_config_by_job_size
)
from dag_factory.terminus_dag_factory import add_tags

TCS_IDL_LOADER: Final = 'tcs_idl_loader'


class TCSIDLLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/tsys_processing/tcs-idl-config.yaml"
        self.job_config = read_yamlfile_env(f"{self.dag_config_fname}", self.deploy_env)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
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

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def drop_bq_stgn_tables(self, sql_file_path, table_name, **context):
        logging.info(f'Running cleanup of staging table for {table_name}')
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env)
        run_bq_query(sql)
        logging.info("Rows deleted from staging tables")

    def create_def_args(self, dag_config: dict):
        default_args = self.default_args.copy()
        default_args.update({"business_impact": f'{dag_config.get("business_impact")}',
                             "customer_impact": f'{dag_config.get("customer_impact")}',
                             "sub_capability": f'{dag_config.get("sub_capability")}',
                             "capability": f'{dag_config.get("capability")}'})
        return default_args

    def load_staging_table_to_bq_table(self, sql_file_path, table_name, **context):
        logging.info(f'Running Sql file to load the rows of {table_name} from staging tables to corresponding base table')
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env)
        run_bq_query(sql)
        logging.info("Staging tables data loaded into the TCS tables")

    def build_cluster_creating_task(self, dag_id: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG), self.job_config.get(dag_id).get("job_size")),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=self.job_config.get(dag_id).get(consts.CLUSTER_NAME)
        )

    def dataproc_spark_submit_task(self, dag_id: str):
        arg_list: List[str] = [f'hive.table.name={self.job_config.get(dag_id).get("hive_table_name")}',
                               f'hive.schema.name={self.job_config.get(dag_id).get("hive_schema_name")}',
                               f'hive.where.cond={self.job_config.get(dag_id).get("hive_where_cond")}']
        spark_arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)

        SPARK_JOB = {
            consts.REFERENCE: {consts.PROJECT_ID: self.gcp_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.CLUSTER_NAME)},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: self.job_config.get(dag_id).get(consts.JAR_FILE_URIS),
                consts.FILE_URIS: self.job_config.get(dag_id).get(consts.FILE_URIS),
                consts.MAIN_CLASS: self.job_config.get(dag_id).get(consts.MAIN_CLASS),
                consts.PROPERTIES: self.job_config.get(dag_id).get(consts.PROPERTIES),
                consts.ARGS: spark_arg_list
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
            tags=self.job_config.get(dag_id).get("tags"),
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
                is_paused = read_pause_unpause_setting(TCS_IDL_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)
            cluster_creating = self.build_cluster_creating_task(dag_id)
            dataproc_submit_spark_job = self.dataproc_spark_submit_task(dag_id)
            load_file_stgn_table_rows_bq = PythonOperator(
                task_id="load_stgn_table_rows_to_bq",
                python_callable=self.load_staging_table_to_bq_table,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("sql_file_path"),
                           "table_name": self.job_config.get(dag_id).get("table_name")},
            )

            delete_processed_file_rows_from_stgn = PythonOperator(
                task_id="delete_processed_rows_from_stgn",
                python_callable=self.drop_bq_stgn_tables,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("drop_sql_file_path"),
                           "table_name": self.job_config.get(dag_id).get("table_name")},
            )
            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> cluster_creating \
                >> dataproc_submit_spark_job >> load_file_stgn_table_rows_bq \
                >> delete_processed_file_rows_from_stgn >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logging.info(f'Config file {self.dag_config_fname} not found.')
            return dags

        for key, values in self.job_config.items():
            dags[key] = self.create_dag(key, values, self.create_def_args(values))

        return dags


globals().update(TCSIDLLoader().create_dags())
