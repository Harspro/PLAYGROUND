import logging
import pandas as pd
import numpy as np
import pendulum
from airflow.exceptions import AirflowException

import util.constants as consts

from abc import ABC
from airflow import settings, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from datetime import timedelta, datetime
from typing import Final
from util.gcs_utils import delete_folder
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env, read_file_env,
)
from util.bq_utils import run_bq_query
from dag_factory.terminus_dag_factory import add_tags

EFT_MANUAL_LIMITS_LOADER: Final = 'eft_manual_limits_loader'
logger = logging.getLogger(__name__)


class EftLimitsLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/eft_limits_processing/eft_manual_limits_config.yaml"
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
            'business_impact': 'EFT Manual limits update failed, messages not delivered to Kafka',
            'customer_impact': 'EFT Manual limits update failed'
        }

    def load_eft_limits_to_landing_bq(self, dag_id, **context):
        bucket_name = context['dag_run'].conf.get('bucket')
        file_name = context['dag_run'].conf.get('name')
        bq_client = bigquery.Client()
        logger.info(f'bucket{bucket_name} file {file_name}')
        csv_file_path = f"gs://{bucket_name}/{file_name}"
        df = pd.read_csv(csv_file_path)
        df['FILE_NAME'] = file_name.split("/", 1)[-1]
        bq_client.load_table_from_dataframe(df, self.job_config.get(dag_id).get(consts.LANDING_TABLE))

    def delete_limits_parquet_folder(self, dag_id, obj_prefix):
        return delete_folder(self.job_config.get(dag_id).get(consts.PROCESSING_BUCKET_EXTRACT), obj_prefix)

    def run_sql_to_create_parquet(self, query_file, **context):
        logger.info("Running Sql file to create parquet file from the current limits file")
        folder_file_name = context['dag_run'].conf.get('name')
        file_name = folder_file_name.split("/", 1)[-1]
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + query_file, self.deploy_env)
        logger.info(f'filename:{file_name}')
        sql = sql.replace("<<FILENAME>>", f"{file_name}")
        run_bq_query(sql)
        logger.info("Parquet file created for the current file")

    def validate_min_max_limits(self, **context):
        bucket_name = context['dag_run'].conf.get('bucket')
        file_name = context['dag_run'].conf.get('name')
        logger.info(f'bucket{bucket_name} file {file_name}')
        csv_file_path = f"gs://{bucket_name}/{file_name}"
        df = pd.read_csv(csv_file_path, header=0, dtype={'CUSTOMERID': np.int64, 'DAILY': np.int64, 'MONTHLY': np.int64})
        limits_df = df[(df['DAILY'] > 25000) | (df['MONTHLY'] > 50000)]
        if not limits_df.empty:
            raise AirflowException(f'Invalid limits given for the below customers \n"{limits_df.to_string(index=False)}')

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            description="DAG to load eft limits manually send updated events to Kafka",
            tags=self.job_config.get(dag_id).get(consts.TAGS),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
        )

        with dag:
            start = EmptyOperator(task_id=consts.START_TASK_ID)
            validate_daily_monthly_limit = PythonOperator(
                task_id="validate_daily_monthly_limit",
                python_callable=self.validate_min_max_limits
            )
            load_eft_limits_to_bq = PythonOperator(
                task_id="load_eft_limits_to_landing_bq",
                python_callable=self.load_eft_limits_to_landing_bq,
                op_kwargs={"dag_id": dag_id}
            )
            delete_limits_staging_folder = PythonOperator(
                task_id="delete_triad_staging_folder",
                python_callable=self.delete_limits_parquet_folder,
                op_kwargs={"dag_id": dag_id, consts.OBJ_PREFIX: self.job_config.get(dag_id).get(consts.FOLDER_NAME)}
            )
            run_sql_to_create_parquet = PythonOperator(
                task_id="run_sql_to_create_parquet",
                python_callable=self.run_sql_to_create_parquet,
                op_kwargs={consts.QUERY_FILE: self.job_config.get(dag_id).get(consts.QUERY_FILE)}
            )
            end = EmptyOperator(task_id=consts.END_TASK_ID)
            kafka_writer_task = TriggerDagRunOperator(
                task_id=consts.KAFKA_WRITER_TASK,
                trigger_dag_id=self.job_config.get(dag_id).get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=datetime.now(self.local_tz),
                conf={
                    consts.BUCKET: self.job_config.get(dag_id).get(consts.GCS).get(consts.STAGING_BUCKET),
                    consts.FOLDER_NAME: self.job_config.get(dag_id).get(consts.GCS).get(consts.FOLDER_NAME),
                    consts.FILE_NAME: self.job_config.get(dag_id).get(consts.GCS).get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.DATAPROC_CLUSTER_NAME)
                },
                wait_for_completion=True,
                poke_interval=30
            )

            (start >> validate_daily_monthly_limit >> load_eft_limits_to_bq >> delete_limits_staging_folder
             >> run_sql_to_create_parquet >> kafka_writer_task >> end)
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags


globals().update(EftLimitsLoader().create_dags())
