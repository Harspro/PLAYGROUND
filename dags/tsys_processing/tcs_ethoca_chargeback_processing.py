import logging
import pendulum
import util.constants as consts

from abc import ABC
from airflow.exceptions import AirflowFailException
from airflow import settings, DAG
from airflow.operators.empty import EmptyOperator
from google.cloud.bigquery import DestinationFormat
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import bigquery
from typing import Final
from util.bq_utils import run_bq_query
from util.miscutils import (
    read_file_env,
    read_variable_or_file,
    read_yamlfile_env_suffix,
    read_env_filepattern
)
from dag_factory.terminus_dag_factory import add_tags

TCS_FILE_LOADER: Final = "tcs_ethoca_dispute_loader"
logger = logging.getLogger(__name__)


class TCSEthocaDisputeLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/{consts.CONFIG}/tcs_ethoca_chargeback_config.yaml"
        self.deploy_env_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.job_config = read_yamlfile_env_suffix(f"{self.dag_config_fname}", self.deploy_env, self.deploy_env_suffix)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.default_args = {
            "owner": "team-money-movement-eng",
            "depends_on_past": False,
            "wait_for_downstream": False,
            "retries": 3,
            "retry_delay": timedelta(seconds=10),
            'capability': 'customer-service',
            'severity': 'P3',
            'sub_capability': 'disputes',
            'business_impact': 'Loading latest TCS Dispute non-fraud dispute transaction to Ethoca failed',
            'customer_impact': 'None'
        }

    def run_ethoca_disp_stgn_query(self, sql_file_path, **context):
        logger.info("Running Sql file to create the Ethoca dispute data staging table from TCS Dispute table")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env)
        date_value = self.get_previous_run_date(**context)
        sql = sql.replace("<<<FROM_DATE_VAL>>>", f"{date_value}")
        run_bq_query(sql)
        logger.info("Ethoca dispute data staging table created with both valid and reject records")

    def run_ethoca_disp_valid_record_query(self, valid_sql_path):
        logger.info("Running Sql file to create the Ethoca dispute data table with valid records")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + valid_sql_path, self.deploy_env)
        run_bq_query(sql)
        logger.info("Ethoca dispute data table created with latest valid TCS Dispute details")

    def run_ethoca_disp_reject_record_query(self, reject_sql_path):
        logger.info("Running Sql file to create the Ethoca dispute data table with rejected records")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + reject_sql_path, self.deploy_env)
        run_bq_query(sql)
        logger.info("Ethoca reject dispute data table loaded with rejected records")

    def get_previous_run_date(self, **context):
        date_value = context[consts.DAG_RUN].conf.get("date")
        if date_value is not None:
            return f'"{date_value}"'
        else:
            return "NULL"

    def export_bq_table(self, landing_bucket, dataset_id, table_name):
        destination_folder = f"gs://{landing_bucket}"
        destination_filename_pattern = f"pcb_ethc_non_fraud_{{file_env}}_{datetime.today().strftime('%Y%m%d%H%M%S')}.csv"
        destination_filename = read_env_filepattern(destination_filename_pattern, self.deploy_env)
        logger.info(f"destination_filename : {destination_folder}/{destination_filename}")
        client = bigquery.Client()
        job_config = bigquery.ExtractJobConfig(destination_format=DestinationFormat.CSV, field_delimiter='|')
        dataset_ref = bigquery.DatasetReference(project=self.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID), dataset_id=dataset_id)
        table_ref = bigquery.TableReference(dataset_ref=dataset_ref, table_id=table_name)
        extract_job = client.extract_table(source=table_ref, destination_uris=f'{destination_folder}/{destination_filename}', job_config=job_config)
        extract_job.result()
        if extract_job.done() and extract_job.error_result is None:
            logger.info("Extract job completed successfully")
        else:
            raise AirflowFailException(f"Extract job not completed due to {extract_job.error_result}")

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            description="DAG to send TCS Dispute non-fraud dispute transaction to Ethoca",
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
            run_tcs_dispute_stgng_query = PythonOperator(
                task_id="run_ethoca_disp_stgng_query",
                python_callable=self.run_ethoca_disp_stgn_query,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("sql_file_path")}
            )
            run_tcs_dispute_query_valid = PythonOperator(
                task_id="run_ethoca_disp_valid_records_query",
                python_callable=self.run_ethoca_disp_valid_record_query,
                op_kwargs={"valid_sql_path": self.job_config.get(dag_id).get("valid_sql_path")}
            )
            run_ethoca_disp_reject_record_query = PythonOperator(
                task_id="run_ethoca_disp_reject_records_query",
                python_callable=self.run_ethoca_disp_reject_record_query,
                op_kwargs={"reject_sql_path": self.job_config.get(dag_id).get("reject_sql_path")}
            )
            export_table_to_gcs = PythonOperator(
                task_id="export_table_to_gcs",
                python_callable=self.export_bq_table,
                op_kwargs={
                    consts.LANDING_BUCKET: self.job_config.get(dag_id).get(consts.LANDING_BUCKET),
                    consts.DATASET_ID: self.job_config.get(dag_id).get(consts.DATASET_ID),
                    consts.TABLE_NAME: self.job_config.get(dag_id).get(consts.TABLE_NAME)}
            )
            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> run_tcs_dispute_stgng_query \
                >> run_ethoca_disp_reject_record_query >> run_tcs_dispute_query_valid \
                >> export_table_to_gcs >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags


globals().update(TCSEthocaDisputeLoader().create_dags())
