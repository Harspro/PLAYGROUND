import logging
import os
import re
from datetime import timedelta, datetime
from typing import Final
from copy import deepcopy
import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import util.constants as consts
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.logging_utils import build_spark_logging_info
from util.miscutils import read_file_env, read_variable_or_file, read_yamlfile_env, get_cluster_config_by_job_size

from util.bq_utils import run_bq_query
from tokenized_consent_capture.external_task_status_pokesensor import ExternalTaskPokeSensor
from dag_factory.terminus_dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig

logger = logging.getLogger(__name__)

LOGIN_FRAUD_CAPTURE_CONFIG_FILE: Final = 'login_fraud_kafka_writer_config.yaml'
local_tz = pendulum.timezone('America/Toronto')

DAG_DEFAULT_ARGS = {
    'owner': 'team-ogres-alerts',
    'capability': 'legal-compliance',
    'severity': 'P2',
    'sub_capability': 'tmx',
    'business_impact': 'This will impact the compliance',
    'customer_impact': 'none',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=10)
}


class LoginFraudDataKafkaProcessorDagBuilder(BaseDagBuilder):
    """
        Login Fraud Risk Data Kafka Processing DAG Builder

        A specialized DAG builder for processing Login Fraud Data from BigQuery
        to Kafka. This class orchestrates an end-to-end data pipeline that includes
        evaluating truth data count, exporting to parquet, triggering Kafka writerjigar.patel   .

        ## Purpose
        The LoginFraudDataKafkaProcessorDagBuilder processes Login Authentication Fraud
        from BigQuery by exporting pending records to parquet format and publishing
        them to Kafka, then updating the status to COMPLETED.

        ## Data Flow
        BigQuery Table  → Export to Parquet → Kafka Writer
        """

    def __init__(self, environment_config: EnvironmentConfig):
        super().__init__(environment_config)
        self.local_tz = environment_config.local_tz
        self.gcp_config = environment_config.gcp_config
        self.deploy_env = environment_config.deploy_env
        self.default_args = deepcopy(DAG_DEFAULT_ARGS)

    def validate_record_count(self, **kwargs):
        ti = kwargs['ti']
        record_count = ti.xcom_pull(task_ids=consts.QUERY_BIGQUERY_TABLE_TASK_ID, key='record_count')
        if record_count > 0:
            return consts.KAFKA_WRITER_TASK
        else:
            return consts.END_TASK_ID

    def get_file_date(self, file_path: str):
        file_name = os.path.basename(file_path)
        date_pattern = r"(\d{4})(\d{2})(\d{2})"
        date_str = re.search(date_pattern, file_name).group()
        derived_date = datetime.strptime(date_str, "%Y%m%d").date()
        return derived_date

    def run_bigquery_staging_query(self, query: str, project_id: str, dataset_id: str, table_id: str, file_name: str,
                                   **kwargs):
        """
        Runs bigquery staging query to create staging table.

        :param query: Staging Query.
        :type query: str

        :param project_id: BigQuery project ID.
        :type project_id: str

        :param dataset_id: BigQuery dataset ID.
        :type dataset_id: str

        :param table_id: BigQuery table ID.
        :type table_id: str

        :param file_name: data file.
        :type file_name: str
        """
        logger.info("Running query provided to create staging table in GCP processing zone.")
        logger.info("Extract date from the file name")
        file_create_date = self.get_file_date(file_name) if file_name else None
        file_create_date_filter = f"FILE_CREATE_DT = '{str(file_create_date)}'"
        ext_filter = kwargs["dag_run"].conf.get("file_create_date_filter")
        staging_table_id = f"{project_id}.{dataset_id}.{table_id}"
        if ext_filter:
            file_create_date_filter = f"A.{ext_filter}"
        stg_query = query.replace(f"{{{consts.STAGING_TABLE_ID}}}", staging_table_id).replace(
            f"{{{'file_create_date_filter'}}}", file_create_date_filter)
        logger.info(f"Running stg query: {stg_query}")
        run_bq_query(stg_query)
        bq_data_check_query = f"""SELECT COUNT(*) AS RECORDS_COUNT FROM`{staging_table_id}`"""
        bq_data_check_results = run_bq_query(bq_data_check_query).result()
        rec_count = next(bq_data_check_results).get('RECORDS_COUNT')
        logger.info(f"Staging table record counts: {rec_count}")
        ti = kwargs['ti']
        ti.xcom_push(key='record_count', value=rec_count)
        ti.xcom_push(key='file_create_date', value=file_create_date)

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build and return the Fraud Risk Truth Data Kafka processing DAG."""
        # Prepare default args
        self.default_args.update(config.get(consts.DEFAULT_ARGS, DAG_DEFAULT_ARGS))

        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description=config[consts.DAG][consts.DESCRIPTION],
            schedule=config[consts.DAG]['schedule'],
            start_date=datetime(2026, 1, 1, tzinfo=self.local_tz),
            max_active_runs=config[consts.DAG]['max_active_runs'],
            catchup=config[consts.DEFAULT_ARGS]['catchup'],
            is_paused_upon_creation=config[consts.DAG]['is_paused_upon_creation'],
            dagrun_timeout=timedelta(hours=2),
            tags=config[consts.DAG][consts.TAGS],
            render_template_as_native_obj=config[consts.DAG].get('render_template_as_native_obj', True),
            params={"file_create_date_filter": "", "name": None, "skip_sensor_task": False}
        )

        with dag:

            bq_config = config[consts.DAG].get(consts.BIGQUERY)
            gcs_config = config[consts.DAG].get(consts.GCS)
            bq_stg_project_id = bq_config.get(consts.PROJECT_ID)
            bq_stg_dataset = bq_config.get(consts.DATASET_ID)
            bq_stg_table_id = bq_config.get(consts.TABLE_ID).replace(
                consts.CURRENT_DATE_PLACEHOLDER,
                datetime.now(tz=local_tz).strftime('%Y_%m_%d')
            )
            if bq_config.get(consts.QUERY):
                query = bq_config.get(consts.QUERY)
            elif bq_config.get(consts.QUERY_FILE):
                query_sql_file = f"{settings.DAGS_FOLDER}/" \
                                 f"{bq_config.get(consts.QUERY_FILE)}"
                query = read_file_env(query_sql_file, self.deploy_env)
            else:
                raise AirflowFailException("Please provide query, none found.")

            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            bigquery_stg_query_task = PythonOperator(
                task_id=consts.QUERY_BIGQUERY_TABLE_TASK_ID,
                python_callable=self.run_bigquery_staging_query,
                op_kwargs={consts.QUERY: query,
                           consts.PROJECT_ID: bq_stg_project_id,
                           consts.DATASET_ID: bq_stg_dataset,
                           consts.TABLE_ID: bq_stg_table_id,
                           consts.FILE_NAME: "{{ dag_run.conf['name'] }}"
                           },
                retries=2
            )

            kafka_writer_task = TriggerDagRunOperator(
                task_id=consts.KAFKA_WRITER_TASK,
                trigger_dag_id=config[consts.DAG].get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=datetime.now(local_tz),
                conf={
                    consts.BUCKET: gcs_config.get(consts.BUCKET),
                    consts.NAME: gcs_config.get(consts.BUCKET),
                    consts.FOLDER_NAME: gcs_config.get(consts.FOLDER_NAME),
                    consts.FILE_NAME: gcs_config.get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: config[consts.DAG].get(consts.KAFKA_CLUSTER_NAME)
                },
                wait_for_completion=True
            )

            bq_record_count_check = BranchPythonOperator(
                task_id='bq_record_count_check',
                python_callable=self.validate_record_count,
                retries=2
            )

            dag_id_task_list = [
                {
                    "dag_id": item["dag_id"],
                    "task_id": item["task"]
                }
                for item in config[consts.DAG].get(consts.EXTERNAL_SENSOR_DAG_ID)
            ]
            monitor_source_data_dag = ExternalTaskPokeSensor(
                task_id='monitor_sensor_dag_task',
                external_tasks=dag_id_task_list,
                start_date_delta=timedelta(minutes=-10),
                end_date_delta=timedelta(minutes=180),
                mode='poke',
                poke_interval=60 * config[consts.DAG].get(consts.POKE_INTERVAL_MIN),
                timeout=60 * config[consts.DAG].get(consts.POKE_TIMEOUT_MIN),
                dag=dag
            )
            start_point >> monitor_source_data_dag >> bigquery_stg_query_task >> bq_record_count_check >> kafka_writer_task >> end_point
            bq_record_count_check >> end_point
        return dag


globals().update(DAGFactory().create_dynamic_dags(LoginFraudDataKafkaProcessorDagBuilder,
                                                  config_filename=LOGIN_FRAUD_CAPTURE_CONFIG_FILE,
                                                  config_dir=f'{settings.DAGS_FOLDER}/fraud_risk_truthdata_processing/login_fraud'))
