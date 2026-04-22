import csv
import logging
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Final

import pendulum
from airflow import settings, DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import storage

import util.constants as consts
from util.bq_utils import run_bq_query
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag

from util.miscutils import read_variable_or_file, read_yamlfile_env, read_file_env
from dag_factory.terminus_dag_factory import add_tags

RECON_CONFIG_FILE: Final = 'brccc_doc_generation_recon_config.yaml'
MAIL_PRINT_STATUS_INSERT_FILE: Final = 'mail_print_status_insert_query.sql'
MAIL_PRINT_STATUS_PARQUET_FILE: Final = 'mail_print_status_parquet_query.sql'
MAIL_PRINT_STATUS_COUNT_SQL_FILE: Final = 'mail_print_status_count_query.sql'

RECON_CONFIG_PATH = f'{settings.DAGS_FOLDER}/broadridge_processing/{RECON_CONFIG_FILE}'
MAIL_PRINT_STATUS_INSERT_PATH = f'{settings.DAGS_FOLDER}/broadridge_processing/sql/{MAIL_PRINT_STATUS_INSERT_FILE}'
MAIL_PRINT_STATUS_PARQUET_PATH = f'{settings.DAGS_FOLDER}/broadridge_processing/sql/{MAIL_PRINT_STATUS_PARQUET_FILE}'
MAIL_PRINT_STATUS_COUNT_SQL_PATH = f'{settings.DAGS_FOLDER}/broadridge_processing/sql/{MAIL_PRINT_STATUS_COUNT_SQL_FILE}'

INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False
}


class BroadridgeDocGenerationReconDagBuilder:
    """
        This class processes inbound file from Broadridge that contains ...
    """
    def __init__(self, config_filename: str):
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config = read_yamlfile_env(config_filename, self.deploy_env)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def _extract_value_from_csv(self, bucket: str, folder_name: str, file_name: str, column_index: int, **kwargs):
        client = storage.Client()
        gcs_bucket = client.bucket(bucket)
        blob = gcs_bucket.blob(f'{folder_name}/{file_name}')

        with blob.open('r') as f:
            first_line = f.readline()

        reader = csv.reader([first_line])
        row = next(reader)

        if column_index >= len(row):
            raise IndexError(f"Column index {column_index} is out of range for the CSV with {len(row)} columns.")
        value = row[column_index]

        # Push to XCom for downstream task
        kwargs['ti'].xcom_push(key='broadridge_outbound_filename', value=value)

    def _run_sql_to_insert(self, dag_id: str, **kwargs):
        dag_run_id = kwargs['dag_run'].run_id
        outbound_file_name = kwargs['ti'].xcom_pull(
            key="broadridge_outbound_filename",
            task_ids="task_extract_outbound_filename")

        sql = read_file_env(MAIL_PRINT_STATUS_INSERT_PATH, self.deploy_env)
        sql = (sql
               .replace("<<OUTBOUND_FILE_NAME>>", outbound_file_name)
               .replace("<<DAG_ID>>", dag_id)
               .replace("<<DAG_RUN_ID>>", dag_run_id))

        logging.info(f"Inserting 'MAILED' status into MAIL_PRINT_STATUS for records "
                     f"with status 'AFP_GENERATED' and outboundFileName = '{outbound_file_name}'.")
        run_bq_query(sql)
        logging.info("Insert completed.")

    def _run_sql_to_create_parquet(self, outbound_file_name: str, bucket: str, folder_name: str, file_name: str, dag_id: str, **kwargs):
        dag_run_id = kwargs['dag_run'].run_id

        count_sql = read_file_env(MAIL_PRINT_STATUS_COUNT_SQL_PATH, self.deploy_env).format(
            dag_id=dag_id,
            dag_run_id=dag_run_id
        )

        logging.info("Checking if there are records to export to parquet...")
        count_job = run_bq_query(count_sql)
        record_count = 0
        for row in count_job.result():
            record_count = row.record_count

        if record_count == 0:
            logging.info("No records found to create parquet file. Skipping parquet creation and downstream tasks.")
            raise AirflowSkipException("No records found to create parquet file")

        sql = read_file_env(MAIL_PRINT_STATUS_PARQUET_PATH, self.deploy_env).format(
            outbound_file_name=outbound_file_name,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            parquet_file_path=f"{bucket}/{folder_name}/{file_name}"
        )

        logging.info(f"{sql}")

        logging.info("Running Sql to create parquet file for records marked as mailed.")
        run_bq_query(sql)
        logging.info("Parquet file(s) created.")

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
            Defines structure for the dynamically created DAGs, made of 5 tasks:
            1. Empty Operator start point.
            2. Python Operator: read broadridge recon file and extract outbound AFP file name sent to them for processing.
            3. Python Operator: insert 'MAILED' status for all records send in the outbound file.
            4. Trigger Dag Run Operator: send newly added status as Kafka events to DP.
            5. Empty Operator end point.

            Args:
                dag_id (str): unique identifier to be used.
                config (dict): job configuration provided.
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        with DAG(
                dag_id=dag_id,
                default_args=self.default_args,
                description=(
                    'This DAG processes reconciliation files from Broadridge mail provider, '
                    'updating the status of records associated with outbound customer paper mail to "MAILED" '
                    'in BigQuery and DP for downstream tracking.'
                ),
                render_template_as_native_obj=True,
                schedule=None,
                start_date=datetime(2025, 4, 1, tzinfo=self.local_tz),
                max_active_runs=1,
                catchup=False,
                dagrun_timeout=timedelta(hours=24),
                tags=config[consts.TAGS],
                is_paused_upon_creation=True
        ) as dag:

            dag_config = config[consts.DAG]
            trigger_config = dag_config[consts.TRIGGER_CONFIG]

            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)

            extract_outbound_filename = PythonOperator(
                task_id='task_extract_outbound_filename',
                python_callable=self._extract_value_from_csv,
                op_kwargs={
                    consts.BUCKET: trigger_config.get(consts.BUCKET),
                    consts.FOLDER_NAME: trigger_config.get(consts.FOLDER_NAME),
                    consts.FILE_NAME: trigger_config.get(consts.FILE_NAME),
                    "column_index": dag_config.get("file_extract_column_index")
                }
            )

            run_sql_to_insert_mailed_status = PythonOperator(
                task_id="task_run_sql_to_insert_mailed_status",
                python_callable=self._run_sql_to_insert,
                op_kwargs={
                    consts.DAG_ID: dag_id
                }
            )

            bigquery_config = dag_config.get(consts.BIGQUERY)
            current_time = datetime.now(tz=self.local_tz)
            sub_folder = current_time.strftime('%Y%m%d')
            xcom_outbound_file_name = "{{ task_instance.xcom_pull(task_ids='task_extract_outbound_filename', key='broadridge_outbound_filename') }}"
            run_sql_to_create_parquet = PythonOperator(
                task_id="task_run_sql_to_create_parquet",
                python_callable=self._run_sql_to_create_parquet,
                op_kwargs={
                    'outbound_file_name': xcom_outbound_file_name,
                    consts.BUCKET: bigquery_config.get(consts.BUCKET),
                    consts.FOLDER_NAME: f'{bigquery_config.get(consts.FOLDER_PREFIX)}/{sub_folder}/{xcom_outbound_file_name}',
                    consts.FILE_NAME: bigquery_config.get(consts.FILE_NAME),
                    consts.DAG_ID: dag_id
                }
            )

            kafka_config = dag_config.get('kafka_config')
            kafka_writer_task = TriggerDagRunOperator(
                task_id=kafka_config.get(consts.KAFKA_WRITER_TASK_ID),
                trigger_dag_id=kafka_config.get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=current_time,
                conf={
                    consts.BUCKET: bigquery_config.get(consts.BUCKET),
                    consts.FOLDER_NAME: f'{bigquery_config.get(consts.FOLDER_PREFIX)}/{sub_folder}/{xcom_outbound_file_name}',
                    consts.FILE_NAME: bigquery_config.get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: kafka_config.get(consts.KAFKA_CLUSTER_NAME)
                },
                wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
                poke_interval=dag_config.get(consts.POKE_INTERVAL)
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> extract_outbound_filename >> \
                run_sql_to_insert_mailed_status >> run_sql_to_create_parquet >> \
                kafka_writer_task >> end

            return add_tags(dag)

    def create_all_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.dag_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)

        return dags


brccc_doc_generation_recon_dags_builder = BroadridgeDocGenerationReconDagBuilder(RECON_CONFIG_PATH)
globals().update(brccc_doc_generation_recon_dags_builder.create_all_dags())
