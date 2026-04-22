import logging
from datetime import datetime
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags
from util.bq_utils import run_bq_query
from util.miscutils import (read_variable_or_file, read_yamlfile_env, read_file_env)

logger = logging.getLogger(__name__)
DAG_ID: Final[str] = "re_idv_request_kafka_processing"
GS_TAG: Final[str] = "team-growth-and-sales"


class ReIdvRequestKafkaProcessor:
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.ENV_PLACEHOLDER = '{env}'
        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/re_idv_request_processing/config'

        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def export_data_to_parquet_from_bigquery_fn(self, **context):
        query = self.get_re_idv_customer_sql(self.job_config[consts.DAG][consts.QUERY].get('re_idv_customer'), **context)
        logger.info("Running export_data_to_parquet_from_bigquery with query:")
        run_bq_query(query).to_dataframe()

    def evaluate_customer_count(self, **context):
        query = self.get_re_idv_customer_sql(self.job_config[consts.DAG][consts.QUERY].get('re_idv_customer_count'), **context)
        result_df = run_bq_query(query).to_dataframe()
        customer_count = result_df['customer_count'].iloc[0]
        logger.info(f"Total ABB Customer Count: {customer_count}")
        if customer_count > 0:
            return 'export_data_to_parquet_from_bigquery'
        else:
            return 'no_records_task'

    def get_re_idv_customer_sql(self, sql_file_name, **context) -> str:
        file_path = f'{settings.DAGS_FOLDER}/re_idv_request_processing/sql/{sql_file_name}'
        sql = read_file_env(file_path, self.deploy_env)
        sql = self.format_reminder_query(sql, **context)
        return sql

    def format_reminder_query(self, query, **context) -> str:
        min_hours = context['dag_run'].conf.get('min_hours_between_reminders')
        if not min_hours or min_hours is None or str(min_hours).strip().lower() == 'none':
            min_hours = self.job_config[consts.DAG]['reminder_config']['min_hours_between_reminders']
        max_reminder_count = self.job_config[consts.DAG]['reminder_config']['max_reminder_count']
        query = query.replace("{max_reminder_count}", str(max_reminder_count)).replace("{min_hours_between_reminders}",
                                                                                       str(min_hours))
        logger.info(query)
        return query

    def create_dag(self, dag_id: str) -> DAG:
        dag = DAG(dag_id=dag_id,
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  schedule=None,
                  is_paused_upon_creation=True,
                  catchup=False,
                  default_args=self.job_config[consts.DEFAULT_ARGS],
                  tags=[GS_TAG])

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID, trigger_rule=TriggerRule.NONE_FAILED)

            evaluate_customer_count_task = BranchPythonOperator(
                task_id='evaluate_customer_count',
                python_callable=self.evaluate_customer_count
            )

            export_data_to_parquet_from_bigquery = PythonOperator(
                task_id='export_data_to_parquet_from_bigquery',
                python_callable=self.export_data_to_parquet_from_bigquery_fn,
            )

            no_records_task = PythonOperator(
                task_id='no_records_task',
                python_callable=lambda: logger.info("No ABB records found for email"),
            )

            re_idv_customer_kafka_writer_dag = TriggerDagRunOperator(
                task_id=self.job_config[consts.DAG][consts.TASK_ID].get(consts.KAFKA_WRITER_TASK_ID),
                trigger_dag_id=self.job_config[consts.DAG][consts.DAG_ID].get(consts.KAFKA_TRIGGER_DAG_ID),
                conf={
                    consts.BUCKET: self.job_config[consts.DAG].get(consts.BUCKET),
                    consts.FOLDER_NAME: self.job_config[consts.DAG].get(consts.FOLDER_NAME),
                    consts.FILE_NAME: self.job_config[consts.DAG].get(consts.FILE_NAME),
                },
                wait_for_completion=self.job_config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=self.job_config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.local_tz)
            )

            start_point >> evaluate_customer_count_task

            evaluate_customer_count_task >> export_data_to_parquet_from_bigquery >> re_idv_customer_kafka_writer_dag >> end_point
            evaluate_customer_count_task >> no_records_task >> end_point

            return add_tags(dag)

    def create(self) -> dict:
        dag = {DAG_ID: self.create_dag(DAG_ID)}
        return dag


globals().update(ReIdvRequestKafkaProcessor('re_idv_request_processing_config.yaml').create())
