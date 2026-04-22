import logging
import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from google.cloud import bigquery
from typing import Final
from util.miscutils import (read_variable_or_file, read_yamlfile_env, read_file_env)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
DAG_ID: Final[str] = "edsa-reminder-later-email-processing"
GS_TAG: Final[str] = "team-growth-and-sales"
LOOKBACK_START_HOURS: Final[str] = "72"
LOOKBACK_END_HOURS: Final[str] = "48"


class EdsaReminderLaterEmailProcessor:
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.ENV_PLACEHOLDER = '{env}'
        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/edsa_reminder_later_email_processing/'
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def generate_sql_with_time_window(self, sql_file_name, lookback_start_hours, lookback_end_hours) -> str:
        """
        :param sql_file_name: sql file name, which need to have parameterized loock back hours
        :param lookback_start_hours: Default is 72 hours - looks back update_dt < current_time -  lookback_start_hours
        :param lookback_end_hours: Default is 48 hours - looks back update_dt > current_time -  lookback_end_hours
        :return: sql query from the provided file
        """
        if lookback_start_hours is None:
            lookback_start_hours = LOOKBACK_START_HOURS
        if lookback_end_hours is None:
            lookback_end_hours = LOOKBACK_END_HOURS

        file_path = f'{settings.DAGS_FOLDER}/edsa_reminder_later_email_processing/sql/{sql_file_name}'
        sql = read_file_env(file_path, self.deploy_env)
        sql = sql.replace("{lookback_start_hours}", str(lookback_start_hours)).replace("{lookback_end_hours}", str(lookback_end_hours))
        logger.info(sql)
        return sql

    def evaluate_account_count(self, **context):
        conf = context['dag_run'].conf
        client = bigquery.Client()
        query = self.generate_sql_with_time_window(
            self.job_config[consts.DAG][consts.QUERY].get('email_recipient_account_count'),
            conf.get('lookback_start_hours'), conf.get('lookback_end_hours'))
        result = client.query(query).result()
        account_count = [row.account_count for row in result][0]
        logger.info(f"Total Email Recipient Account Count: {account_count}")
        if account_count > 0:
            return 'export_data_to_parquet_from_bigquery'
        else:
            return 'no_records_task'

    def create_dag(self, dag_id: str) -> DAG:

        dag = DAG(dag_id=dag_id,
                  schedule="0 9,15 * * *",
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  is_paused_upon_creation=True,
                  catchup=False,
                  default_args=self.job_config[consts.DEFAULT_ARGS],
                  tags=[GS_TAG])

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID, trigger_rule=TriggerRule.NONE_FAILED)

            evaluate_account_count_task = BranchPythonOperator(
                task_id='evaluate_account_count',
                python_callable=self.evaluate_account_count
            )

            export_data_to_parquet_from_bigquery = BigQueryInsertJobOperator(
                task_id='export_data_to_parquet_from_bigquery',
                configuration={
                    "query": {
                        "query": self.generate_sql_with_time_window(
                            self.job_config[consts.DAG][consts.QUERY].get('email_recipient'),
                            f"{{{{ dag_run.conf.get('lookback_start_hours', {LOOKBACK_START_HOURS}) }}}}",
                            f"{{{{ dag_run.conf.get('lookback_end_hours', {LOOKBACK_END_HOURS}) }}}}"
                        ),
                        "useLegacySql": False
                    }
                },
                location=self.gcp_config.get(consts.BQ_QUERY_LOCATION)
            )

            edsa_reminder_later_email_recipients_kafka_writer_dag = TriggerDagRunOperator(
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

            no_records_task = PythonOperator(
                task_id='no_records_task',
                python_callable=lambda: logger.info("No edsa records found for reminder email"),
            )

            start_point >> evaluate_account_count_task
            # Branch condition: If email count > 0
            evaluate_account_count_task >> export_data_to_parquet_from_bigquery >> edsa_reminder_later_email_recipients_kafka_writer_dag >> end_point
            # Branch condition: If email count is 0
            evaluate_account_count_task >> no_records_task >> end_point

            return add_tags(dag)

    def create(self) -> dict:
        dags = {}
        dags[DAG_ID] = self.create_dag(DAG_ID)
        return dags


globals().update(EdsaReminderLaterEmailProcessor('edsa_remider_later_config.yaml').create())
