import logging
from datetime import timedelta, datetime
from typing import Final, Optional

import pandas as pd
import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob
from pandas import DataFrame
from airflow.models.dagrun import DagRun

import util.constants as consts
from util.bq_utils import run_bq_query
from util.constants import TORONTO_TIMEZONE_ID
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.gcs_utils import delete_folder
from util.miscutils import read_variable_or_file, read_yamlfile_env, read_file_env
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

INITIAL_DEFAULT_ARGS: Final = {
    "owner": "gremlins",
    'capability': 'account-management',
    'severity': 'P2',
    'sub_capability': 'card-management',
    'business_impact': 'This will impact instant card issuance for SCMS',
    'customer_impact': 'Customer card instant issuance',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}


class InstantIssuanceReconciliation:

    def __init__(self, config_filename: str, config_dir: str = None):
        self.local_tz = pendulum.timezone(TORONTO_TIMEZONE_ID)
        self.timestamp_format = '%Y-%m-%d %H:%M:%S'
        self.current_timestamp = datetime.now(tz=self.local_tz).astimezone(pendulum.timezone("UTC")).replace(
            tzinfo=None)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/scms_instant_issuance_reconciliation/config'
        self.config_dir = config_dir
        self.sql_dir = f'{settings.DAGS_FOLDER}/scms_instant_issuance_reconciliation/sql'

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    @staticmethod
    def create_table(config: dict, table_id: str, table_schema: str) -> None:
        big_query_config = config["big_query"]
        table = big_query_config["mapping"][table_id]

        logger.info(f"Creating {table_id} table if it doesn't exist")
        schema = [bigquery.SchemaField.from_api_repr(s) for s in big_query_config[table_schema]]
        client = bigquery.Client()
        client.create_table(bigquery.Table(table, schema=schema), exists_ok=True)

    @staticmethod
    def replace_tokens_from_config(query: str, config: dict) -> str:
        mapping = config["big_query"]["mapping"]
        env_query = read_file_env(query)
        for key, value in mapping.items():
            env_query = env_query.replace(f"{{{key}}}", value)
        return env_query

    @staticmethod
    def get_first_value_if_exists(dataframe: DataFrame, column_name: str) -> str:
        return str(dataframe[column_name].values[0]) if column_name in dataframe.columns else ""

    @staticmethod
    def process_schedule_interval(schedule_interval: str) -> Optional[str]:
        return None if schedule_interval == 'None' else schedule_interval

    def get_window_timestamp(self, window: int) -> str:
        window_timestamp = self.current_timestamp - timedelta(minutes=window)
        return window_timestamp.strftime(self.timestamp_format)

    def setup_run(self, config: dict) -> None:
        kafka_config = config["kafka_writer"]
        self.create_table(config, "audit_table_id", "audit_table_schema")
        self.create_table(config, "control_table_id", "control_table_schema")
        delete_folder(kafka_config["gcs"]["bucket"], kafka_config["gcs"]["folder_name"])

    def validate_dag_run(self, dag_id: str, config: dict, **context) -> bool:
        self.setup_run(config)
        automatic_run = context['params'].get('allow_manual_run')
        lookback_window = context['params'].get('lookback_window')
        validate_windows = lookback_window == 0 or context['params'].get('tolerance_window') < lookback_window

        last_dag_run_job = self.query_last_processed_run(dag_id, config).result()
        last_dag_run_total = last_dag_run_job.total_rows
        context['ti'].xcom_push(key='last_dag_run_total', value=last_dag_run_total)
        if last_dag_run_total != 0:
            new_start_time = (pd.to_datetime(
                self.get_first_value_if_exists(last_dag_run_job.to_dataframe(), 'windowEndTime'))).strftime(
                self.timestamp_format)
            context['ti'].xcom_push(key='last_dag_run_end_time',
                                    value=new_start_time)

        logger.info(f"Automatic run is {automatic_run}")
        if automatic_run:
            if config['source_type'] == 'rt11' and lookback_window == 0 and last_dag_run_total == 0:
                logger.info("Skipping run... must pass lookback and tolerance window...")
                return False

            logger.info(f"Lookback window is valid: {validate_windows}")
            if not validate_windows:
                logger.warning("Skipping run ... lookback window is smaller than tolerance window")
            return validate_windows
        else:
            if lookback_window == 0:
                logger.info("No lookback window given... checking control table for start time...")
                logger.info(f"Found {last_dag_run_total} entries in the control table")
                no_entries = last_dag_run_total != 0
                if not no_entries:
                    logger.warning("Control table has no first entry... skipping run...")
                return no_entries
            else:
                logger.warning(
                    f"Lookback window {lookback_window} was given but automatic run is disabled... skipping run... ")
                return False

    def validate_query(self, config: dict, context: dict) -> str:
        total_missing_card_query = self.replace_tokens_from_config(f'{self.sql_dir}/total_entries_count.sql', config)
        missing_cards_total = run_bq_query(total_missing_card_query).to_dataframe()['total'].values[0]
        context['ti'].xcom_push(key='missing_cards_total', value=missing_cards_total)
        if missing_cards_total == 0:
            logger.info("No missing card entries found, skipping sending to topic...")
            return "done"
        else:
            threshold = context['params'].get('missing_record_threshold')
            if threshold < missing_cards_total:
                raise ValueError(
                    f"Number of card entries {missing_cards_total} is greater than supplied threshold {threshold}... failing dag run for manual input instead...")

            find_null_card_numbers_query = self.replace_tokens_from_config(f'{self.sql_dir}/find_null_card_numbers.sql',
                                                                           config)
            null_card_numbers_total = run_bq_query(find_null_card_numbers_query).to_dataframe()['total'].values[0]
            if null_card_numbers_total != 0:
                logger.warning(
                    f'There are {null_card_numbers_total} null card numbers, excluding these entries from being sent')

            export_missing_cards_query = self.replace_tokens_from_config(f'{self.sql_dir}/export_missing_cards.sql',
                                                                         config)
            run_bq_query(export_missing_cards_query)
            return "send_to_kafka"

    def query_missing_cards_from_source_type_table(self, config: dict, **context) -> str:
        source_type = config["source_type"]
        lookback_window = context['params'].get('lookback_window')
        tolerance_window = context['params'].get('tolerance_window')
        start_time = "" if lookback_window == 0 else self.get_window_timestamp(lookback_window)
        context['ti'].xcom_push(key='start_time', value=start_time)
        end_time = self.get_window_timestamp(tolerance_window)
        context['ti'].xcom_push(key='end_time', value=end_time)
        last_dag_run_total = context['ti'].xcom_pull(task_ids='validate_dag_run', key='last_dag_run_total')

        if last_dag_run_total == 0:
            logger.info(
                f"Control table has {last_dag_run_total} entries for source table {source_type}")
            if source_type == 'orchestration':
                logger.info(
                    f"Initial load will get all entries from orchestration table, ignoring start time {start_time} with end time as {end_time}")
            else:
                logger.info(f"Fetching all missing cards with start time as {start_time} and end time as {end_time}")
            missing_cards_query = (
                self.replace_tokens_from_config(f'{self.sql_dir}/{source_type}/find_missing_cards.sql', config)
                .replace(f"{{{'start_time'}}}", start_time)
                .replace(f"{{{'end_time'}}}", end_time))
        else:
            if start_time == "":
                start_time = context['ti'].xcom_pull(task_ids='validate_dag_run', key='last_dag_run_end_time')
            logger.info(
                f"Control table has entries, finding missing cards with start time after {start_time} "
                f"and end time as {end_time}")
            missing_cards_query = (
                self.replace_tokens_from_config(f'{self.sql_dir}/{source_type}/find_missing_cards_with_window.sql',
                                                config)
                .replace(f"{{{'start_time'}}}", start_time)
                .replace(f"{{{'end_time'}}}", end_time))
        run_bq_query(missing_cards_query)
        logger.info(f"find missing cards query: {missing_cards_query}")

        return self.validate_query(config, context)

    def log_replacement_cards_to_audit_table(self, config: dict) -> None:
        source_type = config["source_type"]

        logger.info("Logging replacement cards to audit table")
        log_replacement_card_query = self.replace_tokens_from_config(
            f'{self.sql_dir}/{source_type}/log_cards_to_audit.sql', config)
        run_bq_query(log_replacement_card_query).result()

    def save_to_control_table(self, dag_id: str, config: dict, context: dict) -> None:
        logger.info("Logging run to control table")
        dr: DagRun = context["dag_run"]
        dag_execution_time_str = dr.start_date.strftime(self.timestamp_format)
        parent_dag_id = (context['dag_run'].conf or {}).get('parent_dag_id')
        parent_trigger_type = (context['dag_run'].conf or {}).get('run_type')

        if parent_dag_id:
            trigger_type = parent_trigger_type
        else:
            trigger_type = dr.run_type
        start_time = context['ti'].xcom_pull(task_ids='get_and_save_missing_cards', key='start_time')
        auto_start_time = context['ti'].xcom_pull(task_ids='validate_dag_run', key='last_dag_run_end_time')
        end_time = context['ti'].xcom_pull(task_ids='get_and_save_missing_cards', key='end_time')
        missing_cards_total = context['ti'].xcom_pull(task_ids='get_and_save_missing_cards', key='missing_cards_total')

        logger.info("parent_dag_id: %s \ntrigger_type: %s\nstart_time: %s\nend_time: %s\ndag_execution_time_str: %s\nmissing_cards_total: %s", parent_dag_id, trigger_type, start_time, end_time, dag_execution_time_str, missing_cards_total)

        log_run_to_control_query = (
            self.replace_tokens_from_config(f'{self.sql_dir}/log_run_to_control.sql', config)
            .replace(f"{{{'dag_id'}}}", dag_id)
            .replace(f"{{{'start_time'}}}", auto_start_time if start_time == "" else start_time)
            .replace(f"{{{'end_time'}}}", end_time)
            .replace(f"{{{'dag_execution_time'}}}", dag_execution_time_str)
            .replace(f"{{{'trigger_type'}}}", trigger_type)
            .replace(f"{{{'replacement_count'}}}", str(missing_cards_total))
        )

        run_bq_query(log_run_to_control_query).result()

    def query_last_processed_run(self, dag_id: str, config: dict) -> QueryJob:
        source_type = config["source_type"]

        logger.info(f'Finding last processed run for {source_type} dag in the control table')
        last_processed_run_query = (
            self.replace_tokens_from_config(f'{self.sql_dir}/find_last_processed_run.sql', config)
            .replace(f"{{{'dag_id'}}}", dag_id))
        return run_bq_query(last_processed_run_query)

    def validate_automatic_run_task(self, dag_id: str, config: dict) -> ShortCircuitOperator:
        return ShortCircuitOperator(
            task_id="validate_dag_run",
            python_callable=self.validate_dag_run,
            op_kwargs={consts.DAG_ID: dag_id, consts.CONFIG: config}
        )

    def process_missing_cards_task(self, config: dict) -> BranchPythonOperator:
        return BranchPythonOperator(
            task_id="get_and_save_missing_cards",
            python_callable=self.query_missing_cards_from_source_type_table,
            op_kwargs={consts.CONFIG: config}
        )

    def log_replacement_cards_task(self, config: dict) -> PythonOperator:
        return PythonOperator(
            task_id="log_replacement_cards",
            python_callable=self.log_replacement_cards_to_audit_table,
            op_kwargs={consts.CONFIG: config}
        )

    def send_to_kafka_connector_task(self, config: dict) -> TriggerDagRunOperator:
        kafka_writer_config = config["kafka_writer"]

        return TriggerDagRunOperator(
            task_id="send_to_kafka",
            trigger_dag_id=kafka_writer_config["dag_id"],
            trigger_rule="none_failed",
            logical_date=datetime.now(self.local_tz),
            conf={
                consts.BUCKET: kafka_writer_config["gcs"]["bucket"],
                consts.CLUSTER_NAME: kafka_writer_config["dataproc_cluster_name"],
                consts.FOLDER_NAME: kafka_writer_config["gcs"]["folder_name"],
                consts.FILE_NAME: kafka_writer_config["gcs"]["file_name"]
            },
            do_xcom_push=True,
            wait_for_completion=True,
            poke_interval=30
        )

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            description=config["description"],
            tags=config["tags"],
            schedule=self.process_schedule_interval(config["schedule_interval"]),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            params={
                "tolerance_window": config["tolerance_window"],
                "lookback_window": config["lookback_window"],
                "missing_record_threshold": config["missing_record_threshold"],
                "allow_manual_run": config["allow_manual_run"],
                "Running this dag manually may result in missing records": ""},
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            default_args=INITIAL_DEFAULT_ARGS)
        with dag:
            if config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id="start", trigger_rule='none_failed')
            done = EmptyOperator(task_id="done", trigger_rule='none_failed',
                                 on_success_callback=lambda context: self.save_to_control_table(dag_id, config,
                                                                                                context))

            validate_dag_run = self.validate_automatic_run_task(dag_id, config)
            process_missing_cards = self.process_missing_cards_task(config)
            send_to_kafka = self.send_to_kafka_connector_task(config)
            log_replacement_cards = self.log_replacement_cards_task(config)

            start >> validate_dag_run >> process_missing_cards >> send_to_kafka >> log_replacement_cards >> done

        return add_tags(dag)

    def generate_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.job_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)
        return dags
