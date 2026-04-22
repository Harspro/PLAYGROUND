from dag_factory.terminus_dag_factory import add_tags
from abc import ABC, abstractmethod
import logging
import pendulum
from datetime import timedelta, date, datetime
from copy import deepcopy
from typing import Final
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG, settings
from airflow.utils.task_group import TaskGroup
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from airflow.utils.trigger_rule import TriggerRule
from util.constants import (
    DEFAULT_ARGS, READ_PAUSE_DEPLOY_CONFIG, STAGING_BUCKET,
    STAGING_FOLDER, BIGQUERY, SCHEDULE_INTERVAL, DAG as DAG_STR
)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix,
)
from table_comparator.utils import execute_bigquery_job
import util.constants as consts
INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Data Movement',
    'business_impact': 'N/A',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}
logger = logging.getLogger(__name__)


class TableComparatorBase(ABC):
    def __init__(self, config_filename: str):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.storage_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]

        config_dir = f'{settings.DAGS_FOLDER}/table_comparator'
        self.job_config = read_yamlfile_env_suffix(
            f'{config_dir}/{config_filename}',
            self.deploy_env, self.storage_suffix)
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')

    def get_schedule(self, dag_config: dict):
        return dag_config.get(SCHEDULE_INTERVAL, None)

    def generate_dags(self) -> dict:
        dags = {}
        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule(dag_config),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            render_template_as_native_obj=True,
            tags=dag_config.get(consts.TAGS, []),
            doc_md="""
                ### BigQuery Table Comparison DAG
                Compares two BigQuery tables record by record based on provided configuration.
                If `comparison_columns` is null or empty, all common non-PK columns will be compared.
                If `mismatch_report_table_name` is null or empty, no detailed report table will be created (counts will be direct).
                If `mismatch_report_view_name` is provided, a view will be created on top of the mismatch report table.
                If `mismatch_report_view_name` is not provided but `mismatch_report_table_name` exists, a default view will be created in pcb-{env}-curated project with same dataset and table name.
                Optional fields: `gcp_project_id`, `record_load_date_column`, `specific_load_dates`, `comparison_columns`, `mismatch_report_table_name`, `mismatch_report_view_name`."""
        )
        with dag:
            if dag_config.get(READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(
                    dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            pipeline_start = EmptyOperator(task_id="start")
            generate_queries_task = self.generate_queries(dag_config)

            # Task group for mismatch report table operations
            with TaskGroup(group_id="mismatch_report_table_operations") as mismatch_report_group:
                create_mismatch_report_table_schema_task = self.create_mismatch_report_table_schema(
                    dag_config)
                create_mismatch_report_table_task = self.create_mismatch_report_table(
                    dag_config)
                create_mismatch_report_view_task = self.create_mismatch_report_view(
                    dag_config)

                # Set dependencies within the task group
                create_mismatch_report_table_schema_task >> create_mismatch_report_table_task >> create_mismatch_report_view_task

            get_schema_mismatches_task = self.get_schema_mismatches(dag_config)
            get_mismatched_count_task = self.get_mismatched_count(dag_config)
            get_missing_in_source_count_task = self.get_missing_in_source_count(
                dag_config)
            get_missing_in_target_count_task = self.get_missing_in_target_count(
                dag_config)
            report_summary_task = self.report_summary(dag_config)
            pipeline_end = EmptyOperator(task_id='end')

            pipeline_start >> generate_queries_task >> mismatch_report_group >> [
                get_schema_mismatches_task,
                get_mismatched_count_task,
                get_missing_in_source_count_task,
                get_missing_in_target_count_task
            ] >> report_summary_task >> pipeline_end

        return add_tags(dag)

    def generate_comparison_queries(self, **kwargs):
        pass

    def report_results(self, **kwargs):
        pass

    def generate_queries(self, dag_config: dict):
        return PythonOperator(
            task_id='generate_comparison_queries_task',
            python_callable=self.generate_comparison_queries,
            op_kwargs=dag_config
        )

    def create_mismatch_report_table_schema(self, dag_config: dict):
        return PythonOperator(
            task_id='create_mismatch_report_table_schema_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='create_table_schema_query') }}",
                'fetch_results': False,
            })

    def create_mismatch_report_table(self, dag_config: dict):
        return PythonOperator(
            task_id='create_mismatch_report_table_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='mismatch_query') }}",
                'fetch_results': False,
            })

    def create_mismatch_report_view(self, dag_config: dict):
        return PythonOperator(
            task_id='create_mismatch_report_view_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='create_view_query') }}",
                'fetch_results': False,
            })

    def get_schema_mismatches(self, dag_config: dict):
        return PythonOperator(
            task_id='get_schema_mismatches_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='schema_mismatch_query') }}",
                'fetch_results': True,
            })

    def get_mismatched_count(self, dag_config: dict):
        return PythonOperator(
            task_id='get_mismatched_count_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='count_mismatched_query') }}",
                'fetch_results': True,
            })

    def get_missing_in_source_count(self, dag_config: dict):
        return PythonOperator(
            task_id='get_missing_in_source_count_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='count_missing_in_source_query') }}",
                'fetch_results': True,
            })

    def get_missing_in_target_count(self, dag_config: dict):
        return PythonOperator(
            task_id='get_missing_in_target_count_task',
            python_callable=execute_bigquery_job,
            op_kwargs={
                'query_string': "{{ ti.xcom_pull(task_ids='generate_comparison_queries_task', key='count_missing_in_target_query') }}",
                'fetch_results': True,
            })

    def report_summary(self, dag_config: dict):
        return PythonOperator(
            task_id='report_summary_task',
            python_callable=self.report_results,
            op_kwargs=dag_config,
            trigger_rule=TriggerRule.ALL_DONE
            # Ensure report runs even if some checks fail
        )
