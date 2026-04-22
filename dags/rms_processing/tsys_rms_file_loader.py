import pendulum
from typing import Final
import logging
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tsys_processing.generic_file_loader import GenericFileLoader
import util.constants as consts
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from util.miscutils import read_variable_or_file

GCP_CONFIG = read_variable_or_file('gcp_config')
logger = logging.getLogger(__name__)
local_tz = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=local_tz)
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"
CREATE_DATE_COLUMN: Final = 'create_date_column'
INITIAL_DEFAULT_ARGS = {
    "owner": "team-risk-management-eng",
    "capability": "Risk Management - RMS",
    "severity": "P3",
    "sub_capability": "RecoveryManagementSystem",
    "business_impact": 'RecoveryManagementSystem transactions will not be processed successfully',
    "customer_impact": 'No Impact',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


class TsysRmsBatchLoader(GenericFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict, **context):
        with TaskGroup(group_id='postprocessing') as postprocessing:
            get_file_create_dt_task = PythonOperator(
                task_id='get_file_create_dt_task',
                trigger_rule='none_failed',
                python_callable=self.get_file_create_dt,
                op_kwargs={'dag_id': dag_id,
                           'dag_config': dag_config}
            )
            control_table_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                           'output_dir': self.get_output_dir_path(dag_config)}
            )
            outbound_decision = self.outbound_decision(dag_id)

            get_file_create_dt_task >> outbound_decision >> control_table_task
        return postprocessing

    def get_file_create_dt(self, dag_id: str, dag_config: dict, **context):
        bigquery_client = bigquery.Client()
        bigquery_config = dag_config.get(consts.BIGQUERY)
        bq_processing_project_name = GCP_CONFIG.get(consts.PROCESSING_ZONE_PROJECT_ID)
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        create_date_column = bigquery_config.get(consts.TABLES).get('TRLR').get(CREATE_DATE_COLUMN)
        trlr_table = bigquery_config.get(consts.TABLES).get('TRLR').get(consts.TABLE_NAME)
        create_date_sql = f"""
                               SELECT MAX({create_date_column}) AS {create_date_column}
                               FROM
                               {bq_processing_project_name}.{bq_dataset_name}.{trlr_table}_{consts.EXTERNAL_TABLE_SUFFIX}{consts.COLUMN_TRANSFORMATION_SUFFIX}
                            """
        logging.info(f"extracting file_create_dt using the query {create_date_sql}")
        create_dt_result = bigquery_client.query(create_date_sql).result().to_dataframe()
        file_create_dt = create_dt_result[create_date_column].values[0]
        if file_create_dt:
            logging.info(f"file create date for this execution is {file_create_dt}")
            context['ti'].xcom_push(key='file_create_dt', value=file_create_dt.strftime('%Y-%m-%d'))
        else:
            raise AirflowException(f"file_create_dt is not found for {dag_id}")

    def outbound_decision(self, dag_id: str):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        outbound_trigger = var_dict.get('outbound_trigger', True)
        if outbound_trigger:
            return TriggerDagRunOperator(
                task_id=f"trigger_{self.job_config.get(dag_id).get('trigger_dag_id')}",
                trigger_dag_id=self.job_config.get(dag_id).get('trigger_dag_id'),
                conf={'inbound_dag_run_id': '{{ dag_run.run_id }}',
                      'file_create_dt': "{{ ti.xcom_pull(task_ids='postprocessing.get_file_create_dt_task', key='file_create_dt') }}"},
                wait_for_completion=True,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                retries=0,
                poke_interval=60,
            )
        else:
            return DummyOperator(task_id="skipped_outbound_file_gen")

    def create_dags(self) -> dict:
        if self.job_config:
            dags = {}

            for job_id, config in self.job_config.items():
                self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
                dags[job_id] = self.create_dag(job_id, config)
                dags[job_id].tags = config.get(consts.TAGS)

            return dags
        else:
            return {}
