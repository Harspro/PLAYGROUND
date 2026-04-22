import json
import logging

import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

import util.constants as consts
from util.bq_utils import run_bq_query
from util.miscutils import read_variable_or_file, read_file_env, read_yamlfile_env, save_job_to_control_table
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BigQueryPatchDAG:
    def __init__(self, config_dir: str = None, config_filename: str = None):
        self.default_args = {
            'owner': "team-growth-and-sales-alerts",
            'capability': "Data Patch on Big Query",
            'severity': "P3",
            'sub_capability': "Big Query",
            'business_impact': "NA",
            'depends_on_past': False,
            'start_date': pendulum.today('America/Toronto').add(days=-1),
            'retries': 0
        }

        self.sql_dir = f'{settings.DAGS_FOLDER}/bq_data_patch/sql/'
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/bq_data_patch/config'

        if config_filename is None:
            config_filename = 'bq_data_patch_config.yaml'

        self.data_patch_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    def read_sql_file(self, sql_file):
        file_path = self.sql_dir + sql_file
        sql = read_file_env(file_path, self.deploy_env)
        return sql

    def execute_bigquery_patch(self, sql_file, parameters=None):
        query = self.read_sql_file(sql_file)

        # Apply parameter replacements if they exist
        if parameters:
            for param_key, param_value in parameters.items():
                placeholder = f"{{{param_key}}}"
                query = query.replace(placeholder, str(param_value))
                logger.info(f"Replaced {placeholder} with {param_value}")

        logger.info(f"Executing BigQuery job for SQL file: {sql_file}")
        job_name = run_bq_query(query)
        logger.info(f"BigQuery job completed for {sql_file}, job_name: {job_name}")

    def build_control_record_saving_job(self, sql_file: str, dag_owner=str, **context):
        job_params_str = json.dumps({
            'sql_file': sql_file,
            'dag_owner': dag_owner
        })
        save_job_to_control_table(job_params_str, **context)

    def create_dag(self, dag_id: str, sql_file: str, dag_owner=None, dag_schedule=None, tags=None, parameters=None):

        default_args = self.default_args.copy()
        if dag_owner:
            default_args['owner'] = dag_owner

        if sql_file is None:
            logger.warning(f"Skipping task creation for DAG '{dag_id}' because 'sql_file' is missing.")
            return None

        if tags is None:
            tags = []

        with DAG(
                dag_id=dag_id,
                default_args=default_args,
                schedule=dag_schedule,
                catchup=False,
                is_paused_upon_creation=True,
                dagrun_timeout=timedelta(hours=5),
                tags=tags
        ) as dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            execute_bigquery_patch = PythonOperator(
                task_id=f'execute_{sql_file}_patch',
                python_callable=self.execute_bigquery_patch,
                op_args=[sql_file],
                op_kwargs={'parameters': parameters}
            )

            control_table_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'sql_file': sql_file, 'dag_owner': dag_owner}
            )

            start_point >> execute_bigquery_patch >> control_table_task >> end_point

        return add_tags(dag)

    def generate_dags(self):
        dags = {}
        if not self.data_patch_config:
            logger.info('data patch config is empty.')
            return dags

        for dag_id, config in self.data_patch_config.items():
            dag_owner = config.get("dag_owner")
            sql_filename = config.get("sql_file")
            schedule = config.get("dag_schedule")
            tags = config.get("tags", [])
            parameters = config.get("parameters")
            if not sql_filename:
                logger.warning(f"Skipping DAG {dag_id} due to missing 'sql_file' in config.")
                continue
            logger.info(f"Creating DAG for {dag_id} with SQL file: {sql_filename} and owner: {dag_owner}")
            dags[dag_id] = self.create_dag(dag_id, sql_filename, dag_owner, schedule, tags, parameters)

        return dags


dags = BigQueryPatchDAG().generate_dags()
globals().update(dags)
