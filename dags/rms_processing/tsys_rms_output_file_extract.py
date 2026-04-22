import json
import logging
from abc import ABC
import pendulum
from airflow.exceptions import AirflowFailException

import util.constants as consts
from airflow import settings
from airflow.models import Variable
from airflow.exceptions import AirflowException
from typing import Final, Union
from datetime import timedelta, datetime
from google.cloud import bigquery
from util.logging_utils import build_spark_logging_info
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    get_cluster_config_by_job_size,
    read_variable_or_file,
    read_yamlfile_env_suffix,
    read_env_filepattern,
    save_job_to_control_table, read_file_env)
from util.bq_utils import run_bq_query, table_exists
from payments_processing import payments_common as commons
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']

local_tz = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=local_tz)
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"
BATCH_NUM_TABLE: Final = 'batch_num_table'
sql_file_path = f'{settings.DAGS_FOLDER}/rms_processing/sql/'
OUTPUT_FILE_CREATION: Final = 'output_file_creation'
FETCH_SEQUENCE_NUMBER_TABLE: Final = 'fetch_sequence_number_table'
FETCH_SEQUENCE_NUMBER_FIELD: Final = 'fetch_sequence_number_field'
SEQ_NUMBER_FIELD_PLACEHOLDER: Final = '{seq_number_field}'
SEQ_NUMBER_TABLE_PLACEHOLDER: Final = '{seq_number_table}'
FILE_CREATE_DT_PLACEHOLDER: Final = '{file_create_dt}'
OUTPUT_TABLE_PLACEHOLDER: Final = '{output_table_name}'
INITIAL_SEQ_VALUE_PLACEHOLDER: Final = '{initial_seq_value}'
BATCH_NUM_PLACEHOLDER: Final = '{batch_num_val}'


def get_output_folder_path(dag_config: dict):
    return f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{dag_config.get('PROCESSING_OUTPUT_DIR')}"


def get_interim_folder_path(dag_config: dict):
    return f"gs://{dag_config[consts.PROCESSING_BUCKET]}/{dag_config.get('PROCESSING_INTERIM_DIR')}"


class OutputFileCreation(ABC):

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.deploy_env_suffix = self.gcp_config['deploy_env_storage_suffix']
        self.job_config = read_yamlfile_env_suffix(f'{config_dir}/{config_filename}', self.deploy_env,
                                                   self.deploy_env_suffix)
        self.default_args = {
            "owner": "TBD",
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True
        }

        self.local_tz = pendulum.timezone('America/Toronto')

    def build_cluster_creating_task(self, dag_id: str, job_size: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=self.job_config.get(dag_id).get(consts.CLUSTER_NAME)
        )

    def get_output_file_job(self, dag_config):
        return PythonOperator(
            task_id="get_output_file_name",
            python_callable=self.get_output_file_name,
            op_kwargs={
                'dag_config': dag_config}
        )

    def build_control_record_saving_job(self, file_name: str, output_dir: str, **context):
        job_params_str = json.dumps({
            'source_filename': file_name,
            'extract_path': output_dir
        })
        save_job_to_control_table(job_params_str, **context)

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'file_name': "{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}",
                       'output_dir': f"gs://{dag_config.get(consts.DESTINATION_BUCKET)}"}
        )

    def get_output_file_name(self, dag_config: dict, **context):
        spark_config = dag_config.get(consts.SPARK)
        output_job_args = spark_config.get(consts.OUTPUT_JOB_ARGS)
        output_file_extension = read_env_filepattern(output_job_args['filename.extension'], self.deploy_env)
        output_file_name = f"{output_job_args['output.filename']}_{JOB_DATE_TIME}.{output_file_extension}"

        context['task_instance'].xcom_push(key='output_file_name', value=output_file_name)
        logger.info(f"Output file name: {output_file_name}")

    def get_batch_num_val(self, dag_id):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        return var_dict.get('batch_num_val', {}) if var_dict else None

    def get_body_segment_list(self, dag_config: dict):
        bigquery_config = dag_config.get(consts.BIGQUERY)
        segment_configs = bigquery_config.get(consts.SEGMENT_ARGS)

        return [segment_name for segment_name in segment_configs]

    def generate_batch_num(self, dag_id: str, batch_num_table: str, **context):
        bigquery_client = bigquery.Client()
        batch_param_table = f"pcb-{DEPLOY_ENV}-processing.domain_account_management.{batch_num_table}"
        if table_exists(bigquery_client, batch_param_table):
            extract_batch_num = f""" SELECT MAX(PARAM_VALUE) AS param_value FROM {batch_param_table}"""
            logging.info(f"{batch_param_table} exists extracting batch num value using {extract_batch_num}")
            extract_batch_num_result = bigquery_client.query(extract_batch_num).result().to_dataframe()
            batch_num_value = extract_batch_num_result['param_value'].values[0]
            logging.info(f"batch_num_value for this execution of {dag_id} is {batch_num_value}")
            if batch_num_value:
                context['ti'].xcom_push(key='batch_num_value', value=batch_num_value)
            else:
                raise AirflowException(f"batch value for {dag_id} not set properly")
        else:
            initial_value = self.get_batch_num_val(dag_id)
            if initial_value:
                logging.info(f"initial batch num val for this execution of {dag_id} is {initial_value}")
                create_insert_sql = f"""
                                      CREATE TABLE IF NOT EXISTS
                                      {batch_param_table} (
                                      PARAM_NAME STRING,
                                      PARAM_VALUE INTEGER,
                                      CREATE_DT DATE);

                                      INSERT INTO {batch_param_table}
                                      (PARAM_NAME, PARAM_VALUE, CREATE_DT) VALUES
                                      ( "BATCH_FILE_NUM", {initial_value}, CURRENT_DATE());
                                   """
                run_bq_query(create_insert_sql)
                context['ti'].xcom_push(key='batch_num_value', value=initial_value)
            else:
                raise AirflowException(f"initial batch value for {dag_id} not set properly")

    def fetch_batch_num(self, dag_id: str, dag_config: dict):
        bigquery_config = dag_config.get(consts.BIGQUERY)
        batch_num_table = bigquery_config.get(BATCH_NUM_TABLE)
        if batch_num_table:
            return PythonOperator(
                task_id="generate_batch_num",
                python_callable=self.generate_batch_num,
                op_kwargs={'dag_config': dag_config,
                           'dag_id': dag_id,
                           'bigquery_config': bigquery_config,
                           'batch_num_table': batch_num_table}
            )
        else:
            return DummyOperator(task_id="skip_gen_batch_num")

    def append_to_landing_tables(self, dag_id: str, dag_config: dict, segment_name: str):
        client = bigquery.Client()
        logger.info(f"current output data will merged into output landing tables for : {dag_id}")
        bigquery_config = dag_config.get(consts.BIGQUERY)
        segment_configs = bigquery_config.get(consts.SEGMENT_ARGS)
        output_table = segment_configs.get(segment_name).get('output_table_name')
        output_table_name = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.{output_table}"
        input_table = segment_configs.get(segment_name).get('output_table_name')
        input_table_name = f"pcb-{DEPLOY_ENV}-processing.domain_account_management.{input_table}"
        loading_sql = f"""
                        CREATE TABLE IF NOT EXISTS
                        {output_table_name}
                        AS
                        SELECT *
                        FROM {input_table_name}
                        LIMIT 0;

                        INSERT INTO {output_table_name}
                        SELECT *
                        FROM {input_table_name};
                    """
        loading_sql_results = client.query(loading_sql)
        loading_sql_results.result()
        if loading_sql_results.done() and loading_sql_results.error_result is None:
            logger.info(f"Successfully merged {output_table_name} with data from {input_table_name}")
        else:
            raise AirflowFailException(f"failed to create {output_table_name}")

    def append_landing_tables_task(self, dag_id: str, dag_config: dict, segment_name: str):
        return PythonOperator(
            task_id=f"append_to_landing_{segment_name}_table",
            python_callable=self.append_to_landing_tables,
            op_kwargs={'dag_config': dag_config,
                       'dag_id': dag_id,
                       'segment_name': segment_name}
        )

    def build_append_task_groups(self, dag_id, dag_config: dict, body_segments: Union[list, set]):
        task_groups = []

        for segment_name in body_segments:
            task_groups.append(self.append_landing_tables_task(dag_id, dag_config, segment_name))

        return task_groups

    def load_output_tables(self, segment_configs: dict, segment_name: str, file_create_dt: str, batch_num_value: str):
        client = bigquery.Client()
        logger.info(f"file create date for this execution is: {file_create_dt}")
        output_table = segment_configs.get(segment_name).get('output_table_name')
        output_table_name = f"{output_table}"
        sql_file = f"{output_table}.sql"
        if sql_file is not None and len(sql_file) > 0:
            logger.info(f"Sql file for the task is: {sql_file}")
            create_table_sql = read_file_env(sql_file_path + sql_file, self.deploy_env)
            extract_table = client.query(create_table_sql.replace(FILE_CREATE_DT_PLACEHOLDER, file_create_dt)
                                         .replace(OUTPUT_TABLE_PLACEHOLDER, output_table_name))
            extract_table.result()
            if extract_table.done() and extract_table.error_result is None:
                logger.info("Extract job completed successfully")
            else:
                raise AirflowFailException(f"Extract job not completed due to {extract_table.error_result}")

    def extract_output_data_task(self, dag_config: dict, bigquery_config: dict, segment_name):
        segment_configs = bigquery_config.get(consts.SEGMENT_ARGS)
        source_dag_id = dag_config.get('source_dag_id')
        task_id = f"load_output_table_{segment_name}"
        return PythonOperator(
            task_id=task_id,
            trigger_rule='none_failed',
            python_callable=self.load_output_tables,
            op_kwargs={'bigquery_config': bigquery_config,
                       'segment_configs': segment_configs,
                       'segment_name': segment_name,
                       'source_dag_id': source_dag_id,
                       'file_create_dt': "{{ dag_run.conf['file_create_dt'] }}",
                       'batch_num_value': "{{ ti.xcom_pull(task_ids='generate_batch_num', key='batch_num_value') }}"
                       }
        )

    def insert_into_key_control_table(self, dag_config: dict, dag_id: str, file_create_dt: str):
        client = bigquery.Client()
        logger.info(f"Inserting new accounts to key control table : {dag_id}")
        bigquery_config = dag_config.get(consts.BIGQUERY)
        output_table = bigquery_config.get('key_table')
        output_table_name = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.{output_table}"
        sql_file = f"{output_table}.sql"
        if sql_file is not None and len(sql_file) > 0:
            logger.info(f"Sql file for the task is: {sql_file}")
            create_table_sql = read_file_env(sql_file_path + sql_file, self.deploy_env)
            final_query = create_table_sql.replace(FILE_CREATE_DT_PLACEHOLDER, file_create_dt).replace(OUTPUT_TABLE_PLACEHOLDER, output_table_name)
            logger.info(f"sql: {final_query}")
            extract_table = client.query(final_query)
            extract_table.result()
            if extract_table.done() and extract_table.error_result is None:
                logger.info("Extract job completed successfully")
            else:
                raise AirflowFailException(f"Extract job not completed due to {extract_table.error_result}")

    def update_key_control_table(self, dag_id: str, dag_config: dict, **context):
        return PythonOperator(
            task_id='insert_into_key_control_table',
            trigger_rule='none_failed',
            python_callable=self.insert_into_key_control_table,
            op_kwargs={'dag_config': dag_config,
                       'dag_id': dag_id,
                       'file_create_dt': "{{ dag_run.conf['file_create_dt'] }}",
                       }
        )

    def create_output_data(self, dag_config: dict, body_segments: Union[list, set]):
        task_groups = []

        for segment_name in body_segments:
            bigquery_config = dag_config.get(consts.BIGQUERY)
            task_groups.append(self.extract_output_data_task(dag_config, bigquery_config, segment_name))

        return task_groups

    def build_extractor(self, dag_id: str, dag_config: dict):
        """
        Initializing spark for creating the output file ,
        using the BQ data. Spark parameters are maintained in configuration file itself
        """
        spark_config = dag_config.get(consts.SPARK)
        extractor_job_args = spark_config.get(consts.OUTPUT_JOB_ARGS)
        output_folder = get_output_folder_path(dag_config)
        interim_folder = get_interim_folder_path(dag_config)

        extractor_job_args['output.filepath'] = f"{output_folder}"
        extractor_job_args['interim.filepath'] = f"{interim_folder}"
        extractor_job_args['file.name'] = "{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}"
        extractor_job_args['transformer.job'] = None

        arglist = []
        for k, v in extractor_job_args.items():
            arglist.append(f'{k}={v}')

        arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args,
                                           arg_list=arglist)
        spark_output_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.CLUSTER_NAME)},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.OUTPUT_JAR_URI],
                consts.MAIN_CLASS: spark_config[consts.OUTPUT_MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arglist
            }
        }
        return DataprocSubmitJobOperator(
            task_id="extractor_job",
            job=spark_output_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def move_output_file_to_landing(self, dag_config: dict):
        """
        File moving task from processing to landing
        """
        return GCSToGCSOperator(
            task_id='move_file_to_outbound_landing',
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            source_bucket=dag_config.get(consts.STAGING_BUCKET),
            source_object=f"{dag_config.get('PROCESSING_OUTPUT_DIR')}/{{{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name')}}}}",
            destination_bucket=dag_config.get(consts.DESTINATION_BUCKET),
            destination_object="{{ ti.xcom_pull(task_ids='get_output_file_name', key='output_file_name') }}"

        )

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        """
        Main DAG which is triggered to extract output file
        """
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            schedule=None,
            is_paused_upon_creation=True
        )
        with (dag):
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(OUTPUT_FILE_CREATION, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_creating_task = self.build_cluster_creating_task(dag_id, job_size)
            get_output_file_job = self.get_output_file_job(dag_config)
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            append_landing_tables_task = self.build_append_task_groups(dag_id, dag_config,
                                                                       self.get_body_segment_list(dag_config))
            update_key_control_task = self.update_key_control_table(dag_id, dag_config)
            extractor_spark_job = self.build_extractor(dag_id, dag_config)
            move_output_file_to_landing = self.move_output_file_to_landing(dag_config)
            start = DummyOperator(task_id='Start')
            end = DummyOperator(task_id='End')

            start >> get_output_file_job >> cluster_creating_task

            leading_task = cluster_creating_task

            segment_dependency_groups = dag_config.get(consts.BIGQUERY).get(consts.SEGMENT_DEPENDENCY_GROUPS)

            for segment_dependency_group in segment_dependency_groups:
                group_name = segment_dependency_group.get(consts.GROUP_NAME)
                group_start = DummyOperator(task_id=f'{group_name}_start')
                group_end = DummyOperator(task_id=f'{group_name}_end')
                leading_task >> group_start
                leading_task = group_end
                group_segments_str = segment_dependency_group.get(consts.SEGMENTS)
                group_segment_list = set([segment.strip() for segment in group_segments_str.split(consts.COMMA)])

                group_tasks = self.create_output_data(dag_config, group_segment_list)

                group_start >> group_tasks >> group_end

            (leading_task >> extractor_spark_job >> append_landing_tables_task >> update_key_control_task
             >> move_output_file_to_landing >> control_table_task >> end)

        return add_tags(dag)

    def create_dags(self) -> dict:
        if self.job_config:
            dags = {}

            for job_id, config in self.job_config.items():
                self.default_args.update(config.get(consts.DEFAULT_ARGS))
                dags[job_id] = self.create_dag(job_id, config)
                dags[job_id].tags = config.get(consts.TAGS)

            return dags
        else:
            return {}
