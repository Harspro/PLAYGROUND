import json
import logging
import re
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from google.cloud import bigquery

import util.constants as consts
from bigquery_loader.bigquery_loader_base import BigQueryLoader, LoadingFrequency, INITIAL_DEFAULT_ARGS
from util.bq_utils import parse_join_config
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_yamlfile_env,
    save_job_to_control_table,
    get_dagrun_last,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag,
    split_table_name
)
from dag_factory.terminus_dag_factory import add_tags
from airflow.models.dagrun import DagRun

logger = logging.getLogger(__name__)

BIGQUERY_DELTA_LOADER: Final = 'bigquery_delta_loader'
TRIGGER_DAG_ID: Final = 'trigger_dag_id'


class BigQueryDeltaLoader(BigQueryLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

    def build_fetching_job(self, cluster_name: str, config: dict, execution_id: str):
        gcs_config = config.get(consts.GCS)
        if not gcs_config:
            gcs_config = {
                consts.BUCKET: f"pcb-{self.deploy_env}-staging-extract",
                consts.STAGING_FOLDER: f"{config[consts.FETCH_SPARK_JOB].get(consts.DBNAME)}/delta_load"
            }
            config[consts.GCS] = gcs_config

        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]
        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")
        spark_config = self.dbconfig[db_name][consts.FETCH_SPARK_JOB].copy()

        spark_config.update(config[consts.FETCH_SPARK_JOB])
        source_schema, source_tablename = split_table_name(spark_config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME))
        staging_folder = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{source_schema}/{source_tablename}/{execution_id}"

        arg_list = [spark_config[consts.APPLICATION_CONFIG], staging_folder]

        if consts.ARGS in spark_config:
            for k, v in spark_config[consts.ARGS].items():
                arg_list.append(f'{k}={v}')

        if not any(consts.DB_READ_PARALLELISM in arg for arg in arg_list):
            arg_list.append(consts.DB_READ_PARALLELISM + '=4')

        if consts.PROPERTIES not in spark_config:
            spark_config[consts.PROPERTIES] = consts.DEFAULT_SPARK_SETTINGS

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)
        spark_job_dict = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: spark_config[consts.PROPERTIES]
            }
        }
        return spark_job_dict

    def build_transformation_config(self, bq_client, context):
        execution_id = context.get('execution_id')
        config = context.get(consts.CONFIG)
        gcs_config = config.get(consts.GCS).copy()
        bq_config = config[consts.BIGQUERY]
        spark_config = config[consts.FETCH_SPARK_JOB]

        source_schema, source_tablename = split_table_name(spark_config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME))
        parquet_files_path = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{source_schema}/{source_tablename}/{execution_id}/*.parquet"

        bq_project_name = bq_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_proc_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_table_name = bq_config[consts.TABLE_NAME]

        bq_ext_table_id = f"{bq_proc_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}"

        partition_field = bq_config.get('partition_field')
        clustering_fields = bq_config.get('clustering_fields')
        logger.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        logger.info(f'partition: {partition_str}, cluster: {clustering_str}')

        additional_columns = bq_config.get(consts.ADD_COLUMNS) or []
        add_file_name_config = bq_config.get(consts.ADD_FILE_NAME_COLUMN)
        if add_file_name_config:
            additional_columns.append(f"'{parquet_files_path}' AS {add_file_name_config.get(consts.ALIAS)}")

        exclude_columns = bq_config.get(consts.DROP_COLUMNS)

        join_spec = parse_join_config(bq_client, bq_config.get(consts.JOIN), bq_table_id)

        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.PARTITION: partition_str,
                consts.CLUSTERING: clustering_str,
                consts.ID: bq_table_id
            },
            consts.JOIN_SPECIFICATION: join_spec
        }

    def build_loading_job(self, **context):
        insert_fetch_cols = context.get(consts.CONFIG, {}).get(consts.BIGQUERY, {}).get(consts.INSERT_FETCH_COLS, None)
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        transformed_view = self.apply_transformations(bq_client, transformation_config)

        if insert_fetch_cols:
            loading_sql = f"""
                CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)}
                    LIMIT 0;
                INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}` ({transformed_view.get(consts.COLUMNS)})
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)};
            """
        else:
            loading_sql = f"""
                CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)}
                    LIMIT 0;
                INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)};
            """

        logger.info(loading_sql)
        query_job = bq_client.query(loading_sql)
        query_job.result()

    def build_cluster_creating_task(self, cluster_name, job_size: str, is_sqlserver=None):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size, is_sqlserver),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=cluster_name
        )

    def build_fetching_data_task(self, cluster_name: str, config, execution_id):
        return DataprocSubmitJobOperator(
            task_id="fetch_db",
            job=self.build_fetching_job(cluster_name, config, execution_id),
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
        )

    def build_bq_load_task(self, dag_id: str, config: dict, execution_id):
        return PythonOperator(
            task_id="load_bq",
            python_callable=self.build_loading_job,
            op_kwargs={
                'dag_id': dag_id,
                'execution_id': execution_id,
                consts.CONFIG: config,
            }
        )

    def validate_chunk_load_job(self, dag_id: str, config: dict, **context):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        last_start_date = ""
        last_end_date = ""
        bq_results = get_dagrun_last(dag_id)
        if bq_results.total_rows != 0:
            results = bq_results.to_dataframe()
            job_params = json.loads(results['job_params'].values[0])
            last_start_date = str(job_params['start_date'])
            logger.info(f"last_start_date:, {last_start_date}")
            last_end_date = str(job_params['end_date'])
            logger.info(f"last_end_date:, {last_end_date}")
        curr_date = datetime.now(self.local_tz)
        if config.get('dag', {}).get('same_day_run'):
            from_date = curr_date.strftime("%Y-%m-%d")
            to_date = from_date
        else:
            to_date = curr_date.strftime("%Y-%m-%d")
            from_date = (curr_date - timedelta(days=1)).strftime("%Y-%m-%d")
        if config.get('batch'):
            batch_config = config['batch']
            if config['batch'].get('time_from') and config['batch'].get('time_to'):
                fromdt = str(from_date) + f" {batch_config['time_from']}"
                todt = str(to_date) + f" {batch_config['time_to']}"
        else:
            fromdt = str(from_date) + " 00:00:00"
            todt = str(to_date) + " 00:00:00"

        if var_dict:
            overwrite_start_date = var_dict.get('overwrite_loading_date')
            if overwrite_start_date:
                from_date = str(var_dict.get('start_date'))
                to_date = str(var_dict.get('end_date'))
                fromdt = f"{from_date} {var_dict.get('time_from')}"
                todt = f"{to_date} {var_dict.get('time_to')}"

        logger.info(f"fromdt:, {fromdt}")
        logger.info(f"todt:, {todt}")

        context['ti'].xcom_push(key='load_data_from', value=f'{fromdt}')
        context['ti'].xcom_push(key='load_data_to', value=f'{todt}')

        execution_id = re.sub(r'\W+', '-', f"delta_{fromdt}_{todt}")
        context['ti'].xcom_push(key='execution_id', value=f'{execution_id}')

        config[consts.DAG_ID] = dag_id
        if last_start_date == fromdt and last_end_date == todt:
            return "save_job_to_control_table"
        else:
            return f"{consts.CLUSTER_CREATING_TASK_ID}"

    def validate_chunk_load_task(self, dag_id: str, config: dict):
        return BranchPythonOperator(
            task_id='check_last_executation',
            python_callable=self.validate_chunk_load_job,
            op_kwargs={'dag_id': dag_id,
                       consts.CONFIG: config}
        )

    def build_control_record_saving_job(self, fetch_data_range: str, start_date: str, end_date: str, **context):
        job_params_str = json.dumps({
            'fetch_data_range': fetch_data_range,
            'start_date': start_date,
            'end_date': end_date
        })
        save_job_to_control_table(job_params_str, **context)

    def build_postprocessing_task_group(self, dag_id: str, config: dict, fromdt: str, todt: str):
        return PythonOperator(
            task_id='save_job_to_control_table',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'start_date': fromdt,
                       'end_date': todt,
                       'fetch_data_range': config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_WHERE_CONDITION],
                       'tables_info': self.extract_tables_info(config)
                       }
        )

    def trigger_dag(self, config: dict):
        def conf_callable(context, **kwargs):
            dr: DagRun = context["dag_run"]
            run_type = dr.run_type
            parent_dag_id = context.get("dag").dag_id
            logger.info("parent_dag_id: %s \ntrigger_type: %s\n", parent_dag_id, run_type)
            return {
                "run_type": run_type,
                "parent_dag_id": parent_dag_id,
            }

        return TriggerDagRunOperator(
            task_id='trigger_dag',
            trigger_dag_id=config[consts.TRIGGER_CONFIG].get(TRIGGER_DAG_ID),
            wait_for_completion=config[consts.TRIGGER_CONFIG].get(consts.WAIT_FOR_COMPLETION),
            conf=conf_callable
        )

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule_interval(config),
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            max_active_runs=1,
            is_paused_upon_creation=True,
            tags=config.get(consts.TAGS, [])
        )

        with dag:
            if config.get('batch') and config['batch'].get('read_pause_deploy_config'):
                is_paused = read_pause_unpause_setting(BIGQUERY_DELTA_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            is_sqlserver = config.get(consts.IS_SQLSERVER, None)
            start_point = EmptyOperator(task_id='start')

            end_point = EmptyOperator(task_id='end')

            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size, is_sqlserver)
            last_executation_task = self.validate_chunk_load_task(dag_id, config)
            fromdt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_from') }}"
            todt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_to') }}"
            lowerbound = str(fromdt)
            upperbound = str(todt)
            pcol = config[consts.FETCH_SPARK_JOB]['args']['pcb.bigquery.loader.source.partition.column']

            config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_LOWER_BOUND] = lowerbound
            config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_UPPER_BOUND] = upperbound
            config[consts.FETCH_SPARK_JOB][consts.ARGS][
                consts.PCB_WHERE_CONDITION] = f"{pcol} >= '{fromdt}' AND {pcol} < '{todt}'"

            execution_id = "{{ ti.xcom_pull(task_ids='check_last_executation', key='execution_id') }}"
            fetching_data_task = self.build_fetching_data_task(cluster_name, config, execution_id)

            loading_task = self.build_bq_load_task(dag_id, config, execution_id)
            control_table_task = self.build_postprocessing_task_group(dag_id, config, fromdt, todt)

            if config.get('trigger_config'):
                trigger_dag_task = self.trigger_dag(config)

                control_table_task >> trigger_dag_task

            start_point >> last_executation_task >> cluster_creating_task >> fetching_data_task >> loading_task >> control_table_task >> end_point
            last_executation_task >> control_table_task

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
