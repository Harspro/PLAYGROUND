from datetime import datetime, timedelta
from airflow import settings, DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import pendulum
import json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_variable_or_file, get_cluster_name_for_dag, ENV_PLACEHOLDER, read_file_env, save_job_to_control_table
from util.bq_utils import check_bq_table_exists, create_or_replace_table, table_exists, \
    apply_schema_sync_transformation, apply_timestamp_transformation, apply_join_transformation, parse_join_config, \
    apply_column_transformation, create_external_table, schema_preserving_load

import logging
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from tsys_processing.generic_file_loader import GenericFileLoader
from dag_factory.terminus_dag_factory import add_tags

GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']

STMT_AUDIT_CONTROL_TABLE_SCHEMA = f"{settings.DAGS_FOLDER}/tsys_processing/STMT_AUDIT_CONTROL_TABLE_SCHEMA.json"
STMT_AUDIT_CONTROL_TABLE = f"pcb-{DEPLOY_ENV}-curated.domain_payments.STMT_AUDIT_CONTROL"
logger = logging.getLogger(__name__)
local_tz = datetime.now(pendulum.timezone('America/Toronto'))
CURRENT_DATETIME = local_tz.strftime('%Y-%m-%dT%H:%M:%S.%f')
var = "CNT"
sql_file_path = f'{settings.DAGS_FOLDER}/tsys_processing/sql/'


class TsysPcbPcmcStmtFileLoader(GenericFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

    def check_record_count(self, bigquery_config: dict, segment_name: str, **context):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        file_type = bigquery_config.get(consts.FILE_TYPE)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)

        if record_count_column:
            query = f""" select count(*) AS {var}
                        FROM `pcb-{self.deploy_env}-processing.domain_account_management.STMT_HO50_INDEXCTL_{file_type}_DBEXT`
                        WHERE MAST_RECORD_ID = '{segment_name}'
                    """
            logger.info(query)
            tsys_count = bigquery_client.query(query).result().to_dataframe()
            logger.info(tsys_count)
            num = tsys_count[var].values[0]
            if segment_name == "SA10":
                context['ti'].xcom_push(key='sa10_rec_count', value=int(num))

            if int(num) > 0:
                return f'{segment_name}.parse_file'
            else:
                return f'{segment_name}.rec_count_validate'
        else:
            return f'{segment_name}.parse_file'

    def validate_rec_count_job(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str, file_name: str,
                               **context):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        file_type = bigquery_config.get(consts.FILE_TYPE)
        if table_config.get(consts.LANDING_PAUSE):
            pass
        elif record_count_column:
            query = f""" select count(*) AS {var}
                        FROM `pcb-{self.deploy_env}-processing.domain_account_management.STMT_HO50_INDEXCTL_{file_type}_DBEXT`
                        WHERE MAST_RECORD_ID = '{segment_name}'"""
            result = bigquery_client.query(query).result().to_dataframe()
            trailer_count = result[var].values[0]
            if trailer_count > 0:
                query = f"SELECT COUNT(1) AS {var} FROM {bq_ext_table_id}"
                result = bigquery_client.query(query).result().to_dataframe()
                insert_count = result[var].values[0]
                if trailer_count != insert_count:
                    raise AirflowException(
                        f"trailer count {trailer_count} does not match bigquery count {insert_count}")

    def post_processing_table_job(self, table_config: dict, file_name, sa10_rec_count, **context):
        if table_config.get('count_check_skip') and int(sa10_rec_count) < 1:
            logger.info(f"No detail part in file. SA10_REC_COUNT is {sa10_rec_count}")
            return
        client = bigquery.Client()
        project_name = table_config.get(consts.PROJECT_ID).replace(ENV_PLACEHOLDER, self.deploy_env)
        dataset_name = table_config.get(consts.DATASET_ID)
        table_name = table_config.get(consts.TABLE_NAME)
        sql_file = table_config.get(consts.QUERY_FILE)
        data_load_type = table_config.get(consts.DATA_LOAD_TYPE)
        file_type = ''
        if table_config.get(consts.FILE_TYPE):
            file_type = consts.UNDERSCORE + table_config.get(consts.FILE_TYPE)
        if table_config.get('table_exists'):
            table_check = table_config.get('table_exists').replace(consts.FILE_TYPE_PLACEHOLDER, file_type)
            logger.info(f"Checking for table {table_config.get(consts.TABLE_NAME)}")
            if not table_exists(client, table_check):
                logger.info(f"Table not found : {table_config.get(consts.TABLE_NAME)}")
                return
        output_table = f"{project_name}.{dataset_name}.{table_name}"
        output_table = output_table.replace(consts.FILE_TYPE_PLACEHOLDER, file_type)
        task_name = table_config.get(consts.TASK_NAME)
        logger.info('Starting with task {}', task_name)

        if sql_file is not None and len(sql_file) > 0:
            logger.info(f"Sql file for the task is: {sql_file}")
            table_sql = read_file_env(sql_file_path + sql_file, self.deploy_env)
            table_sql = table_sql.replace(consts.FILE_TYPE_PLACEHOLDER, file_type)
            if data_load_type == consts.APPEND_ONLY:
                loading_sql = f"""
                                  CREATE TABLE IF NOT EXISTS
                                   `{output_table}`
                                   AS
                                      {table_sql}
                                      LIMIT 0;

                                  INSERT INTO `{output_table}`
                                  {table_sql};
                              """
                logger.info(loading_sql)
                client.query_and_wait(loading_sql)
            elif data_load_type == consts.FULL_REFRESH:
                insert_stmt = f"""
                            INSERT INTO `{output_table}`
                            SELECT * FROM ({table_sql})
                        """
                create_ddl = f"""
                            CREATE OR REPLACE TABLE
                            `{output_table}`
                            AS
                            {table_sql};
                        """
                schema_preserving_load(create_ddl, insert_stmt, output_table, bq_client=client)
            else:
                raise AirflowException(f"unsupported data_load_type {data_load_type}")

    def fetch_file_create_dt(self, dag_config: dict, **context):
        bigquery_client = bigquery.Client()
        table_details = dag_config.get(consts.BIGQUERY).get('file_create_dt_table')
        file_create_dt_query = f"""
                     SELECT CAST(file_create_dt AS STRING) AS file_create_dt
                     FROM {table_details}
                 """
        result = bigquery_client.query(file_create_dt_query).result().to_dataframe()
        file_create_dt = result['file_create_dt'].values[0]

        logger.info(file_create_dt)

        context['ti'].xcom_push(key='file_create_dt', value=file_create_dt)

    def start_audit_task(self, inbound_file_name: str, **context):
        bigquery_client = bigquery.Client()
        dag_id = context['dag'].dag_id
        dag_run_id = context['run_id']
        stmt_audit_table_id = f"{STMT_AUDIT_CONTROL_TABLE}"
        if not check_bq_table_exists(STMT_AUDIT_CONTROL_TABLE):
            create_or_replace_table(STMT_AUDIT_CONTROL_TABLE, STMT_AUDIT_CONTROL_TABLE_SCHEMA)
        logging.info(f"Starting audit control for {dag_id} and {dag_run_id}")

        start_audit_control = f"""INSERT INTO {stmt_audit_table_id}
            (DAG_RUN_ID, DAG_ID, INBOUND_FILE_NAME, JOB_START_TIME) VALUES
            ( "{dag_run_id}", "{dag_id}", "{inbound_file_name}", '{CURRENT_DATETIME}') """

        logging.info(
            f"Inserting the job run details for {dag_id} and {dag_run_id} into table {stmt_audit_table_id} "
            f"using the query {start_audit_control}")

        bigquery_client.query_and_wait(start_audit_control)

        context['ti'].xcom_push(key='job_start_time', value=CURRENT_DATETIME)

    def build_transformation_config(self, bigquery_config: dict, output_dir: str, segment_name: str):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)

        parquet_files_path = f"{output_dir}/{segment_name}/*.parquet"

        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        bq_table_name = table_config[consts.TABLE_NAME]
        file_type = bigquery_config.get('file_type')

        bq_ext_table_id = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{file_type}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_dataset_name}.{bq_table_name}"

        partition_field = bigquery_config.get(consts.TABLES).get(segment_name).get('partition_field')
        clustering_fields = bigquery_config.get(consts.TABLES).get(segment_name).get('clustering_fields')

        landing_pause = bigquery_config.get(consts.TABLES).get(segment_name).get(consts.LANDING_PAUSE)

        additional_columns = table_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = table_config.get(consts.DROP_COLUMNS) or []
        join_spec = table_config.get(consts.JOIN) or []
        return {
            consts.FILE_TYPE: file_type,
            consts.LANDING_PAUSE: landing_pause,
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.PARTITION: partition_field,
            consts.CLUSTERING: clustering_fields,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_table_id
            },
            consts.JOIN_SPECIFICATION: join_spec,
        }

    def build_control_record_saving_job_with_file_create_dt(self, file_name: str, output_dir: str, file_create_dt: str, **context):
        job_params_str = json.dumps({
            'source_filename': file_name,
            'extract_path': output_dir,
            'file_create_dt': file_create_dt
        })
        save_job_to_control_table(job_params_str, **context)

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        with TaskGroup(group_id='post_processing') as postprocessing:
            fetch_file_create_dt_task = PythonOperator(
                task_id='fetch_file_create_dt_task',
                python_callable=self.fetch_file_create_dt,
                op_kwargs={'dag_config': dag_config}
            )
            post_processing_start = EmptyOperator(task_id=f'{consts.POST_PROCESSING_TABLES}_start')
            post_processing_tables = dag_config.get(consts.POST_PROCESSING_TABLES)

            post_processing_start >> fetch_file_create_dt_task

            leading_task = fetch_file_create_dt_task

            for post_processing_table in post_processing_tables:
                task_name = post_processing_table.get(consts.TASK_NAME)
                post_processing_tables_task = self.post_processing_tables_job(post_processing_table, task_name)
                leading_task >> post_processing_tables_task
                leading_task = post_processing_tables_task

            post_processing_end = EmptyOperator(task_id=f'{consts.POST_PROCESSING_TABLES}_end')
            leading_task >> post_processing_end
            leading_task = post_processing_end

            control_table_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job_with_file_create_dt,
                op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                           'output_dir': self.get_output_dir_path(dag_config),
                           'file_create_dt': "{{ ti.xcom_pull(task_ids='post_processing.fetch_file_create_dt_task', key='file_create_dt') }}",
                           'tables_info': self.extract_tables_info(dag_config)}
            )

            leading_task >> control_table_task

        return postprocessing

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        logger.info(transformation_config)
        file_type = transformation_config.get(consts.FILE_TYPE)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))
        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            add_columns = [col.replace(consts.FILE_TYPE_PLACEHOLDER, file_type) for col in add_columns]
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view
        join_spec = transformation_config.get(consts.JOIN_SPECIFICATION)
        for join_config in join_spec:
            join_parse_spec = parse_join_config(bigquery_client, join_config,
                                                transformed_data.get(consts.ID),
                                                join_type=join_config.get(consts.JOIN_TYPE, consts.LEFT_JOIN))
            join_transform_view = apply_join_transformation(bigquery_client, transformed_data, join_parse_spec)
            transformed_data = join_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)

        if transformation_config.get(consts.LANDING_PAUSE):
            pass
        elif table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict, segment_name: str):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)
        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)
        partition_field = transformation_config.get(consts.PARTITION)
        clustering_fields = transformation_config.get(consts.CLUSTERING)
        if self.is_trailer(segment_name) and table_exists(bigquery_client, transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)):
            self.duplicate_trailer_check(bigquery_config, transformation_config, transformed_view, segment_name)
        partition_str = ""
        if partition_field:
            partition_str = f"PARTITION BY {partition_field}"

        clustering_str = ""
        if clustering_fields:
            clustering_str = "CLUSTER BY " + ", ".join(clustering_fields)

        if consts.LANDING_PAUSE in bigquery_config.get(consts.TABLES).get(segment_name):
            pass
        elif data_load_type == consts.APPEND_ONLY:
            loading_sql = f"""
                   CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                    AS
                       SELECT {transformed_view.get(consts.COLUMNS)}
                       FROM {transformed_view.get(consts.ID)}
                       LIMIT 0;

                   INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                   SELECT {transformed_view.get(consts.COLUMNS)}
                   FROM {transformed_view.get(consts.ID)};
               """
            logger.info(loading_sql)
            bigquery_client.query_and_wait(loading_sql)

        elif data_load_type == consts.FULL_REFRESH:
            loading_sql = f"""
                   CREATE OR REPLACE TABLE
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                    {partition_str}
                    {clustering_str}
                    AS
                       SELECT {transformed_view.get(consts.COLUMNS)}
                       FROM {transformed_view.get(consts.ID)};
               """
            logger.info(loading_sql)
            bigquery_client.query_and_wait(loading_sql)
        else:
            raise AirflowException(f"unsupported data_load_type {data_load_type}")

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        with TaskGroup(group_id='preprocessing') as preprocessing:
            start_audit_task = PythonOperator(
                task_id="start_audit_task",
                python_callable=self.start_audit_task,
                op_kwargs={'inbound_file_name': "{{ dag_run.conf['name']}}"},
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )
            filtering_task = self.build_filtering_task(cluster_name, dag_config)
            start_audit_task >> filtering_task
        return preprocessing

    def post_processing_tables_job(self, table_config: dict, task_name: str, **context):
        return PythonOperator(
            task_id=task_name,
            trigger_rule='none_failed',
            python_callable=self.post_processing_table_job,
            op_kwargs={
                'table_config': table_config,
                'file_name': "{{ dag_run.conf['name']}}",
                'sa10_rec_count': "{{ ti.xcom_pull(task_ids='SA10.record_count_check', key='sa10_rec_count') }}"}
        )

    def trigger_decision(self, dag_id: str):
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        outbound_trigger = var_dict.get('outbound_trigger', True)
        if outbound_trigger:
            return self.trigger_dag_job(dag_id)
        else:
            return EmptyOperator(task_id="skipped_outbound_file_gen")

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            cluster_name = get_cluster_name_for_dag(dag_id)
            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)

            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')

            file_staging_task = self.build_file_staging_task(dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            start >> file_staging_task >> cluster_creating_task >> preprocess_task

            leading_task = preprocess_task

            segment_dependency_groups = dag_config.get(consts.SPARK).get(consts.SEGMENT_DEPENDENCY_GROUPS)

            for segment_dependency_group in segment_dependency_groups:
                group_name = segment_dependency_group.get(consts.GROUP_NAME)
                group_start = EmptyOperator(task_id=f'{group_name}_start')
                group_end = EmptyOperator(task_id=f'{group_name}_end')
                leading_task >> group_start
                leading_task = group_end
                group_segments_str = segment_dependency_group.get(consts.SEGMENTS)
                group_segment_list = set([segment.strip() for segment in group_segments_str.split(consts.COMMA)])

                group_tasks = self.build_body_task_groups(cluster_name, dag_config, group_segment_list)

                group_start >> group_tasks >> group_end

            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            trigger_decision_job = self.trigger_decision(dag_id)

            leading_task >> control_table_task >> trigger_decision_job >> end

        return add_tags(dag)

    def trigger_dag_job(self, dag_id: str, **context):
        return TriggerDagRunOperator(
            task_id="trigger_dag",
            trigger_dag_id=self.job_config.get(dag_id).get('trigger_dag_id'),
            conf={'file_create_dt': "{{ ti.xcom_pull(task_ids='post_processing.fetch_file_create_dt_task', key='file_create_dt') }}",
                  'file_name': "{{ dag_run.conf['name']}}",
                  'sa10_rec_count': "{{ ti.xcom_pull(task_ids='SA10.record_count_check', key='sa10_rec_count') }}"
                  },
            wait_for_completion=False,
            trigger_rule='none_failed',
            retries=0,
            poke_interval=60,
        )
