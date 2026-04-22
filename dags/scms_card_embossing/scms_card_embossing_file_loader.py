import logging
import uuid
import pendulum
from copy import deepcopy
from datetime import timedelta, datetime
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from typing import Final
import util.constants as consts
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists,
    apply_join_transformation, submit_transformation, run_bq_query,
    parse_join_config)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (get_cluster_name_for_dag,
                            save_job_to_control_table,
                            read_file_env)
from tsys_processing.generic_file_loader import GenericFileLoader
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-ogres-alerts",
    'capability': 'account-management',
    'severity': 'P2',
    'sub_capability': 'card-management',
    'business_impact': 'This will impact the card management',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


class CardEmbossingLoader(GenericFileLoader):

    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.local_tz = pendulum.timezone('America/Toronto')
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def add_uuid_current_load(self, bigquery_client, source_table: dict, file_uuid: str, expiration_hours: object = 6):
        source_table_id = source_table.get(consts.ID)
        columns = source_table.get(consts.COLUMNS)
        logger.info(f'source_table_id: {source_table_id}')
        logger.info(f'columns: {columns}')
        logger.info(f'file_uuid: {file_uuid}')

        columns_uuid_view_id = f'{source_table_id}{consts.COLUMN_UUID_SUFFIX}'
        columns_transform_view_ddl = f"""
                CREATE OR REPLACE VIEW
                    `{columns_uuid_view_id}`
                OPTIONS (
                    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
                    description = "View for column transformation"
                ) AS
                    SELECT  {columns}, '{file_uuid}' AS FILE_UUID
                    FROM `{source_table_id}`;
            """

        return submit_transformation(bigquery_client, columns_uuid_view_id, columns_transform_view_ddl)

    def apply_transformations(self, bigquery_client, transformation_config: dict, file_uuid: str):
        logger.info(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))
        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
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
        transformed_data = self.add_uuid_current_load(bigquery_client, transformed_data, file_uuid)
        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict, segment_name: str,
                                  **context):
        bigquery_client = bigquery.Client()
        ti = context['task_instance']
        file_uuid = ti.xcom_pull(task_ids=consts.START_TASK_ID, key=consts.FILE_UUID)
        transformed_view = self.apply_transformations(bigquery_client, transformation_config, file_uuid)
        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)
        partition_field = transformation_config.get(consts.PARTITION)
        clustering_fields = transformation_config.get(consts.CLUSTERING)
        if self.is_trailer(segment_name) and table_exists(bigquery_client,
                                                          transformation_config.get(consts.DESTINATION_TABLE).get(
                                                              consts.ID)):
            self.duplicate_trailer_check(bigquery_config, transformation_config, transformed_view, segment_name)
        partition_str = ""
        if partition_field:
            partition_str = f"PARTITION BY {partition_field}"

        clustering_str = ""
        if clustering_fields:
            clustering_str = "CLUSTER BY " + ", ".join(clustering_fields)

        if data_load_type == consts.APPEND_ONLY:
            loading_sql = f"""
                            CREATE TABLE IF NOT EXISTS
                            `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                            {partition_str}
                            {clustering_str}
                            AS
                            SELECT {transformed_view.get(consts.COLUMNS)}
                            FROM {transformed_view.get(consts.ID)}
                            LIMIT 0;
                            INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                            SELECT {transformed_view.get(consts.COLUMNS)}
                            FROM {transformed_view.get(consts.ID)};
               """
        else:
            raise AirflowFailException(f"unsupported data_load_type {data_load_type}")
        logger.info(loading_sql)
        query_job = bigquery_client.query(loading_sql)
        query_job.result()

    def create_uuid(self, job_type, **context):
        uuid_str = str(uuid.uuid4())
        context['ti'].xcom_push(key=consts.FILE_UUID, value=uuid_str)
        context['ti'].xcom_push(key=consts.START_TIME, value=datetime.now(self.local_tz).strftime('%Y-%m-%d %H:%M:%S.%f'))
        context['ti'].xcom_push(key=consts.JOB_TYPE, value=job_type)

    def start_task(self, job_type):
        return PythonOperator(
            task_id=consts.START_TASK_ID,
            python_callable=self.create_uuid,
            op_args=[job_type]
        )

    def run_bigquery_staging_query(self, query: str, project_id: str, dataset_id: str, table_id: str, curated_view: str, output_uri_landing: str, **kwargs):
        logger.info("Running query provided to create staging table in GCP processing zone.")
        ti = kwargs['ti']
        file_uuid = ti.xcom_pull(task_ids=consts.START_TASK_ID, key=consts.FILE_UUID)
        staging_table_id = f"{project_id}.{dataset_id}.{table_id}"
        logger.info(f"curated view: {curated_view}")
        logger.info(f"output uri landing: {output_uri_landing}")
        stg_query = (query.replace(f"{{{consts.STAGING_TABLE_ID}}}", staging_table_id)
                     .replace(f"{{{'uuid_filter'}}}", file_uuid)
                     .replace(f"{{{'curated_table'}}}", curated_view)
                     .replace(f"{{{'output_uri_landing'}}}", output_uri_landing))

        logger.info(f"Running stg query: {stg_query}")
        run_bq_query(stg_query)
        query_count = f"SELECT COUNT(1) AS REC_CNT FROM {staging_table_id}"
        logger.info(f"Running record count query: {query_count}")
        results = run_bq_query(query_count).to_dataframe()
        num = results['REC_CNT'].values[0]
        logger.info(f"record count: {num}")
        ti.xcom_push(key=consts.RECORD_COUNT, value=str(num))
        ti.xcom_push(key=consts.TARGET_TABLE_ID, value=f"{self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]}.{dataset_id}.{curated_view}")
        if not (num > 0):
            raise AirflowSkipException(f'Number of record: {num}, skip downstream')

    def build_preprocessing_kafka_task(self, dag_config: dict):
        kafka_writer_config = dag_config.get(consts.KAFKA_WRITER)
        kafka_stg_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        kafka_stg_dataset = dag_config.get(consts.BIGQUERY).get(consts.DATASET_ID)
        kafka_stg_table_id = (kafka_writer_config.get(consts.TABLE_ID)
                              .replace(consts.CURRENT_DATE_PLACEHOLDER,
                                       datetime.now(self.local_tz).strftime('%Y_%m_%d')
                                       ))
        output_uri_landing = f'gs://pcb-{self.deploy_env}-staging-extract/' + f'{dag_config.get(consts.GCS).get(consts.FOLDER_NAME)}/{dag_config.get(consts.GCS).get(consts.FILE_NAME)}-*.parquet'
        if kafka_writer_config.get(consts.QUERY):
            query = kafka_writer_config.get(consts.QUERY)
        elif kafka_writer_config.get(consts.QUERY_FILE):
            query_sql_file = f"{settings.DAGS_FOLDER}/" \
                             f"{kafka_writer_config.get(consts.QUERY_FILE)}"
            query = read_file_env(query_sql_file, self.deploy_env)
        else:
            raise AirflowFailException("Please provide query, none found.")
        return PythonOperator(
            task_id=consts.KAFKA_PREPROCESSING_TASK,
            python_callable=self.run_bigquery_staging_query,
            trigger_rule='none_failed',
            op_kwargs={consts.QUERY: query,
                       consts.PROJECT_ID: kafka_stg_project_id,
                       consts.DATASET_ID: kafka_stg_dataset,
                       consts.TABLE_ID: kafka_stg_table_id,
                       consts.CURATED_VIEW: kafka_writer_config.get(consts.CURATED_VIEW),
                       consts.OUTPUT_URI_LANDING: output_uri_landing
                       },
            retries=2
        )

    def on_success_callback_entry(self, context):
        ti = context['ti']
        num = ti.xcom_pull(task_ids=consts.KAFKA_PREPROCESSING_TASK, key=consts.RECORD_COUNT)
        ti.xcom_push(key=consts.RECORD_COUNT, value=num)

    def kafka_trigger_task(self, dag_config: dict):
        return TriggerDagRunOperator(
            task_id=consts.KAFKA_WRITER_TASK,
            trigger_dag_id=dag_config.get(consts.KAFKA_WRITER).get(consts.KAFKA_TRIGGER_DAG_ID),
            logical_date=datetime.now(self.local_tz),
            conf={
                consts.BUCKET: dag_config.get(consts.GCS).get(consts.BUCKET),
                consts.NAME: dag_config.get(consts.GCS).get(consts.BUCKET),
                consts.FOLDER_NAME: dag_config.get(consts.GCS).get(consts.FOLDER_NAME),
                consts.FILE_NAME: dag_config.get(consts.GCS).get(consts.FILE_NAME),
                consts.CLUSTER_NAME: dag_config.get(consts.KAFKA_WRITER).get(consts.KAFKA_CLUSTER_NAME)
            },
            wait_for_completion=True,
            retries=1,
            on_success_callback=self.on_success_callback_entry,
        )

    def check_bq_view_curated(self, bigquery_config: dict, view_name: str, **context):
        data_set = f"{self.gcp_config[consts.CURATED_ZONE_PROJECT_ID]}.{bigquery_config.get(consts.DATASET_ID)}"
        sql_metadata = f"SELECT table_name FROM {data_set}.INFORMATION_SCHEMA.VIEWS WHERE table_name = '{view_name}'"
        logger.info(f"Running query: {sql_metadata}")
        bq_data_check_results = run_bq_query(sql_metadata)
        logger.info(f"bq_data_check_results: {bq_data_check_results}")
        bq_result = run_bq_query(sql_metadata).result()
        logger.info(f"total_rows: {bq_result.total_rows}")
        if bq_result.total_rows != 0:
            return consts.KAFKA_PREPROCESSING_TASK
        else:
            return consts.TRIGGER_VIEW_CREATE_DAG

    def curated_view_dag_trigger(self, dag_config: dict):
        return TriggerDagRunOperator(
            task_id=consts.TRIGGER_VIEW_CREATE_DAG,
            trigger_dag_id=dag_config.get(consts.KAFKA_WRITER).get(consts.CURATED_VIEW_TRIGGER_DAG_ID),
            logical_date=datetime.now(self.local_tz),
            wait_for_completion=True,
            retries=1
        )

    def check_curated_view(self, dag_config: dict):
        return BranchPythonOperator(
            task_id="bq_view_curated_check",
            python_callable=self.check_bq_view_curated,
            op_kwargs={
                'bigquery_config': dag_config.get(consts.BIGQUERY),
                'view_name': dag_config.get(consts.KAFKA_WRITER).get(consts.CURATED_VIEW)}
        )

    def validate_row_count(self, bigquery_client, bq_ext_table_id: str, segment_name: str,
                           record_count_column: str, context, tsys_count: str, is_redefine=False):
        if self.is_trailer(segment_name):
            logger.info("########## inside if condition for trailer #######")
            query = f"SELECT * FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe()
            num = results[record_count_column].values[0]
            context['ti'].xcom_push(key=f"{consts.RECORD_COUNT}", value=f'{results.to_json()}')
            context['ti'].xcom_push(key=f"{consts.READ_RECORD_COUNT}", value=f'{num}')
        elif is_redefine:
            logger.info("########## Redefines : %s segment record count validation skipped #######", segment_name)
        elif int(tsys_count) > 0:
            logger.info("########## inside else condition for other than trailer #######")
            query = f"SELECT COUNT(1) AS {record_count_column} FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe()
            num = results[record_count_column].values[0]
            if int(tsys_count) != int(num):
                raise AirflowFailException(f"tsys count {tsys_count} does not match bigquery count {num}")

    def insert_audit_log_entry(self, status: str, **context):
        table_id = f"pcb-{self.deploy_env}-landing.domain_card_management.CARD_EMBOSSING_FILE_AUDIT_LOG"
        try:
            ti = context['task_instance']
            batch_run_uuid = ti.xcom_pull(task_ids=consts.START_TASK_ID, key=consts.FILE_UUID)
            run_start = ti.xcom_pull(task_ids=consts.START_TASK_ID, key=consts.START_TIME)
            logger.info(f"run_start: {run_start}")
            job_name = ti.xcom_pull(task_ids=consts.START_TASK_ID, key=consts.JOB_TYPE)
            source_location = context['dag_run'].conf.get('bucket')
            target_location = ti.xcom_pull(task_ids=consts.KAFKA_PREPROCESSING_TASK, key=consts.TARGET_TABLE_ID)
            trlr_cnt = ti.xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate", key=f"{consts.READ_RECORD_COUNT}")
            read_cnt = int(trlr_cnt) if trlr_cnt else 0
            count = ti.xcom_pull(task_ids=consts.KAFKA_WRITER_TASK, key=consts.RECORD_COUNT)
            write_cnt = int(count) if count else 0
            run_end = datetime.now(self.local_tz).strftime('%Y-%m-%d %H:%M:%S.%f')
            file_name = context['dag_run'].conf.get('name')
            dag_id = context['dag'].dag_id
            log_sql = f""" INSERT INTO `{table_id}`
                            (FILE_UUID, DAG_ID, JOB_TYPE, JOB_NAME, RUN_START, RUN_END, SOURCE_LOCATION, TARGET_LOCATION, TRLR_CNT, WRITE_CNT, STATUS, FILE_NAME, REC_LOAD_TIMESTAMP)
                            VALUES ('{batch_run_uuid}', '{dag_id}', 'ETL', '{job_name}', '{run_start}', '{run_end}', '{source_location}', '{target_location}', {read_cnt}, {write_cnt}, '{status}', '{file_name}', CURRENT_TIMESTAMP());
                           """
            logger.info(f"Query: {log_sql}")
            run_bq_query(log_sql)
        except Exception as e:
            logger.error(f"Error inserting audit log: {e}")

    def success_audit_log_entry(self, dag_config: dict):
        return PythonOperator(
            task_id='success_audit_log_entry',
            trigger_rule='all_success',
            python_callable=self.insert_audit_log_entry,
            op_kwargs={'status': "Success"}
        )

    def fail_audit_log_entry(self, dag_config: dict):
        return PythonOperator(
            task_id='fail_audit_log_entry',
            trigger_rule='one_failed',
            python_callable=self.insert_audit_log_entry,
            op_kwargs={'status': "fail"}
        )

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
            job_type = dag_config.get(consts.JOB_TYPE)
            start = self.start_task(job_type)
            end = EmptyOperator(task_id='End', trigger_rule='none_failed')

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
            kafka_stg_query_task = self.build_preprocessing_kafka_task(dag_config)
            kafka_trigger = self.kafka_trigger_task(dag_config)
            check_curated_view_task = self.check_curated_view(dag_config)
            curated_view_dag_trigger_task = self.curated_view_dag_trigger(dag_config)
            success_audit_task = self.success_audit_log_entry(dag_config)
            fail_audit_task = self.fail_audit_log_entry(dag_config)

            leading_task >> check_curated_view_task >> curated_view_dag_trigger_task >> kafka_stg_query_task >> kafka_trigger >> control_table_task >> [success_audit_task, fail_audit_task] >> end
            check_curated_view_task >> kafka_stg_query_task

        return add_tags(dag)
