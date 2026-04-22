from airflow import DAG, settings
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from google.cloud import bigquery
from typing import Final
from util.gcs_utils import list_blobs_with_prefix, delete_folder
from util.logging_utils import build_spark_logging_info
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env, get_cluster_config_by_job_size,
)
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation
)
import logging
import pendulum
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags


TSYS_TRIAD_FILE_LOADER: Final = 'tsys_triad_file_loader'

logger = logging.getLogger(__name__)


class TsysTriadFileBigqueryLoader:
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/config/tsys_triad_file_loader_config.yaml"
        self.job_config = read_yamlfile_env(f"{self.dag_config_fname}", self.deploy_env)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.default_args = {
            "owner": "team-money-movement-eng",
            "depends_on_past": False,
            "wait_for_downstream": False,
            "retries": 3,
            "retry_delay": timedelta(seconds=10),
            'capability': 'Payments',
            'severity': 'P3',
            'sub_capability': 'EMT',
            'business_impact': 'Daily orchestration process of TSYS Triad file not done',
            'customer_impact': 'None'
        }

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def build_cluster_creating_task(self, dag_id: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG), self.job_config.get(dag_id).get(consts.DATAPROC_JOB_SIZE)),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=self.job_config.get(dag_id).get(consts.CLUSTER_NAME)
        )

    def build_outcome_task_groups(self, dag_id, body_segments: list):
        task_groups = []

        for segment_name in body_segments:
            task_groups.append(self.build_segment_task_group(dag_id, segment_name))

        return task_groups

    def get_outcome_segment_list(self, dag_id):
        segment_configs = self.job_config.get(dag_id).get("processing_outcomes")
        return [segment_name for segment_name in segment_configs]

    def check_record_segment(self, bigquery_config: dict, segment_name: str):
        source_bucket = bigquery_config.get(consts.PROCESSING_BUCKET_EXTRACT)
        prefix = f'{bigquery_config.get(consts.FOLDER_NAME)}/{segment_name}'
        filename = list_blobs_with_prefix(source_bucket, prefix, delimiter=None)
        if not filename:
            return f'{segment_name}.absent_in_file'
        else:
            return f'{segment_name}.load_into_bq'

    def delete_triad_parquet_folder(self, bigquery_config, obj_prefix):
        return delete_folder(bigquery_config.get(consts.PROCESSING_BUCKET_EXTRACT), obj_prefix)

    def trigger_dag(self, trigger_dag_id: str):
        return TriggerDagRunOperator(
            task_id=f"trigger__{trigger_dag_id}",
            trigger_dag_id=trigger_dag_id,
            wait_for_completion=False,
            poke_interval=30
        )

    def build_segment_task_group(self, dag_id, segment_name: str):
        segment_configs = self.job_config.get(dag_id).get("processing_outcomes")
        segment_config = segment_configs.get(segment_name)
        bigquery_config = self.job_config.get(dag_id).get(consts.BIGQUERY)
        gcs_table_staging_dir = segment_config.get(consts.STAGING_FOLDER)
        project_id = bigquery_config.get(consts.PROJECT_ID)
        dataset_ID = bigquery_config.get(consts.DATASET_ID)
        table_name = segment_config.get(consts.TABLE_NAME)
        trigger_dag_id = segment_config.get('trigger_dag_id')

        with TaskGroup(group_id=segment_name) as segment_task_group:
            check_record_task = self.build_record_check(bigquery_config, segment_name)
            loading_task = self.build_segment_loading_task(table_name, project_id, dataset_ID, gcs_table_staging_dir, bigquery_config)
            end_segment = EmptyOperator(task_id='absent_in_file')
            if trigger_dag_id:
                logging.info(f'Creating trigger_dag_id task: {trigger_dag_id}')
                trigger_dag_id_task = self.trigger_dag(trigger_dag_id)
                check_record_task >> loading_task >> trigger_dag_id_task
            else:
                check_record_task >> loading_task

            check_record_task >> end_segment

        return segment_task_group

    def build_record_check(self, bigquery_config: dict, segment_name: str):
        return BranchPythonOperator(
            task_id="record_present_check",
            python_callable=self.check_record_segment,
            op_kwargs={"bigquery_config": bigquery_config, "segment_name": segment_name}
        )

    def load_parquet_files_to_bq(self, table_name, project_id, dataset_id, gcs_table_staging_dir, bigquery_config):
        logger.info(f"Starting import of data for table {table_name} from {gcs_table_staging_dir}")
        processing_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_ext_table_id = f"{processing_project_id}.{bigquery_config[consts.DATASET_ID]}.{table_name}_BFEXT"
        bq_landing_table_id = f"{project_id}.{dataset_id}.{table_name}"

        additional_columns = bigquery_config.get(consts.ADD_COLUMNS) or []

        transformation_config = {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: gcs_table_staging_dir,
            consts.ADD_COLUMNS: additional_columns,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_landing_table_id
            }
        }

        bq_client = bigquery.Client()
        staged_view = self.apply_transformations(bq_client, transformation_config)
        insertion_stmt = f"""
            INSERT INTO `{bq_landing_table_id}`
            SELECT {staged_view.get(consts.COLUMNS)}
            FROM `{staged_view.get(consts.ID)}`;
        """

        logger.info(insertion_stmt)
        bq_client.query(insertion_stmt).result()
        logger.info(f"Finished loading of data in table {table_name}")

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        logger.info(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))

        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view
        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)
        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def copy_from_landing_to_processing_task(self, dag_id: str):
        return GCSToGCSOperator(
            task_id="copy_from_landing_to_processing",
            gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
            source_bucket="{{ dag_run.conf['bucket'] }}",
            source_object="{{ dag_run.conf['name'] }}",
            destination_bucket=self.job_config.get(dag_id).get(consts.STAGING_FOLDER),
            destination_object="{{ dag_run.conf['name'] }}"
        )

    def build_segment_loading_task(self, table_name, project_id, dataset_id, gcs_table_staging_dir, bigquery_config):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.load_parquet_files_to_bq,
            op_kwargs={
                consts.TABLE_NAME: table_name,
                consts.PROJECT_ID: project_id,
                consts.DATASET_ID: dataset_id,
                "gcs_table_staging_dir": gcs_table_staging_dir,
                "bigquery_config": bigquery_config}
        )

    def delete_triad_staging_folder(self, bigquery_config):
        return PythonOperator(
            task_id="delete_triad_staging_folder",
            python_callable=self.delete_triad_parquet_folder,
            op_kwargs={"bigquery_config": bigquery_config, consts.OBJ_PREFIX: bigquery_config.get(consts.FOLDER_NAME)}
        )

    def dataproc_spark_submit_task(self, dag_id: str):

        arg_list = [f"pcb.triad.processor.data.file.name= gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{{{{dag_run.conf['name']}}}}",
                    f'pcb.triad.processor.tsys.triad.file.type={self.job_config.get(dag_id).get("triad_file_type")}']
        if consts.SEGMENT_ARGS in self.job_config[dag_id]:
            for k, v in self.job_config[dag_id][consts.SEGMENT_ARGS].items():
                arg_list.append(f'{k}={v}')

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)
        SPARK_JOB = {
            consts.REFERENCE: {consts.PROJECT_ID: self.gcp_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.CLUSTER_NAME)},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: self.job_config.get(dag_id).get(consts.JAR_FILE_URIS),
                consts.MAIN_CLASS: self.job_config.get(dag_id).get(consts.MAIN_CLASS),
                consts.PROPERTIES: self.job_config.get(dag_id).get(consts.PROPERTIES),
                consts.ARGS: arg_list
            }
        }
        return DataprocSubmitJobOperator(
            task_id="dataproc_spark_submit_task",
            job=SPARK_JOB,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            region=self.dataproc_config.get(consts.LOCATION),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            description="Daily orchestration process of TSYS Triad file",
            tags=self.job_config.get(dag_id).get(consts.TAGS),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
        )

        with dag:
            start = EmptyOperator(task_id=consts.START_TASK_ID)
            cluster_creating = self.build_cluster_creating_task(dag_id)
            copy_from_landing_to_processing = self.copy_from_landing_to_processing_task(dag_id)
            delete_triad_staging_dir = self.delete_triad_staging_folder(self.job_config.get(dag_id).get(consts.BIGQUERY))
            dataproc_submit_spark_job = self.dataproc_spark_submit_task(dag_id)
            outcome_segment_tasks = self.build_outcome_task_groups(dag_id, self.get_outcome_segment_list(dag_id))

            start >> cluster_creating >> copy_from_landing_to_processing \
                  >> delete_triad_staging_dir >> dataproc_submit_spark_job \
                  >> outcome_segment_tasks

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags


globals().update(TsysTriadFileBigqueryLoader().create_dags())
