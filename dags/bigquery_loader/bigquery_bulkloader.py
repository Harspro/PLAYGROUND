import logging
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import List

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from bigquery_loader.bigquery_batchloader import BigQueryBatchLoader
from bigquery_loader.bigquery_batchloader import get_chunked_dates
from bigquery_loader.bigquery_loader_base import LoadingFrequency
from bigquery_loader.bulk_loader_config import RDB2BQBulkLoaderConfig, RDB2BQTableConfig, RDB2BQGCSConfig, \
    RDB2BQChunkConfig, RDB2BQSparkConfig
from util.bq_utils import (drop_bq_table)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    get_ephemeral_cluster_config
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

BIGQUERY_BULK_LOADER: Final = 'bigquery_bulk_loader'


class BigQueryBulkLoader(BigQueryBatchLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)

    def build_spark_fetch_job_config(self, task_grp_id: str, bulk_config: RDB2BQBulkLoaderConfig,
                                     spark_config: RDB2BQSparkConfig, gcs_config: RDB2BQGCSConfig,
                                     table_config: RDB2BQTableConfig,
                                     start_dt=None,
                                     end_dt=None) -> dict:

        # spark job arg list
        staging_folder = self._get_staging_full_path(gcs_config, table_config, task_grp_id)
        arg_list: List[str] = [spark_config.application_config, staging_folder]
        arg_list.append(f'{consts.SCHEMA_QUALIFIED_TABLE_NAME}={table_config.ods_table_name}')

        # set parallelism
        parallelism = table_config.read_parallelism if table_config.read_parallelism else 1
        arg_list.append(consts.DB_READ_PARALLELISM + f'={parallelism}')

        if table_config.primary_key:
            arg_list.append(f'pcb.bigquery.loader.source.primary.key.column={table_config.primary_key}')

        if table_config.fetch_size:
            arg_list.append(f'pcb.bigquery.loader.source.fetchsize={table_config.fetch_size}')

        part_col = None
        if table_config.chunk_config:
            part_col = table_config.chunk_config.partition_col

        if part_col:
            arg_list.append(f'pcb.bigquery.loader.source.partition.column={part_col}')

        if part_col and start_dt and end_dt:
            lower_bound = str(start_dt) + " 00:00:00"
            upper_bound = str(end_dt) + " 00:00:00"
            arg_list.append(f"{consts.PCB_LOWER_BOUND}={lower_bound}")
            arg_list.append(f"{consts.PCB_UPPER_BOUND}={upper_bound}")
            arg_list.append(f"{consts.PCB_WHERE_CONDITION}={part_col} >= '{start_dt}' AND {part_col} < '{end_dt}'")

        spark_config.properties = {**consts.DEFAULT_SPARK_SETTINGS,
                                   **spark_config.properties} if spark_config.properties else consts.DEFAULT_SPARK_SETTINGS

        default_args = {**self.default_args,
                        **bulk_config.dag_config.default_args} if bulk_config.dag_config.default_args else self.default_args
        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=default_args, arg_list=arg_list)

        spark_job_dict = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: bulk_config.ephemeral_cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config.jar_file_uris,
                consts.MAIN_CLASS: spark_config.main_class,
                consts.FILE_URIS: spark_config.file_uris,
                consts.ARGS: arg_list,
                consts.PROPERTIES: spark_config.properties
            }
        }
        logging.info(f"spark job config:{spark_job_dict}")
        return spark_job_dict

    def _get_staging_full_path(self, gcs_config: RDB2BQGCSConfig, table_config: RDB2BQTableConfig,
                               task_grp_id: str) -> str:
        staging_path = self._get_staging_path(gcs_config, table_config, task_grp_id)
        return f"gs://{gcs_config.bucket}/{staging_path}"

    def _get_staging_path(self, gcs_config: RDB2BQGCSConfig, table_config: RDB2BQTableConfig, task_grp_id: str) -> str:
        target_dataset = table_config.bq_load_config.dataset_id
        target_table = table_config.bq_load_config.bq_table_name
        return f"{gcs_config.staging_folder}/{target_dataset}/{target_table}/{task_grp_id}"

    def build_transformation_config(self, bq_client, context):
        task_grp_id = context.get('tgrp_id')
        bulk_config: RDB2BQBulkLoaderConfig = context.get(consts.CONFIG)
        table_config: RDB2BQTableConfig = context.get("table_config")

        gcs_config = bulk_config.gcs
        bq_config = table_config.bq_load_config

        staging_path = self._get_staging_full_path(gcs_config, table_config, task_grp_id)
        parquet_files_path = f"{staging_path}/*.parquet"

        bq_project_name = bq_config.project_id or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_proc_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_table_name = bq_config.bq_table_name

        bq_ext_table_id = f"{bq_proc_project_name}.{bq_config.dataset_id}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_config.dataset_id}.{bq_table_name}"

        partition_field = bq_config.bq_table_partition_field
        clustering_fields = bq_config.bq_table_cluster_fields
        logging.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        logging.info(f'partition: {partition_str}, cluster: {clustering_str}')

        additional_columns = bq_config.add_columns or []
        add_file_name_config = bq_config.add_file_name_col
        if add_file_name_config:
            additional_columns.append(f"'{parquet_files_path}' AS {add_file_name_config}")

        exclude_columns = bq_config.drop_columns

        # join_spec and merge_spec is not supported now
        join_spec = None
        merge_spec = None
        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.PARTITION: partition_str,
                consts.CLUSTERING: clustering_str,
                consts.ID: bq_table_id,

            },
            consts.JOIN_SPECIFICATION: join_spec,
            consts.MERGE_SPECIFICATION: merge_spec
        }

    def build_loading_job(self, **context):
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        transformed_view = self.apply_transformations(bq_client, transformation_config)

        table_config: RDB2BQTableConfig = context.get("table_config")
        bq_table_name_full = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        if table_config.drop_if_exists:
            logging.info(f"Dropping BQ tabl:{bq_table_name_full}")
            drop_bq_table(bq_table_name_full)

        loading_sql = f"""
            CREATE TABLE IF NOT EXISTS
                `{bq_table_name_full}`
            {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
            {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
            AS
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)}
                LIMIT 0;

            INSERT INTO `{bq_table_name_full}`
            SELECT {transformed_view.get(consts.COLUMNS)}
            FROM {transformed_view.get(consts.ID)};
        """

        logging.info(f"BQ loading SQL:{loading_sql}")
        query_job = bq_client.query(loading_sql)
        query_job.result()

    def get_chunked_dates(self, chunk_config: RDB2BQChunkConfig, dag_id: str):
        config_dict = {'chunk_size_in_months': chunk_config.chunk_size_in_months,
                       'start_date': chunk_config.start_date,
                       'end_date': chunk_config.end_date}
        return get_chunked_dates(config_dict, dag_id)

    def create_loading_tasks(self, bulk_config: RDB2BQBulkLoaderConfig, table_config: RDB2BQTableConfig, dag_id: str,
                             upstream_task):
        if table_config.chunk_config:
            return self.create_chunked_loading_tasks(bulk_config, table_config, dag_id, upstream_task)
        else:
            return self.create_single_loading_task(bulk_config, table_config, dag_id, upstream_task)

    def _cleanup_stg_folder_task(self, bulk_config: RDB2BQBulkLoaderConfig, table_config: RDB2BQTableConfig,
                                 task_grp_id: str):
        staging_folder = self._get_staging_path(bulk_config.gcs, table_config, task_grp_id)
        cleanup_task = GCSDeleteObjectsOperator(task_id=f"cleanup_stg_folder_{task_grp_id}",
                                                bucket_name=bulk_config.gcs.bucket, prefix=f"{staging_folder}/")
        return cleanup_task

    def create_single_loading_task(self, bulk_config: RDB2BQBulkLoaderConfig, table_config: RDB2BQTableConfig,
                                   dag_id: str, upstream_task):
        task_groups = []
        chunk_idx = 0
        target_table_name_under = table_config.bq_load_config.bq_table_name.replace('.', '_')
        task_grp_id = f'{target_table_name_under}_{chunk_idx}'
        with TaskGroup(group_id=task_grp_id) as task_grp:
            cleanup_task = self._cleanup_stg_folder_task(bulk_config, table_config, task_grp_id)
            fetching_task = DataprocSubmitJobOperator(
                task_id=f"fetch_db_{target_table_name_under}",
                job=self.build_spark_fetch_job_config(task_grp_id, bulk_config, bulk_config.spark_config,
                                                      bulk_config.gcs,
                                                      table_config),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
            )

            loading_task = PythonOperator(
                task_id=f"load_bq_{target_table_name_under}",
                python_callable=self.build_loading_job,
                op_kwargs={'tgrp_id': task_grp_id, consts.CONFIG: bulk_config, "table_config": table_config}
            )

            if upstream_task:
                upstream_task >> cleanup_task >> fetching_task >> loading_task
            else:
                cleanup_task >> fetching_task >> loading_task

            task_groups.append(task_grp)

        return task_groups

    def create_chunked_loading_tasks(self, bulk_config: RDB2BQBulkLoaderConfig, table_config: RDB2BQTableConfig,
                                     dag_id: str, upstream_task):
        chunk_config = table_config.chunk_config
        dt_pairs = self.get_chunked_dates(chunk_config, dag_id)
        num_chunks = len(dt_pairs)

        task_groups = []
        for chunk_idx in range(num_chunks):
            dt_pair = dt_pairs[chunk_idx]
            from_dt, to_dt = dt_pair[0], dt_pair[1]
            task_grp_id = f'c{chunk_idx}_{from_dt}_{to_dt}'

            with TaskGroup(group_id=task_grp_id) as task_grp:
                source_table_name_under = table_config.ods_table_name.replace('.', '_')
                fetching_task = DataprocSubmitJobOperator(
                    task_id=f"fetch_db_{source_table_name_under}",
                    job=self.build_spark_fetch_job_config(task_grp_id, bulk_config, bulk_config.spark_config,
                                                          bulk_config.gcs,
                                                          table_config, from_dt, to_dt),
                    region=self.dataproc_config.get(consts.LOCATION),
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
                )

                loading_task = PythonOperator(
                    task_id=f"load_bq_{source_table_name_under}",
                    python_callable=self.build_loading_job,
                    op_kwargs={'tgrp_id': task_grp_id, consts.CONFIG: bulk_config, "table_config": table_config}
                )

                fetching_task >> loading_task
                if chunk_idx == 0:
                    upstream_task >> fetching_task

                task_groups.append(task_grp)

        for idx, task_grp in enumerate(task_groups[:-1]):
            task_grp >> task_groups[idx + 1]

        return task_groups

    def create_dag(self, dag_id: str, bulk_config: RDB2BQBulkLoaderConfig) -> DAG:
        logging.info(f"Creating dag:{dag_id}")
        tags = []
        if bulk_config.dag_config.tags is not None:
            cust_tags = bulk_config.dag_config.tags.split(",")
            for t in cust_tags:
                tags.append(t)

        dag = DAG(
            dag_id=dag_id,
            is_paused_upon_creation=True,
            default_args={**self.default_args,
                          **bulk_config.dag_config.default_args} if bulk_config.dag_config.default_args else self.default_args,
            start_date=datetime(2022, 1, 1, tzinfo=self.local_tz),
            end_date=None,
            schedule=None,
            tags=tags,
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            max_active_tasks=bulk_config.dag_config.concurrency if bulk_config.dag_config.concurrency else None,
        )

        with dag:

            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_ephemeral_cluster_config(self.deploy_env,
                                                            self.gcp_config.get(consts.NETWORK_TAG),
                                                            bulk_config.cluster_workers),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=bulk_config.ephemeral_cluster_name
            )

            start_point >> cluster_creating_task

            table_configs = bulk_config.table_configs
            upstream_task = cluster_creating_task
            for table_config in table_configs:
                loading_task_groups = self.create_loading_tasks(bulk_config, table_config, dag_id, upstream_task)
                loading_task_groups[-1] >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config_dict in self.job_config.items():
            logging.info(f"===========bulk loader:{job_id}================")
            bulk_config = RDB2BQBulkLoaderConfig.from_dict(config_dict)
            dags[job_id] = self.create_dag(job_id, bulk_config)

        return dags
