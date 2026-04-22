import logging
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.utils.task_group import TaskGroup
import util.constants as consts
from bigquery_loader.bigquery_batchloader import (
    BigQueryBatchLoader,
    LoadingFrequency,
    get_chunked_range,
    get_chunked_dates,
    parse_merge_config,
    get_filter_rows
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    get_cluster_config_by_job_size
)
import copy

logger = logging.getLogger(__name__)


class BigQueryCustomQueryBatchLoader(BigQueryBatchLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)

    def build_transformation_config(self, bq_client, context):
        config = context.get(consts.CONFIG)
        bq_config = config[consts.BIGQUERY]
        parquet_files_path = f"{self.get_staging_folder(config)}/*.parquet"
        bq_project_name = bq_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_proc_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_table_name = bq_config[consts.TABLE_NAME]

        bq_ext_table_id = f"{bq_proc_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}"

        partition_field = bq_config.get('partition_field')
        clustering_fields = bq_config.get('clustering_fields')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        additional_columns = bq_config.get(consts.ADD_COLUMNS) or []
        add_file_name_config = bq_config.get(consts.ADD_FILE_NAME_COLUMN)
        if add_file_name_config:
            additional_columns.append(f"'{parquet_files_path}' AS {add_file_name_config.get(consts.ALIAS)}")

        exclude_columns = bq_config.get(consts.DROP_COLUMNS) or []

        join_spec = bq_config.get(consts.JOIN) or []
        merge_spec = parse_merge_config(config.get(consts.MERGE))

        filter_rows = get_filter_rows(bq_config, context.get('dag').dag_id)
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
            consts.JOIN_SPECIFICATION: join_spec,
            consts.MERGE_SPECIFICATION: merge_spec,
            consts.FILTER_ROWS: filter_rows
        }

    def get_staging_folder(self, config: dict):
        gcs_config = config.get(consts.GCS)
        if not gcs_config:
            gcs_config = {
                consts.BUCKET: f"pcb-{self.deploy_env}-staging-extract",
                consts.STAGING_FOLDER: f"{config[consts.FETCH_SPARK_JOB].get(consts.DBNAME)}/custom-query-batch-loading"
            }
        return f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{config[consts.BIGQUERY].get(consts.TABLE_NAME)}"

    def build_fetching_job(self, cluster_name: str, config: dict):
        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]

        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")

        spark_config = copy.deepcopy(self.dbconfig[db_name][consts.FETCH_SPARK_JOB])
        spark_job_args = copy.deepcopy(config[consts.FETCH_SPARK_JOB])
        spark_config.update(spark_job_args)

        arg_list = [spark_config[consts.APPLICATION_CONFIG], self.get_staging_folder(config)]

        query = spark_config[consts.ARGS][consts.PCB_CUSTOM_QUERY]

        spark_config[consts.ARGS][consts.PCB_CUSTOM_QUERY] = query.format(start_date=f"'{spark_config[consts.ARGS][consts.PCB_LOWER_BOUND]}'", end_date=f"'{spark_config[consts.ARGS][consts.PCB_UPPER_BOUND]}'")

        if consts.ARGS in spark_config:
            for k, v in spark_config[consts.ARGS].items():
                arg_list.append(f'{k}={v}')

        if not any(consts.DB_READ_PARALLELISM in arg for arg in arg_list):
            arg_list.append(consts.DB_READ_PARALLELISM + '=1')

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

    def create_loading_tasks(self, config: dict, cluster_name: str, job_size: str, dag_id: str, next_task):
        is_sqlserver = config.get(consts.IS_SQLSERVER, None)
        if config['batch'].get('chunk_size_in_row_count', None):
            dtpairs = get_chunked_range(config['batch'])
        else:
            dtpairs = get_chunked_dates(config['batch'], dag_id)
        num_chunks = len(dtpairs)
        tgroups = []
        for chunk_idx in range(num_chunks):
            dtpair = dtpairs[chunk_idx]
            fromdt, todt = dtpair[0], dtpair[1]
            if config['batch'].get('chunk_size_in_row_count', None):
                lowerbound = str(fromdt)
                upperbound = str(todt)
            else:
                lowerbound = str(fromdt) + " 00:00:00"
                upperbound = str(todt) + " 00:00:00"

            tgrp_id = f'c{chunk_idx}_{fromdt}_{todt}'

            with TaskGroup(group_id=tgrp_id) as tgrp:
                cluster_creating_task = DataprocCreateClusterOperator(
                    task_id=consts.CLUSTER_CREATING_TASK_ID,
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    cluster_config=get_cluster_config_by_job_size(self.deploy_env,
                                                                  self.gcp_config.get(consts.NETWORK_TAG), job_size, is_sqlserver),
                    region=self.dataproc_config.get(consts.LOCATION),
                    cluster_name=cluster_name
                )

                config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_LOWER_BOUND] = lowerbound
                config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_UPPER_BOUND] = upperbound

                config[consts.DAG_ID] = dag_id

                fetching_task = DataprocSubmitJobOperator(
                    task_id="fetch_db",
                    job=self.build_fetching_job(cluster_name, config),
                    region=self.dataproc_config.get(consts.LOCATION),
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
                )
                loading_task = PythonOperator(
                    task_id="load_bq",
                    python_callable=self.build_loading_job,
                    op_kwargs={'tgrp_id': tgrp_id, consts.CONFIG: config}
                )

                cluster_creating_task >> fetching_task >> loading_task
                if chunk_idx == 0:
                    next_task >> cluster_creating_task

                tgroups.append(tgrp)

        for idx, tgrp in enumerate(tgroups[:-1]):
            tgrp >> tgroups[idx + 1]

        return tgroups

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags


globals().update(BigQueryCustomQueryBatchLoader(config_filename='bigquery_custom_query_batchloader_config.yaml', loading_frequency=LoadingFrequency.Onetime).create_dags())
