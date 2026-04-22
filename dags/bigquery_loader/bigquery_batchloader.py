import logging
from copy import deepcopy
from datetime import timedelta, datetime
from typing import Final, List

import pendulum
from airflow import DAG
from airflow import settings
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from bigquery_loader.bigquery_loader_base import BigQueryLoader, LoadingFrequency, INITIAL_DEFAULT_ARGS
from util.bq_utils import drop_bq_table
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_file_env,
    read_yamlfile_env,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag,
    read_variable_or_file,
    split_table_name
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

BIGQUERY_BATCH_LOADER: Final = 'bigquery_batch_loader'


def get_chunked_dates(batchcfg, var_key: str):
    start_date, end_date = batchcfg['start_date'], batchcfg['end_date']
    chunk_size = batchcfg['chunk_size_in_months']
    if chunk_size < 1 or chunk_size > 12:
        raise Exception("Chunk size in months is out of range 1-12")

    # Allow for override
    var_dict = Variable.get(var_key, deserialize_json=True, default_var={})
    if var_dict:
        logger.info('Read from variable')
        start_date_var = var_dict.get('start_date', {})
        end_date_var = var_dict.get('end_date', {})
        chunk_size_var = var_dict.get('chunk_size_in_months', {})

        if start_date_var:
            start_date = start_date_var
        if end_date_var:
            end_date = end_date_var
        if chunk_size_var:
            chunk_size = chunk_size_var
        logger.info(f'start_date: {start_date}, end_date: {end_date}, chunk_size: {chunk_size}')

    freqval = f"{chunk_size}MS"
    import pandas as pd
    start_date = pd.Timestamp(start_date).date()
    end_date = pd.Timestamp(end_date).date()

    dtrange = pd.date_range(start_date, end_date, freq=freqval)
    dtlist = [d.date() for d in dtrange]
    if len(dtlist) == 0 or dtlist[0] != start_date:
        dtlist.insert(0, start_date)
    if dtlist[-1] != end_date:
        dtlist.append(end_date)

    dtpairs = []
    for i, d in enumerate(dtlist[:-1]):
        dtpairs.append((d, dtlist[i + 1]))

    return dtpairs


def get_chunked_range(batchcfg, var_key: str):
    lower_bound, upper_bound = int(batchcfg['lower_bound']), int(batchcfg['upper_bound'])
    chunk_size = batchcfg['chunk_size_in_row_count']
    if chunk_size < 0:
        raise Exception("Chunk size range should not be negative")

    logger.info(f'start_range: {lower_bound}, end_range: {upper_bound}, chunk_size: {chunk_size}')

    number_range = list(range(lower_bound, upper_bound, chunk_size))
    if number_range[-1] < upper_bound:
        number_range[-1] = upper_bound
    if lower_bound >= upper_bound:
        raise AirflowFailException("lower_bound is greater than upper_bound")

    dtpairs = []
    for i, d in enumerate(number_range[:-1]):
        dtpairs.append((d, number_range[i + 1]))

    return dtpairs


def get_filter_rows(bigquerycfg, dag_id: str) -> List[str]:
    filter_rows = []
    if consts.FILTER_ROWS in bigquerycfg:
        filter_rows = bigquerycfg[consts.FILTER_ROWS]

    # Allow for override
    var_dict = Variable.get(dag_id, deserialize_json=True, default_var=None)
    if var_dict:
        logger.info('Read from variable')
        filter_rows_list = var_dict.get(consts.FILTER_ROWS, [])
        if filter_rows_list:
            filter_rows = filter_rows_list

    logger.info(f'filter_rows: {filter_rows}')
    return filter_rows


def delete_bq_table(prior_task, dag_id, bq_config):
    var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
    delete_table_task = None
    if var_dict or bq_config.get(consts.WRITE_DISPOSITION):
        delete_table = var_dict.get('overwrite_bq_table')
        if delete_table or (bq_config.get(consts.WRITE_DISPOSITION)
                            and bq_config.get(consts.WRITE_DISPOSITION).upper() == consts.WRITE_TRUNCATE):
            logger.info('Creating bq delete table task')

            bq_project_name = bq_config.get(consts.PROJECT_ID)
            bq_dataset_id = bq_config.get(consts.DATASET_ID)
            bq_table_name = bq_config[consts.TABLE_NAME]
            bq_table_id = f"{bq_project_name}.{bq_dataset_id}.{bq_table_name}"
            logger.info(f'bq_table_id: {bq_table_id}')
            delete_table_task = PythonOperator(
                task_id="delete_bq_table",
                python_callable=drop_bq_table,
                op_args=[bq_table_id]
            )
            prior_task >> delete_table_task

    if delete_table_task:
        return delete_table_task
    else:
        return prior_task


def parse_merge_config(merge_config):
    if not merge_config:
        return

    merge_join_columns = merge_config.get(consts.JOIN_COLUMNS)
    logger.info(f'merge_join_columns = {merge_join_columns}')
    merge_join_columns_str = ""
    if merge_join_columns:
        merge_join_conditions = [f'T.{x}=S.{x}' for x in merge_join_columns]
        merge_join_columns_str = " AND ".join(merge_join_conditions)
    logger.info(f'merge_join_columns_str = {merge_join_columns_str}')

    update_merge_statement = ""
    update_merge_config = merge_config.get(consts.MERGE_MATCHED)
    if update_merge_config:
        update_merge_statement = (
            f"WHEN MATCHED THEN "
            f"UPDATE SET T.{update_merge_config.get(consts.MATCHED_TARGET_COLUMN)} "
            f"= S.{update_merge_config.get(consts.MATCHED_SOURCE_COLUMN)}"
        )

    return {
        consts.JOIN_CLAUSE: merge_join_columns_str,
        consts.UPDATE_STATEMENT: update_merge_statement
    }


class BigQueryBatchLoader(BigQueryLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

    def build_fetching_job(self, tgrp_id: str, cluster_name: str, config: dict):
        gcs_config = config.get(consts.GCS)
        if not gcs_config:
            gcs_config = {
                consts.BUCKET: f"pcb-{self.deploy_env}-staging-extract",
                consts.STAGING_FOLDER: f"{config[consts.FETCH_SPARK_JOB].get(consts.DBNAME)}/onetime"
            }
            config[consts.GCS] = gcs_config

        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]
        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")
        spark_config = self.dbconfig[db_name][consts.FETCH_SPARK_JOB].copy()

        spark_config.update(config[consts.FETCH_SPARK_JOB])

        source_schema, source_tablename = split_table_name(spark_config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME))

        staging_folder = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{source_schema}/{source_tablename}/{tgrp_id}"

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
        tgrp_id = context.get('tgrp_id')
        config = context.get(consts.CONFIG)
        gcs_config = config.get(consts.GCS).copy()
        bq_config = config[consts.BIGQUERY]
        spark_config = config[consts.FETCH_SPARK_JOB]

        source_schema, source_tablename = split_table_name(spark_config[consts.ARGS].get(consts.SCHEMA_QUALIFIED_TABLE_NAME))
        parquet_files_path = f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}/{source_schema}/{source_tablename}/{tgrp_id}/*.parquet"

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
                consts.ID: bq_table_id,
                consts.WRITE_DISPOSITION: bq_config.get(consts.WRITE_DISPOSITION)
            },
            consts.JOIN_SPECIFICATION: join_spec,
            consts.MERGE_SPECIFICATION: merge_spec,
            consts.FILTER_ROWS: filter_rows
        }

    def build_loading_job(self, **context):
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        transformed_view = self.apply_transformations(bq_client, transformation_config)
        destination_table_config = transformation_config.get(consts.DESTINATION_TABLE)
        loading_sql = \
            f"""CREATE TABLE IF NOT EXISTS `{destination_table_config.get(consts.ID)}`
                {destination_table_config.get(consts.PARTITION)}
                {destination_table_config.get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)}
                    LIMIT 0;
                INSERT INTO `{destination_table_config.get(consts.ID)}`
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)};
            """

        logger.info(loading_sql)
        query_job = bq_client.query(loading_sql)
        query_job.result()

    def build_merging_job(self, **context):
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        merge_spec = transformation_config.get(consts.MERGE_SPECIFICATION)

        if not merge_spec:
            raise Exception("Illegal state: merge_spec is not available.")

        transformed_view = self.apply_transformations(bq_client, transformation_config)

        merge_condition = merge_spec.get(consts.JOIN_CLAUSE)
        update_statement = merge_spec.get(consts.UPDATE_STATEMENT)

        merging_sql = f"""
            MERGE `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}` T
            USING `{transformed_view.get(consts.ID)}` S
            ON {merge_condition}
            WHEN NOT MATCHED THEN
                INSERT ({transformed_view.get(consts.COLUMNS)})
                VALUES ({transformed_view.get(consts.COLUMNS)})
            {update_statement}
        """

        logger.info(merging_sql)
        bq_client = bigquery.Client()
        query_job = bq_client.query(merging_sql)
        query_job.result()

    def enrich_loading_config(self, config: dict, dag_id, lowerbound, upperbound, fromdt, todt):
        loading_config = deepcopy(config)
        pcol = config[consts.FETCH_SPARK_JOB][consts.ARGS].get(consts.PCB_PARTITION_COLUMN)
        where_condition = config[consts.FETCH_SPARK_JOB][consts.ARGS].get(consts.PCB_WHERE_CONDITION)

        if where_condition:
            where_condition += f" AND ({pcol} >= '{fromdt}' AND {pcol} < '{todt}')"
        else:
            where_condition = f" {pcol} >= '{fromdt}' AND {pcol} < '{todt}'"

        loading_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_WHERE_CONDITION] = where_condition

        column_list = config[consts.FETCH_SPARK_JOB][consts.ARGS].get(consts.PCB_SELECT_COlUMNS)

        if not column_list:
            column_list_file_location = config[consts.FETCH_SPARK_JOB][consts.ARGS].get(consts.PCB_SELECT_COlUMNS_FILE_LOCATION)
            if column_list_file_location:
                column_list_file_location = f'{settings.DAGS_FOLDER}/{column_list_file_location}'
                logging.info(f"column_list_file_location is: {column_list_file_location}")
                column_list = read_file_env(column_list_file_location)

        if column_list:
            loading_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_SELECT_COlUMNS] = column_list

        loading_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_LOWER_BOUND] = lowerbound
        loading_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_UPPER_BOUND] = upperbound

        loading_config[consts.DAG_ID] = dag_id

        return loading_config

    def create_loading_tasks(self, config: dict, cluster_name: str, job_size: str, dag_id: str, next_task):
        is_sqlserver = config.get(consts.IS_SQLSERVER, None)
        if config['batch'].get('chunk_size_in_row_count', None):
            dtpairs = get_chunked_range(config['batch'], dag_id)
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

            with (TaskGroup(group_id=tgrp_id) as tgrp):
                cluster_creating_task = DataprocCreateClusterOperator(
                    task_id=consts.CLUSTER_CREATING_TASK_ID,
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    cluster_config=get_cluster_config_by_job_size(self.deploy_env,
                                                                  self.gcp_config.get(consts.NETWORK_TAG), job_size, is_sqlserver),
                    region=self.dataproc_config.get(consts.LOCATION),
                    cluster_name=cluster_name
                )

                loading_config = self.enrich_loading_config(config=config, dag_id=dag_id,
                                                            lowerbound=lowerbound, upperbound=upperbound,
                                                            fromdt=fromdt, todt=todt)

                fetching_task = DataprocSubmitJobOperator(
                    task_id="fetch_db",
                    job=self.build_fetching_job(tgrp_id, cluster_name, loading_config),
                    region=self.dataproc_config.get(consts.LOCATION),
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
                )

                loading_task = PythonOperator(
                    task_id="load_bq",
                    python_callable=self.build_loading_job,
                    op_kwargs={'tgrp_id': tgrp_id, consts.CONFIG: loading_config}
                )

                cluster_creating_task >> fetching_task >> loading_task
                if chunk_idx == 0:
                    next_task >> cluster_creating_task

                tgroups.append(tgrp)

        for idx, tgrp in enumerate(tgroups[:-1]):
            tgrp >> tgroups[idx + 1]

        return tgroups

    def enrich_merging_config(self, config: dict, dag_id, lowerbound, upperbound, fromdt, todt):
        merging_config = deepcopy(config)
        pcol = config[consts.FETCH_SPARK_JOB]['args']['pcb.bigquery.loader.source.partition.column']

        merging_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_LOWER_BOUND] = lowerbound
        merging_config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_UPPER_BOUND] = upperbound
        merging_config[consts.FETCH_SPARK_JOB][consts.ARGS][
            consts.PCB_WHERE_CONDITION] = f"{pcol} >= '{fromdt}' AND {pcol} < '{todt}'"
        merging_config[consts.DAG_ID] = dag_id

        return merging_config

    def create_merging_tasks(self, config: dict, cluster_name: str, job_size: str, dag_id: str, next_task):
        # do not use the same variable key for merge
        is_sqlserver = config.get(consts.IS_SQLSERVER, None)
        dtpairs = get_chunked_dates(config[consts.MERGE], dag_id + '-merge')
        num_chunks = len(dtpairs)

        tgroups = []
        for chunk_idx in range(num_chunks):
            dtpair = dtpairs[chunk_idx]
            fromdt, todt = dtpair[0], dtpair[1]
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

                merging_config = self.enrich_merging_config(config=config, dag_id=dag_id,
                                                            lowerbound=lowerbound, upperbound=upperbound,
                                                            fromdt=fromdt, todt=todt)
                fetching_task = DataprocSubmitJobOperator(
                    task_id="fetch_db",
                    job=self.build_fetching_job(tgrp_id, cluster_name, merging_config),
                    region=self.dataproc_config.get(consts.LOCATION),
                    project_id=self.dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
                )

                merging_task = PythonOperator(
                    task_id="merge_into_bq",
                    python_callable=self.build_merging_job,
                    op_kwargs={'tgrp_id': tgrp_id, consts.CONFIG: merging_config}
                )

                cluster_creating_task >> fetching_task >> merging_task
                if chunk_idx == 0:
                    next_task >> cluster_creating_task

                tgroups.append(tgrp)

        for idx, tgrp in enumerate(tgroups[:-1]):
            tgrp >> tgroups[idx + 1]

        return tgroups

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule_interval(config),
            start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
            max_active_runs=1,
            is_paused_upon_creation=True,
            catchup=False,
            dagrun_timeout=timedelta(hours=72)
        )

        with dag:
            if config.get('batch') and config['batch'].get('read_pause_deploy_config'):
                is_paused = read_pause_unpause_setting(BIGQUERY_BATCH_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            start_point = EmptyOperator(task_id='start')
            delete_table_task = delete_bq_table(start_point, dag_id, config['bigquery'])
            end_point = EmptyOperator(task_id='end')

            if not config['batch'].get('skip'):
                loading_task_groups = self.create_loading_tasks(config, cluster_name, job_size, dag_id,
                                                                delete_table_task)
            else:
                loading_task_groups = None

            if consts.MERGE in config:
                leading_task = delete_table_task if not loading_task_groups else loading_task_groups[-1]
                merging_task_groups = self.create_merging_tasks(config, cluster_name, job_size, dag_id, leading_task)
                merging_task_groups[-1] >> end_point
            else:
                loading_task_groups[-1] >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
