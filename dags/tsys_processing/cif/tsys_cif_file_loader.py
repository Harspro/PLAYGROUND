import logging
from datetime import timedelta, datetime

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from tsys_processing.cif.constants import CIF_CARD_XREF_SQL_PATH, MULTI_CARD_UNION_SQL_PATH, \
    CIF_ONE_CURRENT_CARD_SQL_PATH, ACCOUNT_SEGMENT, CARD_SEGMENT, CIF_MERGE_TRANSFORM_SQL_PATH, \
    CIF_SEGMENT_PREV_SQL_PATH, PREV_SUFFIX
from tsys_processing.tsys_file_loader_base import TSYS_FILE_LOADER
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists,
    submit_transformation, get_table_columns)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import get_cluster_name_for_dag, read_file_env, get_file_date_parameter
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class TsysCIFFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.ephemeral_cluster_name = 'cif-file-processing-cluster'

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        return self.build_filtering_task(cluster_name, dag_config)

    def apply_transformations(self, bigquery_client, transformation_config: dict, segment_name: str = None):
        """
            Applies a series of transformations to CIF file data based on the provided configuration. It overrides the
            apply_transformations method in the base class.

            This function orchestrates the creation and transformation of tables/views in BigQuery by:
            - Creating an external table from CIF parquet files post spark processing.
            - Optionally applying column transformations such as addition and removal of columns.
            - Applying timestamp transformations to convert timestamps to a specified timezone.
            - Implementing union transformations to dedup and keep the latest CIF data
            - Synchronizing the schema of transformed data with a curated view if it exists.

            :param bigquery_client: The BigQuery client object used for executing transformations.
            :param transformation_config: Configuration dictionary containing specifications for transformations.
            :param segment_name: The name of the segment, which determines specific transformations to apply.
            :return: A dict representing the final transformed data view resulting from the applied transformations.
            :raises AirflowException: Raised if the curated spec is not provided in the transformation configuration.
        """
        if not (destination_table_spec := transformation_config.get(consts.DESTINATION_TABLE)):
            raise AirflowException("[ERROR] Destination specification was not automatically created! Check DAG Tasks!")

        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))

        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)
        destination_table_name = destination_table_spec.get(consts.ID)

        if not self.is_trailer(segment_name):
            # The cif_{segment}_curr data has moved to cif_{segment}_prev so we need to account for this in the merge
            destination_project_dataset = destination_table_name.rsplit(".", 1)[0]
            destination_table_name = f'{destination_project_dataset}.CIF_{segment_name}{PREV_SUFFIX}'

            if table_exists(bigquery_client, destination_table_name):
                rank_partition = transformation_config.get(consts.RANK_PARTITION)
                rank_order_by = transformation_config.get(consts.RANK_ORDER_BY)
                transformed_data = self.apply_merge_transformation(bigquery_client,
                                                                   transformed_data,
                                                                   destination_table_name,
                                                                   rank_partition,
                                                                   rank_order_by,
                                                                   segment_name)
                if CARD_SEGMENT in segment_name:
                    transformed_data = self.apply_cif_one_current_card(bigquery_client, transformed_data,
                                                                       rank_partition,
                                                                       rank_order_by)

        if table_exists(bigquery_client, destination_table_name):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data,
                                                                destination_table_name)

        return transformed_data

    # PCBDF-1271
    @staticmethod
    def apply_cif_one_current_card(bigquery_client, source_table: dict, rank_partition: str, rank_order_by: str,
                                   expiration_hours: object = 6):
        """
            Applies transformations to adjust the CIF CARD data to ensure only one current active card record exists.
            There are cases when an account may have multiple cards. This situation can arise when a user swaps to a
            new credit card for any reason (transferred, upgraded, closed previous). In legacy, this case is handled by
            TechOps intervention and not through code. However, we are trying to handle this in the pipeline.

            This function performs the following operations:\n
            - Creates a temporary table containing records with multiple active cards, modifying certain fields.
            - Constructs a view that selects the latest active card record for each partition, ensuring no duplicate
              active card records exist.

            :param bigquery_client:
                The BigQuery client object used to execute transformations.

            :param source_table:
                Dictionary containing the structure of the source table from which data is to be transformed.
                Must include:
                - `ID`: Identifier of the source table.
                - `COLUMNS`: Columns to be included during the transformation.

            :param rank_partition:
                Column criteria used for partitioning rows during ranking within the transformation.

            :param rank_order_by:
                Column criteria used for ordering rows during ranking within the transformation.

            :param expiration_hours:
                The number of hours before the created tables/views expire. Default is 6 hours.

            :returns:
                A dictionary representing the final view that stores only the latest active card for each partition.

            :raises AirflowException:
                If any required specifications are missing or issues occur during transformation execution.

            :notes:
                - Utilizes utility functions to execute SQL transformations and create temporary tables/views.
                - Logs relevant information such as source table ID, columns, partition criteria, and ordering criteria.
                - Assumes certain constants are defined within the `consts` module.
        """
        source_table_id = source_table.get(consts.ID)
        columns = source_table.get(consts.COLUMNS)
        processing_project_dataset_id = source_table_id.rsplit('.', 1)[0]
        multi_current_cards_temp_table_id = f"{processing_project_dataset_id}.CIF_CARD_MULTI_CURRENT_CARD_RECORDS"
        logger.info(f"""applying cif one current card transformation:
                        source_table_id: {source_table_id}
                        columns: {columns}
                        rank_partition: {rank_partition}
                        rank_order_by: {rank_order_by}""")

        multi_card_union_ddl_str = read_file_env(MULTI_CARD_UNION_SQL_PATH).format(
            multi_card_table_id=multi_current_cards_temp_table_id,
            expiration_hours=expiration_hours,
            source_table_id=source_table_id
        )

        logger.info("Running cif card multi card transformation query:")
        multi_current_card_data = submit_transformation(bigquery_client,
                                                        multi_current_cards_temp_table_id,
                                                        multi_card_union_ddl_str)

        one_current_card_view_id = f"{processing_project_dataset_id}.CIF_CARD_ONE_CURRENT_CARD"
        one_current_card_ddl_str = read_file_env(CIF_ONE_CURRENT_CARD_SQL_PATH).format(
            one_current_card_view_id=one_current_card_view_id,
            expiration_hours=expiration_hours,
            columns=columns,
            curr_transformed_data=multi_current_card_data.get(consts.ID),
            source_table_id=source_table_id,
            rank_partition=rank_partition,
            rank_order_by=rank_order_by
        )
        logger.info("Running cif card one current card transformation query")
        return submit_transformation(bigquery_client, one_current_card_view_id, one_current_card_ddl_str)

    @staticmethod
    def apply_merge_transformation(bigquery_client, source_table: dict, target_table_id: str,
                                   rank_partition: str, rank_order_by: str, segment_name: str,
                                   expiration_hours: object = 6):
        """
            Applies a union transformation to merge data from a source table and a curated view, ensuring the latest
            record is retained for each partition.

            This method constructs a union of records and labels them to select the most recent record per partition
            as defined by the rank criteria in the `rank_partition` and `rank_order_by` parameters. This process
            ensures data consolidation from specified views.

            :param bigquery_client:
                The BigQuery client object used for executing transformations.

            :param source_table:
                Dictionary representing the structure of the source table, containing:
                - `ID`: Identifier for the source table.
                - `COLUMNS`: Columns to be included during the transformation.

            :param target_table_id:
                The identifier of the target table from which to merge results with.

            :param rank_partition:
                Column criteria used for partitioning rows during the ranking process within transformation.

            :param rank_order_by:
                Column criteria used for ordering rows during the ranking process in transformation.

            :param segment_name:
                Name of the segment, which may influence specific logic such as additional column actions.

            :param expiration_hours:
                The number of hours before the created view expires. Default is 6 hours.

            :returns:
                A dictionary representing the final view generated by the union transformation containing
                distinct entries based on the rank criteria.

            :notes:
                - Conditionally alters transformation logic if 'ACCOUNT' appears within the segment name.
                - Facilitates integration of temporary concatenation logic in cases like CIFP_ACTION_CODE sorting.
                - Utilizes the logging module to provide diagnostic information during execution.
        """
        source_table_id = source_table.get(consts.ID)
        columns = source_table.get(consts.COLUMNS)
        logger.info(f'target_table_id: {target_table_id}')
        transform_view_id = f"{source_table_id}_UNION_TRANSFORM"

        source_columns = columns
        target_columns = columns

        if ACCOUNT_SEGMENT in segment_name:  # PCBDF-1485
            source_columns += """
                , CASE CIFP_ACTION_CODE
                    WHEN 'A' THEN 'A'
                    WHEN 'C' THEN 'C'
                    ELSE 'EMPTY'
                END AS CIFP_ACTION_CODE_SORTING_KEY
            """

        logger.info(f"""columns: {columns}
                        rank_partition: {rank_partition}""")

        view_union_transform_str = read_file_env(CIF_MERGE_TRANSFORM_SQL_PATH).format(
            transform_view_id=transform_view_id,
            expiration_hours=expiration_hours,
            source_columns=source_columns,
            source_table_id=source_table_id,
            target_table_id=target_table_id,
            target_columns=target_columns,
            rank_partition=rank_partition,
            rank_order_by=rank_order_by
        )
        logger.info(f'Running view union transform for segment: {segment_name}')
        return submit_transformation(bigquery_client, transform_view_id, view_union_transform_str)

    def build_segment_loading_job(self, transformation_config: dict, segment_name: str, **context):

        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config, segment_name)

        destination_table_spec = transformation_config.get(consts.DESTINATION_TABLE)
        destination_table_qualified_name = destination_table_spec.get(consts.ID)
        transformed_view_qualified_id = transformed_view.get(consts.ID)
        transformed_view_columns = transformed_view.get(consts.COLUMNS)

        insert_query = f"""
            INSERT INTO `{destination_table_qualified_name}`
            SELECT {transformed_view_columns}
            FROM `{transformed_view_qualified_id}`
        """
        # Handle TRAILER segment (append-only)
        if self.is_trailer(segment_name):
            # File validation for TRAILER (only if table exists)
            if table_exists(bigquery_client, destination_table_qualified_name):
                file_name = context.get('dag_run').conf.get('file_name')
                force_reload = context.get('dag_run').conf.get('force_reload', False)

                if not force_reload:
                    self.build_file_validation(file_name, transformation_config, transformed_view)
                else:
                    logger.info(f"Force reload enabled. Skipping file validation for file: {file_name}")

            # TRAILER is append-only - policy tags are preserved automatically
            # CREATE TABLE IF NOT EXISTS preserves existing table structure (including policy tags)
            # INSERT appends new data without affecting existing structure
            loading_sql = f"""
                CREATE TABLE IF NOT EXISTS
                `{destination_table_qualified_name}`
                AS
                SELECT {transformed_view_columns}
                FROM `{transformed_view_qualified_id}`
                LIMIT 0;
                {insert_query};
            """
            logger.info(loading_sql)
            bigquery_client.query_and_wait(loading_sql)

        # Handle ACCOUNT/CARD segments (non-trailer, full refresh)
        else:
            # At this point:
            # - CURR doesn't exist (was just renamed to PREV in previous task)
            # - PREV exists (with all metadata from the renamed CURR, including policy tags)
            destination_project_dataset = destination_table_qualified_name.rsplit('.', 1)[0]
            segment_prev_qualified_name = f"{destination_project_dataset}.CIF_{segment_name}{PREV_SUFFIX}"
            if table_exists(bigquery_client, segment_prev_qualified_name):
                # PREV exists (was just renamed from CURR in previous task)
                # Create CURR with same structure as PREV (preserves metadata including policy tags)
                logger.info(f"PREV table {segment_prev_qualified_name} exists. Creating CURR with same structure to preserve policy tags.")
                create_curr_from_prev_sql = f"""
                    CREATE TABLE IF NOT EXISTS `{destination_table_qualified_name}`
                    LIKE `{segment_prev_qualified_name}`
                """
                logger.info(f"Creating CURR table from PREV table Schema: {create_curr_from_prev_sql}")
                bigquery_client.query_and_wait(create_curr_from_prev_sql)
                # Now CURR exists (empty) with same metadata as PREV
                # Just INSERT - no truncate needed since table is empty
                logger.info(f"Inserting data into CURR: {insert_query}")
                bigquery_client.query_and_wait(insert_query)
                logger.info(f"Successfully loaded data into {destination_table_qualified_name} with policy tags preserved")
            else:
                # Edge case: PREV doesn't exist (shouldn't happen in established DAG, but handle it)
                # Fall back to CREATE OR REPLACE
                logger.warning(f"PREV table {segment_prev_qualified_name} doesn't exist. Using CREATE OR REPLACE (policy tags may not be preserved).")
                partition_str = destination_table_spec.get(consts.PARTITION)
                clustering_str = destination_table_spec.get(consts.CLUSTERING)
                loading_sql = f"""
                    CREATE OR REPLACE TABLE
                    `{destination_table_qualified_name}`
                    {partition_str}
                    {clustering_str}
                    AS
                    SELECT {transformed_view_columns}
                    FROM `{transformed_view_qualified_id}`
                    LIMIT 0;
                    {insert_query};
                """
                logger.info(loading_sql)
                bigquery_client.query_and_wait(loading_sql)

    def append_segment_prev_to_history(self, segment_name: str, transformation_config: dict):
        segment_curr_qualified_name = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        destination_project_dataset = segment_curr_qualified_name.rsplit('.', 1)[0]
        segment_prev_qualified_name = f"{destination_project_dataset}.CIF_{segment_name}{PREV_SUFFIX}"

        self.back_up_table(segment_prev_qualified_name,
                           f'{destination_project_dataset}.CIF_{segment_name}_HISTORY',
                           'CURRENT_DATETIME("America/Toronto") AS insert_time',
                           partitioning_field='DATETIME_TRUNC(insert_time, DAY)')

    @staticmethod
    def drop_segment_prev_table(segment_name: str, transformation_config: dict):
        target_project_dataset = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID).rsplit('.', 1)[0]
        target_table_qualified_name = f"{target_project_dataset}.CIF_{segment_name}{PREV_SUFFIX}"

        logger.info(f"Deleting table: {target_table_qualified_name}")
        bigquery.Client().delete_table(target_table_qualified_name, not_found_ok=True)
        logger.info(f"Deleted table: {target_table_qualified_name}")

    def build_transformation_config(self, bigquery_config: dict, output_dir: str, segment_name: str):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        rank_partition = table_config.get(consts.RANK_PARTITION)
        rank_order_by = table_config.get(consts.RANK_ORDER_BY)
        parquet_files_path = f"{output_dir}/{segment_name}/*.parquet"

        partition_field = table_config.get('partition_field')
        clustering_fields = table_config.get('clustering_fields')
        logger.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        logger.info(f'partition: {partition_str}, cluster: {clustering_str}')

        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        destination_bq_project = bigquery_config.get(consts.PROJECT_ID)
        destination_bq_dataset = bigquery_config.get(consts.DATASET_ID)
        destination_bq_table_id = table_config.get(consts.TABLE_NAME)

        # Always use landing zone for trailer
        if self.is_trailer(segment_name):
            destination_bq_project = self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]

        bq_ext_table_id = f"{bq_processing_project_name}.{destination_bq_dataset}.{destination_bq_table_id}_{consts.EXTERNAL_TABLE_SUFFIX}"
        destination_bq_table_qualified_name = f"{destination_bq_project}.{destination_bq_dataset}.{destination_bq_table_id}"

        additional_columns = table_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = table_config.get(consts.DROP_COLUMNS) or []

        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.RANK_PARTITION: rank_partition,
            consts.RANK_ORDER_BY: rank_order_by,
            consts.DESTINATION_TABLE: {
                consts.ID: destination_bq_table_qualified_name,
                consts.PARTITION: partition_str,
                consts.CLUSTERING: clustering_str
            }
        }

    @staticmethod
    def rename_segment_curr_to_prev(segment_name: str, transformation_config: dict):
        bq_client = bigquery.Client()
        destination_table_spec = transformation_config.get(consts.DESTINATION_TABLE)

        segment_curr_qualified_name = destination_table_spec.get(consts.ID)
        segment_curr_project_dataset_str = segment_curr_qualified_name.rsplit('.', 1)[0]

        segment_prev_table_name = f'CIF_{segment_name}{PREV_SUFFIX}'
        segment_prev_qualified_name = f"{segment_curr_project_dataset_str}.{segment_prev_table_name}"

        renaming_sql = read_file_env(CIF_SEGMENT_PREV_SQL_PATH).format(
            from_table_ref=segment_curr_qualified_name,
            to_table_id=segment_prev_table_name
        )

        logger.info(f"Renaming {segment_curr_qualified_name} to {segment_prev_qualified_name} using: {renaming_sql}")
        query_job = bq_client.query(renaming_sql)
        query_job.result()

    @staticmethod
    def back_up_table(
            table_id: str,
            back_up_table_id: str,
            additional_column: str = None,
            partitioning_field: str = 'FILE_CREATE_DT'
    ):
        logger.info(f"back up data from {table_id} to {back_up_table_id} ")
        columns = '*' if not additional_column else f'{additional_column}, *'

        back_up_sql = f"""
                           CREATE TABLE IF NOT EXISTS
                           `{back_up_table_id}`
                           PARTITION BY {partitioning_field}
                           AS
                           SELECT {columns}
                           FROM `{table_id}`
                           LIMIT 0;
                           INSERT INTO `{back_up_table_id}`
                           SELECT  {columns}
                           FROM `{table_id}`;
                           """
        logger.info(f"back up sql: {back_up_sql}")

        query_job = bigquery.Client().query(back_up_sql)
        query_job.result()

    def back_up_raw_data(self, segment_name: str, transformation_config: dict):
        raw_table_id = f'{transformation_config.get(consts.EXTERNAL_TABLE_ID)}{consts.COLUMN_TRANSFORMATION_SUFFIX}'
        dataset_id = raw_table_id.split('.')[1]
        landing_zone_project = self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]

        file_type = 'daily'
        data_file_location = transformation_config.get(consts.DATA_FILE_LOCATION)

        if 'cif_weekly' in data_file_location:
            file_type = 'weekly'

        self.back_up_table(raw_table_id, f'{landing_zone_project}.{dataset_id}.CIF_{segment_name}_RAW_DATA',
                           f"'{file_type}' AS FILE_TYPE")

    def build_transformation_config_task(self, bigquery_config: dict, output_dir: str, segment_name: str):
        return PythonOperator(
            task_id="build_transformation_config",
            python_callable=self.build_transformation_config,
            op_kwargs={'bigquery_config': bigquery_config,
                       'output_dir': output_dir,
                       'segment_name': segment_name}
        )

    def build_segment_prev_to_history_task(self, segment_name: str):
        return PythonOperator(
            task_id=f"build_append_{segment_name}_prev_to_history_table",
            python_callable=self.append_segment_prev_to_history,
            op_kwargs={
                'segment_name': segment_name,
                'transformation_config': f"{{{{ ti.xcom_pull(task_ids='{segment_name}.build_transformation_config') }}}}"
            }
        )

    def build_drop_segment_prev_task(self, segment_name: str):
        return PythonOperator(
            task_id=f"build_drop_{segment_name}_prev_table",
            python_callable=self.drop_segment_prev_table,
            op_kwargs={
                'segment_name': segment_name,
                'transformation_config': f"{{{{ ti.xcom_pull(task_ids='{segment_name}.build_transformation_config') }}}}"
            }
        )

    def build_segment_curr_to_prev_task(self, segment_name: str):
        return PythonOperator(
            task_id=f'build_{segment_name}_curr_to_{segment_name}_prev',
            python_callable=self.rename_segment_curr_to_prev,
            op_kwargs={
                'segment_name': segment_name,
                'transformation_config': f"{{{{ ti.xcom_pull(task_ids='{segment_name}.build_transformation_config') }}}}"
            }
        )

    def build_segment_loading_task(self, segment_name: str, transformation_config: dict = None):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={
                'transformation_config': f"{{{{ ti.xcom_pull(task_ids='{segment_name}.build_transformation_config') }}}}",
                'segment_name': segment_name
            }
        )

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'file_name': "{{ dag_run.conf['name']}}",
                       'output_dir': self.get_output_dir_path(dag_config),
                       'tables_info': self.extract_tables_info(dag_config)}
        )

    def build_back_up_raw_data_task(self, segment_name: str):
        return PythonOperator(
            task_id="build_back_up_raw_data",
            python_callable=self.back_up_raw_data,
            op_kwargs={
                'segment_name': segment_name,
                'transformation_config': f"{{{{ ti.xcom_pull(task_ids='{segment_name}.build_transformation_config') }}}}"
            }
        )

    def build_file_validation(self, file_name: str, transformation_config: dict, transformed_view: dict) -> bool:
        destination_table_qualified_name = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        external_table_qualified_name = transformed_view.get(consts.ID)
        bigquery_client = bigquery.Client()
        current_file_create_dt_query = f"SELECT FILE_CREATE_DT FROM `{external_table_qualified_name}`"
        logger.info(f"current_file_create_dt_query : {current_file_create_dt_query}")
        result = bigquery_client.query(current_file_create_dt_query).result().to_dataframe()
        current_file_create_dt = result['FILE_CREATE_DT'].values[0]
        logger.info(f"current_file_create_dt is : {current_file_create_dt}")

        max_file_create_dt_query = f"""
            SELECT MAX(FILE_CREATE_DT) as max_file_create_dt
            FROM `{destination_table_qualified_name}`
        """
        logger.info(f"max_file_create_dt_query : {max_file_create_dt_query}")
        max_result = bigquery_client.query(max_file_create_dt_query).result().to_dataframe()
        max_file_create_dt = max_result['max_file_create_dt'].values[0]
        logger.info(f"max_file_create_dt from {destination_table_qualified_name} Table: {max_file_create_dt}")

        if current_file_create_dt < max_file_create_dt:
            error_message = f"""DAG FAILED: Processing file FILE_CREATE_DT ({current_file_create_dt}) is older than max FILE_CREATE_DT ({max_file_create_dt}) in {destination_table_qualified_name} table.
                    To proceed with older FILE_CREATE_DT for File {file_name}, set "force_reload": true in the DAG run configuration."""
            raise AirflowFailException(error_message)
        else:
            logger.info(f"File validation passed. Incoming file {file_name} with FILE_CREATE_DT {current_file_create_dt} will be loaded.")

        return True

    def build_segment_task_group(self, cluster_name: str, dag_config: dict, segment_name: str):
        spark_config = dag_config.get(consts.SPARK)
        parsing_job_config = spark_config.get(consts.PARSING_JOB_ARGS).copy()
        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)
        segment_config = segment_configs.get(segment_name)

        bigquery_config = dag_config.get(consts.BIGQUERY)

        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        output_dir = self.get_output_dir_path(dag_config)
        parsing_job_config['pcb.tsys.processor.output.path'] = f"{output_dir}/{segment_name}"

        with TaskGroup(group_id=segment_name) as segment_task_group:
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config,
                                                           segment_config)
            transformation_config_loading_task = self.build_transformation_config_task(bigquery_config, output_dir,
                                                                                       segment_name)
            loading_task = self.build_segment_loading_task(segment_name)
            if not self.is_trailer(segment_name):
                move_segment_prev_to_history_task = self.build_segment_prev_to_history_task(segment_name)
                drop_segment_prev_task = self.build_drop_segment_prev_task(segment_name)
                move_segment_curr_to_prev_task = self.build_segment_curr_to_prev_task(segment_name)
                post_processing_tasks = self.build_back_up_raw_data_task(segment_name)

            if self.is_trailer(segment_name):
                parsing_task >> transformation_config_loading_task >> loading_task
            else:
                parsing_task >> transformation_config_loading_task >> move_segment_prev_to_history_task >> \
                    drop_segment_prev_task >> move_segment_curr_to_prev_task >> loading_task >> post_processing_tasks
        return segment_task_group

    def bigquery_to_oracle_loader(self, dag_id: str, dag_config: dict, **context):
        with TaskGroup(group_id='bigquery_oracle_loader') as bq_oracle_loader_group:
            dag_trigger_task = self.trigger_bq_to_oracle_dag(dag_id, dag_config)
            get_file_date_task = self.build_file_date_task(dag_id, dag_config)

            get_file_date_task >> dag_trigger_task

        return bq_oracle_loader_group

    def trigger_bq_to_oracle_dag(self, dag_id: str, dag_config: dict):
        return TriggerDagRunOperator(
            task_id=f"trigger_{dag_config.get('bigquery_oracle_loader_dag_id')}",
            trigger_dag_id=self.job_config.get(dag_id).get('bigquery_oracle_loader_dag_id'),
            conf={
                "file_create_dt": "{{ ti.xcom_pull(task_ids='bigquery_oracle_loader.get_file_date', key='file_create_dt_oracle') }}"
            },
            wait_for_completion=False,
            trigger_rule='all_success',
            retries=0,
            poke_interval=60,
        )

    def build_file_date_task(self, dag_id: str, dag_config: dict):
        file_date_column = dag_config.get('file_date_column')
        if file_date_column:
            return PythonOperator(
                task_id='get_file_date',
                python_callable=get_file_date_parameter,
                op_kwargs={'dag_id': dag_id, 'dag_config': dag_config}
            )
        else:
            return EmptyOperator(task_id="skip_file_date")

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=dag_config.get(consts.SCHEDULE_INTERVAL, None),
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            is_paused_upon_creation=True,
            render_template_as_native_obj=True
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TSYS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            file_staging_task = self.build_file_staging_task(dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            trailer_segment_task = self.build_segment_task_group(cluster_name, dag_config, consts.TRAILER_SEGMENT_NAME)
            account_segment_tasks = self.build_segment_task_group(cluster_name, dag_config,
                                                                  ACCOUNT_SEGMENT)
            card_segment_tasks = self.build_segment_task_group(cluster_name, dag_config,
                                                               CARD_SEGMENT)

            # Read SQL File
            sql_query = read_file_env(CIF_CARD_XREF_SQL_PATH, self.deploy_env)

            # SQL task to fully refresh the table using the SQL
            cif_card_xref_refresh = BigQueryInsertJobOperator(
                task_id="refresh_cif_card_xref",
                configuration={
                    "query": {
                        "query": sql_query,
                        "useLegacySql": False,
                    }
                },
                location=self.gcp_config.get(consts.BQ_QUERY_LOCATION)
            )

            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')
            bq_oracle_loader_group = self.bigquery_to_oracle_loader(dag_id, dag_config)

            start >> file_staging_task >> cluster_creating_task >> preprocess_task >> trailer_segment_task >> account_segment_tasks >> control_table_task >> bq_oracle_loader_group >> end
            trailer_segment_task >> card_segment_tasks >> cif_card_xref_refresh >> control_table_task >> bq_oracle_loader_group

        return add_tags(dag)
