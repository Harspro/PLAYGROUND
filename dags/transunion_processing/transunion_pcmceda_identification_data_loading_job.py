import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from google.cloud import bigquery
import util.constants as consts
from util.bq_utils import run_bq_query, table_exists, get_table_columns
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig
from dag_factory.terminus_dag_factory import DAGFactory

logger = logging.getLogger(__name__)
config_file_nm = 'transunion_pcmceda_config.yaml'


class TransUnionDagBuilder(BaseDagBuilder):
    """
    TransUnion PCMCEda Identification Data Loading DAG Builder
    A specialized DAG builder for processing TransUnion PCMCEda identification data files
    into BigQuery. This class orchestrates an end-to-end data pipeline that includes
    table creation, data loading, transformation, and validation.
    ## Purpose
    The TransUnionDagBuilder processes TransUnion PCMCEda identification data files containing
    customer identification and verification information. It loads CSV-formatted input files
    into BigQuery with proper schema mapping and data validation.
    ## Data Flow
    Input: TransUnion CSV files → Staging Table → Transformation → Final Table → Validation
    """

    def __init__(self, environment_config: EnvironmentConfig):
        super().__init__(environment_config)
        self.local_tz = environment_config.local_tz
        self.gcp_config = environment_config.gcp_config
        self.deploy_env = environment_config.deploy_env

    def get_bq_table_id(self, config: dict, env: str = None) -> str:
        """Get the BigQuery table ID for the specified environment."""
        if env is None:
            env = config['bigquery_config']['default_env']
        project_id = config['bigquery_config']['project_id_template'].format(env=env)
        return f"{project_id}.{config['bigquery_config']['dataset_id']}.{config['bigquery_config']['table_name']}"

    def get_staging_table_id(self, config: dict) -> str:
        """Get the staging table ID."""
        processing_project_id = self.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)
        dataset_id = config['bigquery_config']['dataset_id']
        table_name = config['bigquery_config']['table_name']
        return f"{processing_project_id}.{dataset_id}.{table_name}_staging"

    def create_staging_table_with_expiration(self, staging_table_id: str) -> None:
        """Create staging table with 1-day expiration."""
        try:
            staging_ddl = f"""
            CREATE OR REPLACE TABLE `{staging_table_id}`
            OPTIONS (
                expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY),
                description = "Staging table for Transunion PCMCEda identification data - expires in 1 day",
                labels = [("purpose", "staging"), ("source", "transunion"), ("team", "growth-and-sales")]
            )
            AS SELECT * FROM UNNEST([]) AS empty_table
            """

            run_bq_query(staging_ddl)
            logger.info(f"Created staging table {staging_table_id} with 1-day expiration")

        except Exception as e:
            logger.error(f"Error creating staging table: {str(e)}")
            raise Exception(f"Failed to create staging table: {str(e)}")

    def create_table_if_not_exists(self, **context) -> None:
        """Create the target table if it doesn't exist."""
        try:
            config = context['config']
            conf = context.get('dag_run').conf if context.get('dag_run') else {}
            env = conf.get('env', config['bigquery_config']['default_env'])
            logger.info(f"env: {env}")
            table_id = self.get_bq_table_id(config, env)
            client = bigquery.Client()
            table_schema = [bigquery.SchemaField(field['target'], field['type'], mode="NULLABLE")
                            for field in (config['field_mappings']['base_fields'] + config['field_mappings']['question_fields'] + config['field_mappings']['metadata_fields'])]
            if table_exists(client, table_id):
                logger.info(f"Table {table_id} already exists")
            else:
                table = bigquery.Table(table_id, schema=table_schema)
                client.create_table(table)
                logger.info(f"Created table {table_id} with {len(table_schema)} columns")

        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise Exception(f"Failed to create table: {str(e)}")

    def load_data_to_bigquery(self, **context) -> None:
        """Load data from GCS to BigQuery staging table and then to final table."""
        try:
            config = context['config']
            if not context.get('dag_run') or not context['dag_run'].conf:
                raise Exception("No DAG run configuration found")

            conf = context['dag_run'].conf
            bucket = conf.get('bucket')
            file_name = conf.get('name')

            if not bucket or not file_name:
                raise Exception("Bucket and file name must be provided in DAG run configuration")

            env = conf.get('env', config['bigquery_config']['default_env'])
            logger.info(f"env: {env}")
            table_id = self.get_bq_table_id(config, env)

            source_file_uri = f"gs://{bucket}/{file_name}"
            logger.info(f"Loading file: {source_file_uri}")

            client = bigquery.Client()
            staging_table_id = self.get_staging_table_id(config)

            self.create_staging_table_with_expiration(staging_table_id)

            staging_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter=config['staging_config']['field_delimiter'],
                skip_leading_rows=config['staging_config']['skip_leading_rows'],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                ignore_unknown_values=config['staging_config']['ignore_unknown_values'],
                max_bad_records=config['staging_config']['max_bad_records'],
                autodetect=config['staging_config']['autodetect'],
            )

            staging_load_job = client.load_table_from_uri(
                source_file_uri,
                staging_table_id,
                job_config=staging_job_config
            )

            staging_load_job.result()
            logger.info(f"Loaded data to staging table: {staging_table_id}")

            rows_before = client.get_table(table_id).num_rows

            self.transform_and_load_final_table(client, staging_table_id, table_id, file_name, config)

            final_table = client.get_table(table_id)
            rows_after = final_table.num_rows
            rows_inserted = rows_after - rows_before

            context['ti'].xcom_push(key='rows_inserted_this_file', value=rows_inserted)

            logger.info(
                f"Staging table {staging_table_id} will expire automatically in 1 day. "
                f"Successfully loaded data to {table_id}. "
                f"Inserted {rows_inserted} rows from this file (table total: {rows_after}, columns: {len(final_table.schema)})"
            )

        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {str(e)}")
            raise Exception(f"Failed to load data: {str(e)}")

    def transform_and_load_final_table(self, client, staging_table_id: str, final_table_id: str, file_name: str, config: dict) -> None:
        """Transform data from staging table and load to final table."""
        try:
            staging_table_info = get_table_columns(client, staging_table_id)
            staging_columns = staging_table_info.get(consts.COLUMNS).split(consts.COMMA_SPACE)

            logger.info(f"Staging table has {len(staging_columns)} columns: {staging_columns}")

            select_parts = []

            for field in config['field_mappings']['base_fields']:
                field_index = field['index']
                target_field = field['target']
                target_type = field['type']

                if field_index < len(staging_columns):
                    staging_field = staging_columns[field_index]
                    if target_type == "DATETIME":
                        select_parts.append(f"""
                            CASE
                                WHEN `{staging_field}` IS NOT NULL AND `{staging_field}` > 0
                                THEN SAFE.PARSE_DATETIME('{config['timestamp_format']}', CAST(`{staging_field}` AS STRING))
                                ELSE NULL
                            END as {target_field}
                        """)
                    elif target_type == "FLOAT":
                        select_parts.append(f"CAST(`{staging_field}` AS FLOAT64) as {target_field}")
                    else:
                        select_parts.append(f"CAST(`{staging_field}` AS STRING) as {target_field}")
                else:
                    select_parts.append(f"NULL as {target_field}")

            for field in config['field_mappings']['question_fields']:
                field_index = field['index']
                target_field = field['target']

                if field_index < len(staging_columns):
                    staging_field = staging_columns[field_index]
                    select_parts.append(f"CAST(`{staging_field}` AS STRING) as {target_field}")
                else:
                    select_parts.append(f"NULL as {target_field}")

            for field in config['field_mappings']['metadata_fields']:
                target_field = field['target']
                if field['value'] == 'file_name':
                    select_parts.append(f"'{file_name}' as {target_field}")
                else:
                    select_parts.append(f"{field['value']} as {target_field}")

            first_field = staging_columns[config['filtering']['record_type_field_index']] if staging_columns else "field_0"

            query = f"""
            INSERT INTO `{final_table_id}`
            SELECT {', '.join(select_parts)}
            FROM `{staging_table_id}`
            WHERE `{first_field}` = '{config['filtering']['detail_record_value']}'
            """

            logger.info(f"Executing transformation query: {query}")

            run_bq_query(query)

            logger.info(f"Successfully transformed and loaded data to {final_table_id}")

        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise Exception(f"Failed to transform data: {str(e)}")

    def get_trailer_record_count(self, client: bigquery.Client, staging_table_id: str, trailer_record_type: str,
                                 trailer_count_field_index: int, record_type_field_index: int) -> int:
        """Get the expected record count from trailer record."""
        try:
            staging_table_info = get_table_columns(client, staging_table_id)
            staging_columns = staging_table_info.get(consts.COLUMNS).split(consts.COMMA_SPACE)

            if len(staging_columns) <= max(trailer_count_field_index, record_type_field_index):
                logger.info("Staging table does not have enough columns for trailer validation")
                return None

            record_type_field = staging_columns[record_type_field_index]
            count_field = staging_columns[trailer_count_field_index]

            trailer_query = f"""
            SELECT `{count_field}` as expected_count
            FROM `{staging_table_id}`
            WHERE `{record_type_field}` = '{trailer_record_type}'
            LIMIT 1
            """

            query_job = run_bq_query(trailer_query)
            results = query_job.result()

            for row in results:
                try:
                    return int(row.expected_count)
                except (ValueError, TypeError):
                    logger.info(f"Invalid count value in trailer record: {row.expected_count}")
                    return None

            logger.info(f"No trailer record found in staging table {staging_table_id}")
            return None

        except Exception as e:
            logger.error(f"Could not extract trailer count: {str(e)}")
            raise Exception(f"Failed to extract trailer count: {str(e)}")

    def validate_data_loaded(self, **context) -> None:
        """Validate that the correct number of rows were loaded."""
        try:
            config = context['config']
            staging_table_id = self.get_staging_table_id(config)
            client = bigquery.Client()

            actual_count = context['ti'].xcom_pull(key='rows_inserted_this_file', task_ids='load_data_to_bigquery')
            if actual_count is None:
                raise Exception("Could not retrieve the count of rows inserted from this file")

            logger.info(f"Validating {actual_count} rows inserted from this file")

            trailer_record_type = config['filtering']['trailer_record_value']
            trailer_count_field_index = config['filtering']['trailer_count_field_index']
            record_type_field_index = config['filtering']['record_type_field_index']

            expected_count = self.get_trailer_record_count(client, staging_table_id, trailer_record_type, trailer_count_field_index, record_type_field_index)

            if expected_count is None:
                raise Exception(
                    f"Trailer record not found or invalid. Cannot validate data integrity. "
                    f"Inserted {actual_count} rows from this file"
                )

            if actual_count == expected_count:
                logger.info(f"Validation successful: Inserted {actual_count} rows from this file, "
                            f"matching trailer record count of {expected_count}")
            else:
                raise Exception(
                    f"Row count mismatch: Inserted {actual_count} rows from this file, "
                    f"but trailer record indicates {expected_count} records should be loaded"
                )

        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            raise Exception(f"Data validation failed: {str(e)}")

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build and return the TransUnion PCMCEda identification data loading DAG."""
        # Prepare default args
        default_args = self.prepare_default_args(config.get('default_args', {}))
        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=config['dag']['description'],
            schedule=config['dag']['schedule'],
            start_date=datetime(2025, 9, 1, tzinfo=self.local_tz),
            max_active_runs=config['dag']['max_active_runs'],
            catchup=config['default_args']['catchup'],
            is_paused_upon_creation=config['dag']['is_paused_upon_creation'],
            dagrun_timeout=timedelta(hours=2),
            tags=config['dag']['tags'],
            render_template_as_native_obj=config['dag']['render_template_as_native_obj'],
        )

        with dag:
            start_task = EmptyOperator(
                task_id='start',
            )

            create_table_task = PythonOperator(
                task_id='create_table_if_not_exists',
                python_callable=self.create_table_if_not_exists,
                op_kwargs={'config': config}
            )

            load_data_task = PythonOperator(
                task_id='load_data_to_bigquery',
                python_callable=self.load_data_to_bigquery,
                op_kwargs={'config': config}
            )

            validate_data_task = PythonOperator(
                task_id='validate_data_loaded',
                python_callable=self.validate_data_loaded,
                op_kwargs={'config': config}
            )

            end_task = EmptyOperator(
                task_id='end',
            )

            start_task >> create_table_task >> load_data_task >> validate_data_task >> end_task

        return dag


# Create DAGs using the factory
globals().update(DAGFactory().create_dynamic_dags(
    TransUnionDagBuilder,
    config_filename=config_file_nm
))
