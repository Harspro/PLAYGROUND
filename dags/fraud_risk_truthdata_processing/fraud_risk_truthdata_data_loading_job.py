from datetime import timedelta, datetime
from airflow import DAG
from typing import Optional
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery, storage
import logging
import csv
import io
import os

import util.constants as consts
from util.bq_utils import create_external_table, run_bq_query, submit_transformation
from dag_factory.terminus_dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig
from fraud_risk_truthdata_processing.fraud_risk_truthdata_file_validation import FraudRiskTruthDataFileValidation

logger = logging.getLogger(__name__)
config_file_nm = 'fraud_risk_truth_data_config.yaml'


def read_csv_headers_from_gcs(bucket_name: str, file_name: str) -> list:
    """
    Read the CSV header row from a GCS file.

    Args:
        bucket_name: GCS bucket name
        file_name: File path in the bucket

    Returns:
        List of column names from the CSV header
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Read only the first line (header)
        header_bytes = blob.download_as_bytes(end=8192)  # Read first 8KB which should contain the header
        header_text = header_bytes.decode('utf-8-sig')  # Use utf-8-sig to automatically remove BOM

        # Parse the first line as CSV
        csv_reader = csv.reader(io.StringIO(header_text.split('\n')[0]))
        headers = next(csv_reader)

        # Clean up headers (strip whitespace and BOM character if present)
        headers = [h.strip().replace('\ufeff', '') for h in headers]

        logger.info(f"Read {len(headers)} columns from CSV header: {headers}")
        return headers

    except Exception as e:
        logger.error(f"Failed to read CSV header from gs://{bucket_name}/{file_name}: {str(e)}")
        raise AirflowFailException(f"Failed to read CSV header: {str(e)}")


class FraudRiskTruthDataDagBuilder(BaseDagBuilder):
    """
    Fraud Risk Truth Data Loading DAG Builder

    A specialized DAG builder for processing Fraud Risk Truth Data CSV files
    into BigQuery. This class orchestrates an end-to-end data pipeline that includes
    file format validation, mandatory field validation, and data loading.

    ## Purpose
    The FraudRiskTruthDataDagBuilder processes Fraud Risk Truth Data CSV files containing
    fraud risk assessment information. It validates file format and mandatory fields
    before loading data into BigQuery.

    ## Data Flow
    Input: CSV files (TRUTH_DATA_FRAUD_<yyyymmddhhmiss>.csv) → File Validation → BigQuery Table
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

    def _file_validation_task(self, **context) -> Optional[str]:
        """Task to validate file format and mandatory fields before loading to BigQuery"""
        config = context['config']

        if not context.get('dag_run') or not context['dag_run'].conf:
            raise AirflowFailException("No DAG run configuration found")

        conf = context['dag_run'].conf
        bucket = conf.get('bucket')
        file_name = conf.get('name')

        if not bucket or not file_name:
            raise AirflowFailException("Bucket and file name must be provided in DAG run configuration")

        env = conf.get('env', config['bigquery_config']['default_env'])
        logger.info(f"Processing file: {file_name} from bucket: {bucket} in environment: {env}")

        # Construct GCS URI
        gcs_uri = f"gs://{bucket}/{file_name}"

        # Get project ID from GCP config
        processing_project_id = self.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)

        # Create external table ID for validation
        dataset_id = config['staging_config']['dataset_id']
        stg_table_name = config['staging_config']['table_name'] + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        staging_table_id = f"{processing_project_id}.{dataset_id}.{stg_table_name}"

        logger.info(f"""Starting file validation for: {file_name}
                        Creating external table: {staging_table_id}
                        GCS URI: {gcs_uri}""")

        # Initialize validation utils
        file_validation = FraudRiskTruthDataFileValidation(project_id=processing_project_id)

        try:
            # Validate file format
            file_format_result = file_validation.validate_file_format(gcs_uri)
            if not file_format_result["is_valid"]:
                raise AirflowFailException(file_format_result.get("error_message", "File format validation failed"))

            # Create BigQuery client
            bq_client = bigquery.Client(project=processing_project_id)

            # Create External table for validation
            file_format = config['staging_config']['file_format']
            skip_leading_rows = config['staging_config'].get('skip_leading_rows', 1)
            field_delimiter = config['staging_config']['field_delimiter']
            column_definitions = None
            # For CSV files, read headers and create external table with explicit schema
            if file_format.upper() == 'CSV':
                # Read CSV headers from GCS
                csv_headers = read_csv_headers_from_gcs(bucket, file_name)

                # Build column definition string for DDL (all columns as STRING for external tables)
                column_definitions = ',\n                    '.join([f"`{header}` STRING" for header in csv_headers])

            create_external_table(
                bigquery_client=bq_client,
                ext_table_id=staging_table_id,
                file_uri=gcs_uri,
                expiration_hours=config['staging_config'].get('expiration_hours', 6),
                file_format=file_format,
                field_delimiter=field_delimiter,
                skip_leading_rows=skip_leading_rows,
                table_schema=column_definitions
            )

            logger.info(f"External table created successfully: {staging_table_id}")

            # Define validation requirements
            required_columns = config['validation_config']['required_columns']
            mandatory_fields = config['validation_config']['mandatory_fields']

            # Perform file and data validation
            validation_result = file_validation.perform_file_validation(
                table_id=staging_table_id,
                required_columns=required_columns,
                mandatory_fields=mandatory_fields,
                include_violation_details=True
            )

            # Log validation results
            logger.info(f"""Validation completed for table: {staging_table_id}
                            Overall validation status: {validation_result['overall_valid']}""")

            if validation_result['column_validation']:
                logger.info(f"Column validation: {validation_result['column_validation']['is_valid']}")
                if not validation_result['column_validation']['is_valid']:
                    logger.error(f"Missing columns: {validation_result['column_validation']['missing_columns']}")

            if validation_result['field_validation']:
                logger.info(f"Field validation: {validation_result['field_validation']['is_valid']}")
                if not validation_result['field_validation']['is_valid']:
                    logger.error(f"""Total violations: {validation_result['field_validation']['total_violations']}
                                     Null violations: {validation_result['field_validation']['null_violations']}
                                     Empty violations: {validation_result['field_validation']['empty_violations']}""")

            # Determine final result
            if validation_result['overall_valid']:
                logger.info("File validation passed successfully")
                # Store staging table ID in XCom for next task
                context['ti'].xcom_push(key='staging_table_id', value=staging_table_id)
                context['ti'].xcom_push(key='file_name', value=file_name)
                context['ti'].xcom_push(key='env', value=env)
                return "SUCCESS: File validation completed successfully"
            else:
                error_msg = "File validation failed"
                if validation_result.get('column_validation', {}).get('error_message'):
                    error_msg += f" - {validation_result['column_validation']['error_message']}"
                if validation_result.get('field_validation', {}).get('error_message'):
                    error_msg += f" - {validation_result['field_validation']['error_message']}"

                logger.error(error_msg)
                raise AirflowFailException(error_msg)

        except Exception as e:
            logger.error(f"File validation failed: {str(e)}")
            raise AirflowFailException(f"File validation failed: {str(e)}")

    def _load_data_to_bigquery_task(self, **context) -> Optional[str]:
        """Task to load validated data from staging table to final BigQuery table"""
        config = context['config']

        # Get data from previous task
        ti = context['ti']
        staging_table_id = ti.xcom_pull(key='staging_table_id', task_ids='file_validation')
        filename = ti.xcom_pull(key='file_name', task_ids='file_validation')
        file_name = os.path.basename(filename)
        env = ti.xcom_pull(key='env', task_ids='file_validation')

        if not staging_table_id or not file_name:
            raise AirflowFailException("Missing required data from validation task")

        logger.info(f"Loading data from staging table: {staging_table_id}")

        try:
            # Get final table ID
            final_table_id = self.get_bq_table_id(config, env)

            # Create BigQuery client
            client = bigquery.Client()

            # Get staging table schema
            staging_table = client.get_table(staging_table_id)
            staging_columns = [field.name for field in staging_table.schema]

            logger.info(f"Staging table has {len(staging_columns)} columns: {staging_columns}")

            # Map columns from staging to final table
            # Based on the schema, map CSV columns to BigQuery table columns
            column_mapping = config.get('column_mapping', {})

            # Build SELECT statement with column mapping
            select_parts = []
            event_tag_inserted = False
            for staging_col in staging_columns:
                # Map column name if mapping exists, otherwise use as-is
                # Check both exact match and case-insensitive match
                final_col = column_mapping.get(staging_col)
                if not final_col:
                    # Try case-insensitive lookup
                    for key, value in column_mapping.items():
                        if key.lower() == staging_col.lower():
                            final_col = value
                            break

                # If no mapping found, check if it matches required columns (case-insensitive)
                if not final_col:
                    for req_col in config['validation_config']['required_columns']:
                        if req_col.lower() == staging_col.lower():
                            final_col = req_col
                            break

                # If still no match, use staging column as-is
                if not final_col:
                    final_col = staging_col

                select_parts.append(f"`{staging_col}` as `{final_col}`")

                # Insert constant KafkaPublishStatus value after event_tag field
                if not event_tag_inserted and (final_col == 'EventTag' or final_col.lower() == 'event_tag'):
                    select_parts.append("'PENDING' as `KafkaPublishStatus`")
                    event_tag_inserted = True

            # Add metadata fields
            for metadata_field in config.get('metadata_fields', []):
                target_field = metadata_field['target']
                if metadata_field['value'] == 'file_name':
                    select_parts.append(f"'{file_name}' as `{target_field}`")
                else:
                    select_parts.append(f"{metadata_field['value']} as `{target_field}`")

            # Build INSERT query
            insert_query = f"""
            INSERT INTO `{final_table_id}`
            SELECT {', '.join(select_parts)}
            FROM `{staging_table_id}`
            """

            logger.info(f"Executing load query: {insert_query}")

            # Execute the load
            run_bq_query(insert_query)

            # Get final table info
            final_table = client.get_table(final_table_id)
            rows_loaded = final_table.num_rows

            logger.info(f"Successfully loaded data to {final_table_id}. Total rows: {rows_loaded}")

            context['ti'].xcom_push(key='rows_loaded', value=rows_loaded)

            return f"SUCCESS: Loaded {rows_loaded} rows to {final_table_id}"

        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {str(e)}")
            raise AirflowFailException(f"Failed to load data: {str(e)}")

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build and return the Fraud Risk Truth Data loading DAG."""
        # Prepare default args
        default_args = self.prepare_default_args(config.get('default_args', {}))

        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=config['dag']['description'],
            schedule=config['dag']['schedule'],
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            max_active_runs=config['dag']['max_active_runs'],
            catchup=config['default_args']['catchup'],
            is_paused_upon_creation=config['dag']['is_paused_upon_creation'],
            dagrun_timeout=timedelta(hours=2),
            tags=config['dag']['tags'],
            render_template_as_native_obj=config['dag'].get('render_template_as_native_obj', True),
        )

        with dag:
            start_task = EmptyOperator(
                task_id='start',
            )

            file_validation_task = PythonOperator(
                task_id='file_validation',
                python_callable=self._file_validation_task,
                op_kwargs={'config': config}
            )

            load_data_task = PythonOperator(
                task_id='load_data_to_bigquery',
                python_callable=self._load_data_to_bigquery_task,
                op_kwargs={'config': config}
            )

            truthdata_kafka_processing_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID][consts.KAFKA_WRITER_TASK_ID],
                trigger_dag_id=config[consts.DAG][consts.DAG_ID][consts.KAFKA_TRIGGER_DAG_ID],
                wait_for_completion=config[consts.DAG][consts.WAIT_FOR_COMPLETION],
                poke_interval=config[consts.DAG][consts.POKE_INTERVAL],
                logical_date=datetime.now(tz=self.local_tz)
            )

            end_task = EmptyOperator(
                task_id='end',
            )

            start_task >> file_validation_task >> load_data_task >> truthdata_kafka_processing_dag >> end_task

        return dag


# Create DAGs using the factory
globals().update(DAGFactory().create_dynamic_dags(
    FraudRiskTruthDataDagBuilder,
    config_filename=config_file_nm
))
