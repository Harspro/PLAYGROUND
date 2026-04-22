import pendulum
import util.constants as consts
import xml.etree.ElementTree as et
import xmlschema
import pandas as pd
import logging
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from google.cloud import storage
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from typing import Final
from util.miscutils import read_variable_or_file, read_yamlfile_env, read_file_env
from util.bq_utils import create_external_table, run_bq_query
from dag_factory.terminus_dag_factory import add_tags

T5_STATEMENT_CONFIG_FILE: Final = 't5_statement_config.yaml'
DAG_ID: Final = 't5_statement_launcher'
local_tz = pendulum.timezone('America/Toronto')
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
t5_config = read_yamlfile_env(f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/{T5_STATEMENT_CONFIG_FILE}', deploy_env_name)
project_id = t5_config['project_id']
external_project_id = t5_config['external_project_id']
staging_bucket = t5_config['staging_bucket']
xml_file_path_gcs = t5_config['xml_file_path_gcs']
dataset_id = 'domain_tax_slips'
t5_enriced_table: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_transformation_query.sql'
insert_header: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_temenos_header_query.sql'
insert_trailer: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_temenos_trailer_query.sql'
t5_null_validation: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_null_validation_query.sql'
t5_sin_validation: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_sin_validation_query.sql'
t5_null_data: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_null_data_fetch_query.sql'
t5_amounts_validation: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_amounts_validation_query.sql'
enriched_data_query = read_file_env(t5_enriced_table, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
insert_header_query = read_file_env(insert_header, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
insert_trailer_query = read_file_env(insert_trailer, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_null_validation_query = read_file_env(t5_null_validation, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_sin_validation_query = read_file_env(t5_sin_validation, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_null_data_query = read_file_env(t5_null_data, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_amounts_validation_query = read_file_env(t5_amounts_validation, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
logger = logging.getLogger(__name__)

DAG_DEFAULT_ARGS = {
    'owner': "team-digital-adoption-alerts",
    'capability': "account-management",
    'sub_capability': 'N/A',
    'business_impact': 'This dag parses the XML T5 data to BQ table ',
    'customer_impact': 'In the event of a failure T5 tax statement data will not be available',
    'severity': 'P2',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True
)


def fetch_xml_from_gcs():
    """Fetch XML file content from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(staging_bucket)
    blob = bucket.blob(xml_file_path_gcs)
    xml_content = blob.download_as_text()
    return xml_content


def get_xsd_path(tx_yr: str) -> str:
    """Get the XSD file path based on the tax year."""
    xsd_folder_map = {
        '2024': 't5_xmlschm_2024',
        '2025': 't5_xmlschm_2025',
    }
    folder_name = xsd_folder_map.get(str(tx_yr))
    if folder_name is None:
        raise ValueError(f"Unsupported tax year: {tx_yr}. Supported years are: {list(xsd_folder_map.keys())}")
    return f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/xsd/{folder_name}/T619_T5.xsd'


def load_xsd(xsd_file_path):
    """Load and parse the XSD file using xmlschema."""
    # Initialize xmlschema.XMLSchema with the main XSD file
    return xmlschema.XMLSchema(xsd_file_path)


def validate_xml(xml_content, xsd_schema):
    """Validate the XML content against the XSD schema, focusing on error reasons."""
    # Check if XML content is valid
    if not xsd_schema.is_valid(xml_content):
        # Extract just the reason from each validation error
        error_reasons = [
            str(error).split("Reason: ")[-1].split("Schema component:")[0].strip()
            for error in xsd_schema.iter_errors(xml_content)
        ]
        raise ValueError("XML Validation Errors: " + "; ".join(error_reasons))


def extract_tx_yr_from_xml(xml_content: str) -> str:
    """Extract tax year (tx_yr) from XML content."""
    try:
        root = et.fromstring(xml_content)
        # Look for tx_yr in T5Summary element
        tx_yr_element = root.find('.//T5Summary/tx_yr')
        if tx_yr_element is not None and tx_yr_element.text:
            return tx_yr_element.text.strip()
        raise ValueError("tx_yr element not found in T5Summary")
    except et.ParseError as e:
        raise ValueError(f"Failed to parse XML to extract tx_yr: {e}")


def xml_validation_task():
    """Main task to validate XML with XSD."""
    # Fetch XML content from GCS
    xml_content = fetch_xml_from_gcs()

    # Extract tax year from XML content
    tx_yr = extract_tx_yr_from_xml(xml_content)
    logger.info(f"Extracted tax year from XML: {tx_yr}")

    # Get the appropriate XSD path based on tax year
    t5_xsd = get_xsd_path(tx_yr)
    logger.info(f"XSD file path: {t5_xsd}")

    # Load the XSD schema with xmlschema
    xsd_schema = load_xsd(t5_xsd)

    # Validate XML against the XSD schema
    validate_xml(xml_content, xsd_schema)


def list_xml_files(bucket_name, xml_file_path_gcs):
    # Initialize Google Cloud Storage client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(xml_file_path_gcs)

    # Read XML file from GCS
    with blob.open("r") as xml_file:
        tree = et.parse(xml_file)
        root = tree.getroot()

        # Function to recursively parse XML elements
        def parse_element(element, item):
            if len(list(element)) == 0:
                item[element.tag] = element.text
            else:
                for child in list(element):
                    parse_element(child, item)

        # Define mappings from XML elements to CSV file paths
        mappings = [
            ('.//T5Slip', t5_config['t5_tax_slip_csv']),
            ('.//T619', t5_config['t5_tax_header_csv']),
            ('.//T5Summary', t5_config['t5_tax_trailer_csv'])
        ]

        # Process each mapping and write to CSV
        for xpath, csv_path in mappings:
            data = []
            for child in root.findall(xpath):
                item = {}
                parse_element(child, item)
                data.append(item)
            df = pd.DataFrame(data)
            df.to_csv(csv_path, index=False)


def parse_xml():
    list_xml_files(staging_bucket, xml_file_path_gcs)


def create_external_table_header():
    bq_client = bigquery.Client()
    create_external_table(
        bq_client,
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_RAW_HEADER_EXTERNAL',
        file_uri=t5_config['t5_tax_header_csv'],
        file_format='CSV'
    )


def create_external_table_trailer():
    bq_client = bigquery.Client()
    create_external_table(
        bq_client,
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_RAW_TRAILER_EXTERNAL',
        file_uri=t5_config['t5_tax_trailer_csv'],
        file_format='CSV'
    )


def create_external_table_t5slip():
    bq_client = bigquery.Client()
    create_external_table(
        bq_client,
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_RAW_SLIP_EXTERNAL',
        file_uri=t5_config['t5_tax_slip_csv'],
        file_format='CSV'
    )


def generate_and_execute_insert_query(project_id, external_project_id, dataset_id, external_table_id, destination_table_id, source_table):
    # Initialize BigQuery client
    client = bigquery.Client(project=external_project_id)

    # Define table references
    external_table_ref = client.dataset(dataset_id).table(external_table_id)

    # Get the schema of the external table
    external_table = client.get_table(external_table_ref)
    columns = [field.name for field in external_table.schema]

    # Construct the SQL queries
    columns_str = ', '.join(columns)
    select_columns_str = ', '.join([f"CAST({column} AS STRING)" for column in columns])

    # Insert query
    insert_query = f"""
    CREATE OR REPLACE TABLE {project_id}.{dataset_id}.{destination_table_id}
    AS
    SELECT * FROM {project_id}.{dataset_id}.{source_table}
    WHERE 1 = 0;
    INSERT INTO `{project_id}.{dataset_id}.{destination_table_id}` ({columns_str})
    SELECT {select_columns_str}
    FROM `{external_project_id}.{dataset_id}.{external_table_id}`;
    """

    # Execute the insert query
    query_job = client.query(insert_query)
    query_job.result()


def ingest_t5_header_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_RAW_HEADER_EXTERNAL',
        destination_table_id='T5_TAX_RAW_HEADER',
        source_table='T5_TAX_HEADER'
    )


def ingest_t5_slip_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_RAW_SLIP_EXTERNAL',
        destination_table_id='T5_TAX_RAW_SLIP',
        source_table='T5_TAX_SLIP'
    )


def ingest_t5_trailer_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_RAW_TRAILER_EXTERNAL',
        destination_table_id='T5_TAX_RAW_TRAILER',
        source_table='T5_TAX_TRAILER'
    )


def validate_t5_slip_data():
    """
    This function checks if the record count is greater than 0 for a condition,
    logs all the records that match, and fails the task intentionally if the condition is met.
    """
    # SQL to get the count of null records and  cdn_int_amt < 50
    count_sql = t5_null_validation_query
    # SQL to get the count of Duplicate files
    duplicate_file_sql = f"""select count(*) as count from {project_id}.domain_tax_slips.T5_TAX_RAW_HEADER RT
                        inner join {project_id}.domain_tax_slips.T5_TAX_HEADER TS on RT.sbmt_ref_id = TS.sbmt_ref_id"""

    # SQL to get all the null records and  cdn_int_amt < 50
    fetch_sql = t5_null_data_query

    client = bigquery.Client()

    duplicate_query_job = client.query(duplicate_file_sql)
    duplicate_count_result = duplicate_query_job.result()
    duplicate_result_count = list(duplicate_count_result)[0].count
    logger.info(f"duplicate sbmt_ref_id found : {duplicate_result_count}")

    count_query_job = client.query(count_sql)
    count_result = count_query_job.result()
    result_count = list(count_result)[0].count
    logger.info(f"null_records_count: {result_count}")

    # duplicate sin validation
    duplicate_sin_job = client.query(t5_sin_validation_query)
    duplicate_sin_result = duplicate_sin_job.result()
    duplicate_sin_count = list(duplicate_sin_result)[0].count
    logger.info(f"duplicate_sin_count: {duplicate_sin_count}")

    if duplicate_result_count > 0:
        # Fail the task intentionally as same file should not be processed multiple times
        raise Exception("Validation failed: duplicate file sbmt_ref_id found.")

    if result_count > 0:
        # If count > 0, fetch and log the records
        fetch_query_job = client.query(fetch_sql)
        fetch_query_job.result()

        # Fail the task intentionally after logging the records
        raise Exception(f"Validation failed: {result_count} null records found.")

    if duplicate_sin_count > 0:
        # Fail the task intentionally as duplicate sin should not be processed ina file
        raise Exception(f"Validation failed: {duplicate_sin_count} duplicate sin found.")


def validate_counts_and_amounts():
    """
    This function checks if the total count and amount in T5_ENRICHED_TAX_SLIP
    matches the count and amount in T5_TAX_RAW_TRAILER.
    If they do not match, the task will fail.
    """
    # SQL to validate counts and amounts
    validation_sql = t5_amounts_validation_query

    client = bigquery.Client()
    query_job = client.query(validation_sql)
    result = query_job.result()

    validation_status = list(result)[0].validation_status

    if validation_status == 'Match':
        logger.info("Validation successful: Counts and sums match!")
    else:
        raise Exception("Validation failed: Counts or sums do not match!")


def validate_summary_and_slip_bn():
    """
    Validates that T5Summary.bn matches all T5Slip.bn records.
    Fails the job if they don't match.
    This is a CRA 2026 requirement: Summary and Slip BN Number must be the same.
    """
    try:
        logger.info("Starting BN validation between T5Summary and T5Slip records")
        # Query to get T5Summary.bn from trailer
        trailer_query = f"""
            SELECT bn
            FROM `{external_project_id}.domain_tax_slips.T5_TAX_RAW_TRAILER_EXTERNAL`
            LIMIT 1
        """

        logger.info("Executing query to fetch T5Summary.bn from trailer table")
        logger.info(f"Trailer query: {trailer_query}")
        trailer_query_job = run_bq_query(trailer_query)
        trailer_result = trailer_query_job.result()
        trailer_rows = list(trailer_result)

        logger.info(f"Trailer query executed successfully. Records found: {len(trailer_rows)}")
        if len(trailer_rows) == 0:
            logger.error("No trailer record found in T5_TAX_RAW_TRAILER_EXTERNAL")
            raise AirflowFailException("Validation failed: T5Summary (trailer) record not found.")

        summary_bn = trailer_rows[0].bn
        logger.info(f"T5Summary.bn retrieved: {summary_bn}")

        if summary_bn is None:
            logger.error("T5Summary.bn is null in trailer record")
            raise AirflowFailException("Validation failed: T5Summary.bn is null.")

        # Query to get all unique bn values from T5Slip
        slip_query = f"""
            SELECT DISTINCT bn
            FROM `{external_project_id}.domain_tax_slips.T5_TAX_RAW_SLIP_EXTERNAL`
            WHERE bn IS NOT NULL
        """

        logger.info("Executing query to fetch unique T5Slip.bn values")
        logger.info(f"Slip query: {slip_query}")
        slip_query_job = run_bq_query(slip_query)
        slip_result = slip_query_job.result()
        unique_slip_bn = [row.bn for row in slip_result]

        logger.info(f"Slip query executed successfully. Unique BN values found: {len(unique_slip_bn)}")
        if len(unique_slip_bn) == 0:
            logger.error("No valid T5Slip.bn values found in T5_TAX_RAW_SLIP_EXTERNAL")
            raise AirflowFailException("Validation failed: No valid T5Slip.bn values found.")

        # Check if all T5Slip.bn values match T5Summary.bn
        logger.info(f"Comparing T5Summary.bn ({summary_bn}) with {len(unique_slip_bn)} unique T5Slip.bn values")
        mismatched_bn = [bn for bn in unique_slip_bn if str(bn).strip() != str(summary_bn).strip()]

        if len(mismatched_bn) > 0:
            logger.error(f"BN mismatch detected - T5Summary.bn: {summary_bn}")
            logger.error(f"Number of mismatched T5Slip.bn values: {len(mismatched_bn)}")
            logger.error(f"Mismatched T5Slip.bn values: {mismatched_bn}")
            raise AirflowFailException(
                f"Validation failed: T5Summary.bn ({summary_bn}) does not match all T5Slip.bn records. "
                f"Found {len(mismatched_bn)} mismatched values: {mismatched_bn}. "
                f"Summary and Slip Filer Account Number must be the same per CRA 2026 requirements."
            )

        logger.info(f"Validation successful: T5Summary.bn ({summary_bn}) matches all {len(unique_slip_bn)} T5Slip.bn records.")
    except AirflowFailException:
        # Re-raise AirflowFailException as-is (already has proper error message)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during BN validation: {str(e)}")
        logger.exception("Full traceback:")
        raise AirflowFailException(f"BN validation failed with unexpected error: {str(e)}")


with dag:

    add_tags(dag)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    gcs_to_gcs_task = GCSToGCSOperator(
        task_id=consts.GCS_TO_GCS,
        gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=staging_bucket,
        destination_object=xml_file_path_gcs
    )

    xsd_validation_task = PythonOperator(
        task_id='validate_xml_with_xsd',
        python_callable=xml_validation_task,
        retries=0
    )

    xml_to_csv_task = PythonOperator(
        task_id="transform_xml_to_CSV",
        python_callable=parse_xml
    )

    external_table_header_task = PythonOperator(
        task_id="create_external_table_header",
        python_callable=create_external_table_header
    )

    external_table_trailer_task = PythonOperator(
        task_id="create_external_table_trailer",
        python_callable=create_external_table_trailer
    )

    external_table_t5slip_task = PythonOperator(
        task_id="create_external_table_t5slip",
        python_callable=create_external_table_t5slip
    )

    t5_header_ingestion_task = PythonOperator(
        task_id="ingest_t5_header_data",
        python_callable=ingest_t5_header_data
    )

    t5_slip_ingestion_task = PythonOperator(
        task_id="ingest_t5_slip_data",
        python_callable=ingest_t5_slip_data
    )

    t5_trailer_ingestion_task = PythonOperator(
        task_id="ingest_t5_trailer_data",
        python_callable=ingest_t5_trailer_data
    )

    enriched_table_creation_task = BigQueryInsertJobOperator(
        task_id='t5_tax_slip_transformation',
        configuration={
            "query": {
                "query": enriched_data_query,
                "useLegacySql": False,
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    validate_t5_slip_data_task = PythonOperator(
        task_id="validate_t5_slip_data",
        python_callable=validate_t5_slip_data,
        retries=0
    )

    validate_counts_and_amounts_task = PythonOperator(
        task_id="validate_counts_and_amounts",
        python_callable=validate_counts_and_amounts,
        retries=0
    )

    validate_summary_and_slip_bn_task = PythonOperator(
        task_id="validate_summary_and_slip_bn",
        python_callable=validate_summary_and_slip_bn,
        retries=0
    )

    trigger_t5_generate_xml_dag_task = TriggerDagRunOperator(
        task_id='trigger_t5_generate_xml_dag',
        trigger_dag_id='t5_temenos_generate_xml',
        wait_for_completion=True,
        retries=0
    )

    trigger_temenos_t5_kafka_writer_dag_task = TriggerDagRunOperator(
        task_id='trigger_temenos_t5_kafka_writer_dag',
        trigger_dag_id='statement_temenos_t5_kafka_writer',
        wait_for_completion=True
    )

    # Task to insert into T5_TAX_HEADER
    t5_header_insertion_task = BigQueryInsertJobOperator(
        task_id='insert_t5_tax_header',
        configuration={
            "query": {
                "query": insert_header_query,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    # Task to insert into T5_TAX_TRAILER
    t5_trailer_insertion_task = BigQueryInsertJobOperator(
        task_id='insert_t5_tax_trailer',
        configuration={
            "query": {
                "query": insert_trailer_query,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    t5_slip_insertion_task = BigQueryInsertJobOperator(
        task_id='insert_t5_tax_slip',
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO {project_id}.domain_tax_slips.T5_TAX_SLIP
                    SELECT * from {external_project_id}.domain_tax_slips.T5_ENRICHED_TAX_SLIP
                    """,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )

(start_point >> gcs_to_gcs_task >> xsd_validation_task >> xml_to_csv_task >> external_table_header_task >> external_table_trailer_task >> external_table_t5slip_task
 >> t5_header_ingestion_task >> t5_slip_ingestion_task >> t5_trailer_ingestion_task >> enriched_table_creation_task >> validate_t5_slip_data_task
 >> validate_counts_and_amounts_task >> validate_summary_and_slip_bn_task >> trigger_t5_generate_xml_dag_task >> trigger_temenos_t5_kafka_writer_dag_task >> t5_header_insertion_task
 >> t5_trailer_insertion_task >> t5_slip_insertion_task >> end_point)
