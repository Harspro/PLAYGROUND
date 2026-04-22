import pendulum
import util.constants as consts
import xml.etree.ElementTree as et
import pandas as pd
import logging
import xmlschema
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from google.cloud import storage
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from typing import Final
from util.miscutils import read_variable_or_file, read_yamlfile_env_suffix, read_file_env
from util.bq_utils import create_external_table, run_bq_query
from dag_factory.terminus_dag_factory import add_tags

T5_STATEMENT_CONFIG_FILE: Final = 't5_statement_config.yaml'
DAG_ID: Final = 'pds_t5_launcher'
local_tz = pendulum.timezone('America/Toronto')
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
t5_config = read_yamlfile_env_suffix(
    f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/{T5_STATEMENT_CONFIG_FILE}',
    deploy_env_name, deploy_env_suffix)
project_id = t5_config['project_id']
external_project_id = t5_config['external_project_id']
staging_bucket = t5_config['staging_bucket']
xml_file_path_gcs = t5_config['pds_xml_file_path_gcs']
dataset_id = 'domain_tax_slips'
outbound_bucket = t5_config['outbound_bucket']
t5_null_validation: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_pds_null_validation_query.sql'
t5_null_validation_query = read_file_env(t5_null_validation, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_null_data: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_pds_null_data_fetch_query.sql'
t5_null_data_query = read_file_env(t5_null_data, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
t5_amounts_validation: Final = f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/sql/t5_pds_amounts_validation_query.sql'
t5_amounts_validation_query = read_file_env(t5_amounts_validation, read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME])
logger = logging.getLogger(__name__)


DAG_DEFAULT_ARGS = {
    'owner': "team-digital-adoption-alerts",
    'capability': "Account Management",
    'severity': 'P2',
    'sub_capability': 'NA',
    'business_impact': 'This dag parses the XML pds T5 data to BQ table ',
    'customer_impact': 'In the event of a failure T5 tax statement data will not be available',
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
        raise ValueError("XML Validation Failed Errors: " + "; ".join(error_reasons))


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


def trim_xml_fields(xml_content: str) -> str:
    """
    Trim T5Slip fields in the XML to match CRA XSD field length requirements.
    Returns the modified XML content as a string.
    """
    # Field length limits for T5 slip (to match CRA XSD requirements)
    slip_field_limits = {
        'snm': 20,
        'gvn_nm': 12,
        'init': 1,
        'addr_l1_txt': 30,
        'addr_l2_txt': 30,
        'cty_nm': 28,
        'prov_cd': 2,
        'cntry_cd': 3,
        'pstl_cd': 10,
        'rcpnt_fi_acct_nbr': 12,
    }

    try:
        root = et.fromstring(xml_content)

        # Find all T5Slip elements and trim their fields
        for t5_slip in root.findall('.//T5Slip'):
            for field_name, max_length in slip_field_limits.items():
                # Check direct children and nested elements
                element = t5_slip.find(f'.//{field_name}')
                if element is not None and element.text:
                    original_value = element.text
                    if field_name == 'rcpnt_fi_acct_nbr':
                        # Use right-most 12 characters for account number
                        trimmed_value = original_value[-max_length:].zfill(max_length)
                    else:
                        # Use left-most characters for other fields
                        trimmed_value = original_value[:max_length]
                    if original_value != trimmed_value:
                        logger.info(f"Trimmed {field_name}: '{original_value}' -> '{trimmed_value}'")
                    element.text = trimmed_value

        # Convert back to string
        return et.tostring(root, encoding='unicode')
    except et.ParseError as e:
        raise ValueError(f"Failed to parse XML for trimming: {e}")


def save_xml_to_gcs(xml_content: str):
    """Save XML content back to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(staging_bucket)
    blob = bucket.blob(xml_file_path_gcs)
    blob.upload_from_string(xml_content, content_type='application/xml')
    logger.info(f"Saved trimmed XML to gs://{staging_bucket}/{xml_file_path_gcs}")


def xml_validation_task():
    """Main task to validate XML with XSD."""
    # Fetch XML content from GCS
    xml_content = fetch_xml_from_gcs()

    # Extract tax year from XML content
    tx_yr = extract_tx_yr_from_xml(xml_content)
    logger.info(f"Extracted tax year from XML: {tx_yr}")

    # Trim fields to match XSD requirements before validation
    logger.info("Applying field trimming to XML content...")
    trimmed_xml_content = trim_xml_fields(xml_content)

    # Get the appropriate XSD path based on tax year
    t5_xsd = get_xsd_path(tx_yr)
    logger.info(f"XSD file path: {t5_xsd}")

    # Load the XSD schema with xmlschema
    xsd_schema = load_xsd(t5_xsd)

    # Validate trimmed XML against the XSD schema
    validate_xml(trimmed_xml_content, xsd_schema)

    # Save trimmed XML back to GCS for subsequent tasks to use
    save_xml_to_gcs(trimmed_xml_content)


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
            ('.//T5Slip', t5_config['pds_t5_tax_slip_csv']),
            ('.//T619', t5_config['pds_t5_tax_header_csv']),
            ('.//T5Summary', t5_config['pds_t5_tax_trailer_csv'])
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
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_PDS_HEADER_EXTERNAL',
        file_uri=t5_config['pds_t5_tax_header_csv'],
        file_format='CSV'
    )


def create_external_table_trailer():
    bq_client = bigquery.Client()
    create_external_table(
        bq_client,
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_PDS_TRAILER_EXTERNAL',
        file_uri=t5_config['pds_t5_tax_trailer_csv'],
        file_format='CSV'
    )


def create_external_table_t5slip():
    bq_client = bigquery.Client()
    create_external_table(
        bq_client,
        ext_table_id=f'{external_project_id}.domain_tax_slips.T5_TAX_PDS_SLIP_EXTERNAL',
        file_uri=t5_config['pds_t5_tax_slip_csv'],
        file_format='CSV'
    )


def validate_t5_slip_data():
    """
    This function checks if the record count is greater than 0 for a condition,
    logs all the records that match, and fails the task intentionally if the condition is met.
    """
    # SQL to get the count of null records and  cdn_int_amt < 50
    count_sql = t5_null_validation_query
    # SQL to get the count of Duplicate files
    duplicate_file_sql = f"""select count(*) as count from {external_project_id}.domain_tax_slips.T5_TAX_PDS_HEADER_EXTERNAL RT
                        inner join {project_id}.domain_tax_slips.T5_TAX_SLIP TS on RT.sbmt_ref_id = TS.sbmt_ref_id"""

    stmt_id_sql = f""" SELECT COUNTIF(CI1.CUSTOMER_UID IS NULL) AS null_customer_uid_count
                FROM `{external_project_id}.domain_tax_slips.T5_TAX_PDS_SLIP_EXTERNAL` T5Slip
                LEFT JOIN `{project_id}.domain_customer_management.CUSTOMER_IDENTIFIER` CI1
                ON LPAD(CAST(T5Slip.rcpnt_fi_acct_nbr AS STRING), 13, '0') = CI1.CUSTOMER_IDENTIFIER_NO
                AND CI1.type = 'PCF-CUSTOMER-ID' """

    # SQL to get all the null records and  cdn_int_amt < 50
    fetch_sql = t5_null_data_query

    client = bigquery.Client()

    duplicate_query_job = client.query(duplicate_file_sql)
    duplicate_count_result = duplicate_query_job.result()
    duplicate_result_count = list(duplicate_count_result)[0].count
    logger.info(f"duplicate_result_count: {duplicate_result_count}")

    stmt_query_job = client.query(stmt_id_sql)
    stmt_count_result = stmt_query_job.result()
    stmt_result_count = list(stmt_count_result)[0].null_customer_uid_count
    logger.info(f"stmtid_null_count: {stmt_result_count}")

    count_query_job = client.query(count_sql)
    count_result = count_query_job.result()
    result_count = list(count_result)[0].count
    logger.info(f"null_result_count: {result_count}")

    if duplicate_result_count > 0:
        # Fail the task intentionally as same file should not be processed multiple times
        raise Exception(f"Validation failed: {duplicate_result_count} duplicate file sbmt_ref_id found.")

    if stmt_result_count > 0:
        # If count > 0, fetch and log the records
        fetch_query_job = client.query(fetch_sql)
        fetch_query_job.result()

        # Fail the task intentionally as same file should not be processed multiple times
        raise Exception(f"Validation failed: {stmt_result_count} null records for stmt_id found.")

    if result_count > 0:
        # If count > 0, fetch and log the records
        fetch_query_job = client.query(fetch_sql)
        fetch_query_job.result()

        # Fail the task intentionally after logging the records
        raise Exception(f"Validation failed: {result_count} null records found.")


def validate_counts_and_amounts():
    """
    This function checks if the total count and amount in T5_TAX_SLIP
    matches the count and amount in T5_TAX_TRAILER.
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
            FROM `{external_project_id}.domain_tax_slips.T5_TAX_PDS_TRAILER_EXTERNAL`
            LIMIT 1
        """

        logger.info("Executing query to fetch T5Summary.bn from trailer table")
        logger.info(f"Trailer query: {trailer_query}")
        trailer_query_job = run_bq_query(trailer_query)
        trailer_result = trailer_query_job.result()
        trailer_rows = list(trailer_result)

        logger.info(f"Trailer query executed successfully. Records found: {len(trailer_rows)}")
        if len(trailer_rows) == 0:
            logger.error("No trailer record found in T5_TAX_PDS_TRAILER_EXTERNAL")
            raise AirflowFailException("Validation failed: T5Summary (trailer) record not found.")

        summary_bn = trailer_rows[0].bn
        logger.info(f"T5Summary.bn retrieved: {summary_bn}")

        if summary_bn is None:
            logger.error("T5Summary.bn is null in trailer record")
            raise AirflowFailException("Validation failed: T5Summary.bn is null.")

        # Query to get all unique bn values from T5Slip
        slip_query = f"""
            SELECT DISTINCT bn
            FROM `{external_project_id}.domain_tax_slips.T5_TAX_PDS_SLIP_EXTERNAL`
            WHERE bn IS NOT NULL
        """

        logger.info("Executing query to fetch unique T5Slip.bn values")
        logger.info(f"Slip query: {slip_query}")
        slip_query_job = run_bq_query(slip_query)
        slip_result = slip_query_job.result()
        unique_slip_bn = [row.bn for row in slip_result]

        logger.info(f"Slip query executed successfully. Unique BN values found: {len(unique_slip_bn)}")
        if len(unique_slip_bn) == 0:
            logger.error("No valid T5Slip.bn values found in T5_TAX_PDS_SLIP_EXTERNAL")
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


def generate_and_execute_insert_query(
        project_id, external_project_id, dataset_id, external_table_id,
        destination_table_id, include_sbmt_ref_id=False):
    client = bigquery.Client(project=external_project_id)

    # Define table references
    external_table_ref = client.dataset(dataset_id).table(external_table_id)

    # Get the schema of the external table
    external_table = client.get_table(external_table_ref)
    columns = [field.name for field in external_table.schema]

    # Start constructing the SQL queries for the columns and select statements
    columns_str = ', '.join(columns)
    select_columns_str = ', '.join([
        f"LPAD(CAST({column} AS STRING), 12, '0')" if column == "rcpnt_fi_acct_nbr"
        else f"LPAD(CAST({column} AS STRING), 9, '0')" if column == "sin"
        else f"FORMAT('%.2f', CAST({column} AS FLOAT64))" if column in ["tot_cdn_int_amt", "cdn_int_amt"]
        else f"CAST({column} AS STRING)" for column in columns
    ])

    # Add current_datetime('America/Toronto') as create_date for all tables
    columns_str += ", create_date"
    select_columns_str += ", current_datetime('America/Toronto') AS create_date"

    # Conditionally add sbmt_ref_id for specific tables
    if include_sbmt_ref_id:
        columns_str += ", sbmt_ref_id"
        select_columns_str += f""",
            (SELECT CAST(sbmt_ref_id AS STRING)
             FROM `{external_project_id}.domain_tax_slips.T5_TAX_PDS_HEADER_EXTERNAL`
             LIMIT 1) AS sbmt_ref_id"""

    # Construct the final insert query
    insert_query = f"""
    INSERT INTO `{project_id}.{dataset_id}.{destination_table_id}` ({columns_str})
    SELECT {select_columns_str}
    FROM `{external_project_id}.{dataset_id}.{external_table_id}`;
    """

    # Execute the insert query
    query_job = client.query(insert_query)
    query_job.result()


def insert_t5_header_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_PDS_HEADER_EXTERNAL',
        destination_table_id='T5_TAX_HEADER',
        include_sbmt_ref_id=False
    )


def insert_t5_slip_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_PDS_SLIP_EXTERNAL',
        destination_table_id='T5_TAX_SLIP',
        include_sbmt_ref_id=True
    )


def insert_t5_trailer_data():
    generate_and_execute_insert_query(
        project_id,
        external_project_id,
        dataset_id,
        external_table_id='T5_TAX_PDS_TRAILER_EXTERNAL',
        destination_table_id='T5_TAX_TRAILER',
        include_sbmt_ref_id=True
    )


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

    t5_header_ref_task = BigQueryInsertJobOperator(
        task_id='create_t5_header_ref',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE {external_project_id}.domain_tax_slips.T5_TAX_PDS_HEADER_REF AS
                    SELECT * from {external_project_id}.domain_tax_slips.T5_TAX_PDS_HEADER_EXTERNAL
                    """,
                "useLegacySql": False
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

    t5_header_insertion_task = PythonOperator(
        task_id="ingest_t5_header_data",
        python_callable=insert_t5_header_data
    )

    t5_slip_insertion_task = PythonOperator(
        task_id="ingest_t5_slip_data",
        python_callable=insert_t5_slip_data
    )

    t5_trailer_insertion_task = PythonOperator(
        task_id="ingest_t5_trailer_data",
        python_callable=insert_t5_trailer_data
    )

    trigger_t5pds_kafka_dag_task = TriggerDagRunOperator(
        task_id='trigger_t5pds_kafka_dag',
        trigger_dag_id='statement_pds_t5_kafka_writer',
        wait_for_completion=True
    )

    gcs_to_outbound_gcs_task = GCSToGCSOperator(
        task_id='copy_file_to_outbound',
        gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=outbound_bucket,
        destination_object="cra_outbound_t5_slip/{{ dag_run.conf['file_name'] }}"
    )

(start_point >> gcs_to_gcs_task >> xsd_validation_task >> xml_to_csv_task >> external_table_header_task >> external_table_trailer_task >> external_table_t5slip_task >> t5_header_ref_task
 >> validate_t5_slip_data_task >> validate_counts_and_amounts_task >> validate_summary_and_slip_bn_task >> t5_header_insertion_task >> t5_trailer_insertion_task >> t5_slip_insertion_task >> trigger_t5pds_kafka_dag_task
 >> gcs_to_outbound_gcs_task >> end_point)
