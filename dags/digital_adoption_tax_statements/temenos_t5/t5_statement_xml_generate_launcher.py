from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
from typing import Final
from util.miscutils import read_variable_or_file, read_env_filepattern, read_yamlfile_env_suffix
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
import pendulum
import util.constants as consts
import pandas as pd
import io
import pytz
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import xmlschema
from dag_factory.terminus_dag_factory import add_tags


T5_STATEMENT_CONFIG_FILE: Final = 't5_statement_config.yaml'
DAG_ID: Final = 't5_temenos_generate_xml'
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
t5_config = read_yamlfile_env_suffix(
    f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/{T5_STATEMENT_CONFIG_FILE}',
    deploy_env_name, deploy_env_suffix)
project_id = t5_config['project_id']
external_project_id = t5_config['external_project_id']
bucket_name = t5_config['staging_bucket']
outbound_bucket = t5_config['outbound_bucket']

DAG_DEFAULT_ARGS = {
    'owner': "team-digital-adoption-alerts",
    'capability': "Account Management",
    'severity': 'P2',
    'sub_capability': 'NA',
    'business_impact': 'This dag generates the XML T5 data from BQ table ',
    'customer_impact': 'In the event of a failure T5 tax files will not be available',
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


def get_xml_file_name(df_header, df_trailer):
    """Generate the destination file name based on header and trailer data."""
    sbmt_ref_id = df_header.iloc[0]['sbmt_ref_id']
    rpt_tcd = df_trailer.iloc[0]['rpt_tcd'].lower()
    current_timestamp = datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y%m%d%H%M%S')
    file_name = f"cra_outbound_t5_slip/pcb_cra_t5_slip_{rpt_tcd}_{sbmt_ref_id}_{current_timestamp}_{{file_env}}.xml"
    destination_file_name = read_env_filepattern(file_name, deploy_env_name)
    return destination_file_name


def process_header(df_header, root):
    """Process header (T619) and add its elements to the XML root."""
    header_row = df_header.iloc[0]
    t619 = ET.SubElement(root, 'T619')

    trnmtr_acc = ET.SubElement(t619, 'TransmitterAccountNumber')
    trnmtr_nm = ET.SubElement(t619, 'TransmitterName')
    cntc = ET.SubElement(t619, 'CNTC')

    for col, val in header_row.items():
        if pd.notnull(val):
            if col in ['sbmt_ref_id', 'summ_cnt', 'lang_cd', 'TransmitterCountryCode']:
                ET.SubElement(t619, col).text = str(val)
            elif col in ['bn9', 'bn15', 'trust', 'nr4']:  # These belong to TransmitterAccountNumber
                ET.SubElement(trnmtr_acc, col).text = str(val)
            elif col in ['RepID']:  # TransmitterRepID
                trnmtr_id = ET.SubElement(t619, 'TransmitterRepID')
                trnmtr_id.text = str(val)
            elif col in ['l1_nm']:  # TransmitterName
                ET.SubElement(trnmtr_nm, col).text = str(val)
            elif col in ['cntc_nm', 'cntc_area_cd', 'cntc_phn_nbr', 'cntc_extn_nbr', 'cntc_email_area', 'sec_cntc_email_area']:  # CNTC
                ET.SubElement(cntc, col).text = str(val)


def process_slips(df_slips, t5):
    """Process slips (T5Slip) and add its elements to the T5 element."""
    amt_columns = [
        'actl_elg_dvamt', 'actl_dvnd_amt', 'tx_elg_dvnd_pamt', 'tx_dvnd_amt', 'enhn_dvtc_amt',
        'dvnd_tx_cr_amt', 'cdn_int_amt', 'oth_cdn_incamt', 'fgn_incamt', 'fgn_tx_pay_amt',
        'cdn_royl_amt', 'cgain_dvnd_amt', 'acr_annty_amt', 'rsrc_alwnc_amt',
        'cgain_dvnd_1_amt', 'cgain_dvnd_2_amt', 'lk_nt_acr_intamt', 'cgain_dvnd_jan_to_jun_2024'
    ]

    for _, slip_row in df_slips.iterrows():
        t5_slip = ET.SubElement(t5, 'T5Slip')
        rcpnt_nm = ET.SubElement(t5_slip, 'RCPNT_NM')
        rcpnt_addr = ET.SubElement(t5_slip, 'RCPNT_ADDR')
        t5_amt = ET.SubElement(t5_slip, 'T5_AMT')

        for col, val in slip_row.items():
            if pd.notnull(val):
                if col in ['snm', 'gvn_nm', 'init']:  # RCPNT_NM
                    ET.SubElement(rcpnt_nm, col).text = str(val)
                elif col in ['addr_l1_txt', 'addr_l2_txt', 'cty_nm', 'prov_cd', 'cntry_cd', 'pstl_cd']:  # RCPNT_ADDR
                    ET.SubElement(rcpnt_addr, col).text = str(val)
                elif col in amt_columns:  # T5_AMT
                    ET.SubElement(t5_amt, col).text = f"{float(val):.2f}"
                else:
                    ET.SubElement(t5_slip, col).text = str(val)


def process_trailer(df_trailer, t5):
    """Process trailer (T5Summary) and add its elements to the T5 element."""
    trailer_row = df_trailer.iloc[0]
    t5_summary = ET.SubElement(t5, 'T5Summary')

    filr_nm = ET.SubElement(t5_summary, 'FILR_NM')
    filr_addr = ET.SubElement(t5_summary, 'FILR_ADDR')
    t5_cntc = ET.SubElement(t5_summary, 'CNTC')
    t5_tamt = ET.SubElement(t5_summary, 'T5_TAMT')

    summary_amt_columns = [
        'tot_cdn_int_amt', 'tot_actl_elg_dvamt', 'tot_actl_dvnd_amt', 'tot_tx_elg_dvamt',
        'tot_tx_dvnd_amt', 'tot_enhn_dvtc_amt', 'tot_dvnd_tx_cr_amt', 'totr_cdn_incamt',
        'tot_fgn_incamt', 'tot_fgn_tx_pay_amt', 'tot_cdn_royl_amt', 'tot_cgain_dvnd_amt',
        'tot_acr_annty_amt', 'tot_rsrc_alwnc_amt'
    ]

    for col, val in trailer_row.items():
        if pd.notnull(val):
            if col in ['l1_nm', 'l2_nm', 'l3_nm']:  # FILR_NM
                ET.SubElement(filr_nm, col).text = str(val)
            elif col in ['addr_l1_txt', 'addr_l2_txt', 'cty_nm', 'prov_cd', 'cntry_cd', 'pstl_cd']:  # FILR_ADDR
                ET.SubElement(filr_addr, col).text = str(val)
            elif col in ['cntc_nm', 'cntc_area_cd', 'cntc_phn_nbr', 'prov_cd', 'cntc_extn_nbr']:  # CNTC
                ET.SubElement(t5_cntc, col).text = str(val)
            elif col in summary_amt_columns:  # T5_TAMT
                ET.SubElement(t5_tamt, col).text = f"{float(val):.2f}"
            else:
                ET.SubElement(t5_summary, col).text = str(val)


def upload_to_gcs(bucket_name, destination_file_name, xml_bytes):
    """Upload the generated XML to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_file(xml_bytes, content_type='application/xml')


def create_xml(**kwargs):

    client = bigquery.Client()

    query_slips = f"""
        SELECT * EXCEPT(create_date, sbmt_ref_id)
        FROM `{external_project_id}.domain_tax_slips.T5_ENRICHED_TAX_SLIP`
        """

    query_header = f"""
        SELECT * EXCEPT(create_date)
        FROM `{project_id}.domain_tax_slips.T5_TAX_RAW_HEADER`
        """

    query_trailer = f"""
        SELECT * EXCEPT(create_date, sbmt_ref_id)
        FROM `{project_id}.domain_tax_slips.T5_TAX_RAW_TRAILER`
        """

    # Load data into dataframes
    df_slips = client.query(query_slips).to_dataframe()
    df_header = client.query(query_header).to_dataframe()
    df_trailer = client.query(query_trailer).to_dataframe()

    # Get the file name
    destination_file_name = get_xml_file_name(df_header, df_trailer)

    # Create XML structure
    root = ET.Element('Submission', attrib={"xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                                            "xsi:noNamespaceSchemaLocation": "T619_T5.xsd"})

    # Process header, slips, and trailer
    process_header(df_header, root)
    return_elem = ET.SubElement(root, 'Return')
    t5 = ET.SubElement(return_elem, 'T5')
    process_slips(df_slips, t5)
    process_trailer(df_trailer, t5)

    # Convert the XML to a string and pretty print it
    xml_str = ET.tostring(root, encoding='utf-8')
    pretty_xml_str = minidom.parseString(xml_str).toprettyxml(indent=" ")

    # Convert to bytes for uploading
    xml_bytes = io.BytesIO(pretty_xml_str.encode('utf-8'))

    # Upload to GCS
    upload_to_gcs(bucket_name, destination_file_name, xml_bytes)

    return destination_file_name


def fetch_xml_from_gcs(**kwargs):
    """Fetch XML file content from Google Cloud Storage using XCom value for the file path."""
    task_instance = kwargs['task_instance']
    # Retrieve the XML file path from the XCom of the previous task
    xml_file_path_gcs = task_instance.xcom_pull(task_ids='create_xml')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
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


def extract_tx_yr_from_xml(xml_content: str) -> str:
    """Extract tax year (tx_yr) from XML content."""
    try:
        root = ET.fromstring(xml_content)
        # Look for tx_yr in T5Summary element
        tx_yr_element = root.find('.//T5Summary/tx_yr')
        if tx_yr_element is not None and tx_yr_element.text:
            return tx_yr_element.text.strip()
        raise ValueError("tx_yr element not found in T5Summary")
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse XML to extract tx_yr: {e}")


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


def xml_validation_task(**kwargs):
    """Main task to validate XML with XSD."""
    # Fetch XML content from GCS
    xml_content = fetch_xml_from_gcs(**kwargs)

    # Extract tax year from XML content
    tx_yr = extract_tx_yr_from_xml(xml_content)

    # Get the appropriate XSD path based on tax year
    t5_xsd = get_xsd_path(tx_yr)

    # Load the XSD schema with xmlschema
    xsd_schema = load_xsd(t5_xsd)

    # Validate XML against the XSD schema
    validate_xml(xml_content, xsd_schema)


with dag:

    add_tags(dag)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    create_xml_task = PythonOperator(
        task_id='create_xml',
        python_callable=create_xml
    )

    xsd_validation_task = PythonOperator(
        task_id='validate_xml_with_xsd',
        python_callable=xml_validation_task,
        retries=0
    )

    move_gcs_file_task = GCSToGCSOperator(
        task_id='move_file_to_outbound_landing',
        gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
        source_bucket=bucket_name,
        source_object="{{ task_instance.xcom_pull(task_ids='create_xml') }}",
        destination_bucket=outbound_bucket,
        destination_object="{{ task_instance.xcom_pull(task_ids='create_xml') }}",
        move_object=True
    )

    # Set task dependencies
    start_point >> create_xml_task >> xsd_validation_task >> move_gcs_file_task >> end_point
