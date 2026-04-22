from datetime import datetime, timedelta
import pendulum
from util.constants import (
    GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DATAPROC_CONFIG,
    DEPLOY_ENV_STORAGE_SUFFIX, CURATED_ZONE_PROJECT_ID, PROCESSING_ZONE_PROJECT_ID,
    LANDING_ZONE_PROJECT_ID)
from util.miscutils import read_variable_or_file
from typing import Final

############################################################
# constants and Commons
############################################################

# ENV AND GENERAL CONSTANTS
GCP_CONFIG = read_variable_or_file(GCP_CONFIG)
DEPLOY_ENV = GCP_CONFIG[DEPLOYMENT_ENVIRONMENT_NAME]
DEPLOY_ENV_SUFFIX = GCP_CONFIG[DEPLOY_ENV_STORAGE_SUFFIX]
DATAPROC_CONFIG = read_variable_or_file(DATAPROC_CONFIG)
CURATED_PROJECT_ID = GCP_CONFIG.get(CURATED_ZONE_PROJECT_ID)
PROCESSING_PROJECT_ID = GCP_CONFIG.get(PROCESSING_ZONE_PROJECT_ID)
LANDING_PROJECT_ID = GCP_CONFIG.get(LANDING_ZONE_PROJECT_ID)


# TIMEZONE AND DATE DEFINITIONS
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"

# DAG_CONFIG
dag_config = {}

# Others
REGION: Final = "northamerica-northeast1"
TRIG_RULE: Final = 'all_success'
TAGS: Final = ["team-orcs-alerts", "outbound", "ALM"]

# ALM SPECIFIC  CONSTANTS
DATA_ORIGIN: Final = 'pcb'
DATA_DEST: Final = 'tbsm'
PROJ_NAME: Final = 'alm'
OUTBOUND_BUCKET: Final = 'outbound_bucket'
OUTBOUND_FOLDER: Final = 'outbound_folder'
OUTBOUND_FILENAME: Final = 'outbound_filename'
INBOUND_BUCKET: Final = 'inbound_bucket'
INBOUND_FOLDER: Final = 'inbound_folder'
SQL_FILE_PATH: Final = 'sql_path'
BQ_TABLE_NAME: Final = 'table_name'
BQ_SOURCE_PROJECT: Final = 'source_project_id'
BQ_SOURCE_DATASET: Final = 'source_dataset_id'
BQ_DESTINATION_PROJECT: Final = 'destination_project_id'
BQ_DESTINATION_DATASET: Final = 'destination_dataset_id'
BQ_DESTINATION_TABLE_NAME: Final = 'destination_table_name'
FILE: Final = 'file'
PREFIX: Final = 'prefix'
VENDOR: Final = 'vendor'
FORMAT: Final = 'format'
EXTENSION: Final = 'extension'
FREQUENCY: Final = 'frequency'
SCHEMA_EXTERNAL: Final = 'schema_external'
DELIMITER: Final = 'delimiter'
HEADER: Final = 'header'
FOOTER: Final = 'footer'
PAUSE_DAG_BY_CONFIG: Final = 'pause_dag_by_config'
REF_TABLES_FOLDER: Final = 'ref_tables_folder'
ACTIVITY_NAME: Final = 'activity_name'
INBOUND_FILE: Final = 'inbound_file'
FOLDER_NAME: Final = 'folder_name'
LOWERCASE_VENDOR_NAME: Final = "lowercase_vendor_name"
UPPERCASE_VENDOR_NAME: Final = "uppercase_vendor_name"
HEADER_COUNT: Final = "header_count"
TRAILER_COUNT: Final = "trailer_count"
# reference table config
BQ_INVALID_TMP_TABLE_ID: Final = 'bq_invalid_tmp_table_id'
# Validator config
DAYS_DELTA: Final = 'days_delta'
SUFFIX: Final = 'suffix'
SKIP_VALIDATION: Final = 'skip_validation'
VALIDATION_FILE: Final = 'validation_file'
FILE_REGEX_PATTERN: Final = 'file_regex_pattern'
HEADER_CONFIG: Final = 'header_config'
FOOTER_CONFIG: Final = 'footer_config'
DATA_MAPPING: Final = 'data_mapping'
COLUMN_MAP: Final = 'column_map'
MAPPING_SQL: Final = 'mapping_sql'
DEDUP_COLS: Final = 'dedup_cols'
DATA_FILTER: Final = 'data_filter'
SKIP_LEADING_ROWS: Final = 'skip_leading_rows'
DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Data Movement PDS',
    'severity': 'P3',
    'sub_capability': 'alm',
    'business_impact': 'Delay in processing Treasury Reports by TBSM ',
    'customer_impact': 'None',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "tags": ["team-centaurs", "outbound", "ALM"],
    "region": "northamerica-northeast1"
}

# Constant for CTERA To GCS Copy
CTERA_FILE_PATTERN: Final = 'pattern'
CTERA_VENDOR_CODE_MATCHED: Final = 'MATCHED'
CTERA_VENDOR_CODE_NOT_MATCHED: Final = 'NOT MATCHED'
BQ_PARTITION_COLUMN: Final = 'partition_column'
CTERA_SHARED_FOLDER: Final = 'ctera_shared_folder'
CTERA_RAW_FILE: Final = 'raw_file'
CTERA_PROCESS_FILE: Final = 'process_file'
CTERA_BBG_REQ_FILE: Final = 'bbg_req_file'
CTERA_PROCESS: Final = 'process'
CTERA_ARCHIVE: Final = 'archive'
CTERA_BBG_REQ: Final = 'bbg_req'
CTERA_SUCCESS: Final = 'SUCCESS'
CTERA_FAILED: Final = 'FAILED'
CTERA_GCS_FOLDER: Final = 'gcs_folder'
CTERA_LOG_HEADER: Final = "filename|file_pattern|file_type|process_type|gcs_bucket|gcs_folder|current_run_ind|load_dt|process_ts|status"
MAX_ACTIVE_TIS_PER_DAGRUN: Final = 4
CURRENT_RUN_IND: Final = 'Y'
CIBCMN_INVESTMEN_FILE_PATTERN: Final = '.*cibcmn_pcb_alm_investments_\\d{14}\\.csv.(uatv|prod)$'
CIBCMN_CUSTODY_FILE_PATTERN: Final = '.*cibcmn_pcb_alm_custody-holdings_\\d{14}\\.csv.(uatv|prod)$'
ALM_CTERA_FOLDERS: Final = ['raw_file', 'process_file', 'bbg_req_file']
GWL_CASH_FILE_PATTERN: Final = '.*gwl_pcb_alm_cash_\\d{14}\\.(uatv|prod)$'
GWL_CUSTODY_FILE_PATTERN: Final = '.*gwl_pcb_alm_investment-by-deal_\\d{14}\\.(uatv|prod)$'
BBG_RATE_FILE_PATTERN: Final = '.*bbg_pcb_alm_rate_\\d{14}\\.(uatv|prod)$'
BBG_BOND_FILE_PATTERN: Final = '.*bbg_pcb_alm_bond_\\d{14}\\.(uatv|prod)$'
