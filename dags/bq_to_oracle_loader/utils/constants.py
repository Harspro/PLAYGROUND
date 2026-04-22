from datetime import datetime, timedelta
import pendulum
from util.constants import (
    GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DATAPROC_CONFIG,
    DEPLOY_ENV_STORAGE_SUFFIX, CURATED_ZONE_PROJECT_ID
)
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


# TIMEZONE AND DATE DEFINITIONS
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
local_tz = datetime.now(pendulum.timezone('America/Toronto'))
JOB_DATE_TIME = local_tz.strftime('%Y-%m-%dT%H:%M:%S.%f')
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"

# DAG_CONFIG
dag_config = {}

# Others
REGION: Final = "northamerica-northeast1"
TRIG_RULE: Final = 'all_success'
TAGS: Final = ["CME"]
TEMP_TABLE_SUFFIX: Final = 'TEMP'
OUTPUT_FILE_CREATION: Final = 'output_file_creation'

# BIGQUERY
DATASET: Final = 'dataset_id'

# CME SPECIFIC  CONSTANTS
OUTBOUND_BUCKET: Final = 'outbound_bucket'
OUTBOUND_FOLDER: Final = 'outbound_folder'
OUTBOUND_FILENAME: Final = 'outbound_filename'
INBOUND_BUCKET: Final = 'inbound_bucket'
INBOUND_FOLDER: Final = 'inbound_folder'
SQL_FILE_PATH: Final = 'sql_file_path'
BQ_TABLE_NAME: Final = 'table_name'
MODULE_FOLDER: Final = 'module_folder'
BQ_SOURCE_PROJECT: Final = 'source_project_id'
BQ_SOURCE_DATASET: Final = 'source_dataset_id'
BQ_DESTINATION_PROJECT: Final = 'destination_project_id'
BQ_DESTINATION_DATASET: Final = 'destination_dataset_id'
BQ_DESTINATION_TABLE_NAME: Final = 'destination_table_name'
FOLDER_NAME: Final = 'folder_name'
FILE_CREATE_DT_PLACEHOLDER: Final = '{file_create_dt}'
FILE_NAME_PLACEHOLDER: Final = '{file_name}'
INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-digital-adoption-alerts",
    'capability': 'Account Management - Digital Adoption',
    'severity': 'P4',
    'sub_capability': 'NA',
    'business_impact': 'Potential compliance issue due to delay in loading business data',
    'customer_impact': 'Delay in loading business data',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "tags": ["Digital Adoption"],
    "region": "northamerica-northeast1"
}
