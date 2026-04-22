from datetime import datetime, timedelta
import pendulum
from util.constants import (
    GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DEPLOY_ENV_STORAGE_SUFFIX
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

# TIMEZONE AND DATE DEFINITIONS
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"

# ETL SPECIFIC CONSTANTS
SQL_EXTRACT_BQ_TO_BQ: Final = 'SQL_EXTRACT_BQ_TO_BQ'
GCS_CSV_TO_BQ: Final = 'GCS_CSV_TO_BQ'
SOURCE_FILE: Final = 'source_file'
FOLDER: Final = 'folder'
FILE_PATTERN: Final = 'file_pattern'
FILE_OPTIONS: Final = 'file_options'
CSV_FILE: Final = 'CSV'
FIELD_DELIMITER: Final = 'field_delimiter'
SKIP_LEADING_ROWS: Final = 'skip_leading_rows'
SCHEMA_EXTERNAL: Final = 'schema_external'
IGNORE_UNKNOWN_VALUES: Final = 'ignore_unknown_values'
MAX_BAD_RECORDS: Final = 'max_bad_records'
BUCKET: Final = 'bucket'
