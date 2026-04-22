from typing import Final
from datetime import timedelta

############################################################
# constants and Commons
############################################################

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
FOLDER_NAME: Final = 'folder_name'
DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    'capability': 'Digital Adoption- DA',
    'severity': 'P2',
    'sub_capability': 'statement processing etl',
    'business_impact': 'Statement IDX file will not be processed and stmt availability file will not be generated',
    'customer_impact': 'Customers will not be notified for the statements generation',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "tags": ["stmt"],
    "region": "northamerica-northeast1"
}
