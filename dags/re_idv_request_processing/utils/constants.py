from util.constants import (
    GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DEPLOY_ENV_STORAGE_SUFFIX, CURATED_ZONE_PROJECT_ID
)
from util.miscutils import read_variable_or_file

############################################################
# constants and Commons
############################################################

# ENV AND GENERAL CONSTANTS
GCP_CONFIG = read_variable_or_file(GCP_CONFIG)
DEPLOY_ENV = GCP_CONFIG[DEPLOYMENT_ENVIRONMENT_NAME]
DEPLOY_ENV_SUFFIX = GCP_CONFIG[DEPLOY_ENV_STORAGE_SUFFIX]
CURATED_PROJECT_ID = GCP_CONFIG.get(CURATED_ZONE_PROJECT_ID)

DAG_DEFAULT_ARGS = {
    "owner": "team-growth-and-sales-alerts",
    'capability': 'CustomerAcquisition',
    'severity': 'P2',
    'sub_capability': 'Applications',
    'business_impact': 'Re-IDV will not have new Accounts to process',
    'customer_impact': 'None',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 3,
    "tags": ["team-growth-and-sales"],
    "region": "northamerica-northeast1"
}
