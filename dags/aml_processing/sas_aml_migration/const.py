from datetime import datetime
from util.miscutils import read_variable_or_file
from aml_processing.feed_commons import TORONTO_TZ as tzinfo
from airflow import settings


# Constants
class SASAMLMigrationConst:
    DAG_START_TIME = datetime(2023, 1, 1, tzinfo=tzinfo)
    DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

    # staging bucket
    STAGING_BUCKET = f"pcb-{DEPLOY_ENV}-staging-extract"

    # source BQ tables
    TBL_LANDING_CASE_LIVE = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.CASE_LIVE"
    TBL_LANDING_CASE_AGENT_NAMES = f"pcb-{DEPLOY_ENV}-landing.domain_aml.CASE_AGENT_NAMES"
    TBL_LANDING_CASE_COMMENT = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.SAS_COMMENT"
    TBL_LANDING_CASE_ATTACHMENT = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.SAS_ATTACHMENT"
    TBL_LANDING_PARTY_LIVE = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.PARTY_LIVE"
    TBL_LANDING_CASE_X_PARTY = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.CASE_X_PARTY"
    TBL_LANDING_ACCOUNT_CUSTOMER = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCOUNT_CUSTOMER"
    TBL_LANDING_ACCOUNT = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCOUNT"
    TBL_LANDING_CUSTOMER_IDENTIFIER = f"pcb-{DEPLOY_ENV}-landing.domain_customer_management.CUSTOMER_IDENTIFIER"

    TBL_LANDING_INCIDENT_LIVE = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.INCIDENT_LIVE"
    TBL_LANDING_INCIDENT_X_PARTY = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.INCIDENT_X_PARTY"

    TBL_CURATED_ACCOUNT_CUSTOMER = f"pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCOUNT_CUSTOMER"
    TBL_CURATED_ACCOUNT = f"pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCOUNT"
    TBL_CURATED_CUSTOMER_IDENTIFIER = f"pcb-{DEPLOY_ENV}-curated.domain_customer_management.CUSTOMER_IDENTIFIER"

    # dest BQ tables
    TBL_CURATED_CASE_FEED = f"pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.CASE_FEED"
    TBL_CURATED_PERSON_FEED = f"pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.PERSON_FEED"
    TBL_CURATED_PERSON_DERIVED_FEED = f"pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.PERSON_DERIVED_FEED"

    TBL_CURATED_NOTE_FEED = f"pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.NOTE_FEED"

    # dest BQ table schema file paths
    TBL_CURATED_CASE_FEED_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/sas_aml_migration/schemas/CASE_FEED.json"
    TBL_CURATED_PERSON_FEED_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/sas_aml_migration/schemas/PERSON_FEED.json"
    TBL_CURATED_NOTE_FEED_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/sas_aml_migration/schemas/NOTE_FEED.json"
