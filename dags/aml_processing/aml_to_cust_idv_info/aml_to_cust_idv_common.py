from datetime import datetime
from util.miscutils import read_variable_or_file
from aml_processing import feed_commons as commons


class AmlCustIdvConst:
    # Constants Used in the DAG
    DAG_START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)
    DEPLOY_ENV = read_variable_or_file(
        'gcp_config')['deployment_environment_name']

    # Common tables used across tasks in the DAG
    AML_IDV_INFO_TABLE = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_INFO"
    AGG_CUST_IDV_INFO_TABLE = f"pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO"
    TBL_CUST_CTL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.CUST_CTL"
