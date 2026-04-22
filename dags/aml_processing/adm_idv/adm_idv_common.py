from datetime import datetime
from util.miscutils import read_variable_or_file
from aml_processing import feed_commons as commons


# Constants
class ADMIdvConst:
    DAG_START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)
    DEPLOY_ENV = read_variable_or_file(
        'gcp_config')['deployment_environment_name']

    CTL_STATUS_IN_PROGRESS = "IN_PROGRESS"
    CTL_STATUS_COMPLETED = "COMPLETED"

    TBL_ADM_CTL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.ADM_CTL"
    TBL_ADM_DIGITAL_CTL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.ADM_DIGITAL_CTL"
    TBL_AML_IDV_EXCP = f"pcb-{DEPLOY_ENV}-processing.domain_aml.AML_IDV_EXCP"

    TBL_AGG_AML_IDV_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_INFO"
    TBL_AGG_AML_IDV_INFO_NEW = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_INFO_NEW"
    TBL_AGG_CUST_IDV_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO"
    TBL_LANDING_ADM_STD_DISP_APP_DATA = f"pcb-{DEPLOY_ENV}-landing.domain_customer_acquisition.ADM_STD_DISP_APP_DATA"
    TBL_LANDING_ADM_STD_DISP_DT_ELMT = f"pcb-{DEPLOY_ENV}-landing.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT"
    TBL_LANDING_APPLICANT_PERSONAL_IDENTIFIER = f"pcb-{DEPLOY_ENV}-landing.domain_validation_verification.APPLICANT_PERSONAL_IDENTIFIER"
    TBL_STG_ADM_APP_FTF = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_APP_FTF"
    TBL_STG_ADM_APP_NFTF = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_APP_NFTF"
    TBL_CURATED_AML_IDV_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_INFO"

    TBL_STG_APP_FTF_ID = f"pcb-{DEPLOY_ENV}-processing.domain_aml.APP_FTF_ID"
    TBL_STG_APP_PAV_ID = f"pcb-{DEPLOY_ENV}-processing.domain_aml.APP_PAV_ID"
    TBL_STG_ADM_NFTF_SS_FINAL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS_FINAL"
    TBL_STG_ADM_NFTF_DS_FINAL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_FINAL"
    TBL_STG_ADM_DT_ELMT = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_DT_ELMT"
    TBL_STG_ADM_NFTF_SS_FINAL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS_FINAL"
    TBL_LANDING_GEN_VAL_DETAIL = f"pcb-{DEPLOY_ENV}-landing.domain_customer_acquisition.GEN_VAL_DETAIL"
    TBL_STG_GEN_VAL_DETAIL_NFTF = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_GEN_VAL_DETAIL_NFTF"

    TBL_LANDING_ADM_STD_DISP_USER = f"pcb-{DEPLOY_ENV}-landing.domain_customer_acquisition.ADM_STD_DISP_USER"
    TBL_LANDING_LTTR_OVRD_DETAIL_XTO = f"pcb-{DEPLOY_ENV}-landing.domain_customer_acquisition.LTTR_OVRD_DETAIL_XTO"
    TBL_STG_LTTR_OVRD_DETAIL_XTO = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_LTTR_OVRD_DETAIL_XTO"
