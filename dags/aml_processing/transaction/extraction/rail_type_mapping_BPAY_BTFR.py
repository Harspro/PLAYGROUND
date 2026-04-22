# Class for rails : BPAY & BTFR

from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingBPAYBTFR(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_payee_first_name(self) -> str:
        return "bp.B_BILLER_NAME"

    def get_transfer_account_number(self) -> str:
        return "bp.F_ACCOUNT_NO"

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            LEFT JOIN (
                    SELECT
                        instruction_id AS F_FMI_ID,
                        creditor_account_id AS F_ACCT_NO,
                        DATETIME(creationDateTime, 'America/Toronto') AS F_PROCESS_DT,
                        amount AS B_AMT,
                        creditor_name AS R_BILLER_NAME_TEMP,
                        b.BILLER_NO AS R_BILLER_NO,
                        creditor_account_id AS B_BILLER_CUST_ACCT,
                        SPLIT(debtor_name, ' ')[OFFSET(0)] AS B_CUST_FN,
                        ARRAY_REVERSE(SPLIT(debtor_name, ' '))[OFFSET(0)] AS B_CUST_LN,
                        b.BILLER_NAME AS B_BILLER_NAME,
                        creditor_account_id AS F_ACCOUNT_NO,
                        ROW_NUMBER() OVER (PARTITION BY instruction_id,
                                            DATETIME(creationDateTime, 'America/Toronto'),
                                            amount
                                            ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                    FROM
                        FMC_UNNESTED  -- References CTE instead of UNNEST
                        LEFT JOIN (
                                    SELECT
                                        BILLER_NO,
                                        BILLER_NAME
                                    FROM
                                        `{self.project_curated}.domain_payments.BILLER`
                        ) b ON creditor_name = b.BILLER_NAME
            ) bp ON bp.F_FMI_ID = fm_gl_req.FUNDSMOVE_INSTRUCTION_ID
            AND bp.F_PROCESS_DT = fm_gl_req.FUNDSMOVE_PROCESSING_DT
            AND bp.B_AMT = fm_gl_req.AMOUNT
            AND bp.REC_RANK = 1
            {self.user_detail_join}
        """
        return sql_join

    def get_external_cust_name(self) -> str:
        return RailTypeMappingHelper.get_customer_name_from_bill_pay()

    def get_conductor_external_cust_first_name(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_first_name_from_agg()

    def get_device_id(self) -> str:
        return RailTypeMappingHelper.get_device_id_from_user_detail()

    def get_ip_address(self) -> str:
        return RailTypeMappingHelper.get_ip_address_from_user_detail()

    def get_user_name(self) -> str:
        return RailTypeMappingHelper.get_user_name_from_user_detail()

    def get_session_date_time(self) -> str:
        return RailTypeMappingHelper.get_session_date_time_from_user_detail()

    def get_device_type(self) -> str:
        return RailTypeMappingHelper.get_device_type_from_user_detail()

    def get_conductor_external_cust_no(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_no_from_agg()

    def get_conductor_external_cust_last_name(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_last_name_from_agg()
