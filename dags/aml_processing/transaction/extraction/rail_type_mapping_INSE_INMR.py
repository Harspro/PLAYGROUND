# Class for rails : INSE, INMR

from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingINSEINMR(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_external_cust_name(self) -> str:
        return "interacrecipient.int_recipient_name"

    def get_conductor_external_cust_no(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_no_from_agg()

    def get_conductor_external_cust_first_name(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_first_name_from_agg()

    def get_conductor_external_cust_last_name(self) -> str:
        return RailTypeMappingHelper.get_conductor_external_cust_last_name_from_agg()

    def get_interac_ref_number(self) -> str:
        return "interacrecipient.int_x_transfer_reference_no"

    def get_interac_email(self) -> str:
        return "interacrecipient.int_email_address"

    def get_interac_phone(self) -> str:
        return "interacrecipient.int_phone_no"

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

    def get_payee_first_name(self) -> str:
        return "interacrecipient.INT_SENDER_NAME"

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            LEFT JOIN (
                        SELECT
                            instruction_id AS INT_FMI_ID,
                            clearingSystemReference AS INT_X_TRANSFER_REFERENCE_NO,
                            amount AS INT_X_AMT,
                            debtor_account_id AS INT_X_ACCOUNT_NO,
                            debtor_name AS INT_X_ACCOUNT_HOLDER_NAME,
                            NULL AS INT_RECIPIENT_ID,
                            creditor_name AS INT_RECIPIENT_NAME,
                            creditor_email AS INT_EMAIL_ADDRESS,
                            creditor_phone AS INT_PHONE_NO,
                            debtor_name AS INT_SENDER_NAME,
                            ROW_NUMBER() OVER (PARTITION BY instruction_id,
                                                    amount
                                                    ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                        FROM
                            FMC_UNNESTED
            ) interacrecipient ON interacrecipient.INT_FMI_ID = fm_gl_req.FUNDSMOVE_INSTRUCTION_ID
                AND interacrecipient.INT_X_AMT = fm_gl_req.AMOUNT
                AND interacrecipient.REC_RANK = 1
            {self.user_detail_join}
        """
        return sql_join
