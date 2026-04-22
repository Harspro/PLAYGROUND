# Class for rails : INAD , INAR & INRR

from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingINADINARINRR(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_external_cust_name(self) -> str:
        return "interacdepreq.INT_SENDER_NAME"

    def get_conductor_external_cust_first_name(self) -> str:
        return "interacdepreq.INT_SENDER_NAME"

    def get_interac_ref_number(self) -> str:
        return "interacdepreq.INT_TRANSFER_NO"

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

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            LEFT JOIN
                (
                    SELECT
                        instruction_id AS FUNDSMOVE_INSTRUCTION_ID,
                        clearingSystemReference AS INT_TRANSFER_NO,
                        DATETIME(creationDateTime, 'America/Toronto') AS INTERAC_TRANSACTION_DT,
                        amount AS DEPOSIT_AMOUNT,
                        debtor_name AS INT_SENDER_NAME,
                        transactionIdentification AS INTERAC_TRANSACTION_ID,
                        SPLIT(creditor_account_id, '-')[OFFSET(0)] AS INSTITUTION_NUMBER,
                        SPLIT(creditor_account_id, '-')[OFFSET(1)] AS TRANSIT_NUMBER,
                        SPLIT(creditor_account_id, '-')[OFFSET(2)] AS ACCOUNT_NUMBER,
                        creditor_name AS ACCOUNT_HOLDER_NAME,
                        ROW_NUMBER() OVER (PARTITION BY instruction_id,
                                                        DATETIME(creationDateTime, 'America/Toronto'),
                                                        amount
                                                ORDER BY acceptanceDateTime DESC NULLS LAST) REC_RANK
                    FROM
                        FMC_UNNESTED
                ) AS INTERACDEPREQ
                    ON INTERACDEPREQ.FUNDSMOVE_INSTRUCTION_ID = FM_GL_REQ.FUNDSMOVE_INSTRUCTION_ID
                    AND INTERACDEPREQ.DEPOSIT_AMOUNT = FM_GL_REQ.AMOUNT
                    AND (
                        EXTRACT(DATE FROM INTERACDEPREQ.INTERAC_TRANSACTION_DT) = EXTRACT(DATE FROM FM_GL_REQ.FUNDSMOVE_PROCESSING_DT)
                        OR EXTRACT(DATE FROM INTERACDEPREQ.INTERAC_TRANSACTION_DT) = DATE_SUB(EXTRACT(DATE FROM FM_GL_REQ.FUNDSMOVE_PROCESSING_DT), INTERVAL 1 DAY)
                    )
                    AND INTERACDEPREQ.REC_RANK = 1
            {self.user_detail_join}
        """
        return sql_join
