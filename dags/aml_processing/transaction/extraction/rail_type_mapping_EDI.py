# Class for rails : EDI
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase


class RailTypeMappingEDI(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_external_bank_number(self) -> str:
        return "EDI.INSTITUTION_ID"

    def get_external_cust_name(self) -> str:
        return "EDI.CUST_NM"

    def get_conductor_external_cust_first_name(self) -> str:
        return "EDI.CUST_NM"

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            LEFT JOIN
            (SELECT CDF.FUNDSMOVE_INSTRUCTION_ID,CDF.FUNDSMOVE_PROCESSING_DT,EPI.ORIG_FI_DT,EPI.CUST_NM,EPI.INSTITUTION_ID,
            ROW_NUMBER() OVER (PARTITION BY CDF.FUNDSMOVE_INSTRUCTION_ID,CDF.FUNDSMOVE_PROCESSING_DT
                                                    ORDER BY CDF.UPDATE_DT DESC NULLS LAST) REC_RANK
            FROM `{self.project_curated}.domain_payments.CARD_DEPOSIT_FULFILLMENT`  CDF
            INNER JOIN
            `{self.project_curated}.domain_payments.EDI_PYMT_INB_REQ` EPI
            ON EPI.CARD_DEPOSIT_FULFILLMENT_UID=CDF.CARD_DEPOSIT_FULFILLMENT_UID) as EDI
            ON FM_GL_REQ.FUNDSMOVE_INSTRUCTION_ID=EDI.FUNDSMOVE_INSTRUCTION_ID
            AND FM_GL_REQ.FUNDSMOVE_PROCESSING_DT=EDI.FUNDSMOVE_PROCESSING_DT
            AND EDI.REC_RANK = 1
        """
        return sql_join
