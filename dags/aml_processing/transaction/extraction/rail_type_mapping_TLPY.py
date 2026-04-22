# Class for rails : TLPY
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase


class RailTypeMappingTLPY(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_external_cust_name(self) -> str:
        return "TPY.CUST_NM"

    def get_conductor_external_cust_first_name(self) -> str:
        return "TPY.CUST_NM"

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            LEFT OUTER JOIN
            (
                SELECT
                    CDF.FUNDSMOVE_INSTRUCTION_ID,
                    CDF.FUNDSMOVE_PROCESSING_DT,
                    TPY_REQ.CUST_NM,
                    CDF.CARD_DEPOSIT_FULFILLMENT_UID,
                    ROW_NUMBER() OVER (PARTITION BY CDF.FUNDSMOVE_INSTRUCTION_ID,
                                                    CDF.FUNDSMOVE_PROCESSING_DT
                                                    ORDER BY CDF.UPDATE_DT DESC NULLS LAST) REC_RANK
                FROM
                    `{self.project_curated}.domain_payments.CARD_DEPOSIT_FULFILLMENT`  CDF
                    INNER JOIN
                    `{self.project_curated}.domain_payments.TELPAY_PYMT_INB_REQ` TPY_REQ
                        ON TPY_REQ.CARD_DEPOSIT_FULFILLMENT_UID = CDF.CARD_DEPOSIT_FULFILLMENT_UID
            ) as TPY
            ON FM_GL_REQ.FUNDSMOVE_INSTRUCTION_ID = TPY.FUNDSMOVE_INSTRUCTION_ID
            AND EXTRACT(DATE FROM FM_GL_REQ.FUNDSMOVE_PROCESSING_DT) = EXTRACT(DATE FROM TPY.FUNDSMOVE_PROCESSING_DT)
            AND TPY.REC_RANK = 1
        """
        return sql_join
