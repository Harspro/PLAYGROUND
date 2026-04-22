
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingPAD(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_funds_move_join(self) -> str:
        sql_join = f"""
        {self.funds_gl_join}
        {self.user_detail_join}
        LEFT JOIN (
                SELECT
                    in_cpa.AMOUNT,
                    FORMAT_DATETIME('%Y-%m-%d %X',PARSE_DATE('%y%j', SUBSTR(in_cpa.PAYMENT_DATE, 2, 6 ))) PAYMENT_DATE,
                    in_cpa.CUSTOMER_NUMBER CUSTOMER_NUMBER,
                    in_cpa.BANK_ACCT_NUMBER,
                    in_cpa.CUSTOMER_NAME,
                    ROW_NUMBER() OVER (PARTITION BY in_cpa.PAYMENT_DATE,
                                                in_cpa.AMOUNT,
                                                in_cpa.CUSTOMER_NUMBER
                                    ORDER BY in_cpa.UPDATE_DT DESC NULLS LAST) REC_RANK
                FROM
                    `{self.project_curated}.domain_payments.CPA_AUTOPAY_REQ` in_cpa
                WHERE
                    in_cpa.REQUEST_TYPE = 'DEBIT'
            ) cpa
            ON cpa.AMOUNT = txn_agg.ORIGINAL_AMT
            AND cpa.PAYMENT_DATE = txn_agg.TRANSACTION_DT
            AND cpa.CUSTOMER_NUMBER = txn_agg.CARD_NO
            AND cpa.REC_RANK = 1

        """
        return sql_join

    def get_external_cust_name(self) -> str:
        return "cpa.CUSTOMER_NAME"

    def get_conductor_external_cust_first_name(self) -> str:
        return "cpa.CUSTOMER_NAME"

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

    def get_external_bank_number(self) -> str:
        return "cpa.BANK_ACCT_NUMBER"
