# Class for rails : EPUL & EPSH
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingEPULEPSH(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_funds_move_join(self) -> str:
        sql_join = f"""
            {self.funds_gl_join}
            {self.user_detail_join}
            LEFT JOIN (
                    SELECT
                        instruction_id AS F_FMI_ID,
                        DATETIME(creationDateTime, 'America/Toronto') AS F_PROCESS_DT,
                        amount AS E_AMT,
                        SPLIT(debtor_account_id, '-')[OFFSET(0)] AS E_INST_ID,
                        SPLIT(debtor_account_id, '-')[OFFSET(1)] AS E_TRANSIT_NO,
                        SPLIT(debtor_account_id, '-')[OFFSET(2)] AS E_ACCT_NO,
                        debtor_name,
                        ROW_NUMBER() OVER (PARTITION BY instruction_id,
                                                DATETIME(creationDateTime, 'America/Toronto'),
                                                amount
                                            ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                    FROM
                        FMC_UNNESTED
            ) eft_pcb ON eft_pcb.F_FMI_ID = fm_gl_req.FUNDSMOVE_INSTRUCTION_ID
                AND eft_pcb.F_PROCESS_DT = fm_gl_req.FUNDSMOVE_PROCESSING_DT
                AND eft_pcb.E_AMT = fm_gl_req.AMOUNT
                AND eft_pcb.REC_RANK = 1
        """
        return sql_join

    def get_external_cust_name(self) -> str:
        return "eft_pcb.debtor_name"

    def get_external_bank_number(self) -> str:
        external_bank_number = ("eft_pcb.E_INST_ID || "
                                + "'-' || eft_pcb.E_TRANSIT_NO || "
                                + "'-' || eft_pcb.E_ACCT_NO ")
        return external_bank_number

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
