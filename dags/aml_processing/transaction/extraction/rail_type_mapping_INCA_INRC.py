# Class for rails : INCA, INRC

from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingINCAINRC(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_external_cust_name(self) -> str:
        return "interac_recv.int_sender_name"

    def get_conductor_external_cust_first_name(self) -> str:
        return "interac_recv.int_sender_name"

    def get_interac_ref_number(self) -> str:
        return "interac_recv.transfer_reference_number"

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
            LEFT JOIN (
                        SELECT
                            instruction_id AS int_fmi_inst_id,
                            debtor_name AS int_sender_name,
                            debtor_account_id AS int_account_uid,
                            clearingSystemReference AS transfer_reference_number,
                            ROW_NUMBER() OVER (PARTITION BY instruction_id
                                                ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                        FROM
                            FMC_UNNESTED
            ) interac_recv ON interac_recv.int_fmi_inst_id = fm_gl_req.fundsmove_instruction_id
                AND interac_recv.REC_RANK = 1

            {self.user_detail_join}
        """
        return sql_join
