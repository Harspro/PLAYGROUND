# Class for rails : NPFI & NPFO
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_helper import RailTypeMappingHelper


class RailTypeMappingNPFINPFO(RailTypeMappingBase):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_funds_move_join(self) -> str:
        if self.rail_type.upper() == 'NPFI':
            rail_join = """
                LEFT JOIN (
                    SELECT
                        instruction_id AS F_FMI_ID,
                        DATETIME(creationDateTime, 'America/Toronto') AS F_PROCESS_DT,
                        amount AS EXT_AMT,
                        SPLIT(debtor_account_id, '-')[OFFSET(0)] AS EXT_INST_ID,
                        SPLIT(debtor_account_id, '-')[OFFSET(1)] AS EXT_TRANSIT_NO,
                        SPLIT(debtor_account_id, '-')[OFFSET(2)] AS EXT_ACCT_NO,
                        debtor_name AS EXT_CUST_LONGNAME,
                        ROW_NUMBER() OVER (PARTITION BY instruction_id,
                                            DATETIME(creationDateTime, 'America/Toronto'),
                                            amount
                            ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                    FROM
                        FMC_UNNESTED
                ) EFTEXTORIGINREQ ON EFTEXTORIGINREQ.F_FMI_ID = FM_GL_REQ.FUNDSMOVE_INSTRUCTION_ID
                AND EXTRACT(DATE FROM EFTEXTORIGINREQ.F_PROCESS_DT) = EXTRACT(DATE FROM FM_GL_REQ.FUNDSMOVE_PROCESSING_DT)
                AND EFTEXTORIGINREQ.EXT_AMT = FM_GL_REQ.AMOUNT
                AND EFTEXTORIGINREQ.REC_RANK = 1
            """
        else:
            rail_join = f"""
                LEFT JOIN
                (
                    SELECT
                        EFT_EXTORIGIN_REQ.FUNDSMOVE_INSTRUCTION_ID    F_FMI_ID,
                        EFT_EXTORIGIN_REQ.FUNDSMOVE_PROCESSING_DT     F_PROCESS_DT,
                        EFT_EXTORIGIN_REQ.AMOUNT                      EXT_AMT,
                        EFT_EXTORIGIN_REQ.ORIGIN_INSTITUTION_ID       EXT_INST_ID,
                        EFT_EXTORIGIN_REQ.ORIGIN_TRANSIT_NUMBER       EXT_TRANSIT_NO,
                        EFT_EXTORIGIN_REQ.ORIGIN_ACCOUNT_NUMBER       EXT_ACCT_NO,
                        EFT_EXTORIGIN_REQ.ORIGIN_LONG_NAME            EXT_CUST_LONGNAME,
                        ROW_NUMBER() OVER (PARTITION BY EFT_EXTORIGIN_REQ.FUNDSMOVE_INSTRUCTION_ID,
                                            EFT_EXTORIGIN_REQ.FUNDSMOVE_PROCESSING_DT,
                                            EFT_EXTORIGIN_REQ.AMOUNT
                                            ORDER BY EFT_EXTORIGIN_REQ.UPDATE_DT DESC NULLS LAST) REC_RANK
                    FROM(
                        SELECT
                            FUNDSMOVE_INSTRUCTION_ID,
                            FUNDSMOVE_PROCESSING_DT,
                            AMOUNT,
                            ORIGIN_INSTITUTION_ID,
                            ORIGIN_TRANSIT_NUMBER,
                            ORIGIN_ACCOUNT_NUMBER,
                            ORIGIN_LONG_NAME,
                            EFT_EXTORIGIN_REQ_UID,
                            EFT_REQTYPE_CD,
                            UPDATE_DT
                        FROM
                            `{self.project_curated}.domain_payments.EFT_EXTORIGIN_REQ`
                        WHERE
                            EFT_REQTYPE_CD IN ('NPFO')
                        ) AS EFT_EXTORIGIN_REQ
                        INNER JOIN (
                            SELECT
                                EFT_EXTORIGIN_REQ_UID,
                                STATUS
                            FROM
                                `{self.project_curated}.domain_payments.EFT_EXTORIGIN_REQ_STAT`
                            WHERE
                                STATUS = 'REQUEST-ACCEPTED'
                        ) AS EFT_EXTORIGIN_REQ_STAT
                        ON EFT_EXTORIGIN_REQ.EFT_EXTORIGIN_REQ_UID = EFT_EXTORIGIN_REQ_STAT.EFT_EXTORIGIN_REQ_UID
                    ) EFTEXTORIGINREQ
                    ON EFTEXTORIGINREQ.F_FMI_ID = FM_GL_REQ.FUNDSMOVE_INSTRUCTION_ID
                    AND EXTRACT(DATE FROM EFTEXTORIGINREQ.F_PROCESS_DT) = EXTRACT(DATE FROM FM_GL_REQ.FUNDSMOVE_PROCESSING_DT)
                    AND EFTEXTORIGINREQ.EXT_AMT = FM_GL_REQ.AMOUNT
                    AND EFTEXTORIGINREQ.REC_RANK = 1
            """
        sql_join = f"""
          {self.funds_gl_join}
          {self.user_detail_join}
          /* npfi npfo */
          {rail_join}
          /* npfi npfo */
        """
        return sql_join

    def get_external_cust_name(self) -> str:
        return "EFTEXTORIGINREQ.EXT_CUST_LONGNAME"

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
        external_bank_number = "EFTEXTORIGINREQ.EXT_INST_ID || " \
                               + "'-' || EFTEXTORIGINREQ.EXT_TRANSIT_NO || " \
                               + "'-' || EFTEXTORIGINREQ.EXT_ACCT_NO "
        return external_bank_number
