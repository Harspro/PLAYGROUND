from aml_processing.sas_aml_migration.tasks.sas_aml_migration_task import SASAMLMigrationTask
from aml_processing.sas_aml_migration.const import SASAMLMigrationConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log


class CaseFeedFromExistingCases(SASAMLMigrationTask):

    def execute(self):
        self.case_feed_extraction_from_existing_SAS_cases()

    def case_feed_extraction_from_existing_SAS_cases(self):
        sql = f"""
            INSERT INTO `{SASAMLMigrationConst.TBL_CURATED_CASE_FEED}` (
                CASE_FILE_NUMBER,
                CASE_NAME,
                STATUS,
                CASE_TYPE,
                CASE_SUB_TYPE,
                OPEN_DATE,
                CLOSE_DATE,
                RESOLVED_DATE,
                EXPIRY_DATE,
                REVIEW_DATE,
                DISPUTE_OPEN_DATE,
                POLICE_FILE_NUMBER,
                DESCRIPTION,
                NARRATIVE,
                RESOLUTION,
                CASE_ACTION_TYPE,
                POTENTIAL_LOSSES,
                PREVENTED_LOSSES,
                RECOVERED_TOTAL,
                ACTUAL_LOSSES,
                MONEY_LAUNDERED,
                INSURANCE_PAID,
                SUSPICIOUS_FUNDS,
                CASE_OWNER
            )
            SELECT
                cl.CASE_ID                                                      AS CASE_FILE_NUMBER,
                NULL                                                            AS CASE_NAME,
                'CLOSED'                                                        AS STATUS,
                NULL                                                            AS CASE_TYPE,
                NULL                                                            AS CASE_SUB_TYPE,
                EXTRACT(DATE FROM cl.OPEN_DTTM)                                 AS OPEN_DATE,
                EXTRACT(DATE FROM cl.CLOSE_DTTM)                                AS CLOSE_DATE,
                EXTRACT(DATE FROM cl.CLOSE_DTTM)                                AS RESOLVED_DATE,
                NULL                                                            AS EXPIRY_DATE,
                NULL                                                            AS REVIEW_DATE,
                NULL                                                            AS DISPUTE_OPEN_DATE,
                NULL                                                            AS POLICE_FILE_NUMBER,
                NULL                                                            AS DESCRIPTION,
                NULL                                                            AS NARRATIVE,
                CASE
                    WHEN cl.CASE_DISPOSITION_CD = 'STRFILED' THEN 'SUSPICIOUS'
                    ELSE 'FALSE_ALARM'
                    END                                                         AS RESOLUTION,
                CASE
                    WHEN cl.CASE_DISPOSITION_CD = 'STRFILED' THEN 'SAR_FILED'
                    ELSE 'NO_ACTION'
                    END                                                         AS CASE_ACTION_TYPE,
                NULL                                                            AS POTENTIAL_LOSSES,
                NULL                                                            AS PREVENTED_LOSSES,
                NULL                                                            AS RECOVERED_TOTAL,
                NULL                                                            AS ACTUAL_LOSSES,
                NULL                                                            AS MONEY_LAUNDERED,
                NULL                                                            AS INSURANCE_PAID,
                NULL                                                            AS SUSPICIOUS_FUNDS,
                can.EMAIL_ADDRESS                                               AS CASE_OWNER
            FROM
                `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` cl
                LEFT JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_AGENT_NAMES}` can
                ON UPPER(cl.UPDATE_USER_ID) = UPPER(can.CASE_AGENT_NAME)
        """
        run_bq_dml_with_log(
            pre='Inserting existing SAS AML cases',
            post='Finished inserting existing SAS AML cases into Case Feed table',
            sql=sql)
