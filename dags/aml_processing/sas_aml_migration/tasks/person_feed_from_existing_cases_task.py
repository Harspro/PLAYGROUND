from aml_processing.sas_aml_migration.tasks.sas_aml_migration_task import SASAMLMigrationTask
from aml_processing.sas_aml_migration.const import SASAMLMigrationConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log
import logging


class PersonFeedFromExistingCases(SASAMLMigrationTask):

    def execute(self):
        logging.info('Starting the Person Feed Extraction from existing SAS Cases Task.')

        # Extract All Person-to-Case records
        self.insert_all_person_to_case_records_from_existing_SAS_cases()

        # Extract other Person-to-Case records based on available Account-to-Case records
        self.insert_person_to_case_records_from_account_to_case()

        logging.info('Finished the Person Feed Extraction from existing SAS Cases Task.')

    def insert_all_person_to_case_records_from_existing_SAS_cases(self):
        sql = f"""
            INSERT INTO `{SASAMLMigrationConst.TBL_CURATED_PERSON_FEED}`
            (
                CASE_FILE_NUMBER,
                CUSTOMER_NUMBER,
                TAX_ID,
                FIRST_NAME,
                LAST_NAME,
                MIDDLE_NAME,
                AKA,
                CASE_ROLE,
                DATE_OF_BIRTH,
                ADDRESS,
                CITY,
                STATE,
                ZIP,
                COUNTRY,
                PHONE,
                SECONDARY_ID_NUMBER,
                SECONDARY_ID_TYPE,
                EMPLOYER,
                OCCUPATION
            )
            SELECT DISTINCT
                c.CASE_ID                                             AS CASE_FILE_NUMBER,
                SUBSTR(p.party_id , (INSTR(p.party_id , '-') + 1))    AS CUSTOMER_NUMBER,
                SAFE_CAST(NULL AS STRING)                             AS TAX_ID,
                SAFE_CAST(NULL AS STRING)                             AS FIRST_NAME,
                SAFE_CAST(NULL AS STRING)                             AS LAST_NAME,
                SAFE_CAST(NULL AS STRING)                             AS MIDDLE_NAME,
                SAFE_CAST(NULL AS STRING)                             AS AKA,
                'SUSPECT'                                             AS CASE_ROLE,
                SAFE_CAST(NULL AS DATE)                               AS DATE_OF_BIRTH,
                SAFE_CAST(NULL AS STRING)                             AS ADDRESS,
                SAFE_CAST(NULL AS STRING)                             AS CITY,
                SAFE_CAST(NULL AS STRING)                             AS STATE,
                SAFE_CAST(NULL AS STRING)                             AS ZIP,
                SAFE_CAST(NULL AS STRING)                             AS COUNTRY,
                SAFE_CAST(NULL AS STRING)                             AS PHONE,
                SAFE_CAST(NULL AS STRING)                             AS SECONDARY_ID_NUMBER,
                SAFE_CAST(NULL AS STRING)                             AS SECONDARY_ID_TYPE,
                SAFE_CAST(NULL AS STRING)                             AS EMPLOYER,
                SAFE_CAST(NULL AS STRING)                             AS OCCUPATION
            FROM
                `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p
                INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_X_PARTY}` cxp
                    ON p.PARTY_RK = cxp.PARTY_RK
                INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` c
                    ON c.CASE_RK = cxp.CASE_RK
                    AND p.PARTY_CATEGORY_CD = 'PTY'
            WHERE
                NOT EXISTS
                (
                    SELECT
                        person.CASE_FILE_NUMBER,
                        person.CUSTOMER_NUMBER
                    FROM
                        `{SASAMLMigrationConst.TBL_CURATED_PERSON_FEED}` person
                    WHERE
                        person.CASE_FILE_NUMBER = c.CASE_ID
                        AND person.CUSTOMER_NUMBER = SUBSTR(p.party_id , (INSTR(p.party_id , '-') + 1))
                )
        """
        run_bq_dml_with_log(
            pre='Inserting All Person-to-Case Records from existing SAS AML cases',
            post='Finished inserting All Person to Case Records into Person Feed table',
            sql=sql)

    def insert_person_to_case_records_from_account_to_case(self):
        sql = f"""
            INSERT INTO `{SASAMLMigrationConst.TBL_CURATED_PERSON_FEED}`
            (
                CASE_FILE_NUMBER,
                CUSTOMER_NUMBER,
                TAX_ID,
                FIRST_NAME,
                LAST_NAME,
                MIDDLE_NAME,
                AKA,
                CASE_ROLE,
                DATE_OF_BIRTH,
                ADDRESS,
                CITY,
                STATE,
                ZIP,
                COUNTRY,
                PHONE,
                SECONDARY_ID_NUMBER,
                SECONDARY_ID_TYPE,
                EMPLOYER,
                OCCUPATION
            )
            SELECT DISTINCT
                account_case.CASE_ID                                  AS CASE_FILE_NUMBER,
                account_custid.CUSTOMER_IDENTIFIER_NO                 AS CUSTOMER_NUMBER,
                SAFE_CAST(NULL AS STRING)                             AS TAX_ID,
                SAFE_CAST(NULL AS STRING)                             AS FIRST_NAME,
                SAFE_CAST(NULL AS STRING)                             AS LAST_NAME,
                SAFE_CAST(NULL AS STRING)                             AS MIDDLE_NAME,
                SAFE_CAST(NULL AS STRING)                             AS AKA,
                'SUSPECT'                                             AS CASE_ROLE,
                SAFE_CAST(NULL AS DATE)                               AS DATE_OF_BIRTH,
                SAFE_CAST(NULL AS STRING)                             AS ADDRESS,
                SAFE_CAST(NULL AS STRING)                             AS CITY,
                SAFE_CAST(NULL AS STRING)                             AS STATE,
                SAFE_CAST(NULL AS STRING)                             AS ZIP,
                SAFE_CAST(NULL AS STRING)                             AS COUNTRY,
                SAFE_CAST(NULL AS STRING)                             AS PHONE,
                SAFE_CAST(NULL AS STRING)                             AS SECONDARY_ID_NUMBER,
                SAFE_CAST(NULL AS STRING)                             AS SECONDARY_ID_TYPE,
                SAFE_CAST(NULL AS STRING)                             AS EMPLOYER,
                SAFE_CAST(NULL AS STRING)                             AS OCCUPATION
            FROM
                (
                    SELECT
                        c.CASE_ID,
                        LPAD(SUBSTR(p.PARTY_ID, (INSTR(p.PARTY_ID, '-') + 1)),11,'0') AS ACCOUNT_NO
                    FROM
                        `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p
                        INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_X_PARTY}` cxp
                            ON p.PARTY_RK = cxp.PARTY_RK
                        INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` c
                            ON c.CASE_RK = cxp.CASE_RK
                            AND p.PARTY_CATEGORY_CD = 'ACC'
                ) AS account_case
                INNER JOIN
                (
                    SELECT
                        a.ACCOUNT_NO,
                        ci.CUSTOMER_IDENTIFIER_NO
                    FROM
                        `{SASAMLMigrationConst.TBL_LANDING_ACCOUNT_CUSTOMER}` ac
                        INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_ACCOUNT}` a
                            ON ac.ACCOUNT_UID = a.ACCOUNT_UID
                        INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CUSTOMER_IDENTIFIER}` ci
                            ON ac.CUSTOMER_UID = ci.CUSTOMER_UID
                            AND ci.TYPE = 'PCF-CUSTOMER-ID'
                            AND ci.DISABLED_IND = 'N'
                            AND ac.ACTIVE_IND = 'Y'
                            AND ac.ACCOUNT_CUSTOMER_ROLE_UID = 1
                ) AS account_custid
                    ON account_case.ACCOUNT_NO = account_custid.ACCOUNT_NO
            WHERE
                NOT EXISTS
                (
                    SELECT
                        person.CASE_FILE_NUMBER,
                        person.CUSTOMER_NUMBER
                    FROM
                        `{SASAMLMigrationConst.TBL_CURATED_PERSON_FEED}` person
                    WHERE
                        person.CASE_FILE_NUMBER = account_case.CASE_ID
                        AND person.CUSTOMER_NUMBER = account_custid.CUSTOMER_IDENTIFIER_NO
                )
        """
        run_bq_dml_with_log(
            pre='Inserting Person-to-Case Records based on available Account-to-Case records',
            post='Finished inserting Person to Case Records into Person Feed table',
            sql=sql)
