from aml_processing.sas_aml_migration.tasks.sas_aml_migration_task import SASAMLMigrationTask
from aml_processing.sas_aml_migration.const import SASAMLMigrationConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from util.miscutils import read_variable_or_file

DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']
TBL_STG_SAS_DERIVED_CASE = f"`pcb-{DEPLOY_ENV}-processing.domain_aml.STG_SAS_DERIVED_CASE`"


class CaseFeedFromDerivedCases(SASAMLMigrationTask):

    def execute(self):
        self._create_stg_derived_case()
        self._2_1_notes_linked_acct_cust()
        self._2_2_notes_linked_alert()
        self._2_3_attachments_linked_acct_cust()
        self._2_4_attachments_linked_alerts()
        self._2_6_insert_into_case_feed_table()
        self._2_7_insert_into_note_feed_table()
        self._2_8_insert_into_person_feed_table()
        self._2_9_create_person_derived_feed()

    def _create_stg_derived_case(self):
        sql = f"""CREATE OR REPLACE TABLE {TBL_STG_SAS_DERIVED_CASE} (
                CASE_FILE_NUMBER STRING,
                NOTE_SUBJECT STRING,
                NOTE_DETAIL STRING,
                NOTE_DATE_TIME DATETIME)
        """
        run_bq_dml_with_log(
            pre=f"Creating staging table: {TBL_STG_SAS_DERIVED_CASE}",
            post=f"Completed creating staging table:{TBL_STG_SAS_DERIVED_CASE}",
            sql=sql)

    def _2_1_notes_linked_acct_cust(self):
        sql = f""" --2.1
            INSERT INTO {TBL_STG_SAS_DERIVED_CASE} (
                CASE_FILE_NUMBER,
                NOTE_SUBJECT,
                NOTE_DETAIL,
                NOTE_DATE_TIME)
            SELECT
                'CASE-' || SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)) AS caseFileNumber
                ,'NOTE' || ' ' || n.author_id          AS noteSubject
                ,n.comment_txt                         AS noteDetail
                ,n.created_dttm                        AS noteDateTime
            FROM `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n
                 INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON CAST (p.PARTY_RK as STRING) = n.object_id
            WHERE p.PARTY_CATEGORY_CD = 'PTY'
                 AND n.object_type_id = 545002
            UNION DISTINCT
            SELECT
                'CASE-' || ACCOUNT_CUSTID.CUSTOMER_IDENTIFIER_NO AS caseFileNumber
                ,'NOTE' || ' ' || NOTE_NOCASE.agent_Name    AS noteSubject
                ,NOTE_NOCASE.NOTE_DETAIL              AS noteDetail
                ,NOTE_NOCASE.note_DateTime            AS noteDateTime
            FROM
                (SELECT
                    LPAD(SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)),11,'0') AS account_no
                    ,n.author_id                           AS agent_Name
                    ,n.comment_txt                         AS note_Detail
                    ,n.created_dttm                        AS note_DateTime
                FROM
                    `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n
                     INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON CAST(p.PARTY_RK as string)= n.object_id
                WHERE  p.PARTY_CATEGORY_CD = 'ACC'
                      AND n.object_type_id = 545002
                ) NOTE_NOCASE
                  INNER JOIN (
                        SELECT a.MAST_ACCOUNT_ID, ci.CUSTOMER_IDENTIFIER_NO
                        FROM `{SASAMLMigrationConst.TBL_CURATED_ACCOUNT_CUSTOMER}` ac
                            ,`{SASAMLMigrationConst.TBL_CURATED_ACCOUNT}` a
                            ,`{SASAMLMigrationConst.TBL_CURATED_CUSTOMER_IDENTIFIER}` ci
                        WHERE
                            ac.ACCOUNT_UID = a.ACCOUNT_UID
                            AND ac.CUSTOMER_UID = ci.CUSTOMER_UID
                            AND ci.type = 'PCF-CUSTOMER-ID'
                            AND ci.disabled_ind = 'N'
                            AND ac.ACTIVE_IND = 'Y'
                            AND ac.account_customer_role_uid = 1
                        ) ACCOUNT_CUSTID ON NOTE_NOCASE.account_no = ACCOUNT_CUSTID.MAST_ACCOUNT_ID
        """
        run_bq_dml_with_log(
            pre='2.1 Inserting notes linked to account or customer',
            post='2.1 Finished inserting notes linked to account or customer',
            sql=sql)

    def _2_2_notes_linked_alert(self):
        sql = f""" --2.2
            INSERT INTO {TBL_STG_SAS_DERIVED_CASE} (
                CASE_FILE_NUMBER,
                NOTE_SUBJECT,
                NOTE_DETAIL,
                NOTE_DATE_TIME)
            SELECT
            'CASE-' || SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)) AS caseFileNumber
            ,i.incident_id || ' ' || n.author_id          AS noteSubject
            ,n.comment_txt                                AS noteDetail
            ,n.created_dttm                               AS noteDateTime
            FROM  `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_LIVE}`   i
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_X_PARTY}`  ixp ON i.incident_rk = ixp.incident_rk
                   inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON ixp.party_rk = p.party_rk
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON cast(i.INCIDENT_RK as string) = n.object_id
            WHERE p.party_category_cd = 'PTY'
                  AND n.object_type_id=545001
            UNION DISTINCT
            SELECT
                'CASE-' || account_custid.mast_account_id     AS casefilenumber,
                INCIDENT_NOTE_NOCASE.incident_id || ' ' ||INCIDENT_NOTE_NOCASE.agent_name         AS notesubject,
                INCIDENT_NOTE_NOCASE.note_detail                   AS notedetail,
                INCIDENT_NOTE_NOCASE.note_datetime                 AS notedatetime
            FROM
                (
                    SELECT
                        lpad(substr(p.party_id,(instr(p.party_id, '-') + 1)), 11, '0')                       AS account_no
                        ,i.incident_id                                                                       AS incident_id
                        ,n.author_id                                                                         AS agent_name
                        ,n.comment_txt                                                                       AS note_Detail
                        ,n.created_dttm                                                                      AS note_datetime
                    FROM `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_LIVE}`   i
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_X_PARTY}`  ixp ON i.incident_rk = ixp.incident_rk
                   inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON ixp.party_rk = p.party_rk
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON cast(i.INCIDENT_RK as string) = n.object_id
                    WHERE  p.party_category_cd = 'ACC'
                        AND n.object_type_id=545001
                ) INCIDENT_NOTE_NOCASE
            INNER JOIN (
                  SELECT
                      a.mast_account_id,
                      ci.customer_identifier_no
                  FROM
                      `{SASAMLMigrationConst.TBL_CURATED_ACCOUNT_CUSTOMER}` ac
                      ,`{SASAMLMigrationConst.TBL_CURATED_ACCOUNT}` a
                      ,`{SASAMLMigrationConst.TBL_CURATED_CUSTOMER_IDENTIFIER}` ci
                  WHERE
                      ac.account_uid = a.account_uid
                      AND ac.customer_uid = ci.customer_uid
                      AND ci.type = 'PCF-CUSTOMER-ID'
                      AND ci.disabled_ind = 'N'
                      AND ac.active_ind = 'Y'
                      AND ac.account_customer_role_uid = 1
              ) ACCOUNT_CUSTID
                ON INCIDENT_NOTE_NOCASE.account_no = account_custid.mast_account_id
        """
        run_bq_dml_with_log(
            pre='2.2. Inserting notes linked to alert',
            post='2.2. Finished inserting notes linked to alert',
            sql=sql)

    def _2_3_attachments_linked_acct_cust(self):
        sql = f""" --2.3
            INSERT INTO {TBL_STG_SAS_DERIVED_CASE} (
                CASE_FILE_NUMBER,
                NOTE_SUBJECT,
                NOTE_DETAIL,
                NOTE_DATE_TIME)
            SELECT
                'CASE-' || SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)) AS caseFileNumber
                ,'ATTACHMENT'                               AS noteSubject
                ,a.attachment_id || '-' || a.attachment_nm  AS noteDetail
                ,a.created_dttm                             AS noteDateTime
            FROM
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_ATTACHMENT}`  a
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON a.object_id = cast(n.comment_id as STRING)
                   inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON cast (p.PARTY_RK as string)= n.object_id
            WHERE p.party_category_cd = 'PTY'
            AND n.object_type_id=545002
            UNION DISTINCT
            SELECT
                'CASE-' || ACCOUNT_CUSTID.CUSTOMER_IDENTIFIER_NO    AS caseFileNumber
                ,'ATTACHMENT'                                       AS noteSubject
                ,ATTACHMENT_NOCASE.attachment_FileName              AS noteDetail
                ,ATTACHMENT_NOCASE.attachment_DateTime              AS noteDateTime
            FROM
            (
            SELECT
                LPAD(SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)),11,'0') AS account_no
                ,a.attachment_id || '-' || a.attachment_nm  AS attachment_FileName
                ,a.created_dttm                             AS attachment_DateTime
            FROM `{SASAMLMigrationConst.TBL_LANDING_CASE_ATTACHMENT}`  a
                   INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON a.object_id = cast(n.comment_id as STRING)
                   inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON cast (p.PARTY_RK as string)= n.object_id
            WHERE p.party_category_cd = 'ACC'
            AND n.object_type_id=545002
            ) ATTACHMENT_NOCASE
            INNER JOIN (
                      SELECT a.MAST_ACCOUNT_ID, ci.CUSTOMER_IDENTIFIER_NO
                      FROM `{SASAMLMigrationConst.TBL_CURATED_ACCOUNT_CUSTOMER}` ac
                      ,`{SASAMLMigrationConst.TBL_CURATED_ACCOUNT}` a
                      ,`{SASAMLMigrationConst.TBL_CURATED_CUSTOMER_IDENTIFIER}` ci
                      WHERE
                        ac.ACCOUNT_UID = a.ACCOUNT_UID
                        AND ac.CUSTOMER_UID = ci.CUSTOMER_UID
                        AND ci.type = 'PCF-CUSTOMER-ID'
                        AND ci.disabled_ind = 'N'
                        AND ac.ACTIVE_IND = 'Y'
                        AND ac.account_customer_role_uid = 1
                      ) ACCOUNT_CUSTID
                      ON ATTACHMENT_NOCASE.account_no = ACCOUNT_CUSTID.MAST_ACCOUNT_ID"""
        run_bq_dml_with_log(
            pre='2.3. Inserting attachments linked to account or customer',
            post='2.3. Finished inserting attachments linked to account or customer',
            sql=sql)

    def _2_4_attachments_linked_alerts(self):
        sql = f""" --2.4
            INSERT INTO {TBL_STG_SAS_DERIVED_CASE} (
                CASE_FILE_NUMBER,
                NOTE_SUBJECT,
                NOTE_DETAIL,
                NOTE_DATE_TIME)
            SELECT
                'CASE-' || SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)) AS CASE_FILE_NUMBER
                ,i.incident_id || ' ' ||'ATTACHMENT'         AS NOTE_SUBJECT
                ,a.attachment_id || '-' || a.attachment_nm   AS NOTE_DETAIL
                ,a.created_dttm                              AS NOTE_DATE_TIME
                FROM
                       `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_LIVE}`   i
                       INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_X_PARTY}`  ixp ON i.incident_rk = ixp.incident_rk
                       inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON ixp.party_rk = p.party_rk
                       INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON cast(i.INCIDENT_RK as string) = n.object_id
                       INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_ATTACHMENT}`  a ON  a.object_id = cast(n.comment_id as STRING)
                WHERE  p.party_category_cd = 'PTY'
                       AND n.object_type_id=545001
                UNION DISTINCT
                SELECT
                    'CASE-' ||  ACCOUNT_CUSTID.CUSTOMER_IDENTIFIER_NO   AS casefilenumber
                    ,ATTACHMENT_NOCASE.incident_id || ' ' || 'ATTACHMENT'     AS noteSubject
                    ,ATTACHMENT_NOCASE.attachment_FileName              AS noteDetail
                    ,ATTACHMENT_NOCASE.attachment_DateTime              AS noteDateTime
                FROM
                    (
                        SELECT
                            LPAD(SUBSTR(p.party_id, (INSTR(p.party_id, '-') + 1)),11,'0') AS account_no
                            , i.incident_id
                            ,a.attachment_id || '-' || a.attachment_nm  AS attachment_FileName
                            ,a.created_dttm                             AS attachment_DateTime  -- TBD: should this be date of incident or attachment?
                        FROM
                             `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_LIVE}`   i
                            INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_INCIDENT_X_PARTY}`  ixp ON i.incident_rk = ixp.incident_rk
                            inner join `{SASAMLMigrationConst.TBL_LANDING_PARTY_LIVE}` p ON ixp.party_rk = p.party_rk
                            INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n ON cast(i.INCIDENT_RK as string) = n.object_id
                            INNER JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_ATTACHMENT}`  a ON  a.object_id = cast(n.comment_id as STRING)
                        WHERE p.party_category_cd = 'ACC'
                        AND n.object_type_id=545001
                    ) ATTACHMENT_NOCASE
                INNER JOIN (
                            SELECT
                            a.mast_account_id,
                            ci.customer_identifier_no
                            FROM
                            `{SASAMLMigrationConst.TBL_CURATED_ACCOUNT_CUSTOMER}` ac
                            ,`{SASAMLMigrationConst.TBL_CURATED_ACCOUNT}` a
                            ,`{SASAMLMigrationConst.TBL_CURATED_CUSTOMER_IDENTIFIER}` ci
                            WHERE
                            ac.account_uid = a.account_uid
                            AND ac.customer_uid = ci.customer_uid
                            AND ci.type = 'PCF-CUSTOMER-ID'
                            AND ci.disabled_ind = 'N'
                            AND ac.active_ind = 'Y'
                            AND ac.account_customer_role_uid = 1
                    ) ACCOUNT_CUSTID ON ATTACHMENT_NOCASE.account_no = account_custid.mast_account_id"""
        run_bq_dml_with_log(
            pre='2.4. Inserting notes linked to account or customer',
            post='2.4. Finished inserting notes linked to account or customer',
            sql=sql)

    def _2_6_insert_into_case_feed_table(self):
        sql = f""" --2.6
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
                CASE_FILE_NUMBER AS CASE_FILE_NUMBER
                ,NULL as CASE_NAME
                , 'CLOSED' as STATUS
                , NULL as CASE_TYPE
                , NULL as CASE_SUB_TYPE
                , PARSE_DATE('%Y%m%d','20230601') as OPEN_DATE
                , PARSE_DATE('%Y%m%d','20230601') as CLOSE_DATE
                , PARSE_DATE('%Y%m%d','20230601') as RESOLVED_DATE
                , NULL as EXPIRY_DATE
                , NULL as REVIEW_DATE
                , NULL as DISPUTE_OPEN_DATE
                , NULL as POLICE_FILE_NUMBER
                , NULL as DESCRIPTION
                , NULL as NARRATIVE
                , 'FALSE_ALARM' as RESOLUTION
                , 'NO_ACTION' as CASE_ACTION_TYPE
                , NULL as POTENTIAL_LOSSES
                , NULL as PREVENTED_LOSSES
                , NULL as RECOVERED_TOTAL
                , NULL as ACTUAL_LOSSES
                , NULL as MONEY_LAUNDERED
                , NULL as INSURANCE_PAID
                , NULL as SUSPICIOUS_FUNDS
                , 'Kamila.Llacuna@pcbank.ca' as CASE_OWNER
            FROM
                (
                    SELECT distinct CASE_FILE_NUMBER
                    FROM {TBL_STG_SAS_DERIVED_CASE}
                )
        """
        run_bq_dml_with_log(
            pre='2.6. Inserting notes linked to account or customer',
            post='2.6. Finished inserting notes linked to account or customer',
            sql=sql)

    def _2_8_insert_into_person_feed_table(self):
        sql = f""" --2.8
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
                 SELECT
                        x.CASE_FILE_NUMBER
                        ,x.customer_identifier_no as CUSTOMER_NUMBER
                        , NULL as TAX_ID
                        , NULL as FIRST_NAME
                        , NULL as LAST_NAME
                        , NULL as MIDDLE_NAME
                        , NULL as AKA
                        , 'OTHER' as CASE_ROLE
                        , NULL as DATE_OF_BIRTH
                        , NULL as ADDRESS
                        , NULL as CITY
                        , NULL as STATE
                        , NULL as ZIP
                        , NULL as COUNTRY
                        , NULL as PHONE
                        , NULL as SECONDARY_ID_NUMBER
                        , NULL as SECONDARY_ID_TYPE
                        , NULL as EMPLOYER
                        , NULL as OCCUPATION
                        FROM
                             (
                               SELECT caseNumberWithAccount.CASE_FILE_NUMBER,
                                      accToCust.customer_identifier_no
                                FROM (
                                      SELECT distinct CASE_FILE_NUMBER
                                      FROM {TBL_STG_SAS_DERIVED_CASE}
                                      WHERE STARTS_WITH(CASE_FILE_NUMBER, 'CASE-')
                                         AND LENGTH (CASE_FILE_NUMBER) < 18
                                      ) caseNumberWithAccount
                               INNER JOIN
                               ( SELECT
                                    a.mast_account_id
                                    ,ci.customer_identifier_no
                                    ,ROW_NUMBER() OVER (PARTITION BY a.mast_account_id ORDER BY a.UPDATE_DT DESC) r_rank
                                 FROM
                                    `{SASAMLMigrationConst.TBL_CURATED_ACCOUNT_CUSTOMER}` ac
                                    ,`{SASAMLMigrationConst.TBL_CURATED_ACCOUNT}` a
                                    ,`{SASAMLMigrationConst.TBL_CURATED_CUSTOMER_IDENTIFIER}` ci
                                 WHERE
                                    ac.account_uid = a.account_uid
                                    AND ac.customer_uid = ci.customer_uid
                                    AND ci.type = 'PCF-CUSTOMER-ID'
                                    AND ci.disabled_ind = 'N'
                                    AND ac.active_ind = 'Y'
                                    AND ac.account_customer_role_uid = 1
                                ) accToCust ON accToCust.mast_account_id = SUBSTR(caseNumberWithAccount.CASE_FILE_NUMBER, (INSTR(caseNumberWithAccount.CASE_FILE_NUMBER, '-') + 1))
                                WHERE accToCust.r_rank = 1
                                UNION ALL
                                SELECT DISTINCT  CASE_FILE_NUMBER
                                      ,SUBSTR(CASE_FILE_NUMBER, (INSTR(CASE_FILE_NUMBER, '-') + 1)) customer_identifier_no
                                FROM {TBL_STG_SAS_DERIVED_CASE}
                                WHERE STARTS_WITH(CASE_FILE_NUMBER, 'CASE-')
                                     AND LENGTH (CASE_FILE_NUMBER) >= 18
                            ) x
                        """
        run_bq_dml_with_log(
            pre='2.8. Inserting person feed from derived cases',
            post='2.8. Finished inserting person feed from derived cases',
            sql=sql)

    def _2_7_insert_into_note_feed_table(self):
        sql = f""" --2.7
               INSERT INTO `{SASAMLMigrationConst.TBL_CURATED_NOTE_FEED}` (
                    CASE_FILE_NUMBER,
                    NOTE_SUBJECT,
                    NOTE_DETAIL,
                    NOTE_DATE_TIME
                )
                SELECT CASE_FILE_NUMBER,
                       CASE WHEN NOTE_SUBJECT = '' THEN NULL ELSE NOTE_SUBJECT END AS NOTE_SUBJECT,
                       CASE WHEN NOTE_DETAIL = '' THEN
                         NULL
                       ELSE
                         REGEXP_REPLACE(REGEXP_REPLACE(NOTE_DETAIL, r'\\|', ';'), r'\\r?\\n', '*newline*')
                       END AS NOTE_DETAIL,
                       NOTE_DATE_TIME
                FROM {TBL_STG_SAS_DERIVED_CASE}
                WHERE LENGTH(NOTE_DETAIL) <= 6000
                UNION ALL
                SELECT CASE_FILE_NUMBER,
                       CONCAT(NOTE_SUBJECT,' (', (pos + 1), '/', CEILING(LENGTH(NOTE_DETAIL) / 6000), ')') AS NOTE_SUBJECT,
                       CASE WHEN NOTE_DETAIL = '' THEN
                        NULL
                       ELSE
                        SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(NOTE_DETAIL, r'\\|', ';'), r'\\r?\\n', '*newline*'), (CAST(pos AS INT64) * 6000) + 1, 6000)
                       END AS NOTE_DETAIL,
                       NOTE_DATE_TIME
                FROM {TBL_STG_SAS_DERIVED_CASE}
                INNER JOIN UNNEST(GENERATE_ARRAY(0, (LENGTH(NOTE_DETAIL) - 1) / 6000)) AS pos
                WHERE LENGTH(NOTE_DETAIL)  > 6000
          """
        run_bq_dml_with_log(
            pre='2.7. Inserting notes feed from derived cases',
            post='2.7. Finished inserting notes feed from derived cases',
            sql=sql)

    def _2_9_create_person_derived_feed(self):
        sql = f"""CREATE OR REPLACE TABLE `{SASAMLMigrationConst.TBL_CURATED_PERSON_DERIVED_FEED}` AS
                 SELECT p.*
                 FROM `{SASAMLMigrationConst.TBL_CURATED_PERSON_FEED}` p
                 WHERE STARTS_WITH(p.CASE_FILE_NUMBER, 'CASE-')
                 AND LENGTH (CASE_FILE_NUMBER) < 18
                """
        run_bq_dml_with_log(
            pre='2.9. Creating table person derived feed ',
            post='2.9. Finished creating table person derived feed ',
            sql=sql)
