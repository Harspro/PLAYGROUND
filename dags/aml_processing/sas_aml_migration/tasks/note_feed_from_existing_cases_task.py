from aml_processing.sas_aml_migration.tasks.sas_aml_migration_task import SASAMLMigrationTask
from aml_processing.sas_aml_migration.const import SASAMLMigrationConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log


class NoteFeedFromExistingCases(SASAMLMigrationTask):

    def execute(self):
        self.note_feed_extraction_from_existing_SAS_cases()

    def note_feed_extraction_from_existing_SAS_cases(self):
        sql = f"""
                INSERT INTO `{SASAMLMigrationConst.TBL_CURATED_NOTE_FEED}` (
                    CASE_FILE_NUMBER,
                    NOTE_SUBJECT,
                    NOTE_DETAIL,
                    NOTE_DATE_TIME
                )
                SELECT DISTINCT
                  c.case_id AS caseFileNumber,
                  CONCAT('NOTE', ' ', n.author_id) AS notesubject,
                  (CASE WHEN comment_txt = '' THEN NULL ELSE REGEXP_REPLACE(REGEXP_REPLACE(comment_txt, r'|', ';'), r'\\r?\\n', '*newline*') END) AS noteDetail,
                  n.created_dttm AS noteDateTime
                FROM
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` c
                JOIN
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}` n ON CAST(c.case_rk AS STRING) = n.object_id
                WHERE
                  LENGTH(n.comment_txt) <= 6000
                  AND
                  n.object_type_id = 545000

                UNION DISTINCT

                SELECT DISTINCT
                  c.case_id AS caseFileNumber,
                  CONCAT('NOTE', ' ', n.author_id, ' (', (pos + 1), '/', CEILING(LENGTH(comment_txt) / 6000), ')') AS notesubject,
                  (CASE WHEN comment_txt = '' THEN NULL ELSE SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(comment_txt, r'|', ';'), r'\\r?\\n', '*newline*'), (CAST(pos AS INT64) * 6000) + 1, 6000) END) AS noteDetail,
                  n.created_dttm AS noteDateTime
                FROM
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}`  n
                JOIN `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` c ON CAST(c.case_rk AS STRING) = n.object_id
                JOIN
                  UNNEST(GENERATE_ARRAY(0, (LENGTH(comment_txt) - 1) / 6000)) AS pos
                WHERE
                  LENGTH(n.comment_txt) > 6000
                AND
                  n.object_type_id = 545000

                UNION DISTINCT

                SELECT DISTINCT
                  c.case_id AS caseFileNumber,
                  'ATTACHMENT' AS noteSubject,
                  CONCAT(a.attachment_id, '-', a.attachment_nm) AS noteDetail,
                  a.created_dttm AS noteDateTime
                FROM
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_LIVE}` c
                JOIN
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_COMMENT}` n ON CAST(c.case_rk AS STRING) = n.object_id
                JOIN
                  `{SASAMLMigrationConst.TBL_LANDING_CASE_ATTACHMENT}` a ON CAST(n.comment_id AS STRING) = a.object_id
                WHERE n.object_type_id = 545000
        """
        run_bq_dml_with_log(
            pre='Inserting existing SAS AML Note Feed with short notes',
            post='Finished inserting existing SAS AML cases with short notes into Note Feed table',
            sql=sql)
