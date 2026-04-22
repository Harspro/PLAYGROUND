CREATE OR REPLACE TABLE pcb-{env}-processing.domain_account_management.AT41_COMMON_INFO_TEMP
OPTIONS (
expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
)
AS
SELECT
  EXTRACT(YEAR FROM FILE_CREATE_DT) AS YYYY,
  EXTRACT(MONTH FROM FILE_CREATE_DT) AS MM,
  EXTRACT(DAY FROM FILE_CREATE_DT) AS DD,
  (select MAX(REC_LOAD_DT_TM_STAMP) FROM pcb-{env}-processing.domain_account_management.AT00_TEMP ) AS REC_LOAD_DT_TM_STAMP,
  * EXCEPT (FILE_CREATE_DT,REC_LOAD_TIMESTAMP)

FROM
  pcb-{env}-landing.domain_account_management.AT41_COMMON_INFO
WHERE
    file_create_dt = '{file_create_dt}'