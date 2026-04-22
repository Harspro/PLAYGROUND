EXPORT DATA
  OPTIONS  (
    uri = 'gs://pcb-{env}-staging-extract/bill-refresh-process/bill-refresh-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
  BILLER_ID,
  BILLER_NAME,
  BILLER_PROVINCE,
  BILLER_CITY,
  BILLER_STATUS,
FROM
  `pcb-{env}-landing.domain_payments.BILLER_REFRESH_RAW`
WHERE
  FILE_NAME = '<<FILE_NAME>>'
);