EXPORT DATA
  OPTIONS  (
    uri = 'gs://pcb-{env}-staging-extract/tsys-pcb-customer-level-status/tsys-pcb-customer-level-status-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    cast(mast_customer_id as string) as mast_customer_id,
    cast(cm05_bankruptcy_type AS string) AS status_cm05_bankruptcy_type,
    FORMAT_TIMESTAMP('%FT%H:%M:%E9S', CURRENT_TIMESTAMP()) AS eventTime,
    ROW_NUMBER() OVER (PARTITION BY mast_customer_id ORDER BY file_create_dt DESC) AS row_num
  FROM `pcb-{env}-curated.domain_customer_management.CM05`
)
WHERE row_num = 1
)