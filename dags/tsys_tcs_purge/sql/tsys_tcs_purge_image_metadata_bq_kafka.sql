EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/tsys-tcs-case-purge-images/tsys-tcs-purge-case-images-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE
  )
AS
WITH
  tsys_purge_image_metadata_latest_load AS (
  SELECT
    MAX(img_ll.rec_load_timestamp) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA` AS img_ll )
SELECT
  tsys_purge.PARENT_CASE_ID,
  tsys_purge.CASE_ID,
  tsys_purge.GCP_PROJECT,
  tsys_purge.GCP_BUCKET,
  tsys_purge.FOLDER,
  tsys_purge.FILE_NAME
FROM
  `pcb-{env}-curated.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA` tsys_purge
INNER JOIN
  tsys_purge_image_metadata_latest_load img_ll
ON
  img_ll.LATEST_REC_LOAD_TIMESTAMP = tsys_purge.REC_LOAD_TIMESTAMP;