SELECT
  case_details.*,
  image_metadata.UNIQUE_DOCUMENT_ID,
  image_metadata.GCP_PROJECT,
  image_metadata.GCP_BUCKET,
  image_metadata.FOLDER,
  image_metadata.FILE_NAME
FROM
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS` case_details
LEFT JOIN
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA` image_metadata
ON case_details.CASE_ID = image_metadata.CASE_ID;
