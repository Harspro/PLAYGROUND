MERGE INTO {dest_gcp_table} AS tsys_image_metadata USING (
  SELECT
    '{string_position_0}' AS PARENT_CASE_ID,
    '{string_position_1}' AS CASE_ID,
    '{string_position_2}' AS UNIQUE_DOCUMENT_ID,
    '{src_gcp_project}' AS GCP_PROJECT,
    '{src_gcp_bucket}' AS GCP_BUCKET,
    '{src_gcp_folder}' AS FOLDER,
    '{file_name}' AS FILE_NAME,
    SAFE_CAST('{execution_datetime}' AS DATETIME) AS REC_LOAD_TIMESTAMP,
    '{dag_id}' AS JOB_ID
) AS inbound_data ON tsys_image_metadata.UNIQUE_DOCUMENT_ID = inbound_data.UNIQUE_DOCUMENT_ID
WHEN NOT MATCHED THEN
INSERT (
  PARENT_CASE_ID,
  CASE_ID,
  GCP_PROJECT,
  GCP_BUCKET,
  FOLDER,
  FILE_NAME,
  REC_LOAD_TIMESTAMP,
  JOB_ID,
  UNIQUE_DOCUMENT_ID
)
VALUES (
  inbound_data.PARENT_CASE_ID,
  inbound_data.CASE_ID,
  inbound_data.GCP_PROJECT,
  inbound_data.GCP_BUCKET,
  inbound_data.FOLDER,
  inbound_data.FILE_NAME,
  inbound_data.REC_LOAD_TIMESTAMP,
  inbound_data.JOB_ID,
  inbound_data.UNIQUE_DOCUMENT_ID
)
WHEN MATCHED THEN
UPDATE SET
  PARENT_CASE_ID = inbound_data.PARENT_CASE_ID,
  CASE_ID = inbound_data.CASE_ID,
  GCP_PROJECT = inbound_data.GCP_PROJECT,
  GCP_BUCKET = inbound_data.GCP_BUCKET,
  FOLDER = inbound_data.FOLDER,
  FILE_NAME = inbound_data.FILE_NAME,
  REC_LOAD_TIMESTAMP = inbound_data.REC_LOAD_TIMESTAMP;