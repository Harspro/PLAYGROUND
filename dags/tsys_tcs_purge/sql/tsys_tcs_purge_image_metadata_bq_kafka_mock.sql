CREATE
OR REPLACE TABLE `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK` (
  PARENT_CASE_ID STRING,
  CASE_ID STRING,
  GCP_PROJECT STRING,
  GCP_BUCKET STRING,
  FOLDER STRING,
  FILE_NAME STRING
) OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE),
  description = 'This table is use by TCS Purge GCS to Kafka Mock records SQL file.'
);

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`(
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  )
VALUES
  (
    'C-9124',
    'D-50578',
    'pcb-{env}-landing',
    'tsys-outbound-pcb{env_suffix}',
    'casepurge_images',
    'C-9124_D-50578_2025011501.png'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`(
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  )
VALUES
  (
    'C-9235',
    'D-50689',
    'pcb-{env}-landing',
    'tsys-outbound-pcb{env_suffix}',
    'casepurge_images',
    'C-9235_D-50689_2025011502.png'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`(
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  )
VALUES
  (
    'C-9346',
    'D-50790',
    'pcb-{env}-landing',
    'tsys-outbound-pcb{env_suffix}',
    'casepurge_images',
    'C-9346_D-50790_2025011503.png'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`(
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  )
VALUES
  (
    'C-9457',
    'D-50891',
    'pcb-{env}-landing',
    'tsys-outbound-pcb{env_suffix}',
    'casepurge_images',
    'C-9457_D-50891_2025011504.png'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`(
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  )
VALUES
  (
    'C-9568',
    'D-50992',
    'pcb-{env}-landing',
    'tsys-outbound-pcb{env_suffix}',
    'casepurge_images',
    'C-9568_D-50992_2025011505.png'
  );

EXPORT DATA OPTIONS (
  uri = 'gs://pcb-{env}-staging-extract/tsys-tcs-case-purge-images/tsys-tcs-purge-case-mock-images-*.parquet',
  format = 'PARQUET',
  OVERWRITE = TRUE
) AS (
  SELECT
    PARENT_CASE_ID,
    CASE_ID,
    GCP_PROJECT,
    GCP_BUCKET,
    FOLDER,
    FILE_NAME
  FROM
    `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_IMAGE_METADATA_MOCK`
)