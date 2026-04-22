CREATE
OR REPLACE TABLE `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK` (
  RECORD_TYPE STRING,
  PARENT_CASE_ID STRING,
  CASE_ID STRING,
  ACCOUNT_ID STRING,
  WORK_STATUS STRING,
  FRAUD_INDICATOR STRING,
  APPLICATION_NAME STRING
) OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE),
  description = 'This table is use by TCS Purge GCS to Kafka Mock records SQL file.'
);

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK` (
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  )
VALUES
  (
    '002',
    'C-9124',
    'D-50578',
    '00000115446',
    'Rep-ResolvedMIC',
    'Y',
    'FTB-USA-CustomerService-CAP'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK`(
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  )
VALUES
  (
    '002',
    'C-9235',
    'D-50689',
    '00000098679',
    'Resolved-CardholderRebill',
    'Y',
    'FTB-USA-CustomerService-CAP'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK`(
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  )
VALUES
  (
    '002',
    'C-9346',
    'D-50790',
    '00000107910',
    'Resolved-FraudCB',
    'Y',
    'FTB-USA-CustomerService-CAP'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK`(
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  )
VALUES
  (
    '002',
    'C-9457',
    'D-50891',
    '00000101560',
    'Resolved-WriteOff',
    'Y',
    'FTB-USA-CustomerService-CAP'
  );

INSERT INTO
  `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK`(
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  )
VALUES
  (
    '002',
    'C-9568',
    'D-50992',
    '00000118100',
    'Resolved-Completed',
    'Y',
    'FTB-USA-CustomerService-CAP'
  );

EXPORT DATA OPTIONS (
  uri = 'gs://pcb-{env}-staging-extract/tsys-tcs-case-purge-details/tsys-tcs-purge-case-mock-records-*.parquet',
  format = 'PARQUET',
  OVERWRITE = TRUE
) AS (
  SELECT
    RECORD_TYPE,
    PARENT_CASE_ID,
    CASE_ID,
    ACCOUNT_ID,
    WORK_STATUS,
    FRAUD_INDICATOR,
    APPLICATION_NAME
  FROM
    `pcb-{env}-landing.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS_MOCK`
);