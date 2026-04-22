CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT DISTINCT
    '2O' || LPAD(OFF.OFFER_ID, 15, ' ') ||
    IFNULL(LPAD(OFF.OFFER_TYPE, 30, ' '), LPAD(' ', 30, ' ')) ||
    IFNULL(LPAD(FORMAT_DATE('%d-%b-%y', OFF.OFFER_START_DATE), 9, ' '), LPAD(' ', 9, ' ')) ||
    IFNULL(LPAD(FORMAT_DATE('%d-%b-%y', OFF.OFFER_END_DATE), 9, ' '), LPAD(' ', 9, ' ')) ||
    IFNULL(OFF.OFFER_AVAILABLE, ' ') ||
    IFNULL(LPAD(CAST(OFF.OFFER_SEQ AS STRING), 5, ' '), LPAD(' ', 5, ' ')) ||
    IFNULL(LPAD(FORMAT_DATE('%d-%b-%y', OFF.OFFER_CREATED_DATE), 9, ' '), LPAD(' ', 9, ' ')) ||
    IFNULL(OFF.ACTIVATION_STATUS, ' ') ||
    IFNULL(OFF.CAMPAIGN_TYPE, LPAD(' ', 3, ' ')) ||
    IFNULL(LPAD(MR.MEDIA_REFERENCE, 20, ' '), LPAD(' ', 20, ' ')) ||
    LPAD(' ', 41, ' ') AS record,
    CL.TSYS_ACCOUNT_ID,
    'O' AS REC_TYPE,
    SA10.SA10_BILLING_CYCLE AS CYCLE_NO
  FROM
  pcb-{env}-landing.domain_account_management.SA10 AS SA10
JOIN
  pcb-{env}-landing.domain_marketing.CUSTOMER_LOOKUP AS CL
ON
  SA10.SA10_ACCT_ID = CL.TSYS_ACCOUNT_ID
JOIN
  pcb-{env}-landing.domain_marketing.CME_CUSTOMER AS CC
ON
  CL.CUSTOMER_REF = CC.CUSTOMER_REF
JOIN
  pcb-{env}-landing.domain_marketing.CUSTOMER_MEDIA AS CM
ON
  CC.CUSTOMER_MEDIA_REF = CM.CUSTOMER_MEDIA_REF
JOIN
  pcb-{env}-landing.domain_marketing.OFFER AS OFF
ON
  CC.OFFER_ID = OFF.OFFER_ID
JOIN
  pcb-{env}-landing.domain_marketing.CHANNEL AS CH
ON
  OFF.OFFER_ID = CH.OFFER_ID
JOIN
  pcb-{env}-landing.domain_marketing.MEDIA_REFERENCE AS MR
ON
  MR.MEDIA_REFERENCE = CH.MEDIA_REFERENCE
  AND MR.MEDIA_LANGUAGE = CC.LANGUAGE
WHERE
  SA10.file_create_dt = '{file_create_dt}'
  AND SA10.FILE_NAME = '{file_name}'
  AND CC.LAST_OFFER_STATUS = 'Pending'
  AND CH.CHANNEL IN ('STATEMENT',
    'ESTATEMENT',
    'STMT MSG')
  AND CC.CUSTOMER_OFFER_EXPIRY_DATE > CURRENT_DATETIME("America/Toronto")
  AND OFF.OFFER_END_DATE > CURRENT_DATETIME("America/Toronto")
  AND OFFER_START_DATE <= CURRENT_DATETIME("America/Toronto")