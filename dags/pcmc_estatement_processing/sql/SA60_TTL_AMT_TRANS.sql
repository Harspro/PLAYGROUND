CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SUM(SA60_AMT_TRANS) AS TTL_AMT_TRANS,
  SA60_ACCT_ID,
  PARENT_CARD_NUM
FROM (
  SELECT
    SUM(SA60_AMT_TRANS) AS SA60_AMT_TRANS,
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA60`
  WHERE
    FILE_CREATE_DT = '{file_create_dt}'
    AND FILE_NAME = '{file_name}'
    AND SA60_DEB_CRED_IND = 'D'
    AND IFNULL(SA60_TRANS_SUM_INDICATOR, 'X') != 'Y'
    AND SA60_MERCHANT_ID IN (
    SELECT
      SUBSTR(MERCHANT_ID, 1, 15)
    FROM
      `pcb-{env}-landing.domain_retail.GLOBAL_PAYMENT_MERCHANT` )
  GROUP BY
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  UNION ALL
  SELECT
    SUM(SA60_AMT_TRANS) * -1 AS SA60_AMT_TRANS,
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA60`
  WHERE
    FILE_CREATE_DT = '{file_create_dt}'
    AND FILE_NAME = '{file_name}'
    AND SA60_DEB_CRED_IND = 'C'
    AND SA60_MERCHANT_ID IN (
    SELECT
      SUBSTR(MERCHANT_ID, 1, 15)
    FROM
      `pcb-{env}-landing.domain_retail.GLOBAL_PAYMENT_MERCHANT` )
  GROUP BY
    SA60_ACCT_ID,
    PARENT_CARD_NUM )
GROUP BY
  SA60_ACCT_ID,
  PARENT_CARD_NUM