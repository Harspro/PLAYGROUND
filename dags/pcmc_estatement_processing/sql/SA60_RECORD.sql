CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH SA60_SEQ AS(
  SELECT
    *,
    ROW_NUMBER()
      OVER(
        PARTITION BY
          ACCT_ID,
          PARENT_NUM
        ORDER BY
          SA60_PAYMENT_TRANS_INDICATOR DESC,
          SA60_CUST_TYPE,
          SA60_ACCOUNT_NUMB,
          SEQ_NUM
      ) AS RNK
  FROM `pcb-{env}-processing.domain_account_management.SA60_RECORD_MID_DBEXT`)
SELECT * EXCEPT(SA60_PAYMENT_TRANS_INDICATOR,
                SA60_CUST_TYPE ,
                SA60_ACCOUNT_NUMB,
                RNK)
         REPLACE(RNK AS SEQ_NUM)
FROM SA60_SEQ