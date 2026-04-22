INSERT INTO `pcb-{env}-landing.domain_loyalty.PCO_MISSING_TRANSACTIONS`
  WITH pcf_esso_transactions AS (
    SELECT
      CI.CUSTOMER_UID AS pcf_customer_id,
      AT31.AT31_ACCOUNT_NUMB_ORIG AS card_number,
      CAST(AT31.AT31_DATE_TRANSACTION AS DATE) AS transaction_date,
      AT31.AT31_AMT_TRANSACTION_ORIGINAL AS transaction_amount,
      AT31.AT31_TIME_POST AS transaction_time,
      AT31.AT31_MERCHANT_ID AS merchant_id
    FROM `pcb-{env}-curated.domain_account_management.AT31` AT31
    INNER JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM
      ON AT31.AT31_ACCOUNT_NUMB_ORIG = AM.CARD_NUMBER
    INNER JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC
      ON AM.ACCOUNT_CUSTOMER_UID = AC.ACCOUNT_CUSTOMER_UID
    INNER JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI
      ON AC.CUSTOMER_UID = CI.CUSTOMER_UID
      AND UPPER(CI.TYPE) = 'PCF-CUSTOMER-ID'
    WHERE AT31.AT31_DATE_TRANSACTION >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_transaction} DAY)
      AND AT31.AT31_MERCHANT_ID IN (
        SELECT MRCH_ID
        FROM `pcb-{env}-curated.domain_loyalty.CAMPAIGN_MERCHANT`
        WHERE CAMPAIGN_NUM = '{campaign_num}'
      )
  ),
  customer_wallet_mapping AS (
    SELECT DISTINCT
      pcf.pcf_customer_id,
      pcf.card_number,
      pcf.transaction_date,
      pcf.transaction_amount,
      pcf.transaction_time,
      pcf.merchant_id,
      we.walletId
    FROM pcf_esso_transactions pcf
    INNER JOIN `pcb-{env}-curated.domain_loyalty.PC_OPTIMUM_WALLET_EVENT` we
      ON CONCAT('00000000', CAST(pcf.pcf_customer_id AS STRING)) = we.pcfCustomerId
  ),
  missing_points_transactions AS (
    SELECT
      cwm.walletId,
      cwm.pcf_customer_id,
      cwm.card_number,
      cwm.transaction_date,
      cwm.transaction_amount,
      cwm.transaction_time,
      cwm.merchant_id
    FROM customer_wallet_mapping cwm
    LEFT JOIN `{table_cnsld_latest}` pco
      ON cwm.walletId = pco.wallettransaction_walletid
      AND cwm.transaction_date = DATE(pco.wallettransaction_transactiondatetime)
      AND cwm.transaction_time = CAST(TIME(pco.wallettransaction_transactiondatetime) AS STRING)
      AND cwm.transaction_amount = CAST(pco.value AS NUMERIC)
      AND pco.trans_dt <= CURRENT_DATE()
      AND cwm.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_transaction} DAY)
    WHERE pco.wallettransaction_walletid IS NULL
  )
SELECT
  walletId AS wallet_id,
  pcf_customer_id,
  card_number,
  transaction_date,
  transaction_amount,
  transaction_time,
  merchant_id,
  CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM missing_points_transactions;