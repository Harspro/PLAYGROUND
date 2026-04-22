SELECT
  COALESCE((
    SELECT
      BT1_NUMBER_OF_DEBITS
    FROM
      `pcb-{env}-landing.domain_payments.CC_BATCH_TRLR`
    WHERE
      FILE_CREATE_DT='{file_create_dt}'), 0) AS OUTBOUND_RECORD_COUNT,
  COALESCE((
    SELECT
      CAST((BT1_NET_AMT_OF_BATCH/100) AS NUMERIC)
    FROM
      `pcb-{env}-landing.domain_payments.CC_BATCH_TRLR`
    WHERE
      FILE_CREATE_DT='{file_create_dt}'), 0) AS OUTBOUND_TOTAL_AMOUNT;