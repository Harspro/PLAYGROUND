INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0121` (
  WITH
    depositor_0500_data AS (
    SELECT
      DISTINCT cdic_500_data.DEPOSITOR_UNIQUE_ID
    FROM
      `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS cdic_500_data
    WHERE
      cdic_500_data.REC_LOAD_TIMESTAMP = (
      SELECT
        MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP)
      FROM
        `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500)),
    link_request_data AS(
    SELECT
      * EXCEPT(STATUS,
        RNK,
        RECORD_LOAD_TIMESTAMP)
    FROM (
      SELECT
        eft_lnk_req.CUSTOMER_UID,
        eft_lnk_req.INSTITUTION_ID,
        eft_lnk_req.TRANSIT_NUMBER,
        eft_lnk_req.ACCOUNT_NUMBER,
        eft_lnk_req.REQUEST_DATE AS CREATE_DT,
        eft_lnk_req.NICKNAME,
        eft_lnk_req.RECORD_LOAD_TIMESTAMP,
        eft_lnk_req.STATUS,
        eft_lnk_req.INITIATION_TOKEN,
        ROW_NUMBER() OVER(PARTITION BY eft_lnk_req.CUSTOMER_UID, eft_lnk_req.INSTITUTION_ID, eft_lnk_req.TRANSIT_NUMBER, eft_lnk_req.ACCOUNT_NUMBER ORDER BY eft_lnk_req.RECORD_LOAD_TIMESTAMP DESC ) RNK
      FROM
        `pcb-{env}-curated.domain_payments.EFT_LINK_REQUEST` AS eft_lnk_req)
    WHERE
      RNK=1
      AND LOWER(STATUS) IN ('agent-approved',
        'auto-approved') ),
    ext_acc_link_name_data AS (
    SELECT
      * EXCEPT(RNK)
    FROM (
      SELECT
        eft_lnk_req_ac_txn.CUSTOMER_ID AS CUSTOMER_UID,
        eft_lnk_req_ac_txn.INSTITUTION_NUMBER AS INSTITUTION_ID,
        eft_lnk_req_ac_txn.TRANSIT_NUMBER,
        eft_lnk_req_ac_txn.ACCOUNT_NUMBER,
        eft_lnk_req_ac_txn.INITIATION_TOKEN,
        eft_lnk_req_ac_txn.CUSTOMER_NAME,
        ROW_NUMBER() OVER(PARTITION BY eft_lnk_req_ac_txn.CUSTOMER_ID, eft_lnk_req_ac_txn.INITIATION_TOKEN, eft_lnk_req_ac_txn.INSTITUTION_NUMBER, eft_lnk_req_ac_txn.TRANSIT_NUMBER, eft_lnk_req_ac_txn.ACCOUNT_NUMBER ORDER BY eft_lnk_req_ac_txn.RECORD_LOAD_TIMESTAMP DESC ) RNK
      FROM
        `pcb-{env}-curated.domain_payments.EFT_LINK_REQUEST_ACCOUNT_TRANSACTIONS` AS eft_lnk_req_ac_txn)
    WHERE
      RNK =1 ),
    eft_ext_data AS (
    SELECT
      * EXCEPT(RNK)
    FROM (
      SELECT
        eft_ext_acc.CUSTOMER_UID,
        eft_ext_acc.INSTITUTION_ID,
        eft_ext_acc.TRANSIT_NUMBER,
        eft_ext_acc.ACCOUNT_NUMBER,
        eft_ext_acc.NICKNAME,
        eft_ext_acc.EFT_EXTERNAL_ACCOUNT_UID,
        ROW_NUMBER() OVER(PARTITION BY eft_ext_acc.CUSTOMER_UID, eft_ext_acc.INSTITUTION_ID, eft_ext_acc.TRANSIT_NUMBER, eft_ext_acc.ACCOUNT_NUMBER ORDER BY eft_ext_acc.RECORD_LOAD_TIMESTAMP DESC ) RNK
      FROM
        `pcb-{env}-curated.domain_payments.EFT_EXTERNAL_ACCOUNT` AS eft_ext_acc
      WHERE
        eft_ext_acc.DELETED_IND = FALSE
        AND eft_ext_acc.VERIFIED_IND = TRUE)
    WHERE
      RNK = 1 ),
    external_data AS ( (
      SELECT
        * EXCEPT(RNK)
      FROM (
        SELECT
          CAST(eft_ext_acc_old.CUSTOMER_UID AS STRING) AS CUSTOMER_UID,
          eft_ext_acc_old.INSTITUTION_ID,
          eft_ext_acc_old.TRANSIT_NUMBER,
          eft_ext_acc_old.ACCOUNT_NUMBER,
          eft_ext_acc_old.NICKNAME AS PAYER_NAME,
          CAST(eft_ext_acc_old.EFT_EXTERNAL_ACCOUNT_UID AS STRING) AS EFT_EXTERNAL_ACCOUNT_UID,
          FORMAT_DATETIME('%Y-%m-%d',eft_ext_acc_old.CREATE_DT) AS CREATE_DT,
          ROW_NUMBER() OVER (PARTITION BY eft_ext_acc_old.CUSTOMER_UID, eft_ext_acc_old.INSTITUTION_ID, eft_ext_acc_old.TRANSIT_NUMBER, eft_ext_acc_old.ACCOUNT_NUMBER ORDER BY eft_ext_acc_old.CREATE_DT DESC) RNK
        FROM
          `pcb-{env}-curated.domain_payments.EFT_EXTERNAL_ACCOUNT_OLD` AS eft_ext_acc_old
        WHERE
          eft_ext_acc_old.CREATE_DT < '2024-12-04'
          AND LOWER(eft_ext_acc_old.DELETED_IND) = 'n'
          AND LOWER(eft_ext_acc_old.VERIFIED_IND) = 'y' )
      WHERE
        RNK=1 )
    UNION ALL (
      SELECT
        eft_ext_dat.CUSTOMER_UID,
        eft_ext_dat.INSTITUTION_ID,
        eft_ext_dat.TRANSIT_NUMBER,
        eft_ext_dat.ACCOUNT_NUMBER,
        IFNULL(ext_acc_lnk_name_dat.CUSTOMER_NAME,IFNULL(lnk_req_data.NICKNAME, eft_ext_dat.NICKNAME)) AS PAYER_NAME,
        CAST(eft_ext_dat.EFT_EXTERNAL_ACCOUNT_UID AS STRING) AS EFT_EXTERNAL_ACCOUNT_UID,
        CREATE_DT
      FROM
        eft_ext_data AS eft_ext_dat
      INNER JOIN
        link_request_data AS lnk_req_data
      ON
        eft_ext_dat.CUSTOMER_UID = lnk_req_data.CUSTOMER_UID
        AND eft_ext_dat.INSTITUTION_ID = lnk_req_data.INSTITUTION_ID
        AND eft_ext_dat.TRANSIT_NUMBER = lnk_req_data.TRANSIT_NUMBER
        AND eft_ext_dat.ACCOUNT_NUMBER = lnk_req_data.ACCOUNT_NUMBER
      LEFT JOIN
        ext_acc_link_name_data AS ext_acc_lnk_name_dat
      ON
        lnk_req_data.CUSTOMER_UID = ext_acc_lnk_name_dat.CUSTOMER_UID
        AND lnk_req_data.INSTITUTION_ID = ext_acc_lnk_name_dat.INSTITUTION_ID
        AND lnk_req_data.TRANSIT_NUMBER = ext_acc_lnk_name_dat.TRANSIT_NUMBER
        AND lnk_req_data.ACCOUNT_NUMBER = ext_acc_lnk_name_dat.ACCOUNT_NUMBER
        AND lnk_req_data.INITIATION_TOKEN = ext_acc_lnk_name_dat.INITIATION_TOKEN ) ),
    transfer_data AS (
    SELECT
      epsh_epull_data.ACCOUNT_INFO[0] AS INSTITUTION_ID,
      epsh_epull_data.ACCOUNT_INFO[1] AS TRANSIT_NUMBER,
      epsh_epull_data.ACCOUNT_INFO[2] AS ACCOUNT_NUMBER,
      FORMAT_DATETIME('%Y-%m-%d',epsh_epull_data.CREATE_DT) AS CREATE_DATE,
      epsh_epull_data.DIRECTION,
      ROW_NUMBER() OVER(PARTITION BY epsh_epull_data.ACCOUNT_INFO[0], epsh_epull_data.ACCOUNT_INFO[1], epsh_epull_data.ACCOUNT_INFO[2] ORDER BY epsh_epull_data.CREATE_DT DESC) RNK,
      ROW_NUMBER() OVER(PARTITION BY epsh_epull_data.ACCOUNT_INFO[0], epsh_epull_data.ACCOUNT_INFO[1], epsh_epull_data.ACCOUNT_INFO[2], epsh_epull_data.DIRECTION ORDER BY epsh_epull_data.CREATE_DT DESC) RNK_OUT,
    FROM (
      SELECT
        cred_tnsfer_txn_info.acceptanceDateTime AS CREATE_DT,
        SPLIT(cred_tnsfer_txn_info.creditorAccount.identification.other.identification,'-') AS ACCOUNT_INFO,
        'OUT' AS DIRECTION
      FROM
        `pcb-{env}-curated.domain_payments.PCB_FUNDS_MOVE_CREATED` AS pcb_fnd_mv_crted,
        UNNEST(fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS cred_tnsfer_txn_info
      WHERE
        LOWER(pcb_fnd_mv_crted.PAYMENT_RAIL) = 'epsh'
      UNION ALL
      SELECT
        cred_tnsfer_txn_info.acceptanceDateTime AS CREATE_DT,
        SPLIT(cred_tnsfer_txn_info.debtorAccount.identification.other.identification,'-') AS ACCOUNT_INFO,
        'IN' AS DIRECTION
      FROM
        `pcb-{env}-curated.domain_payments.PCB_FUNDS_MOVE_CREATED` AS pcb_fnd_mv_crted,
        UNNEST(fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS cred_tnsfer_txn_info
      WHERE
        LOWER(pcb_fnd_mv_crted.PAYMENT_RAIL) = 'epul' ) AS epsh_epull_data),
    fundsmove_data AS (
    SELECT
      ed.CUSTOMER_UID,
      ed.EFT_EXTERNAL_ACCOUNT_UID,
      ed.INSTITUTION_ID,
      ed.TRANSIT_NUMBER,
      ed.ACCOUNT_NUMBER,
      ei.FUNDSMOVE_INSTRUCTION_UID,
      fi.FUNDSMOVE_SCHEDULE_UID,
      fs.FIRST_OCCURS_ON,
      fs.LAST_OCCURS_ON,
      fs.CANCELLED_ON,
      fs.FREQUENCY,
      fs.DAY_OF_WEEK,
      fs.DAY_OF_MONTH,
      ei.CREATE_DT
    FROM
      external_data ed
    LEFT JOIN
      `pcb-{env}-curated.domain_payments.EFT_INSTRUCTION` ei
    ON
      CAST(ed.EFT_EXTERNAL_ACCOUNT_UID AS INT) = ei.EFT_EXTERNAL_ACCOUNT_UID
    LEFT JOIN
      `pcb-{env}-curated.domain_payments.FUNDSMOVE_INSTRUCTION` fi
    ON
      ei.FUNDSMOVE_INSTRUCTION_UID = fi.FUNDSMOVE_INSTRUCTION_UID AND fi.FUNDSMOVE_TYPE_UID = 5
    LEFT JOIN
      `pcb-{env}-curated.domain_payments.FUNDSMOVE_SCHEDULE` fs
    ON
      fi.FUNDSMOVE_SCHEDULE_UID = fs.FUNDSMOVE_SCHEDULE_UID
    WHERE
      (fs.LAST_OCCURS_ON > CURRENT_DATETIME('America/Toronto')
        OR fs.LAST_OCCURS_ON IS NULL)
      AND (fs.CANCELLED_ON > CURRENT_DATETIME('America/Toronto')
        OR fs.CANCELLED_ON IS NULL) ),
    monthly_calculations AS (
    SELECT
      *,
      CASE
        WHEN FREQUENCY = 'MONTHLY' AND DAY_OF_MONTH IS NOT NULL THEN
      -- Calculate next monthly occurrence by day of month
      CASE
        WHEN FIRST_OCCURS_ON >= CURRENT_DATE() THEN FIRST_OCCURS_ON
        ELSE CASE
        WHEN DAY_OF_MONTH >= EXTRACT(DAY FROM CURRENT_DATE()) THEN
      -- This month's occurrence
      DATE(CURRENT_DATE() - INTERVAL (EXTRACT(DAY FROM CURRENT_DATE()) - CAST(DAY_OF_MONTH AS INT)) DAY) ELSE
      -- Next month's occurrence
      DATE(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH) - INTERVAL (EXTRACT(DAY FROM DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH)) - CAST(DAY_OF_MONTH AS INT)) DAY)
    END
    END
        WHEN FREQUENCY = 'MONTHLY' AND DAY_OF_WEEK IS NOT NULL THEN
      -- Calculate next monthly occurrence by day of week
      CASE
        WHEN FIRST_OCCURS_ON >= CURRENT_DATE() THEN FIRST_OCCURS_ON ELSE
      -- Find next occurrence in current or next month
      LEAST( DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL (CAST(DAY_OF_WEEK AS INT) - 1) DAY), DATE_ADD(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL (CAST(DAY_OF_WEEK AS INT) - 1) DAY) )
    END
        ELSE NULL
    END
      AS monthly_next_date
    FROM
      fundsmove_data ),
    all_outbound_fund_transfers AS (
    SELECT
      cust_data.DEPOSITOR_UNIQUE_ID,
      ext_data.PAYER_NAME AS PAYEE_NAME,
      ext_data.INSTITUTION_ID AS INSTITUTION_NUMBER,
      ext_data.TRANSIT_NUMBER,
      ext_data.ACCOUNT_NUMBER,
      (
      SELECT
        pcma_cdic_0233.CURRENCY_CODE
      FROM
        `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233
      WHERE
        LOWER(pcma_cdic_0233.MI_CURRENCY_CODE) = 'cad'
        AND pcma_cdic_0233.REC_LOAD_TIMESTAMP = (
        SELECT
          MAX(pcma_cdic_0233.REC_LOAD_TIMESTAMP)
        FROM
          `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` pcma_cdic_0233)) AS CURRENCY_CODE,
      CAST(NULL AS STRING) AS JOINT_ACCOUNT_FLAG,
      FORMAT_DATE("%Y%m%d", DATE(ext_data.CREATE_DT)) AS START_DATE,
      FORMAT_DATE("%Y%m%d", DATE(trnsfer_data.CREATE_DATE)) AS LAST_FUNDS_TRANSFER,
      FORMAT_DATE("%Y%m%d", DATE(trnsfer_data_out.CREATE_DATE)) AS LAST_OUTBOUND_FUNDS_TRANSFER,
      CAST(
        CASE
          WHEN LOWER(FREQUENCY) = 'once' THEN CASE
          WHEN FIRST_OCCURS_ON >= CURRENT_DATE() THEN FORMAT_DATE("%Y%m%d", FIRST_OCCURS_ON)
          ELSE NULL
      END
          WHEN LOWER(FREQUENCY) = 'weekly' THEN CASE
          WHEN FIRST_OCCURS_ON >= CURRENT_DATE() THEN FORMAT_DATE("%Y%m%d", FIRST_OCCURS_ON)
          ELSE FORMAT_DATE("%Y%m%d", DATE_ADD( FIRST_OCCURS_ON, INTERVAL CAST(CEIL(SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), FIRST_OCCURS_ON, DAY), 7)) AS INT) * 7 DAY ))
      END
          WHEN LOWER(FREQUENCY) = 'bi-weekly' THEN CASE
          WHEN FIRST_OCCURS_ON >= CURRENT_DATE() THEN FORMAT_DATE("%Y%m%d", FIRST_OCCURS_ON)
          ELSE FORMAT_DATE("%Y%m%d", DATE_ADD( FIRST_OCCURS_ON, INTERVAL CAST(CEIL(SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), FIRST_OCCURS_ON, DAY), 14)) AS INT) * 14 DAY ))
      END
          WHEN LOWER(FREQUENCY) = 'monthly' THEN FORMAT_DATE("%Y%m%d", monthly_next_date)
      END
        AS STRING) AS NEXT_OUTBOUND_FUNDS_TRANSFER,
      CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
      '{dag_id}' AS JOB_ID
    FROM
      depositor_0500_data AS cust_data
    INNER JOIN
      external_data AS ext_data
    ON
      cust_data.DEPOSITOR_UNIQUE_ID = ext_data.CUSTOMER_UID
    LEFT JOIN
      transfer_data AS trnsfer_data
    ON
      ext_data.INSTITUTION_ID = trnsfer_data.INSTITUTION_ID
      AND ext_data.TRANSIT_NUMBER = trnsfer_data.TRANSIT_NUMBER
      AND ext_data.ACCOUNT_NUMBER = trnsfer_data.ACCOUNT_NUMBER
      AND trnsfer_data.RNK = 1
    LEFT JOIN
      transfer_data AS trnsfer_data_out
    ON
      ext_data.INSTITUTION_ID = trnsfer_data_out.INSTITUTION_ID
      AND ext_data.TRANSIT_NUMBER = trnsfer_data_out.TRANSIT_NUMBER
      AND ext_data.ACCOUNT_NUMBER = trnsfer_data_out.ACCOUNT_NUMBER
      AND LOWER(trnsfer_data_out.DIRECTION) = 'out'
      AND trnsfer_data_out.RNK_OUT = 1
    LEFT JOIN
      monthly_calculations mc
    ON
      ext_data.CUSTOMER_UID = mc.CUSTOMER_UID
      AND ext_data.INSTITUTION_ID = mc.INSTITUTION_ID
      AND ext_data.TRANSIT_NUMBER = mc.TRANSIT_NUMBER
      AND ext_data.ACCOUNT_NUMBER = mc.ACCOUNT_NUMBER )
  SELECT
    * EXCEPT(RNK)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY DEPOSITOR_UNIQUE_ID, INSTITUTION_NUMBER, TRANSIT_NUMBER, ACCOUNT_NUMBER ORDER BY NEXT_OUTBOUND_FUNDS_TRANSFER ASC) AS RNK
    FROM
      all_outbound_fund_transfers)
  WHERE
    RNK = 1)
