/*Temporary Function to parse Julian Date*/
CREATE TEMP FUNCTION
  PARSE_JULIAN_DATE(x INT64) AS (
    CASE
      WHEN (x <> 0) THEN PARSE_DATE('%Y%j', CAST((x) AS STRING))
  END
    );

/*Temporary Function to calculate account status using Status and Reason Code
    This function concatenates the Status ,  Reason Code and Description*/
  CREATE TEMP FUNCTION
  GET_STATUS_DESC (S STRING, R STRING) RETURNS STRING AS (
     (
      SELECT DISTINCT
        CASE
          WHEN TS2_STATUS = 'DOR' OR TS2_STATUS = 'ACT'
            THEN TS2_DESCRIPTION
          ELSE TS2_STATUS ||' '|| TS2_REASON_CODE || '-' || TS2_DESCRIPTION --Concatenating status, reason code and description
        END
      FROM 
        `pcb-{env}-curated.domain_aml.ACCOUNT_STATUS_DESCRIPTION`
      WHERE
        TS2_STATUS = S
        AND TS2_REASON_CODE = R
     )
  );

  /*Temporary Table to fetch all the customers using the Customer Identifier*/
  CREATE TEMP TABLE all_cust AS (
    SELECT DISTINCT 
      CI.CUSTOMER_UID AS CUSTOMER_UID,
      CI.CUSTOMER_IDENTIFIER_NO
    FROM
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI
    WHERE
      CI.TYPE = 'PCF-CUSTOMER-ID'
      AND CI.DISABLED_IND = 'N'
  );

  /*Temporary Table to fetch all the accounts which were closed more than 7 years ago*/
  CREATE TEMP TABLE excluded_cust AS (
    SELECT DISTINCT
      SUBSTR(AM00.MAST_ACCOUNT_ID, 2) AS ACCOUNT_NO
    FROM
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` AM00
    WHERE
      (
        am00.AM00_DATE_CLOSE IS NOT NULL
        AND am00.AM00_DATE_CLOSE > 0
        AND am00.AM00_DATE_CLOSE < CAST(
          FORMAT_DATE('%Y%j', DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 YEAR))
          AS INT64)
        AND AM00.MAST_ACCOUNT_SUFFIX = 0   
      )
  );

/*Temporary Table to fetch all the accounts of customers*/
  CREATE TEMP TABLE account_customer AS (
    SELECT DISTINCT 
      AC.CUSTOMER_UID AS CUSTOMER_UID,
      A.MAST_ACCOUNT_ID AS ACCOUNT_NO
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC
      LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A
        ON AC.ACCOUNT_UID = A.ACCOUNT_UID
  );

/*Temporary Table to filter customer by excluding the closed accounts*/
  CREATE TEMP TABLE filtered_cust AS (
    SELECT DISTINCT 
      ACTIVE_AC.CUSTOMER_UID,
      CUSTOMER_IDENTIFIER_NO
    FROM
      (
        SELECT
          A.CUSTOMER_UID,
          A.CUSTOMER_IDENTIFIER_NO
        FROM
          all_cust AS A
          INNER JOIN (
            SELECT
              *
            FROM
              account_customer
            WHERE
              ACCOUNT_NO NOT IN (
                SELECT
                  ACCOUNT_NO
                FROM
                  excluded_cust
          )
        ) AS AC ON A.CUSTOMER_UID = AC.CUSTOMER_UID
    ) ACTIVE_AC
  );

/*Temporary Table to fetch all the accounts of customers having a PCMA account*/
  CREATE TEMP TABLE pcma AS (
    SELECT
      a.ACCOUNT_UID,
      a.MAST_ACCOUNT_ID AS ACCOUNT_NO,
      a.OPEN_DT,
      ai.ACCOUNT_IDENTIFIER_NO,
      p.TYPE,
      p.CODE
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
      `pcb-{env}-curated.domain_account_management.ACCOUNT_IDENTIFIER` ai,
      `pcb-{env}-curated.domain_account_management.PRODUCT` p

    WHERE
      a.ACCOUNT_UID = ai.ACCOUNT_UID
      AND a.PRODUCT_UID = p.PRODUCT_UID
      AND ai.DISABLED_IND = 'N'
      AND ((ai.TYPE = 'PCB' AND p.PRODUCT_UID IN (7,9,10)) OR p.PRODUCT_UID = 8  OR p.PRODUCT_UID = 11) --Product UID for PCMA accounts
  );

/*Temporary Table to fetch the primary accounts of customers*/
  CREATE TEMP TABLE primary AS (
    SELECT
      ac.ACCOUNT_UID,
      ci.CUSTOMER_UID,
      ci.CUSTOMER_IDENTIFIER_NO,
      ac.ACCOUNT_CUSTOMER_ROLE_UID,
      c.GIVEN_NAME,
      c.SURNAME
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac ,
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci ,
      `pcb-{env}-curated.domain_customer_management.CUSTOMER` c
    WHERE
      ci.TYPE = 'PCF-CUSTOMER-ID'
      AND ci.DISABLED_IND = 'N'
      AND ac.ACCOUNT_CUSTOMER_ROLE_UID = 1 --Primary account
      AND ac.CUSTOMER_UID = ci.CUSTOMER_UID
      AND ci.CUSTOMER_UID = c.CUSTOMER_UID
  );

/*Temporary Table to fetch the account closed date , Ledger Balance , Ledger Balance Date , Available Balance and Available Balance Date
   using the TSYS tables*/
  CREATE TEMP TABLE current_active AS (
    SELECT
      am00.MAST_ACCOUNT_ID,
      PARSE_JULIAN_DATE(am00.AM00_DATE_CLOSE) as AccountClosedDate,
      am02.AM02_BALANCE_CURRENT * -1 as LedgerBalance ,
      CURRENT_DATE('America/Toronto') as LedgerBalanceDate,
        ((am02.AM02_BALANCE_CURRENT * -1) -AM00_AMT_AUTHORIZATIONS_OUT + AM00_AUTH_PAYMENT_AMT + AM00_LIMIT_CREDIT) as AvailableBalance,
      CURRENT_DATE('America/Toronto') as AvailableBalanceDate
    FROM
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00,
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02
    WHERE
        am00.MAST_ACCOUNT_ID = am02.MAST_ACCOUNT_ID
    AND am00.MAST_ACCOUNT_SUFFIX = 0
    AND am02.AM00_APPLICATION_SUFFIX = 0
  );

/*Temporary Table to fetch all the account statuses*/
  CREATE TEMP TABLE account_status AS
  (
    SELECT DISTINCT
      am00.MAST_ACCOUNT_ID,
      AM00_STATC_CLOSED,
      PARSE_JULIAN_DATE(AM00_DATE_CLOSE) AS CLOSE_DATE,
      CASE
      WHEN am00.AM00_STATC_TIERED_AUTH = 'L7'
        THEN 'FROZEN'
      WHEN am00.AM00_STATC_CLOSED = 'DC'
        THEN 'DECEASED'
      WHEN (am00.AM00_STATC_CHARGEOFF IS NOT NULL) AND (am00.AM00_STATC_CHARGEOFF <> "")
        THEN 'CHARGED_OFF'
      WHEN (AM00_DATE_CLOSE IS NOT NULL AND AM00_DATE_CLOSE <> 0)
        OR ((am00.AM00_STATC_CLOSED IS NOT NULL) AND (am00.AM00_STATC_CLOSED <> ""))
        OR am00.AM00_STATF_FRAUD = 'Y'
          THEN 'CLOSED'
      WHEN (am00.AM00_STATC_WATCH IS NOT NULL) AND (am00.AM00_STATC_WATCH <> "")
        THEN 'OTHER'
      WHEN (am00.AM00_STATC_CREDIT_REVOKED IS NOT NULL) AND (am00.AM00_STATC_CREDIT_REVOKED <> "")
        THEN 'OTHER'
      WHEN am00.AM00_STATC_TIERED_AUTH = '11'
        THEN 'OTHER'
      WHEN (am00.AM00_STATF_INACTIVE IS NOT NULL) AND (am00.AM00_STATF_INACTIVE <> "")
        THEN 'INACTIVE'
      WHEN (am00.AM00_STATC_CURRENT_PAST_DUE IS NOT NULL) AND (am00.AM00_STATC_CURRENT_PAST_DUE <> "")
        THEN 'OTHER'
      WHEN (am00.AM00_STATC_CURRENT_OVERLIMIT IS NOT NULL) AND (am00.AM00_STATC_CURRENT_OVERLIMIT <> "")
        THEN 'OTHER'
      WHEN am00.AM00_STATF_DECLINED_REISSUE = 'Y'
        THEN 'OTHER'
      WHEN DATE_DIFF(CURRENT_DATE('America/Toronto'),PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE,
                                                                                AM02_DATE_CREDIT_BAL_REFUND,
                                                                                AM02_DATE_LAST_ATM,
                                                                                AM02_DATE_LAST_PAYMENT,
                                                                                AM02_DATE_LAST_PURCHASE,
                                                                                AM02_DATE_LAST_CASH,
                                                                                AM02_DATE_LAST_CREDIT,
                                                                                AM02_DATE_LAST_ANN_FEE,
                                                                                AM02_DATE_LAST_CONV_CHECK,
                                                                                AM02_DATE_LAST_NSF_PAYMENT,
                                                                                AM02_DATE_LAST_SYS_GEN_TRANS,
                                                                                AM02_DATE_LAST_DEBIT_ADJ,
                                                                                AM02_DATE_LAST_CREDIT_ADJ,
                                                                                AM02_DATE_LAST_AUTO_PAYMENT,
                                                                                AM02_DATE_LAST_FCHG )), DAY) > 180
        THEN "DORMANT"
      ELSE
        'ACTIVE'
      END
      AS STATUS,
      DATE_DIFF(CURRENT_DATE('America/Toronto'),PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE,
                                                                       AM02_DATE_CREDIT_BAL_REFUND,
                                                                       AM02_DATE_LAST_ATM,
                                                                       AM02_DATE_LAST_PAYMENT,
                                                                       AM02_DATE_LAST_PURCHASE,
                                                                       AM02_DATE_LAST_CASH,
                                                                       AM02_DATE_LAST_CREDIT,
                                                                       AM02_DATE_LAST_ANN_FEE,
                                                                       AM02_DATE_LAST_CONV_CHECK,
                                                                       AM02_DATE_LAST_NSF_PAYMENT,
                                                                       AM02_DATE_LAST_SYS_GEN_TRANS,
                                                                       AM02_DATE_LAST_DEBIT_ADJ,
                                                                       AM02_DATE_LAST_CREDIT_ADJ,
                                                                       AM02_DATE_LAST_AUTO_PAYMENT,
                                                                       AM02_DATE_LAST_FCHG )), DAY)
    AS DAYS_SINCE_LAST_ACTIVITY,
    CURRENT_DATE('America/Toronto') AS DATE_TODAY,
    PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE,
                              AM02_DATE_CREDIT_BAL_REFUND,
                              AM02_DATE_LAST_ATM,
                              AM02_DATE_LAST_PAYMENT,
                              AM02_DATE_LAST_PURCHASE,
                              AM02_DATE_LAST_CASH,
                              AM02_DATE_LAST_CREDIT,
                              AM02_DATE_LAST_ANN_FEE,
                              AM02_DATE_LAST_CONV_CHECK,
                              AM02_DATE_LAST_NSF_PAYMENT,
                              AM02_DATE_LAST_SYS_GEN_TRANS,
                              AM02_DATE_LAST_DEBIT_ADJ,
                              AM02_DATE_LAST_CREDIT_ADJ,
                              AM02_DATE_LAST_AUTO_PAYMENT,
                              AM02_DATE_LAST_FCHG ))
    AS LAST_FIN_ACTIVITY_DATE
      FROM
        `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00 ,
        `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02
      WHERE
          am00.mast_account_id = am02.mast_account_id
      AND am00.MAST_ACCOUNT_SUFFIX = 0
      AND am02.AM00_APPLICATION_SUFFIX = 0  
    );

/*Temporary Table to fetch all the account status description using temp function*/
  CREATE TEMP TABLE status_desc AS
  (
    SELECT DISTINCT
      am00.MAST_ACCOUNT_ID,
      SUBSTR(am00.MAST_ACCOUNT_ID,2) AS ACCOUNT_NO,
      am00.AM00_STATC_CLOSED,
      PARSE_JULIAN_DATE(am00.AM00_DATE_CLOSE) AS CLOSE_DATE,
      CASE
        WHEN am00.AM00_STATC_TIERED_AUTH = 'L7'
          THEN GET_STATUS_DESC('TW',am00.AM00_STATC_TIERED_AUTH)
        WHEN am00.AM00_STATC_CLOSED = 'DC'
          THEN GET_STATUS_DESC('CL',am00.AM00_STATC_CLOSED)
        WHEN (am00.AM00_STATC_CHARGEOFF IS NOT NULL) AND (am00.AM00_STATC_CHARGEOFF <> "")
          THEN GET_STATUS_DESC('CO',am00.AM00_STATC_CHARGEOFF)
        WHEN am00.AM00_STATF_FRAUD = 'Y'
          THEN GET_STATUS_DESC('FR', am00.AM00_STATF_FRAUD)
        WHEN (am00.AM00_STATC_CLOSED IS NOT NULL) AND (am00.AM00_STATC_CLOSED <> "")
          THEN GET_STATUS_DESC('CL',am00.AM00_STATC_CLOSED)
        WHEN (am00.AM00_STATC_WATCH IS NOT NULL) AND (am00.AM00_STATC_WATCH <> "")
          THEN GET_STATUS_DESC('WA',AM00_STATC_WATCH)
        WHEN (am00.AM00_STATC_CREDIT_REVOKED IS NOT NULL) AND (am00.AM00_STATC_CREDIT_REVOKED <> "")
          THEN GET_STATUS_DESC('CR',am00.AM00_STATC_CREDIT_REVOKED)
        WHEN am00.AM00_STATC_TIERED_AUTH = '11'
          THEN GET_STATUS_DESC('TW',am00.AM00_STATC_TIERED_AUTH)
        WHEN (am00.AM00_STATF_INACTIVE IS NOT NULL) AND (am00.AM00_STATF_INACTIVE <> "")
          THEN GET_STATUS_DESC('IN',am00.AM00_STATF_INACTIVE)
        WHEN (am00.AM00_STATC_CURRENT_PAST_DUE IS NOT NULL) AND (am00.AM00_STATC_CURRENT_PAST_DUE <> "")
          THEN GET_STATUS_DESC('PD',am00.AM00_STATC_CURRENT_PAST_DUE)
        WHEN (am00.AM00_STATC_CURRENT_OVERLIMIT IS NOT NULL) AND (am00.AM00_STATC_CURRENT_OVERLIMIT <> "")
          THEN GET_STATUS_DESC('OL',am00.AM00_STATC_CURRENT_OVERLIMIT)
        WHEN am00.AM00_STATF_DECLINED_REISSUE = 'Y'
          THEN  GET_STATUS_DESC('RD',am00.AM00_STATF_DECLINED_REISSUE)
        WHEN DATE_DIFF(CURRENT_DATE('America/Toronto'),PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE,
                                                                                  AM02_DATE_CREDIT_BAL_REFUND,
                                                                                  AM02_DATE_LAST_ATM,
                                                                                  AM02_DATE_LAST_PAYMENT,
                                                                                  AM02_DATE_LAST_PURCHASE,
                                                                                  AM02_DATE_LAST_CASH,
                                                                                  AM02_DATE_LAST_CREDIT,
                                                                                  AM02_DATE_LAST_ANN_FEE,
                                                                                  AM02_DATE_LAST_CONV_CHECK,
                                                                                  AM02_DATE_LAST_NSF_PAYMENT,
                                                                                  AM02_DATE_LAST_SYS_GEN_TRANS,
                                                                                  AM02_DATE_LAST_DEBIT_ADJ,
                                                                                  AM02_DATE_LAST_CREDIT_ADJ,
                                                                                  AM02_DATE_LAST_AUTO_PAYMENT,
                                                                                  AM02_DATE_LAST_FCHG )), DAY) > 180
          THEN GET_STATUS_DESC('DOR','DOR')
      ELSE GET_STATUS_DESC('ACT','ACT')
    END
      AS Status_Description
    FROM
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00 ,
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02 ,
      account_customer
    WHERE
      am00.mast_account_id = am02.mast_account_id
      AND SUBSTR(am00.MAST_ACCOUNT_ID,2) = account_customer.ACCOUNT_NO
      AND am00.MAST_ACCOUNT_SUFFIX = 0
      AND am02.AM00_APPLICATION_SUFFIX = 0
  );

-- /*Temporary Table to fetch all the customers using the Customer Identifier for eve accounts*/
--  CREATE TEMP TABLE all_eve_cust AS (
--    SELECT DISTINCT
--      CI.CUSTOMER_IDENTIFIER_NO
--    FROM
--      pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER CI
--    WHERE
--      CI.TYPE = 'PCF-CUSTOMER-ID'
--  );
--
  /* Temporary table to fetch account_number for eve accounts*/
  CREATE TEMP TABLE eve_acc_nums AS
  (
    SELECT a.MAST_ACCOUNT_ID AS ACCOUNT_NO
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
      `pcb-{env}-curated.domain_account_management.PRODUCT` p
     WHERE a.PRODUCT_UID = p.PRODUCT_UID
     AND p.TYPE = 'SAVINGS'
  );

 /* Temporary table to fetch all the eve accounts data */
 CREATE TEMP TABLE eve_pcma AS
(
    WITH LatestRecord AS (
        SELECT
            ptaed.ACCOUNT_OPEN_DATE AS Open_Date,
            ptaed.CLOSURE_DATE AS Close_Date,
            ptaed.ACCOUNT_ID AS ACCOUNT_NO,
            ptaed.STATUS AS STATUS,
            ptaed.STATUS AS STATUS_DESCRIPTION,
            ptaed.BALANCE_CLOSING AS LEDGER_BALANCE,
            ptaed.POSTING_DATE AS LEDGER_BALANCE_DATE,
            ptaed.BALANCE_CLOSING AS AVAILABLE_BALANCE,
            ptaed.POSTING_DATE AS AVAILABLE_BALANCE_DATE,
            ROW_NUMBER() OVER (PARTITION BY ptaed.ACCOUNT_ID ORDER BY ptaed.record_load_timestamp DESC) AS rn
        FROM
            `pcb-{env}-curated.domain_account_management.PRODUCT_TRANSACT_ACCOUNT_EOD_DETAIL` ptaed)
    SELECT
        LatestRecord.Open_Date,
        LatestRecord.Close_Date,
        eve_acc_nums.ACCOUNT_NO,
        LatestRecord.STATUS,
        LatestRecord.STATUS_DESCRIPTION,
        LatestRecord.LEDGER_BALANCE,
        LatestRecord.LEDGER_BALANCE_DATE,
        LatestRecord.AVAILABLE_BALANCE,
        LatestRecord.AVAILABLE_BALANCE_DATE
    FROM
        LatestRecord
    JOIN
        eve_acc_nums
    ON
        CAST(eve_acc_nums.ACCOUNT_NO AS NUMERIC) = LatestRecord.ACCOUNT_NO
    WHERE
        rn = 1
);

/* Create a Table for the resultant dataset */
  DROP TABLE IF EXISTS `pcb-{env}-curated.cots_aml_verafin.ACCOUNT_PCMA_FEED` ;
  CREATE TABLE `pcb-{env}-curated.cots_aml_verafin.ACCOUNT_PCMA_FEED` AS

  SELECT
    '320'                                                                     AS Institution_Number,
    primary.CUSTOMER_IDENTIFIER_NO                                            AS Customer_Number,
    '2002'                                                                    AS Account_Branch_Number,
    pcma.ACCOUNT_NO                                                           AS Account_Number,
    CASE
        WHEN pcma.TYPE IN ('SAVINGS')
            THEN NULL
        ELSE
            pcma.ACCOUNT_IDENTIFIER_NO
    END                                                                        AS External_Number,
    CASE
      WHEN pcma.TYPE = 'GOAL'
        THEN 'GL'
      WHEN pcma.TYPE IN ('INDIVIDUAL', 'JOINT', 'ADDITIONAL')
        THEN 'DDA'
      WHEN pcma.TYPE IN ('SAVINGS')
        THEN 'SAVINGS'
      ELSE
        'N/A'
    END                                                                       AS Account_Type_Code,
    CASE
      WHEN pcma.CODE = 'MC-PDG'
        THEN 'Goal (BIN 533893)'
      WHEN pcma.CODE IN ('MC-PDI', 'MC-PDA')
        THEN 'Deposit (BIN 533866)'
      WHEN pcma.TYPE IN ('SAVINGS')
        THEN 'SAVINGS'
      ELSE
        'N/A'
    END                                                                       AS Account_Type_Code_Description,
    NULL                                                                      AS Account_Subtype,
    'PERSONAL'                                                                AS Account_Purpose_Flag,
    'CAD'                                                                     AS Currency_Type,
    CASE
    WHEN pcma.TYPE IN ('SAVINGS') THEN
        CASE
            WHEN eve_pcma.Open_Date IS NOT NULL
                THEN EXTRACT(DATE FROM eve_pcma.Open_Date)
            ELSE NULL
        END
    ELSE
        EXTRACT(DATE FROM pcma.OPEN_DT)
    END                                                                       AS Open_Date,
    CASE
    WHEN pcma.TYPE IN ('SAVINGS') THEN
        CASE
            WHEN eve_pcma.Close_Date IS NOT NULL
                THEN eve_pcma.Close_Date
            ELSE NULL
        END
    ELSE
        current_active.AccountClosedDate
    END                                                                       AS Close_Date,
    CASE
      WHEN pcma.TYPE IN ('SAVINGS') AND eve_pcma.STATUS IN ('CLOSE', 'CLOSED')
        THEN 'CLOSED'
      WHEN pcma.TYPE IN ('SAVINGS') AND eve_pcma.STATUS NOT IN ('CLOSE', 'CLOSED')
        THEN 'ACTIVE'
      ELSE
        account_status.STATUS
    END                                                                       AS Status,
    CASE
      WHEN pcma.TYPE IN ('SAVINGS')
        THEN eve_pcma.STATUS
      ELSE
        status_desc.Status_Description
    END                                                                       AS Status_Description,
    NULL                                                                      AS Positive_Pay,
    CASE
        WHEN pcma.TYPE IN ('SAVINGS')
            THEN FORMAT("%.*f",2,CAST(eve_pcma.LEDGER_BALANCE AS FLOAT64))
        ELSE
            FORMAT("%.*f",2,CAST(current_active.LedgerBalance AS FLOAT64))
        END                                                                   AS Ledger_Balance,
    CASE
        WHEN pcma.TYPE IN ('SAVINGS')
            THEN eve_pcma.LEDGER_BALANCE_DATE
        ELSE
            current_active.LedgerBalanceDate
    END                                                                       AS Ledger_Balance_Date,
    CASE
        WHEN pcma.TYPE IN ('SAVINGS')
            THEN FORMAT("%.*f",2,CAST(eve_pcma.AVAILABLE_BALANCE AS FLOAT64))
        ELSE
            FORMAT("%.*f",2,CAST(current_active.AvailableBalance AS FLOAT64))
    END                                                                       AS Available_Balance,
    CASE
        WHEN pcma.TYPE IN ('SAVINGS')
            THEN eve_pcma.AVAILABLE_BALANCE_DATE
        ELSE current_active.AvailableBalanceDate
    END                                                                       AS Available_Balance_Date,
    NULL                                                                      AS Account_Note,
    'GCP'                                                                     AS System_ID,
    NULL                                                                      AS Fintech_Program
  FROM
    pcma LEFT OUTER JOIN primary
      ON pcma.ACCOUNT_UID = primary.ACCOUNT_UID
    LEFT OUTER JOIN current_active
      ON pcma.ACCOUNT_NO = SUBSTR(current_active.MAST_ACCOUNT_ID,2)
    INNER JOIN filtered_cust fc
      ON fc.CUSTOMER_UID = primary.CUSTOMER_UID
    LEFT OUTER JOIN account_status
      ON pcma.ACCOUNT_NO = SUBSTR(account_status.MAST_ACCOUNT_ID,2)
    LEFT OUTER JOIN status_desc
      ON status_desc.ACCOUNT_NO = pcma.ACCOUNT_NO
    LEFT OUTER JOIN eve_pcma
      ON pcma.ACCOUNT_NO  = eve_pcma.ACCOUNT_NO;