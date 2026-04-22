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

/*Temporary Table to fetch all the primary accounts of customers having a PCMC account*/
CREATE TEMP TABLE pcmcprimary AS (
    SELECT DISTINCT
      a.ACCOUNT_UID,
      a.MAST_ACCOUNT_ID AS ACCOUNT_NO,
      a.PRODUCT_UID,
      ac.CUSTOMER_UID,
      ac.ACCOUNT_CUSTOMER_ROLE_UID,
      ci.CUSTOMER_IDENTIFIER_NO,
      c.GIVEN_NAME,
      c.SURNAME,
      a.OPEN_DT,
      p.CODE
    FROM
      `pcb-{env}-curated.domain_customer_management.CUSTOMER` c
      JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac
        ON ac.CUSTOMER_UID = c.CUSTOMER_UID
      JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
        ON ci.customer_uid = ac.customer_uid
      JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` a
        ON ac.ACCOUNT_UID = a.ACCOUNT_UID
      JOIN `pcb-{env}-curated.domain_account_management.PRODUCT` p
        ON a.PRODUCT_UID = p.PRODUCT_UID
    WHERE
      ci.TYPE in ('PCF-CUSTOMER-ID')
      AND ci.DISABLED_IND in ('N')
      AND a.PRODUCT_UID in (1, 2, 3, 4, 5, 6, 12)
      AND ac.ACCOUNT_CUSTOMER_ROLE_UID = 1 -- Primary Account
  );

/*Temporary Table to fetch the account closed date , Ledger Balance , Ledger Balance Date , Available Balance and Available Balance Date
  using the TSYS tables*/
  CREATE TEMP TABLE current_active AS (
    SELECT DISTINCT
          am00.MAST_ACCOUNT_ID,
          PARSE_JULIAN_DATE(am00.AM00_DATE_CLOSE)                                                                      AS AccountClosedDate,
          (am02.AM02_BALANCE_CURRENT * -1)                                                                             AS LedgerBalance, 
          CURRENT_DATE('America/Toronto')                                                                              AS LedgerBalanceDate,
          ((am02.AM02_BALANCE_CURRENT * -1) - AM00_AMT_AUTHORIZATIONS_OUT + AM00_AUTH_PAYMENT_AMT + AM00_LIMIT_CREDIT) AS AvailableBalance,
          CURRENT_DATE('America/Toronto')                                                                              AS AvailableBalanceDate,
          ROUND(am00.AM00_LIMIT_CREDIT,2)                                                                              AS CreditLimit
      FROM 
           `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00,
           `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02 
      WHERE 
           am00.MAST_ACCOUNT_ID = am02.MAST_ACCOUNT_ID
           AND am00.MAST_ACCOUNT_SUFFIX = 0
           AND am02.AM00_APPLICATION_SUFFIX = 0
  );

/*Temporary Table to fetch all the customers*/ 
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

  /*Temporary Table to fetch all the customers who closed their accounts more than 7 years ago*/
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
      AC.CUSTOMER_UID   AS CUSTOMER_UID,
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
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00,
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02,
      account_customer
    WHERE
      am00.mast_account_id = am02.mast_account_id
      AND SUBSTR(am00.MAST_ACCOUNT_ID,2) = account_customer.ACCOUNT_NO
      AND am00.MAST_ACCOUNT_SUFFIX = 0
      AND am02.AM00_APPLICATION_SUFFIX = 0
  );

/*Temporary Table to fetch all the account statuses as per the mapping*/
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
        OR (am00.AM00_STATF_FRAUD = 'Y') 
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
        WHEN DATE_DIFF(CURRENT_DATE('Canada/Eastern'),PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE, 
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
      DATE_DIFF(CURRENT_DATE('Canada/Eastern'),PARSE_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE, 
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
                                                                           AM02_DATE_LAST_FCHG )), DAY) AS DAYS_SINCE_LAST_ACTIVITY,
      CURRENT_DATE('Canada/Eastern') AS DATE_TODAY,
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
                                  AM02_DATE_LAST_AUTO_PAYMENT, AM02_DATE_LAST_FCHG )) AS LAST_FIN_ACTIVITY_DATE

    FROM
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00,
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02
    WHERE
      am00.mast_account_id = am02.mast_account_id
      AND am00.MAST_ACCOUNT_SUFFIX = 0
      AND am02.AM00_APPLICATION_SUFFIX = 0
  );


/* Create a Table for the resultant dataset */
  DROP TABLE IF EXISTS `pcb-{env}-curated.cots_aml_verafin.ACCOUNT_PCMC_FEED` ;
  CREATE TABLE `pcb-{env}-curated.cots_aml_verafin.ACCOUNT_PCMC_FEED` AS

  SELECT DISTINCT
   '320' 						                                                           AS Institution_Number,
   pcmcprimary.CUSTOMER_IDENTIFIER_NO                                          AS Customer_Number,
   '2001'                      	                                               AS Account_Branch_Number,
   pcmcprimary.ACCOUNT_NO                                                      AS Account_Number,
   NULL                                                                        AS External_Number,
   'LN'                                                                        AS Account_Type_Code,
   CASE
      WHEN pcmcprimary.code = 'MX-PCE'        
        THEN 'BIN 518116 World Elite Credit Card'
      WHEN pcmcprimary.code = 'MW-PCH'        
        THEN 'BIN 522879 World Credit Card'
      WHEN pcmcprimary.code in ('MC-PCS', 'MC-PCI')        
        THEN 'BIN 518127 Silver Credit Card'
      WHEN pcmcprimary.code = 'MC-PCP'        
        THEN 'BIN 518127 Silver Preload'                
      WHEN pcmcprimary.code = 'MC-PCR'        
        THEN 'BIN 518127 Silver Rewards Card'
      WHEN pcmcprimary.code = 'MX-PFE'
        THEN 'BIN 516179 PC Insiders World Elite Card'
      ELSE
        'N/A'
    END                                                                        AS Account_Type_Code_Description,
    'PERSONAL'                                                                 AS Account_Purpose_Flag,
    'CAD'                                                                      AS Currency_Type,
    EXTRACT(DATE FROM pcmcprimary.OPEN_DT)                                     AS Open_Date,
    current_active.AccountClosedDate                                           AS Close_Date,
    account_status.STATUS                                                      AS Status,
    status_desc.Status_Description                                             AS Status_Description,
    NULL                                                                       AS Positive_Pay,
    FORMAT("%.*f",2,CAST(current_active.LedgerBalance AS FLOAT64))             AS Ledger_Balance,
    current_active.LedgerBalanceDate                                           AS Ledger_Balance_Date,
    NULL                                                                       AS Initial_Loan_Amount,
    FORMAT("%.*f",2,CAST(current_active.AvailableBalance AS FLOAT64))          AS Available_Balance,
    current_active.AvailableBalanceDate                                        AS Available_Balance_Date,
    NULL                                                                       AS Initial_Credit_Limit,
    FORMAT("%.*f",2,CAST(current_active.CreditLimit AS FLOAT64))               AS Current_Credit_Limit,
    'CREDIT_CARD'                                                              AS Loan_Type,
    NULL                                                                       AS Loan_Code,
    NULL                                                                       AS Loan_Code_Description,
    NULL                                                                       AS Other_Loan_Amount,
    NULL                                                                       AS Other_Loan_Amount_Description,
    NULL                                                                       AS Estimated_Maturity_Date,
    NULL                                                                       AS Payment_Schedule,
    NULL                                                                       AS Other_Schedule_Description,
    NULL                                                                       AS Payment_Amount_Due,
    NULL                                                                       AS Current_Payment_Date,
    NULL                                                                       AS First_Payment_Date,
    NULL                                                                       AS Loan_Sold_Date,
    NULL                                                                       AS Current_Interest_Rate,
    NULL                                                                       AS Annual_Income,
    NULL                                                                       AS Grace_days,
    NULL                                                                       AS Balloon_Amount,
    NULL                                                                       AS Credit_Score,
    NULL                                                                       AS Credit_Score_Date,
    NULL                                                                       AS Line_of_Credit,
    NULL                                                                       AS Loan_Purpose,
    NULL                                                                       AS Loan_Purpose_Description,
    NULL                                                                       AS Loan_Source,
    NULL                                                                       AS Loan_Source_Description,
    NULL                                                                       AS Drawdown_Loan,
    NULL                                                                       AS Secured_Loan,
    NULL                                                                       AS Primary_Collateral,
    NULL                                                                       AS Primary_Collateral_Description,
    NULL                                                                       AS Property_Accept_Date,
    NULL                                                                       AS Property_Type,
    NULL                                                                       AS Property_Code,
    NULL                                                                       AS Property_Code_Description,
    NULL                                                                       AS Property_Number,
    NULL                                                                       AS Property_ID,
    NULL                                                                       AS Property_Description,
    NULL                                                                       AS Property_Year,
    NULL                                                                       AS Property_Make,
    NULL                                                                       AS Property_Model,
    NULL                                                                       AS Property_State_Province,
    NULL                                                                       AS Property_Value,
    NULL                                                                       AS Property_Has_Title,
    NULL                                                                       AS Lockout_Code,
    NULL                                                                       AS Lockout_Description,
    NULL                                                                       AS Lockout_Date,
    NULL                                                                       AS Account_Note,
    'GCP'                                                                      AS System_ID,
    NULL                                                                       AS Fintech_Program,
    NULL                                                                       AS Participated_Flag,
    NULL                                                                       AS Consolidated_Flag
  FROM
    filtered_cust fc INNER JOIN pcmcprimary
      ON fc.CUSTOMER_UID = pcmcprimary.CUSTOMER_UID
    LEFT OUTER JOIN current_active
      ON pcmcprimary.ACCOUNT_NO = SUBSTR(current_active.mast_account_id,2)
    LEFT OUTER JOIN account_status
      ON pcmcprimary.ACCOUNT_NO = SUBSTR(account_status.mast_account_id,2)
    LEFT OUTER JOIN status_desc
      ON status_desc.ACCOUNT_NO = pcmcprimary.ACCOUNT_NO;