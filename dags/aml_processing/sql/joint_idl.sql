WITH
  all_cust AS (
    SELECT DISTINCT
      CI.CUSTOMER_UID AS CUSTOMER_UID,
      CI.CUSTOMER_IDENTIFIER_NO
    FROM
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI
    WHERE
      CI.TYPE = 'PCF-CUSTOMER-ID'
      AND CI.DISABLED_IND = 'N'
  ),
  excluded_cust AS (
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
  ),
  account_customer AS (
    SELECT DISTINCT 
      AC.CUSTOMER_UID           AS CUSTOMER_UID,
      A.MAST_ACCOUNT_ID         AS ACCOUNT_NO
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC
      LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A
        ON AC.ACCOUNT_UID = A.ACCOUNT_UID
  ),
  filtered_cust AS (
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
  ),

  secondary AS (
    SELECT 
      a.ACCOUNT_UID,
      a.MAST_ACCOUNT_ID AS ACCOUNT_NO,
      ci.CUSTOMER_IDENTIFIER_NO AS secondary_pcfCustId,
      ac.ACTIVE_IND,
      ac.OPEN_DT,
      ac.DEACTIVATED_ON
    FROM 
      `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
      `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci,
      `pcb-{env}-curated.domain_account_management.PRODUCT` p
    WHERE 
      ac.ACCOUNT_CUSTOMER_ROLE_UID = 2
      AND ac.ACCOUNT_UID = a.account_uid
      AND ac.CUSTOMER_UID = ci.CUSTOMER_UID
      AND a.PRODUCT_UID = p.PRODUCT_UID
      AND ci.TYPE = 'PCF-CUSTOMER-ID'
      AND ci.DISABLED_IND = 'N'
      AND p.TYPE = 'CREDIT-CARD'
  ),

  primary AS 
  (
    SELECT
      ac.account_uid,
      ci.customer_uid,
      ci.customer_identifier_no primary_pcfCustId,
      ac.account_customer_role_uid,
      c.given_name,
      c.surname,
      ac.account_customer_role_uid
    FROM
      `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
      `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci,
      `pcb-{env}-curated.domain_customer_management.CUSTOMER` c,
      `pcb-{env}-curated.domain_account_management.PRODUCT` p
    WHERE
      ci.type = 'PCF-CUSTOMER-ID'
      AND ci.disabled_ind = 'N'
      AND ac.customer_uid = ci.customer_uid
      AND ci.customer_uid = c.customer_uid
      AND ac.account_customer_role_uid = 1
      AND ac.ACCOUNT_UID = a.account_uid
      AND a.PRODUCT_UID = p.PRODUCT_UID
      AND p.TYPE = 'CREDIT-CARD'
  )
SELECT
  '320'                                           AS Institution_Number,
  primary.primary_pcfCustId                       AS Customer_Number,
  secondary.ACCOUNT_NO                            AS Account_Number,
  'LN'                                            AS Account_Type_Code,
  '2001'                                          AS Joint_Branch_Number,
  secondary.secondary_pcfCustId                   AS Joint_Customer_Number,
  'AUTHORIZED_USER'                               AS Relationship_Type,
  NULL                                            AS CTR_Aggregation,
  NULL                                            AS LCTR_Aggregation,
  CASE
    WHEN secondary.ACTIVE_IND = 'Y' THEN 'ACTIVE'
    ELSE 'CLOSED'
  END                                             AS Status,
    EXTRACT(DATE FROM secondary.OPEN_DT)          AS Creation_Date,
    EXTRACT(DATE FROM secondary.DEACTIVATED_ON)   AS End_Date,
    'GCP'                                         AS SystemID
FROM
  filtered_cust
  INNER JOIN secondary
    ON filtered_cust.CUSTOMER_IDENTIFIER_NO = secondary.secondary_pcfCustId
  LEFT OUTER JOIN primary
    ON primary.account_uid = secondary.account_uid;

