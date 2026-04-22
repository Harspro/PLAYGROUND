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
    AC.CUSTOMER_UID AS CUSTOMER_UID,
    a.MAST_ACCOUNT_ID AS ACCOUNT_NO
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC
    LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON AC.ACCOUNT_UID = A.ACCOUNT_UID
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
phone AS (
  SELECT DISTINCT 
    CT.CUSTOMER_UID AS CUSTOMER_UID,
    CONCAT(
      PC.AREA_CODE,
      '-',
      SUBSTRING(CAST(PC.PHONE_NUMBER AS string), 1, 3),
      '-',
      SUBSTRING(CAST(PC.PHONE_NUMBER AS string), 4, 7)
    ) AS PHONE_NUM_FORMATTED,
    (
      CASE
        WHEN CT.CONTEXT = 'PRIMARY' THEN 'HOME'
        WHEN CT.CONTEXT = 'BUSINESS' THEN 'WORK'
        ELSE 'UNKNOWN'
      END
    ) AS PHONE_TYPE
  FROM
    `pcb-{env}-curated.domain_customer_management.CONTACT` CT
    INNER JOIN `pcb-{env}-curated.domain_customer_management.PHONE_CONTACT` PC ON CT.TYPE = 'PHONE'
    AND CT.CONTEXT IN ('PRIMARY', 'ALTERNATE', 'BUSINESS')
    AND CT.CONTACT_UID = PC.CONTACT_UID
),
email AS (
  SELECT DISTINCT
    CT.CUSTOMER_UID AS CUSTOMER_UID,
    EC.EMAIL_ADDRESS
  FROM
    `pcb-{env}-curated.domain_customer_management.CONTACT` CT
    INNER JOIN `pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT` EC ON CT.TYPE = 'EMAIL'
    AND CT.CONTEXT = 'PRIMARY'
    AND CT.CONTACT_UID = EC.CONTACT_UID
),
contact_feed AS (
  SElECT
    '320' AS Institution_Number,
    A.CUSTOMER_IDENTIFIER_NO AS Customer_Number,
    PHONE_NUM_FORMATTED AS Phone_Number,
    PHONE_TYPE AS Phone_Number_Type,
    EMAIL_ADDRESS AS Email_Address,
    NULL AS Phone_Number_Extension
  FROM
    filtered_cust AS A
    JOIN phone AS P ON A.CUSTOMER_UID = P.CUSTOMER_UID
    LEFT JOIN email AS E ON A.CUSTOMER_UID = E.CUSTOMER_UID
)

SELECT
  *
FROM
  contact_feed;