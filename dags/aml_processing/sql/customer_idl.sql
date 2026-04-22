CREATE TEMP FUNCTION
  PARSE_ACCOUNT_STATUS_JULIAN_DATE(x INT64) AS (
    CASE
      WHEN (x <> 0) THEN PARSE_DATE('%Y%j', CAST((x) AS STRING))
    END
    );


WITH customer AS
(
  SELECT DISTINCT 
    CUSTOMER_UID , 
    NAME_PREFIX , 
    GIVEN_NAME , 
    SURNAME , 
    MIDDLE_NAME , 
    BIRTH_DT,
    CASE
      WHEN UPPER(NAME_PREFIX) IN (
        'PRINCE',
        'EARL',
        'JR',
        'JR.',
        'SIR',
        'LORD',
        'MR',
        'M',
        'BARON',
        'MR.') THEN 'M'
      WHEN UPPER(NAME_PREFIX) IN (
        'MISS',        
        'MRS',
        'MRS.',
        'MS',
        'MS.',
        'MME',
        'PRINCESS',
        'LADY',
        'MLLE',
        'MIS',        
        'MI',
        'MISS.') THEN 'F'
      ELSE 'X'
    END
      AS `Gender`
  FROM 
    `pcb-{env}-curated.domain_customer_management.CUSTOMER`
),

customer_identifier AS 
(
  SELECT DISTINCT 
    ci.CUSTOMER_IDENTIFIER_NO , 
    ci.TYPE , 
    ci.CUSTOMER_UID
  FROM 
    `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
  WHERE 
    ci.TYPE = 'PCF-CUSTOMER-ID'
    AND ci.DISABLED_IND = 'N'
),

ref_occupation AS 
(
  SELECT DISTINCT 
    ro.REF_OCCUPATION_UID , 
    ro.CODE
  FROM 
    `pcb-{env}-curated.domain_customer_management.REF_OCCUPATION` ro
),

customer_employment AS 
(
  SELECT DISTINCT 
    ce.CUSTOMER_UID , 
    ce.REF_OCCUPATION_UID , 
    ce.EMPLOYER_NAME , 
    ce.UPDATE_DT,
    ROW_NUMBER() OVER (PARTITION BY ce.CUSTOMER_UID ORDER BY ce.UPDATE_DT DESC) AS CUSTOMER_EMPLOYMENT_RANK
  FROM 
    `pcb-{env}-curated.domain_customer_management.CUSTOMER_EMPLOYMENT` ce
),

rank_one_customer_employment AS
(
  SELECT 
    *
  FROM 
    customer_employment
  WHERE
    CUSTOMER_EMPLOYMENT_RANK = 1
),

all_cust AS (
  SELECT DISTINCT 
    ci.CUSTOMER_UID AS CUSTOMER_UID,
    ci.CUSTOMER_IDENTIFIER_NO
  FROM
    `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
  WHERE
    ci.TYPE = 'PCF-CUSTOMER-ID'
    AND ci.DISABLED_IND = 'N'
),

excluded_cust AS (
  SELECT DISTINCT 
    SUBSTR(AM00.MAST_ACCOUNT_ID, 2) AS ACCOUNT_NO
  FROM
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
  WHERE
      am00.AM00_DATE_CLOSE IS NOT NULL
      AND am00.AM00_DATE_CLOSE > 0
      AND am00.AM00_DATE_CLOSE < CAST(
        FORMAT_DATE('%Y%j', DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 YEAR))
        AS INT64)
      AND am00.MAST_ACCOUNT_SUFFIX = 0
),

account_customer AS (
  SELECT DISTINCT 
    AC.CUSTOMER_UID   AS CUSTOMER_UID,
    A.MAST_ACCOUNT_ID AS ACCOUNT_NO
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac
    LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` a
      ON ac.ACCOUNT_UID = a.ACCOUNT_UID
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
        all_cust AS a
        INNER JOIN 
        (
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
        ) AS ac ON a.CUSTOMER_UID = ac.CUSTOMER_UID
    )ACTIVE_AC
),

pcma AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.account_uid) pcma_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER`    ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT`            a,
    `pcb-{env}-curated.domain_account_management.PRODUCT`             p
  WHERE
    ac.ACCOUNT_UID = a.ACCOUNT_UID
    AND a.PRODUCT_UID = p.PRODUCT_UID
    AND p.TYPE IN ('INDIVIDUAL' , 'GOAL' , 'JOINT' , 'ADDITIONAL')
  GROUP BY
    CUSTOMER_UID
),

pcmc AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.ACCOUNT_UID) pcma_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER`    ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT`            a,
    `pcb-{env}-curated.domain_account_management.PRODUCT`             p
  WHERE
    ac.ACCOUNT_UID = a.ACCOUNT_UID
    AND a.PRODUCT_UID = p.PRODUCT_UID
    AND p.TYPE = 'CREDIT-CARD'
  GROUP BY
    CUSTOMER_UID
),

frozen AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.ACCOUNT_UID) frozen_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
  WHERE
    ac.ACCOUNT_UID = a.ACCOUNT_UID
    AND a.MAST_ACCOUNT_ID = SUBSTR(am00.MAST_ACCOUNT_ID,2)
    AND am00.AM00_STATC_TIERED_AUTH = 'L7'
    AND am00.MAST_ACCOUNT_SUFFIX = 0
  GROUP BY
    ac.CUSTOMER_UID
),

dormant AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.ACCOUNT_UID) dormant_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` am02
  WHERE
    ac.ACCOUNT_UID = a.ACCOUNT_UID
    AND a.MAST_ACCOUNT_ID = SUBSTR(am02.MAST_ACCOUNT_ID,2)
    AND DATE_DIFF(CURRENT_DATE('America/Toronto'),PARSE_ACCOUNT_STATUS_JULIAN_DATE(GREATEST( AM02_DATE_LAST_FEE, 
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
                                                                                            AM02_DATE_LAST_fCHG )), DAY) > 180
    AND am02.AM00_APPLICATION_SUFFIX = 0
  GROUP BY
    ac.CUSTOMER_UID
),

deceased AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.ACCOUNT_UID) deceased_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
  WHERE
    ac.account_uid = a.account_uid
    AND a.MAST_ACCOUNT_ID = SUBSTR(am00.MAST_ACCOUNT_ID,2)
    AND am00.AM00_STATC_closed = 'DC'
    AND am00.MAST_ACCOUNT_SUFFIX = 0
  GROUP BY
    ac.CUSTOMER_UID
),

closed AS 
(
  SELECT DISTINCT
    ac.CUSTOMER_UID,
    COUNT(ac.ACCOUNT_UID) closed_cnt
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
  WHERE
    ac.ACCOUNT_UID = a.ACCOUNT_UID
    AND a.MAST_ACCOUNT_ID = SUBSTR(am00.MAST_ACCOUNT_ID,2)
    AND   am00.MAST_ACCOUNT_SUFFIX = 0
    AND ((am00.AM00_DATE_CLOSE IS NOT NULL
    AND am00.AM00_DATE_CLOSE <> 0
    )
    OR ((am00.AM00_STATC_CLOSED IS NOT NULL) AND (am00.AM00_STATC_CLOSED <> ""))
    OR am00.AM00_STATF_FRAUD = 'Y'
    OR ((am00.AM00_STATC_CHARGEOFF IS NOT NULL) AND (am00.AM00_STATC_CHARGEOFF <> "")))
  GROUP BY
    ac.CUSTOMER_UID
),

vip AS 
(
  SELECT
    COUNT(*) VIP_COUNT, 
    ac.CUSTOMER_UID
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac,
    `pcb-{env}-curated.domain_account_management.ACCOUNT` a,
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
  WHERE 
    ac.ACCOUNT_UID = a.ACCOUNT_UID 
    AND a.MAST_ACCOUNT_ID = SUBSTR(am00.MAST_ACCOUNT_ID,2) 
    AND am00.AM00_TYPEC_VIP in ('1', '2', '3')
    AND am00.MAST_ACCOUNT_SUFFIX = 0
  GROUP BY 
    ac.CUSTOMER_UID
),

employee_occupation AS 
(
  SELECT DISTINCT
    ce.CUSTOMER_UID,
    ro.CODE occupation_cd,
    ro.ISO_OCCUPATION_CODE,
    ro.SOC_CODE,
    ce.EMPLOYER_NAME,
    rei.CODE industry_cd,
    ce.ANNUAL_HOUSEHOLD_INCOME,
    ce.ANNUAL_PERSONAL_INCOME,
    res.code
  FROM
    (
      SELECT 
        *
      FROM
        (
          SELECT 
            cu.*,
            ROW_NUMBER() OVER(PARTITION BY CUSTOMER_UID ORDER BY UPDATE_DT DESC) rank
          FROM
            `pcb-{env}-curated.domain_customer_management.CUSTOMER_EMPLOYMENT` cu
        )
      WHERE
        rank = 1
     ) ce
  LEFT JOIN `pcb-{env}-curated.domain_customer_management.REF_EMPLOYMENT_INDUSTRY` rei 
    ON ce.REF_EMPLOYMENT_INDUSTRY_UID = rei.REF_EMPLOYMENT_INDUSTRY_UID
  LEFT JOIN `pcb-{env}-curated.domain_customer_management.REF_OCCUPATION` ro
    ON ce.REF_OCCUPATION_UID = ro.REF_OCCUPATION_UID
  LEFT JOIN `pcb-{env}-curated.domain_customer_management.REF_EMPLOYMENT_STATUS` res
    ON ce.REF_EMPLOYMENT_STATUS_UID = res.REF_EMPLOYMENT_STATUS_UID
),

politically_exposed AS 
(
  SELECT DISTINCT
    CUSTOMER_IDENTIFIER_NO,
    ACTIVE_IND AS PEP_IND
  FROM
    (
      SELECT
        ci.CUSTOMER_IDENTIFIER_NO,
        cpe.ACTIVE_IND,
        ROW_NUMBER() OVER
          ( PARTITION BY ci.CUSTOMER_IDENTIFIER_NO
            ORDER BY cpe.UPDATE_DT DESC
          ) AS r1
      FROM
        `pcb-{env}-curated.domain_aml.CUST_POLITICAL_EXPOSURE` cpe
        INNER JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
          ON cpe.CUSTOMER_UID = ci.CUSTOMER_UID
    )
  WHERE
    r1 = 1
),

grouped_filtered_customer AS 
(
  SELECT 
    CUSTOMER_UID,
    COUNT(account_customer.ACCOUNT_NO) accounts_total
  FROM 
    account_customer
  GROUP BY 
    CUSTOMER_UID
),

cust_date AS
(
  SELECT
    CUSTOMER_IDENTIFIER_NO,
    DATE_OPEN
  FROM
    (
      SELECT
        customer_identifier.CUSTOMER_IDENTIFIER_NO,
        MIN(ac.OPEN_DT) AS DATE_OPEN
      FROM
        `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER`  ac
        INNER JOIN customer_identifier 
          ON ac.CUSTOMER_UID = customer_identifier.CUSTOMER_UID
      GROUP BY
        customer_identifier.CUSTOMER_IDENTIFIER_NO
    )
)

SELECT DISTINCT 
  '320'                                                                                 AS `Institution_Number`,
  CASE
      WHEN pcma.pcma_cnt > 0 
        THEN '2002'
      ELSE '2001'
  END                                                                                   AS `Branch_Number`,
  fc.CUSTOMER_IDENTIFIER_NO                                                             AS `Customer_Number`,
  'PERSONAL'                                                                            AS `Customer_Type`,
  CASE 
    WHEN frozen.frozen_cnt = grouped_filtered_customer.accounts_total 
      THEN 'FROZEN'    -- all accounts are frozen
    WHEN deceased.deceased_cnt > 0 
      THEN 'DECEASED'  -- at least one account is marked deceased
    WHEN closed.closed_cnt = grouped_filtered_customer.accounts_total 
      THEN 'CLOSED'    -- all accounts are closed
    WHEN dormant.dormant_cnt = grouped_filtered_customer.accounts_total 
      THEN 'DORMANT'   -- all accounts are dormant
    ELSE 'ACTIVE'
  END                                                                                   AS `Status`,
  customer.SURNAME                                                                      AS `Last_Name`,
  customer.MIDDLE_NAME                                                                  AS `Middle_Name`,
  customer.GIVEN_NAME                                                                   AS `Given_Name`,
  FORMAT_DATE("%F" , customer.BIRTH_DT)                                                 AS `Date_of_Birth`,
  NULL                                                                                  AS `Corporate_Name`,
  NULL                                                                                  AS `Doing_Business_As`,
  customer.GENDER                                                                       AS `Gender`,
  CASE
    WHEN vip.VIP_COUNT > 0 
      THEN 'TRUE'
    ELSE 'FALSE'
  END                                                                                   AS `Employee_Flag`,
  CASE
    WHEN employee_occupation.code in ('STUDENT','HOME_MAKER','RETIRED','UNEMPLOYED')
    THEN '00-0000'
    ELSE employee_occupation.soc_code
  END                                                                                   AS Occupation_Code,
  CASE
    WHEN employee_occupation.code = 'STUDENT' THEN 'Student'
    WHEN employee_occupation.code = 'HOME_MAKER' THEN 'Home Maker'
    WHEN employee_occupation.code = 'RETIRED' Then 'Retired'
    WHEN employee_occupation.code = 'UNEMPLOYED' THEN 'Unemployed'
    ELSE CONCAT(employee_occupation.industry_cd,'-',employee_occupation.occupation_cd)
  END                                                                                   AS Occupation_Title,
  NULL                                                                                  AS `Industry_Code`,
  NULL                                                                                  AS `Industry_Description`,
  employee_occupation.EMPLOYER_NAME                                                     AS `Employer_Name`,
  NULL                                                                                  AS `Employer_CIF`,
  NULL                                                                                  AS `TIN`,
  NULL                                                                                  AS `TIN_Type`,
  NULL                                                                                  AS `Open_Date`,
  NULL                                                                                  AS `Closed_Date`,
  NULL                                                                                  AS `Date_of_Death`,
  'CA'                                                                                  AS `Residency_Country`,
  NULL                                                                                  AS `Citizenship_Country`,
  NULL                                                                                  AS `Politically_Exposed_Person`,
  NULL                                                                                  AS `Money_Service_Business`,
  NULL                                                                                  AS `ATM_Owner`,
  NULL                                                                                  AS `Non_Resident_Alien`,
  NULL                                                                                  AS `Correspondent_Account`,
  NULL                                                                                  AS `Embassy`,
  NULL                                                                                  AS `Manual_Risk_Score`,
  NULL                                                                                  AS `Fintech_Program`,
  employee_occupation.ANNUAL_PERSONAL_INCOME                                            AS `Annual_Personal_Income`,
  employee_occupation.ANNUAL_HOUSEHOLD_INCOME                                           AS `Annual_Household_Income`,
  customer.NAME_PREFIX                                                                  AS `Suffix`
FROM
  filtered_cust fc
  JOIN grouped_filtered_customer 
    ON grouped_filtered_customer.CUSTOMER_UID = fc.CUSTOMER_UID
  JOIN customer 
    ON fc.CUSTOMER_UID = customer.CUSTOMER_UID
  LEFT OUTER JOIN frozen 
    ON frozen.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN dormant 
    ON dormant.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN deceased 
    ON deceased.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN closed 
    ON closed.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN vip 
    ON vip.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN employee_occupation 
    ON employee_occupation.CUSTOMER_UID = fc.CUSTOMER_UID
  LEFT OUTER JOIN politically_exposed 
    ON politically_exposed.CUSTOMER_IDENTIFIER_NO = fc.CUSTOMER_IDENTIFIER_NO
  LEFT OUTER JOIN pcma 
    ON fc.CUSTOMER_UID = pcma.CUSTOMER_UID
  LEFT OUTER JOIN pcmc 
    ON fc.CUSTOMER_UID = pcmc.CUSTOMER_UID
  LEFT OUTER JOIN cust_date
    ON cust_date.CUSTOMER_IDENTIFIER_NO = fc.CUSTOMER_IDENTIFIER_NO;