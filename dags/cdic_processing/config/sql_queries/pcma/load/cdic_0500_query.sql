INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500`
WITH
  pcma_cdic_0501_unique_result AS (
  SELECT
    pcma_cdic_0501.RELATIONSHIP_TYPE_CODE,
    MAX(pcma_cdic_0501.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP,
    pcma_cdic_0501.MI_RELATIONSHIP_TYPE
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0501` AS pcma_cdic_0501
  GROUP BY
    RELATIONSHIP_TYPE_CODE,
    MI_RELATIONSHIP_TYPE
  ORDER BY
    LATEST_REC_LOAD_TIMESTAMP DESC)
SELECT
  CAST(ci_t.CUSTOMER_UID AS STRING) AS DEPOSITOR_UNIQUE_ID,
  ci_t.CUSTOMER_IDENTIFIER_NO AS CUSTOMER_IDENTIFIER_NO,
  acc.MAST_ACCOUNT_ID AS ACCOUNT_UNIQUE_ID,
  CASE
    WHEN LOWER(acr.CODE)='secondary' AND COUNTIF( LOWER(acr.CODE) IN ('secondary', 'primary-card-holder')) OVER (PARTITION BY ac.ACCOUNT_UID) >= 2 THEN ( SELECT RELATIONSHIP_TYPE_CODE FROM pcma_cdic_0501_unique_result WHERE LOWER(MI_RELATIONSHIP_TYPE) = 'pcb-joint')
    ELSE (
  SELECT
    RELATIONSHIP_TYPE_CODE
  FROM
    pcma_cdic_0501_unique_result
  WHERE
    LOWER(MI_RELATIONSHIP_TYPE) = 'pcb-individual')
END
  AS RELATIONSHIP_TYPE_CODE,
  CASE
    WHEN LOWER(acr.CODE) = 'primary-card-holder' THEN 'Y'
    ELSE 'N'
END
  AS PRIMARY_ACCOUNT_HOLDER_FLAG,
  'Y' AS PAYEE_FLAG,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` AS ci_t
INNER JOIN
  `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AS ac
ON
  ci_t.CUSTOMER_UID = ac.CUSTOMER_UID
  AND ac.CUSTOMER_UID NOT IN(9647425,
    8365290,
    9658200)
INNER JOIN
  `pcb-{env}-curated.domain_account_management.ACCOUNT` AS acc
ON
  acc.ACCOUNT_UID = ac.ACCOUNT_UID
INNER JOIN
  `pcb-{env}-curated.domain_account_management.PRODUCT` AS p
ON
  p.PRODUCT_UID = acc.PRODUCT_UID
  AND LOWER(p.TYPE) IN ( 'individual',
    'joint',
    'goal',
    'additional' )
INNER JOIN
  `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER_ROLE` AS acr
ON
  ac.ACCOUNT_CUSTOMER_ROLE_UID = acr.ACCOUNT_CUSTOMER_ROLE_UID
WHERE
  LOWER(ac.ACTIVE_IND) = 'y'
  AND LOWER(ci_t.DISABLED_IND) = 'n'
  AND LOWER(ci_t.TYPE) = 'pcf-customer-id'
  AND LOWER(acr.CODE) IN ('primary-card-holder',
    'secondary');