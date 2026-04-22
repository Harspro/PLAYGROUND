INSERT INTO
  pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP
  (customerId,
    pcfCustomerId,
    customerRole,
    accountId,
    ledgerAccountId,
    ledgerAccountType,
    productCode,
    accountEventType,
    accountOpenDate,
    ledgerCustomerId,
    activeInd)
SELECT
  CAST(acc_cust.CUSTOMER_UID AS STRING) AS customerId,
  cust_idnt.CUSTOMER_IDENTIFIER_NO AS pcfCustomerId,
  acc_cust_rl.CODE AS customerRole,
  CAST(acc.ACCOUNT_UID AS STRING) AS accountId,
  acc.ACCOUNT_NO AS ledgerAccountId,
  acc.TYPE AS ledgerAccountType,
  prd.CODE AS productCode,
  'ONBOARDING' AS accountEventType,
  CAST(acc.OPEN_DT AS DATE) AS accountOpenDate,
  acc_cust.ACCOUNT_CUSTOMER_NO AS ledgerCustomerId,
  'Y' AS activeInd
FROM
  pcb-{env}-landing.domain_account_management.ACCOUNT acc
INNER JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER acc_cust
ON
  acc.ACCOUNT_UID = acc_cust.ACCOUNT_UID
LEFT JOIN
  pcb-{env}-landing.domain_account_management.PRODUCT prd
ON
  acc.PRODUCT_UID = prd.PRODUCT_UID
LEFT JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER_ROLE acc_cust_rl
ON
  acc_cust.ACCOUNT_CUSTOMER_ROLE_UID = acc_cust_rl.ACCOUNT_CUSTOMER_ROLE_UID
LEFT JOIN
  pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER cust_idnt
ON
  acc_cust.CUSTOMER_UID = cust_idnt.CUSTOMER_UID
  AND cust_idnt.TYPE = 'PCF-CUSTOMER-ID'
LEFT JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP acc_rltn
ON
  CAST(acc_rltn.accountId AS INT64) = acc.ACCOUNT_UID
WHERE
  acc_rltn.accountId IS NULL
  AND prd.CODE IN ('MC-PCI',
    'MX-PCE',
    'MW-PCH',
    'MX-PFE',
    'MC-PCS',
    'MC-PCP',
    'MC-PCR');  -- PCMC products


INSERT INTO
  pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP
  (customerId,
    pcfCustomerId,
    customerRole,
    accountId,
    ledgerAccountId,
    ledgerAccountType,
    productCode,
    accountEventType,
    accountOpenDate,
    ledgerCustomerId,
    activeInd)
SELECT
  CAST(acc_cust.CUSTOMER_UID AS STRING) AS customerId,
  cust_idnt.CUSTOMER_IDENTIFIER_NO AS pcfCustomerId,
  acc_cust_rl.CODE AS customerRole,
  CAST(acc.ACCOUNT_UID AS STRING) AS accountId,
  acc.ACCOUNT_NO AS ledgerAccountId,
  acc.TYPE AS ledgerAccountType,
  prd.CODE AS productCode,
  'ONBOARDING' AS accountEventType,
  CAST(acc.OPEN_DT AS DATE) AS accountOpenDate,
  acc_cust.ACCOUNT_CUSTOMER_NO AS ledgerCustomerId,
  'Y' AS activeInd
FROM
  pcb-{env}-landing.domain_account_management.ACCOUNT acc
INNER JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER acc_cust
ON
  acc.ACCOUNT_UID = acc_cust.ACCOUNT_UID
LEFT JOIN
  pcb-{env}-landing.domain_account_management.PRODUCT prd
ON
  acc.PRODUCT_UID = prd.PRODUCT_UID
LEFT JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER_ROLE acc_cust_rl
ON
  acc_cust.ACCOUNT_CUSTOMER_ROLE_UID = acc_cust_rl.ACCOUNT_CUSTOMER_ROLE_UID
LEFT JOIN
  pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER cust_idnt
ON
  acc_cust.CUSTOMER_UID = cust_idnt.CUSTOMER_UID
  AND cust_idnt.TYPE = 'PCF-CUSTOMER-ID'
LEFT JOIN
  pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP acc_rltn
ON
  CAST(acc_rltn.accountId AS INT64) = acc.ACCOUNT_UID
WHERE
  acc_rltn.accountId IS NULL
  AND prd.CODE IN ('MC-PDG',
    'MC-PDI');  -- PCMA products
