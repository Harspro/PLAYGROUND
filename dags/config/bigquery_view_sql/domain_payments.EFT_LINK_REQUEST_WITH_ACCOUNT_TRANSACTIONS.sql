WITH
  LATEST_EFT_LINK_REQUEST AS (
  SELECT
    *
  FROM (
    SELECT
      customerId,
      initiationToken,
      requestDate,
      status,
      institutionNumber,
      transitNumber,
      accountNumber,
      nickname,
      CUSTOMER_IDENTIFIER_UID AS pcf_customer_id,
      INGESTION_TIMESTAMP,
      ROW_NUMBER() OVER (PARTITION BY initiationToken ORDER BY INGESTION_TIMESTAMP DESC) AS row_num
    FROM
      `pcb-{env}-landing.domain_payments.EFT_LINK_REQUEST_VERIFICATION` elv
    LEFT JOIN
      `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
    ON
      CAST(ci.customer_uid AS STRING) = elv.customerId
    WHERE
      ci.type = 'PCF-CUSTOMER-ID'
      AND ci.DISABLED_IND = 'N')
  WHERE
    row_num =1 )

SELECT
  e.institutionNumber AS INSTITUTION_NUMBER,
  e.transitNumber AS TRANSIT_NUMBER,
  e.accountNumber AS ACCOUNT_NUMBER,
  e.requestDate AS REQUEST_DATE,
  e.status AS STATUS,
  e.pcf_customer_id AS PCF_CUSTOMER_ID,
  DATETIME(TIMESTAMP(e.INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
  eftLinkRequestAccountTransactions AS EFT_LINK_REQUEST_ACCOUNT_TRANSACTIONS
FROM
  LATEST_EFT_LINK_REQUEST e
LEFT JOIN
  `pcb-{env}-landing.domain_payments.EFT_LINK_REQUEST_ACCOUNT_TRANSACTIONS` t
ON
  e.initiationToken = t.initiationToken