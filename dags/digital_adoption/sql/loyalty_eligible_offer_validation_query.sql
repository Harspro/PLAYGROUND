SELECT
  COUNT(1) AS record_count
FROM
  `pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT`
WHERE
  DATE(ingestion_timestamp) = CURRENT_DATE()
  AND metadata.stream_name IS NULL;