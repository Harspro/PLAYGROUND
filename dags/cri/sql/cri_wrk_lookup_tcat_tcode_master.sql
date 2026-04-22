INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_MASTER`
SELECT
  TransactionCategory AS TCat,
  TransactionCode AS TCode,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM
  (
    SELECT DISTINCT TransactionCategory, TransactionCode
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_TRANSACTIONS_LATEST`
  );
