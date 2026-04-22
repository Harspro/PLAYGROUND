INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_ACCOUNT_MASTER`
  (
    AccountId,
    ProductType,
    FirstDayOfEvaluation,
    LastDayOfEvaluation,
    BalanceAtStart,
    OpenDate,
    RecLoadTimestamp,
    JobId)
SELECT
  AccountId,
  ProductType,
  FirstDayOfEvaluation,
  LastDayOfEvaluation,
  BalanceAtStart,
  OpenDate,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER_LATEST`;
