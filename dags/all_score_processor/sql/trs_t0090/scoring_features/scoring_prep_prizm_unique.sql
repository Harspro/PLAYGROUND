INSERT INTO
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PRIZM_UNIQUE`
(
  FSALDU,
  PRIZM,
  SG,
  LS,
  YEAR,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  prizm_2023_unique.FSALDU AS FSALDU,
  CAST(prizm_2023_unique.PRIZM AS STRING) AS PRIZM,
  prizm_2023_unique.SG AS SG,
  prizm_2023_unique.LS AS LS,
  '2023' AS YEAR,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{deploy-env}-creditrisk.SHARE_CUST_RISK_ACQ.PRIZM_2023_UNIQUE` as prizm_2023_unique;