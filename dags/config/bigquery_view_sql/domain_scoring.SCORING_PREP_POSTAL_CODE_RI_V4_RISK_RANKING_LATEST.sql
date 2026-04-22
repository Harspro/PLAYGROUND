  WITH
  scoring_prep_postal_code_ri_v4_risk_ranking_latest_load AS (
  SELECT
    MAX(scoring_prep_postal_code_ri_v4_risk_ranking.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_POSTAL_CODE_RI_V4_RISK_RANKING` AS scoring_prep_postal_code_ri_v4_risk_ranking )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_POSTAL_CODE_RI_V4_RISK_RANKING` AS scoring_prep_postal_code_ri_v4_risk_ranking
INNER JOIN
  scoring_prep_postal_code_ri_v4_risk_ranking_latest_load AS scoring_prep_postal_code_ri_v4_risk_ranking_ll
ON
  scoring_prep_postal_code_ri_v4_risk_ranking.REC_LOAD_TIMESTAMP = scoring_prep_postal_code_ri_v4_risk_ranking_ll.LATEST_REC_LOAD_TIMESTAMP;