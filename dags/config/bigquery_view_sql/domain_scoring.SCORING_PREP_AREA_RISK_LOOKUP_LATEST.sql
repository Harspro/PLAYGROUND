WITH
  scoring_prep_area_risk_lookup_latest_load AS (
    SELECT
      MAX(scoring_prep_area_risk_lookup.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_AREA_RISK_LOOKUP`
        AS scoring_prep_area_risk_lookup
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_AREA_RISK_LOOKUP`
    AS scoring_prep_area_risk_lookup
INNER JOIN
  scoring_prep_area_risk_lookup_latest_load AS scoring_prep_area_risk_lookup_ll
  ON
    scoring_prep_area_risk_lookup.REC_LOAD_TIMESTAMP
    = scoring_prep_area_risk_lookup_ll.LATEST_REC_LOAD_TIMESTAMP;