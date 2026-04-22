WITH
  scoring_prep_existing_pcmc_pcma_latest_load AS (
    SELECT
      MAX(scoring_prep_existing_pcmc_pcma.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_EXISTING_PCMC_PCMA`
        AS scoring_prep_existing_pcmc_pcma
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_EXISTING_PCMC_PCMA`
    AS scoring_prep_existing_pcmc_pcma
INNER JOIN
  scoring_prep_existing_pcmc_pcma_latest_load AS scoring_prep_existing_pcmc_pcma_ll
  ON
    scoring_prep_existing_pcmc_pcma.REC_LOAD_TIMESTAMP
    = scoring_prep_existing_pcmc_pcma_ll.LATEST_REC_LOAD_TIMESTAMP;