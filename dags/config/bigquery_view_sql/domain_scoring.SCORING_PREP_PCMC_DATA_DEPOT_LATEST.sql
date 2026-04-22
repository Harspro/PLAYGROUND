WITH
  scoring_prep_pcmc_data_depot_latest_load AS (
  SELECT
    MAX(scoring_prep_pcmc_data_depot.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` AS scoring_prep_pcmc_data_depot )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` AS scoring_prep_pcmc_data_depot
INNER JOIN
  scoring_prep_pcmc_data_depot_latest_load AS scoring_prep_pcmc_data_depot_ll
ON
  scoring_prep_pcmc_data_depot.REC_LOAD_TIMESTAMP = scoring_prep_pcmc_data_depot_ll.LATEST_REC_LOAD_TIMESTAMP;