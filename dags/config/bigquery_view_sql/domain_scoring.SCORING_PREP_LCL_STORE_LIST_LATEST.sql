WITH
  scoring_prep_lcl_store_list_latest_load AS (
    SELECT
      MAX(scoring_prep_lcl_store_list.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_LCL_STORE_LIST`
        AS scoring_prep_lcl_store_list
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_LCL_STORE_LIST`
    AS scoring_prep_lcl_store_list
INNER JOIN
  scoring_prep_lcl_store_list_latest_load AS scoring_prep_lcl_store_list_ll
  ON
    scoring_prep_lcl_store_list.REC_LOAD_TIMESTAMP
    = scoring_prep_lcl_store_list_ll.LATEST_REC_LOAD_TIMESTAMP;