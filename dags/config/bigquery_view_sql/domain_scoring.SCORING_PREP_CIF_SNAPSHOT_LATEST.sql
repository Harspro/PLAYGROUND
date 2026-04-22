WITH
  scoring_prep_cif_snapshot_latest_load AS (
    SELECT
      MAX(scoring_prep_cif_snapshot.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_CIF_SNAPSHOT`
        AS scoring_prep_cif_snapshot
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CIF_SNAPSHOT`
    AS scoring_prep_cif_snapshot
INNER JOIN
  scoring_prep_cif_snapshot_latest_load AS scoring_prep_cif_snapshot_ll
  ON
    scoring_prep_cif_snapshot.REC_LOAD_TIMESTAMP
    = scoring_prep_cif_snapshot_ll.LATEST_REC_LOAD_TIMESTAMP;