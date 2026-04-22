WITH scoring_prep_pre_app_score_latest_load AS (
    SELECT
        MAX(
            scoring_prep_pre_app_score.REC_LOAD_TIMESTAMP
        ) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
        `pcb-{env}-landing.domain_scoring.SCORING_PREP_PRE_APP_SCORE` AS scoring_prep_pre_app_score
)
SELECT
    *
EXCEPT
    (LATEST_REC_LOAD_TIMESTAMP)
FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_PRE_APP_SCORE` AS scoring_prep_pre_app_score
    INNER JOIN scoring_prep_pre_app_score_latest_load AS scoring_prep_pre_app_score_ll ON scoring_prep_pre_app_score.REC_LOAD_TIMESTAMP = scoring_prep_pre_app_score_ll.LATEST_REC_LOAD_TIMESTAMP;