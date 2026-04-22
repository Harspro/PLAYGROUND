WITH scoring_prep_credscore_engineered_features_latest_load AS (
    SELECT
        MAX(
            scoring_prep_credscore_engineered_features.REC_LOAD_TIMESTAMP
        ) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
        `pcb-{env}-landing.domain_scoring.SCORING_PREP_CREDSCORE_ENGINEERED_FEATURES` AS scoring_prep_credscore_engineered_features
)
SELECT
    *
EXCEPT
    (LATEST_REC_LOAD_TIMESTAMP)
FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CREDSCORE_ENGINEERED_FEATURES` AS scoring_prep_credscore_engineered_features
    INNER JOIN scoring_prep_credscore_engineered_features_latest_load AS scoring_prep_credscore_engineered_features_ll ON scoring_prep_credscore_engineered_features.REC_LOAD_TIMESTAMP = scoring_prep_credscore_engineered_features_ll.LATEST_REC_LOAD_TIMESTAMP;