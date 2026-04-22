WITH scoring_prep_cred_das_history_base_ll_01 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_01`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_01`)
),
scoring_prep_cred_das_history_base_ll_02 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_02`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_02`)
),
scoring_prep_cred_das_history_base_ll_03 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_03`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_03`)
),
scoring_prep_cred_das_history_base_ll_04 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_04`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_04`)
),
scoring_prep_cred_das_history_base_ll_05 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_05`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_05`)
),
scoring_prep_cred_das_history_base_ll_06 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_06`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_06`)
),
scoring_prep_cred_das_history_base_ll_07 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_07`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_07`)
),
scoring_prep_cred_das_history_base_ll_08 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_08`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_08`)
),
scoring_prep_cred_das_history_base_ll_09 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_09`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_09`)
),
scoring_prep_cred_das_history_base_ll_10 AS (
    SELECT *
    FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_10`
    WHERE REC_LOAD_TIMESTAMP = (SELECT MAX(REC_LOAD_TIMESTAMP) FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_10`)
)
SELECT
    t1.*,
    t2.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t3.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t4.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t5.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t6.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t7.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t8.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t9.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID),
    t10.* EXCEPT (MAST_ACCOUNT_ID, REC_LOAD_TIMESTAMP, JOB_ID)
FROM
    scoring_prep_cred_das_history_base_ll_01 t1
    INNER JOIN scoring_prep_cred_das_history_base_ll_02 t2
        ON t1.MAST_ACCOUNT_ID = t2.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_03 t3
        ON t1.MAST_ACCOUNT_ID = t3.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_04 t4
        ON t1.MAST_ACCOUNT_ID = t4.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_05 t5
        ON t1.MAST_ACCOUNT_ID = t5.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_06 t6
        ON t1.MAST_ACCOUNT_ID = t6.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_07 t7
        ON t1.MAST_ACCOUNT_ID = t7.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_08 t8
        ON t1.MAST_ACCOUNT_ID = t8.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_09 t9
        ON t1.MAST_ACCOUNT_ID = t9.MAST_ACCOUNT_ID
    INNER JOIN scoring_prep_cred_das_history_base_ll_10 t10
        ON t1.MAST_ACCOUNT_ID = t10.MAST_ACCOUNT_ID