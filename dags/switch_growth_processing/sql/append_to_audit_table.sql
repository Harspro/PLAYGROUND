CREATE TABLE IF NOT EXISTS `pcb-{env}-landing.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE_RECORDS_AUDIT` AS
SELECT
    product,
    txn_id,
    event_type,
    event_detail,
    risk_group,
    event_value,
    event_timestamp,
    google_email,
    not_google_email,
    gclid_id,
    feature_1,
    feature_2,
    feature_3,
    feature_4,
    feature_5,
    CURRENT_DATETIME('America/Toronto') AS rec_load_timestamp,
    '{run_id}' AS dag_run_id
FROM
    `pcb-{env}-curated.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE`
WHERE
    1 = 0;

INSERT INTO
    `pcb-{env}-landing.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE_RECORDS_AUDIT` (
        product,
        txn_id,
        event_type,
        event_detail,
        risk_group,
        event_value,
        event_timestamp,
        google_email,
        not_google_email,
        gclid_id,
        feature_1,
        feature_2,
        feature_3,
        feature_4,
        feature_5,
        rec_load_timestamp,
        dag_run_id
    )
SELECT
    product,
    txn_id,
    event_type,
    event_detail,
    risk_group,
    event_value,
    event_timestamp,
    google_email,
    not_google_email,
    gclid_id,
    feature_1,
    feature_2,
    feature_3,
    feature_4,
    feature_5,
    CURRENT_DATETIME('America/Toronto') AS rec_load_timestamp,
    '{run_id}' AS dag_run_id
FROM
    `pcb-{env}-curated.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE`;