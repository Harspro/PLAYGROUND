CREATE TABLE IF NOT EXISTS `{table_vendor_pcf_current_switch_file}` AS
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
    feature_5
FROM
    `pcb-{env}-curated.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE`
WHERE
    1 = 0;

TRUNCATE TABLE `{table_vendor_pcf_current_switch_file}`;

INSERT INTO
    `{table_vendor_pcf_current_switch_file}` (
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
        feature_5
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
    feature_5
FROM
    `pcb-{env}-curated.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE`