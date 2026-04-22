CREATE
OR REPLACE VIEW `pcb-{env}-curated.cots_digital_marketing.PCF_CURRENT_SWITCH_FILE` AS
SELECT
    *
EXCEPT
(
        risk_group,
        event_value,
        google_email,
        not_google_email
    ),
    substr(trim(risk_group), 1, 1) AS risk_group,
    substr(trim(event_value), 1, 1) AS event_value,
    TO_HEX(SHA256(google_email)) AS google_email,
    TO_HEX(SHA256(not_google_email)) AS not_google_email
FROM
    `pcb-{deploy-env}-creditrisk.STRAT_SWITCH_ACQ_ARCHIVE.pcf_current_switch_file`