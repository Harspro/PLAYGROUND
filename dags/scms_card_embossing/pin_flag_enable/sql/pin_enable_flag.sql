EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/embossing-pin-flag-enable/embossing-pin-flag-enable-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(SELECT
        DISTINCT SAFE_CAST(CARD_NUMBER AS STRING) AS CARD_NUMBER,
        SAFE_CAST(STATUS AS STRING) AS STATUS
FROM `pcb-{env}-landing.domain_card_management.EMBOSSING_PIN_FLAG_ENABLE`);