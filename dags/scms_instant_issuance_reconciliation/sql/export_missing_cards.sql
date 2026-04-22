EXPORT DATA
    OPTIONS (
    uri = '{output_uri_landing}',
    format = 'PARQUET',
    overwrite = true)
AS
    (SELECT
        SAFE_CAST(orchestrationId AS INT64) AS orchestrationId,
        SAFE_CAST(cardNumber AS STRING) AS cardNumber,
        SAFE_CAST(panSeqNumber AS STRING) AS panSeqNumber,
        SAFE_CAST(reasonCode AS STRING) AS reasonCode,
        SAFE_CAST(reasonDescription AS STRING) AS reasonDescription
     FROM `{view_id}`
     WHERE cardNumber IS NOT NULL);
