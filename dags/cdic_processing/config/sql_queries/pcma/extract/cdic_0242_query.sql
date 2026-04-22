EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
SELECT
  '' AS ISO_Currency_Code,
  '' AS Foreign_Currency_CAD_FX
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0242` AS pcma_cdic_0242;