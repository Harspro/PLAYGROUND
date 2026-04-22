EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
SELECT
  '' AS Account_Unique_ID,
  '' AS Account_Number,
  '' AS Beneficiary_ID,
  '' AS SIA_Individual_Flag,
  '' AS Interest_In_Deposit_Flag,
  '' AS Interest_In_Deposit,
  '' AS IB_LEI
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0153` AS pcma_cdic_0153;