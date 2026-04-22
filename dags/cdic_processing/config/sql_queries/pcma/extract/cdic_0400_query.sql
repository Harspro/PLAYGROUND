EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
SELECT
  '' AS Account_Unique_ID,
  '' AS Transaction_Number,
  '' AS Transaction_Item_Number,
  '' AS Created_Date,
  '' AS Posted_Date,
  '' AS Transaction_Value,
  '' AS Foreign_Value,
  '' AS Transaction_Code,
  '' AS Currency_Code,
  '' AS Debit_Credit_Flag
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0400`;