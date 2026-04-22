EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
SELECT
  '' AS Account_Unique_ID,
  '' AS Account_Number,
  '' AS Name,
  '' AS First_Name,
  '' AS Middle_Name,
  '' AS Last_Name,
  '' AS Address_1,
  '' AS Address_2,
  '' AS City,
  '' AS Province,
  '' AS Postal_Code,
  '' AS Country,
  '' AS SIA_Individual_Flag,
  '' AS Interest_In_Deposit_Flag,
  '' AS Interest_In_Deposit
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0152`;