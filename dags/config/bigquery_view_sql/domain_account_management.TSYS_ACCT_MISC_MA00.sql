SELECT
  MAST_ACCOUNT_ID,
    MAST_REC_TYPE,
    MA00_SEGMENT_LENGTH,
    MA00_CLIENT_NUM,
    MA00_APPLICATION_NUM,
    MA00_APPLICATION_SUFFIX,
    FILE_CREATE_DT,
    REC_LOAD_TIMESTAMP
FROM
  pcb-{env}-landing.domain_account_management.TSYS_ACCT_MISC_MA00
  where file_create_dt=(select MAX(file_create_dt) from pcb-{env}-landing.domain_account_management.TSYS_ACCT_MISC_MA00)