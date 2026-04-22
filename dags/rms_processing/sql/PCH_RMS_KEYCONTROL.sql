INSERT INTO
  {output_table_name}
SELECT
  acno.account_number,
  (
  SELECT
    MAX(POSTING_DATE)
  FROM
    pcb-{env}-processing.domain_account_management.RMS_SCORE_HEDR
  ),
  DATETIME('{file_create_dt}')
FROM (
  SELECT
    distinct(account_number)
  FROM
    pcb-{env}-landing.domain_account_management.RMS_NEW_ACCOUNT_01
  WHERE
    file_create_dt = '{file_create_dt}'
  UNION DISTINCT
  SELECT
    distinct(account_number)
  FROM
    pcb-{env}-landing.domain_account_management.RMS_MAINT_ACCOUNT_04
  WHERE
    file_create_dt = '{file_create_dt}' ) acno
LEFT JOIN
  pcb-{env}-landing.domain_account_management.PCH_RMS_KEYCONTROL AS prk
ON
  prk.account_number = acno.account_number
WHERE
  prk.account_number IS NULL ;