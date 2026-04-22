SELECT
  COUNT(*) as COUNT
FROM
  `pcb-{env}-landing.domain_payments.BILLER_REFRESH_RAW`
WHERE
  FILE_NAME = '<<FILE_NAME>>'