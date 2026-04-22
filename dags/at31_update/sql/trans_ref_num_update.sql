UPDATE
  `pcb-{env}-landing.domain_account_management.AT31`
SET
  at31_reference_num = REGEXP_REPLACE(at31_reference_num, r'^(.{11})19', r'\114')
WHERE
  CAST(SUBSTR(at31_reference_num,12,2) AS string)='19'
  AND at31_reference_num IN (
  SELECT
    at31_reference_num
  FROM
    `pcb-{env}-landing.domain_account_management.AT31` at31
  LEFT JOIN
    `pcb-{env}-landing.domain_payments.FUNDSMOVE_GL_REQ` fgr
  ON
    CAST(SUBSTR(at31.at31_reference_num,12,11) AS string)=fgr.REFERENCE_NUMBER
  WHERE
    file_create_dt > '2025-04-01'
    AND at31_date_post > '2025-04-01'
    AND at31_debit_credit_indicator = 'C'
    AND at31_transaction_category = 19
    AND at31_transaction_code = 108
    AND (CAST(SUBSTR(at31_reference_num,12,2) AS string) = '14'
      OR (CAST(SUBSTR(at31_reference_num,12,2) AS string)='19'
        AND fgr.gl_action_cd LIKE '%NPFI-PAYPEN%')) )