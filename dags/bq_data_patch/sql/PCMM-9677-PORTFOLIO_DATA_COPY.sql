INSERT INTO
  pcb-{env}-landing.domain_ledger.PORTFOLIO_DATA
SELECT
  *
FROM
  pcb-{env}-landing.domain_payments.PORTFOLIO_DETL
WHERE
  FILE_CREATE_DT > (SELECT MAX(FILE_CREATE_DT) FROM pcb-{env}-landing.domain_ledger.PORTFOLIO_DATA );
