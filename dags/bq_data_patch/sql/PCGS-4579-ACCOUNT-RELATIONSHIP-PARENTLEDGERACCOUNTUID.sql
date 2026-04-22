UPDATE pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP AS AR
SET AR.parentLedgerAccountId = ACC.ACCOUNT_NO
FROM pcb-{env}-landing.domain_account_management.ACCOUNT AS ACC
WHERE AR.parentAccountId = CAST(ACC.ACCOUNT_UID AS STRING)
  AND AR.productCode = 'SV-ESA'
  AND AR.parentLedgerAccountId IS NULL;