INSERT INTO pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP (
    customerId,
    pcfCustomerId,
    customerRole,
    accountId,
    parentAccountId,
    ledgerAccountId,
    ledgerAccountType,
    parentLedgerAccountId,
    parentLedgerAccountType,
    productCode,
    accountEventType,
    arrangementId,
    accountOpenDate,
    accountDeactivatedDate,
    pcbApplicationReference,
    parentLedgerCustomerId,
    ledgerCustomerId,
    activeInd
)
SELECT
    ar.customerId,
    ar.pcfCustomerId,
    ar.customerRole,
    ar.accountId,
    ar.parentAccountId,
    ar.ledgerAccountId,
    ar.ledgerAccountType,
    ar.parentLedgerAccountId,
    ar.parentLedgerAccountType,
    ar.productCode,
    'MERGE' AS accountEventType,
    ar.arrangementId,
    ar.accountOpenDate,
    ar.accountDeactivatedDate,
    ar.pcbApplicationReference,
    ar.parentLedgerCustomerId,
    ar.ledgerCustomerId,
    'N' AS activeInd
FROM pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP ar
INNER JOIN pcb-{env}-landing.domain_customer_management.CUSTOMER c
    ON CAST(ar.customerId AS INTEGER) = c.CUSTOMER_UID
    AND (LOWER(c.INACTIVE_REASON_CODE) = 'merged' OR c.INACTIVE_REASON_CODE IS NULL)
LEFT JOIN pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER ac
    ON CAST(ar.customerId AS INTEGER) = ac.CUSTOMER_UID
WHERE ar.activeInd IS NULL
    AND ac.CUSTOMER_UID IS NULL;  -- Customer not present in ACCOUNT_CUSTOMER table (MERGE)


INSERT INTO pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP (
    customerId,
    pcfCustomerId,
    customerRole,
    accountId,
    parentAccountId,
    ledgerAccountId,
    ledgerAccountType,
    parentLedgerAccountId,
    parentLedgerAccountType,
    productCode,
    accountEventType,
    arrangementId,
    accountOpenDate,
    accountDeactivatedDate,
    pcbApplicationReference,
    parentLedgerCustomerId,
    ledgerCustomerId,
    activeInd
)
SELECT
    ar.customerId,
    ar.pcfCustomerId,
    ar.customerRole,
    ar.accountId,
    ar.parentAccountId,
    ar.ledgerAccountId,
    ar.ledgerAccountType,
    ar.parentLedgerAccountId,
    ar.parentLedgerAccountType,
    ar.productCode,
    'SPLIT' AS accountEventType,
    ar.arrangementId,
    ar.accountOpenDate,
    ar.accountDeactivatedDate,
    ar.pcbApplicationReference,
    ar.parentLedgerCustomerId,
    ar.ledgerCustomerId,
    'N' AS activeInd
FROM pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP ar
LEFT JOIN pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER ac1
    ON CAST(ar.accountId AS INTEGER) = ac1.ACCOUNT_UID
    AND CAST(ar.customerId AS INTEGER) = ac1.CUSTOMER_UID
LEFT JOIN pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER ac2
    ON CAST(ar.customerId AS INTEGER) = ac2.CUSTOMER_UID
WHERE ar.activeInd IS NULL
    AND ac1.ACCOUNT_UID IS NULL  -- Specific accountId-customerId combination NOT in ACCOUNT_CUSTOMER
    AND ac2.CUSTOMER_UID IS NOT NULL;  -- But customerId IS present in ACCOUNT_CUSTOMER (with different account) (SPLIT)