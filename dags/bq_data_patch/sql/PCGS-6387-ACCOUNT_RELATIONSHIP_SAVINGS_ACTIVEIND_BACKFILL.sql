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
    'CLOSED' AS accountEventType,
    ar.arrangementId,
    ar.accountOpenDate,
    ar.accountDeactivatedDate,
    ar.pcbApplicationReference,
    ar.parentLedgerCustomerId,
    ar.ledgerCustomerId,
    'N' AS activeInd
FROM pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP ar
INNER JOIN pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER ac
    ON CAST(ar.accountId AS INTEGER) = ac.ACCOUNT_UID
    AND CAST(ar.customerId AS INTEGER) = ac.CUSTOMER_UID
    AND ac.ACTIVE_IND = 'N'
WHERE ar.productCode = 'SV-ESA'
    AND (ar.activeInd IS NULL OR ar.activeInd != 'N')
    AND NOT EXISTS (
        SELECT 1
        FROM pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP existing
        WHERE existing.customerId = ar.customerId
            AND existing.accountId = ar.accountId
            AND LOWER(existing.accountEventType) = 'closed'
            AND existing.activeInd = 'N'
    );