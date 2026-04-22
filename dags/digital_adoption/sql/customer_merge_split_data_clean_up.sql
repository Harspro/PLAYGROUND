UPDATE `pcb-{env}-landing.domain_customer_management.CUSTOMER_MERGE_SPLIT`
SET status = 'SUCCESS'
WHERE status = 'FAILURE'
AND victimCustomerId NOT IN (
    SELECT SAFE_CAST(CUSTOMER_UID AS STRING)
    FROM `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER`
);
