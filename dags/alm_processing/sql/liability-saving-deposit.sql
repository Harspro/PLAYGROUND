WITH LatestRecordLoad AS (
    SELECT
        p.ACCOUNT_ID AS ID,
        p.BALANCE_CLOSING AS PRINCIPAL,
        p.PRODUCT_GROUP AS PRODUCT_TYPE,
        p.ACCOUNT_OPEN_DATE AS ACCOUNT_OPEN_DATE,
        p.LEDGER_CUSTOMER_ID AS CUSTOMER_ID,
        CAST(a.parentLedgerAccountId AS NUMERIC) AS PARENTLEDGERACCOUNT_ID,
        p.STATUS AS STATUS,
        p.INTEREST_AMOUNT,
        p.INTEREST_RATE,
        ROW_NUMBER() OVER (PARTITION BY p.ACCOUNT_ID ORDER BY p.RECORD_LOAD_TIMESTAMP DESC) AS rn
    FROM pcb-{env}-curated.domain_account_management.PRODUCT_TRANSACT_ACCOUNT_EOD_DETAIL p
    LEFT JOIN pcb-{env}-curated.domain_account_management.ACCOUNT_RELATIONSHIP a
        ON CAST(p.ACCOUNT_ID AS STRING) = a.ledgerAccountId
        AND a.ledgerAccountType = 'TEMENOS'
)
,
FilteredLatestRecord AS (
    SELECT
        ID,
        COALESCE(PRINCIPAL, 0) AS PRINCIPAL,
        PRODUCT_TYPE,
        ACCOUNT_OPEN_DATE,
        CUSTOMER_ID,
        PARENTLEDGERACCOUNT_ID,
        INTEREST_AMOUNT,
        INTEREST_RATE
    FROM LatestRecordLoad
    WHERE rn = 1 AND STATUS <> 'CLOSE'
)
,
UnnestedInterestDetails AS (
    SELECT
        f.ID,
        f.PRINCIPAL,
        f.PRODUCT_TYPE,
        f.ACCOUNT_OPEN_DATE,
        f.CUSTOMER_ID,
        f.PARENTLEDGERACCOUNT_ID,
        COALESCE(accrual_info.AMOUNT, 0) AS INTEREST_ACCRUED,
        accrual_info.PERIOD_END_DATE,
        tier.EFFECTIVE_RATE AS RATE
    FROM FilteredLatestRecord f
    LEFT JOIN UNNEST(f.INTEREST_AMOUNT) AS interest_amount ON TRUE
    LEFT JOIN UNNEST(interest_amount.ACCRUAL_INFO) AS accrual_info ON TRUE
    LEFT JOIN UNNEST(f.INTEREST_RATE) AS interest_rate ON TRUE
    LEFT JOIN UNNEST(interest_rate.INTEREST_CONDITIONS) AS interest_condition ON TRUE
    LEFT JOIN UNNEST(interest_condition.TIER_DETAILS) AS tier ON TRUE
)
,
FilteredPeriodEnd AS (
    SELECT
        ID,
        PRINCIPAL,
        INTEREST_ACCRUED,
        RATE,
        PRODUCT_TYPE,
        ACCOUNT_OPEN_DATE,
        CUSTOMER_ID,
        PARENTLEDGERACCOUNT_ID,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY PERIOD_END_DATE DESC) AS period_rn
    FROM UnnestedInterestDetails
)

SELECT
    CAST("Demand Deposits" AS STRING) AS ROW_TYPE,
    ID,
    PRINCIPAL,
    INTEREST_ACCRUED,
    RATE,
    PRODUCT_TYPE,
    ACCOUNT_OPEN_DATE,
    CUSTOMER_ID,
    PARENTLEDGERACCOUNT_ID
FROM FilteredPeriodEnd
WHERE period_rn = 1
