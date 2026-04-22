EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/sync-loyalty-profile/wnf-issue-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
    SELECT
        DISTINCT
        SAFE_CAST(rps.pcfCustomerId AS STRING) AS CUSTOMER_IDENTIFIER_NO,
        SAFE_CAST(custIden.CUSTOMER_UID AS STRING) AS CUSTOMER_UID,
        SAFE_CAST('SYNC' AS STRING) AS SYNC_LOYALTY_EVENT_TYPE
    FROM
        `pcb-{env}-curated.domain_loyalty.PC_POINTS_REPROCESS` rps
    LEFT JOIN
        `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` custIden
    ON
        rps.pcfCustomerId = custIden.CUSTOMER_IDENTIFIER_NO
    WHERE
        LOWER(rps.failureReasonCode) = 'wnf'
        AND DATE(rps.INGESTION_TIMESTAMP) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                                              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)