EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/digital-adoption/cli-offer-event-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE
  ) AS

WITH
    OFFER_DETAIL AS (
    SELECT
        offer_priority
    FROM
        `pcb-{env}-landing.domain_account_management.REF_CUSTOMER_OFFER_DETAIL`
    WHERE
        offer_type = 'Credit Limit Increase'
    QUALIFY
        ROW_NUMBER() OVER (ORDER BY offer_id DESC, offer_created_dt DESC) = 1)

SELECT
    CAST(PARSE_DATE('%Y%j', PIVOTED_EVENTS.new_data_1) AS STRING) AS offerCreatedDate,
    CAST(OFFER_DETAIL.offer_priority AS STRING) AS offerPriority,
    CAST(ACCOUNT_CUSTOMER.customer_uid AS STRING) AS customerId,
    CAST(ACCOUNT.account_uid AS STRING) AS accountId,
    CAST(PIVOTED_EVENTS.new_data_9 AS STRING) AS currentValue,
    CAST(PIVOTED_EVENTS.new_data_11 AS STRING) AS eligibleValue,
    CAST(DATE_ADD(
        PARSE_DATE('%Y%j', PIVOTED_EVENTS.new_data_1),
        INTERVAL CAST(PIVOTED_EVENTS.new_data_7 AS INT) DAY
    ) AS STRING) AS offerExpiryDate
FROM
    `pcb-{env}-landing.domain_account_management.EVENTS` AS EVENTS
PIVOT (
    MAX(EVENTS.ev3_event_old_data) AS old_data,
    MAX(EVENTS.ev3_event_new_data) AS new_data FOR EVENTS.ev3_event_field_num IN (1, 7, 9, 11)) AS PIVOTED_EVENTS
JOIN
    `pcb-{env}-landing.domain_account_management.ACCOUNT` AS ACCOUNT
ON
    PIVOTED_EVENTS.ev3_am00_account_id = CAST(ACCOUNT.account_no AS INT)
JOIN
    `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER` AS ACCOUNT_CUSTOMER
ON
    ACCOUNT.account_uid = ACCOUNT_CUSTOMER.account_uid
CROSS JOIN
    OFFER_DETAIL
WHERE
    DATE(PIVOTED_EVENTS.ev3_el01_julian_date) BETWEEN DATE_SUB(DATE('{last_dag_run_date}'), INTERVAL 7 DAY)
        AND DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)
    AND DATE(PIVOTED_EVENTS.rec_load_timestamp) BETWEEN DATE('{last_dag_run_date}')
        AND DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)
    AND PIVOTED_EVENTS.ev3_el01_cust_type = 0
    AND PIVOTED_EVENTS.ev3_el01_type_event = '974'
    AND ACCOUNT_CUSTOMER.account_customer_role_uid = 1
    AND CAST(PIVOTED_EVENTS.new_data_11 AS INT) > CAST(PIVOTED_EVENTS.new_data_9 AS INT);