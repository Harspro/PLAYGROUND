CREATE OR REPLACE VIEW `{view_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS SELECT
    orchestrationId,
    cardNumber,
    panSeqNumber,
    CASE orchestrationEventName
        WHEN 'REQUESTED_CARD_DETAILS' THEN 'CPMS-007'
        WHEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD' THEN 'CPMS-006'
        END AS reasonCode,
    CASE orchestrationEventName
        WHEN 'REQUESTED_CARD_DETAILS' THEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD event missing'
        WHEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD' THEN 'REQUESTED_CARD_DETAILS event missing'
        END AS reasonDescription
FROM `{orchestration_table_id}`
WHERE cardNumber
IN
    (SELECT
        cardNumber
    FROM `{orchestration_table_id}`
    GROUP BY cardNumber
    HAVING COUNT(1) = 1)
AND orchestrationCreateDate <= '{end_time}'
