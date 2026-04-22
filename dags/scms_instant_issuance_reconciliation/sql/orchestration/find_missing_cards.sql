CREATE OR REPLACE VIEW `{view_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS SELECT
    missing_cards.orchestrationId,
    missing_cards.cardNumber,
    missing_cards.panSeqNumber,
    CASE missing_cards.orchestrationEventName
        WHEN 'REQUESTED_CARD_DETAILS' THEN 'CPMS-007'
        WHEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD' THEN 'CPMS-006'
        END AS reasonCode,
    CASE missing_cards.orchestrationEventName
        WHEN 'REQUESTED_CARD_DETAILS' THEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD event missing'
        WHEN 'CUSTOMER_INFO_FOR_REQUESTED_CARD' THEN 'REQUESTED_CARD_DETAILS event missing'
        END AS reasonDescription
FROM
    (SELECT DISTINCT
        orchestrationId,
        cardNumber,
        panSeqNumber,
        orchestrationEventName
    FROM `{orchestration_table_id}`
    WHERE cardNumber
    IN
        (SELECT
            cardNumber
        FROM
            (SELECT DISTINCT
                orchestrationEventName,
                cardNumber
            FROM `{orchestration_table_id}`
            WHERE orchestrationEventName = 'REQUESTED_CARD_DETAILS'
            OR orchestrationEventName = 'CUSTOMER_INFO_FOR_REQUESTED_CARD')
        GROUP BY cardNumber
        HAVING COUNT(1) = 1)
    AND (orchestrationEventName = 'REQUESTED_CARD_DETAILS' OR orchestrationEventName = 'CUSTOMER_INFO_FOR_REQUESTED_CARD')
    AND createDate <= '{end_time}') missing_cards
LEFT JOIN `{audit_table_id}` audit_table
ON missing_cards.cardNumber = audit_table.cardNumber
WHERE audit_table.cardNumber IS NULL
