INSERT INTO `{audit_table_id}`
SELECT
    orchestration_view.orchestrationId,
    orchestration_view.cardNumber,
    orchestration_view.reasonCode,
    orchestration_view.reasonDescription,
    'orchestration' AS source_type,
    current_timestamp() AS auditTimestamp
FROM `{view_id}` orchestration_view
LEFT JOIN
    (SELECT
        orchestrationId,
        MAX(createDate) AS createDate
    FROM `{orchestration_table_id}`
    WHERE orchestrationEventName = 'REQUESTED_CARD_DETAILS' OR orchestrationEventName = 'CUSTOMER_INFO_FOR_REQUESTED_CARD'
    GROUP BY orchestrationId) orchestration_table
ON orchestration_view.orchestrationId = orchestration_table.orchestrationId
