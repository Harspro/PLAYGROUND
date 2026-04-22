INSERT INTO `{audit_table_id}`
SELECT
    orchestration_view.orchestrationId,
    orchestration_view.cardNumber,
    reasonCode,
    reasonDescription,
    orchestrationCreateDate,
    'orchestration' AS source_type,
    current_timestamp() AS auditTimestamp
FROM `{view_id}` orchestration_view
LEFT JOIN `{orchestration_table_id}` orchestration_table
ON orchestration_view.orchestrationId = orchestration_table.orchestrationId
