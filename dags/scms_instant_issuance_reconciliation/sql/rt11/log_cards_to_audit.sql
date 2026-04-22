INSERT INTO `{audit_table_id}`
SELECT
    rt11_view.orchestrationId,
    rt11_view.cardNumber,
    rt11_view.reasonCode,
    rt11_view.reasonDescription,
    'rt11' AS source_type,
    current_timestamp() AS auditTimestamp
FROM `{view_id}` rt11_view
