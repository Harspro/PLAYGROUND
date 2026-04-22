CREATE OR REPLACE VIEW `{view_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS SELECT
    NULL AS orchestrationId,
    cardNumber AS cardNumber,
    0 AS panSeqNumber,
    'CPMS-008' AS reasonCode,
    'Both XML missing' AS reasonDescription
FROM (SELECT
        missing_cards.cardNumber
    FROM (SELECT DISTINCT
            app.cardNumber
        FROM (SELECT
                CAST(TSYS_ACCT_NUM AS string) as cardNumber
            FROM `{card_info_table_id}`
            WHERE PC_MC_CUST_APPL_ID
            IN (SELECT
                    PC_MC_CUST_APPL_ID
                FROM `{application_table_id}`
                WHERE APA_APP_STATUS = 'NEWACCOUNT'
                AND INGESTION_TIMESTAMP BETWEEN '{start_time}' AND '{end_time}')) app
        LEFT JOIN (SELECT
                *
            FROM `{orchestration_table_id}`
            WHERE orchestrationEventName = 'REQUESTED_CARD_DETAILS'
            OR orchestrationEventName = 'CUSTOMER_INFO_FOR_REQUESTED_CARD') orchest
        ON app.cardNumber = orchest.cardNumber
        WHERE orchest.cardNumber IS NULL) missing_cards
    LEFT JOIN `{audit_table_id}` audit_table
    ON missing_cards.cardNumber = audit_table.cardNumber
    WHERE audit_table.cardNumber IS NULL)
