INSERT INTO `{TBL_CURATED_TRANSACTION_FEED_USER_TRACEABILITY}`
(
    TRANSACTION_ID,
    CUSTOMER_NUMBER,
    ACCOUNT_NUMBER,
    TRANSACTION_DATE_TIME,
    POSTED_DATE,
    EXTERNAL_BANK_NUMBER,
    EXTERNAL_CUSTOMER_NAME,
    DEVICE_ID,
    IP_ADDRESS,
    USERNAME,
    SESSION_DATE_TIME,
    INTERAC_REF_NUMBER,
    INTERAC_EMAIL,
    INTERAC_PHONE,
    INTERAC_BANK_ACCOUNT,
    PCF_MONEY_MOVE_REF_NUMBER,
    DEVICE_TYPE,
    RAIL_TYPE
)
SELECT
    txn_agg.TRANSACTION_ID                  AS TRANSACTION_ID,
    txn_agg.PRIMARY_CUSTOMER_NO             AS CUSTOMER_NUMBER,
    txn_agg.ACCOUNT_NO                      AS ACCOUNT_NUMBER,
    txn_agg.TRANSACTION_DT                  AS TRANSACTION_DATE_TIME,
    DATE(txn_agg.POSTED_DT)                 AS POSTED_DATE,
    {external_bank_number}                  AS EXTERNAL_BANK_NUMBER,
    {external_cust_name}                    AS EXTERNAL_CUSTOMER_NAME,
    {device_id}                             AS DEVICE_ID,
    {ip_address}                            AS IP_ADDRESS,
    {user_name}                             AS USERNAME,
    {session_date_time}                     AS SESSION_DATE_TIME,
    {interac_ref_number}                    AS INTERAC_REF_NUMBER,
    {interac_email}                         AS INTERAC_EMAIL,
    {interac_phone}                         AS INTERAC_PHONE,
    NULL                                    AS INTERAC_BANK_ACCOUNT,
    NULL                                    AS PCF_MONEY_MOVE_REF_NUMBER,
    {device_type}                           AS DEVICE_TYPE,
    '{rail_type}'                           AS RAIL_TYPE
FROM
    `{TRANSACTION_AGGREGATE_TABLE}` txn_agg
    {join_condition}
    {funds_move_join}
WHERE
    txn_agg.RAIL_TYPE = '{rail_type}'
    AND txn_agg.REC_LOAD_TIMESTAMP BETWEEN {start_time} AND {end_time}