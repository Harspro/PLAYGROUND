-- Aggregation logic for Daily/Monthly/Initial Load LCL In Store Purchase/Refund Bonus.
CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
SELECT
  * EXCEPT(value, wallettransactionid),
  SAFE_CAST((SUM(value) {sum_condition}) AS INTEGER) AS points_earned,
  '{loyalty_partner}' AS loyalty_partner,
  '{earning_category}' AS earning_category,
  '{transaction_type}' AS transaction_type,
  'IN_STORE' AS purchase_method,
  CURRENT_DATETIME() AS create_dt,
  '{create_user_id}' AS create_user_id,
  CURRENT_DATETIME() AS update_dt,
  '{update_user_id}' AS update_user_id
FROM (
    SELECT
    cnsld_latest.wallettransactionid,
    SAFE_CAST(ee_id.value AS STRING) AS pcf_customer_id,
    {tx_year_month_expr}
    SAFE_CAST(LEFT(ee_payment.masked_card_number, 6) AS INTEGER) AS card_bin,
    cnsld_latest.value
    FROM (
        SELECT
        DISTINCT
          wallettransactionid,
          wallettransaction_walletid,
          wallettransaction_trans_dt,
          value,
          account_campaign_id,
          wallettransaction_parentwallettransactionid,
          wallettransaction_business_effective_ts
        FROM
          `{table_cnsld_latest}` cnsld_latest
        WHERE
          {cnsld_trans_dt_condition}
          AND LOWER(cnsld_latest.wallettransaction_status) = 'settled'
          AND LOWER(cnsld_latest.wallettransaction_type) {wallettransaction_type_condition}
          AND LOWER(cnsld_latest.accounttransactiondetailsentity_accounttype) = 'points'
          AND LOWER(cnsld_latest.wallettransaction_location_storeparentid) = '{storeparentid_condition}'
          {reasoncode_condition}
          AND LOWER(cnsld_latest.event) = '{event_condition}'
    ) cnsld_latest
    INNER JOIN (
      SELECT
      ee_identity_subset.identity_id,
      ee_identity_subset.business_effective_ts,
      ee_identity_subset.identity_state,
      ee_identity_subset.identity_status,
      ee_identity_subset.wallet_id,
      ee_identity_subset.identity_type,
      ee_identity_subset.value
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
        FROM
          `{table_ee_identity}`
        WHERE
          event_header_client_id NOT LIKE 'eestesting%'
          AND LOWER(identity_type) = 'cif'
          AND LOWER(identity_status) = 'active'
          AND business_effective_date >= DATE(2000,01,01)
        ) ee_identity_subset
      WHERE
      ee_identity_subset.RANK_ = 1
    ) ee_id ON cnsld_latest.wallettransaction_walletid=ee_id.wallet_id
    INNER JOIN `{table_ee_wallet_transaction_payment}` ee_payment ON
      (
        {inner_join_condition}
        cnsld_latest.wallettransaction_parentwallettransactionid = ee_payment.wallettransactionid
      )
      AND ABS(DATETIME_DIFF(cnsld_latest.wallettransaction_business_effective_ts, ee_payment.business_effective_ts, SECOND)) <= 1
    WHERE {eepayment_trans_dt_condition}
    AND LEFT(ee_payment.masked_card_number, 6) in ('516179', '518116', '522879', '518127', '533866')
    AND LOWER(ee_payment.payment_type) = 'creditcard'
    GROUP BY pcf_customer_id, {tx_year_month} card_bin, value, wallettransactionid
    )
GROUP BY pcf_customer_id, {tx_year_month} card_bin
HAVING points_earned {points_earned_condition_1};

-- Load staging table into landing zone table.
MERGE `pcb-{env}-landing.domain_loyalty.{final_table_id}` T
USING `{staging_table_id}` S
ON T.PCF_CUSTOMER_ID = S.PCF_CUSTOMER_ID
{tx_year_month_condition}
AND T.CARD_BIN = S.CARD_BIN
AND T.LOYALTY_PARTNER = S.LOYALTY_PARTNER
AND T.EARNING_CATEGORY = S.EARNING_CATEGORY
AND T.TRANSACTION_TYPE = S.TRANSACTION_TYPE
AND T.PURCHASE_METHOD = S.PURCHASE_METHOD
WHEN MATCHED AND S.POINTS_EARNED {points_earned_condition_2} T.POINTS_EARNED THEN
  UPDATE SET
  T.POINTS_EARNED = S.POINTS_EARNED,
  T.UPDATE_DT = S.UPDATE_DT,
  T.UPDATE_USER_ID = S.UPDATE_USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    PCF_CUSTOMER_ID,
    {tx_year_month_column}
    CARD_BIN,
    LOYALTY_PARTNER,
    EARNING_CATEGORY,
    TRANSACTION_TYPE,
    PURCHASE_METHOD,
    POINTS_EARNED,
    CREATE_DT,
    CREATE_USER_ID,
    UPDATE_DT,
    UPDATE_USER_ID
  )
  VALUES (
    S.PCF_CUSTOMER_ID,
    {tx_year_month_value}
    S.CARD_BIN,
    S.LOYALTY_PARTNER,
    S.EARNING_CATEGORY,
    S.TRANSACTION_TYPE,
    S.PURCHASE_METHOD,
    S.POINTS_EARNED,
    S.CREATE_DT,
    S.CREATE_USER_ID,
    S.UPDATE_DT,
    S.UPDATE_USER_ID
  );