-- Aggregation logic for Daily/Monthly/Initial Load ESSO Purchase PCF Base.
CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
SELECT
SAFE_CAST(ee_id.value AS STRING) AS pcf_customer_id,
{tx_year_month_expr}
SAFE_CAST(
      (
      CASE
        WHEN cnsld_latest.account_campaign_id in ('113762119','113762128','113881654','113762136',
                                                  '113762137','113762138','113762139','113835814',
                                                  '113835815','113835816','113835817','113835818',
                                                  '113835819','113835820','115036465','113835812',
                                                  '115521240','115759814','116062513','116430203') THEN '516179' --world elite fee
        WHEN cnsld_latest.account_campaign_id in ('101788706') THEN '518116' --world elite (no fee card)
        WHEN cnsld_latest.account_campaign_id in ('100038699') THEN '522879' --world (no fee card)
        WHEN cnsld_latest.account_campaign_id in ('113762115','113762121','113762130','113762132',
                                                  '113762133','113762134','113762135','100038399',
                                                  '101788705') THEN '518127' --silver (no fee card)
        --WHEN cnsld_latest.account_campaign_id in ('') THEN '533866' --pc individual (savings)
      END
      ) AS INTEGER
  ) AS card_bin,
'ESSO' AS loyalty_partner,
'FB' AS earning_category,
'PURCHASE' AS transaction_type,
'IN_STORE_AND_ONLINE' AS purchase_method,
SAFE_CAST(SUM(cnsld_latest.accounttransactiondetailsentity_amountpoints) AS INTEGER)  AS points_earned,
CURRENT_DATETIME() AS create_dt,
'{create_user_id}' AS create_user_id,
CURRENT_DATETIME() AS update_dt,
'{update_user_id}' AS update_user_id
FROM `{table_cnsld_latest}` cnsld_latest
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
WHERE {cnsld_trans_dt_condition}
AND cnsld_latest.account_campaign_id in
(
'113762119','113762128','113881654','113762136',
'113762137','113762138','113762139','113835814',
'113835815','113835816','113835817','113835818',
'113835819','113835820','115036465','113835812',
'115521240','115759814','116062513','101788706',
'100038699','113762115','113762121','113762130',
'113762132','113762133','113762134','113762135',
'100038399','101788705'{campaing_id}
)
AND LOWER(cnsld_latest.wallettransaction_location_storeparentid) in ('lpqx37','lpjr92','lphf21')
AND LOWER(cnsld_latest.wallettransaction_status) = 'settled'
AND LOWER(cnsld_latest.wallettransaction_type) = 'pcf_earn'
AND LOWER(cnsld_latest.accounttransactiondetailsentity_accounttype) in ('masspromotion','coupon')
GROUP BY pcf_customer_id{tx_year_month}, card_bin
HAVING points_earned >= 1;

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
WHEN MATCHED AND S.POINTS_EARNED > T.POINTS_EARNED THEN
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