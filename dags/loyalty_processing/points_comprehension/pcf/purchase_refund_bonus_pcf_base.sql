-- Aggregation logic for Daily/Monthly/Initial Load all PCF Base.
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
        WHEN cnsld_latest.account_campaign_id in ('113319094', '113319095', '113785861', '113629682', '113629684',
                                                  '113319093') THEN '516179' --world elite fee
        WHEN cnsld_latest.account_campaign_id in ('100038414', '100038416', '116600610') THEN '518116' --world elite (no fee card)
        WHEN cnsld_latest.account_campaign_id in ('100038691', '100038688', '117229276') THEN '522879' --world (no fee card)
        WHEN cnsld_latest.account_campaign_id in ('117288465', '117288466', '101894703', '109313672', '109313673',
                                                  '113955340', '114098789', '107306207', '111224486', '101894702',
                                                  '117713908', '117713917', '117288467', '100038550') THEN '518127' --silver (no fee card)
        --WHEN cnsld_latest.account_campaign_id in ('') THEN '533866' --pc individual (savings)
      END
      ) AS INTEGER
  ) AS card_bin,
'{loyalty_partner}' AS loyalty_partner,
'{earning_category}' AS earning_category,
'{transaction_type}' AS transaction_type,
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
AND cnsld_latest.account_campaign_id {campaign_id_condition}
AND LOWER(cnsld_latest.wallettransaction_status) = 'settled'
AND LOWER(cnsld_latest.wallettransaction_type) = '{wallettransaction_type_condition}'
AND LOWER(cnsld_latest.accounttransactiondetailsentity_accounttype) = 'masspromotion'
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