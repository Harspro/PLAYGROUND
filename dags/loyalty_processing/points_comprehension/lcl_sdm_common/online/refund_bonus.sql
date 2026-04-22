-- Aggregation logic for Daily/Monthly/Initial Load Joe Fresh/Loblaw Online Refund Bonus.
CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
WITH online_bonus AS (
  SELECT
  SAFE_CAST(ee_id.value AS STRING) AS pcf_customer_id,
  cnsld_latest.wallettransactionid as wallettransactionid,
  {tx_year_month_expr_1}
  SAFE_CAST(JSON_VALUE(online_orders_payment.data, '$.first_6_digits') as INTEGER) AS card_bin,
  SAFE_CAST(SUM(cnsld_latest.value) AS INTEGER) AS points_earned
  FROM `{table_cnsld_latest}` cnsld_latest
  INNER JOIN (
    SELECT
    ee_identity_subset_velvet.identity_id,
    ee_identity_subset_velvet.business_effective_ts,
    ee_identity_subset_velvet.identity_state,
    ee_identity_subset_velvet.identity_status,
    ee_identity_subset_velvet.wallet_id,
    ee_identity_subset_velvet.identity_type,
    ee_identity_subset_velvet.value,
    ee_identity_subset_velvet.event_request_body
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
      FROM
        `{table_ee_identity}`
      WHERE
        event_header_client_id NOT LIKE 'eestesting%'
        AND LOWER(identity_type) in ('pcfcreditcard', 'pcfbnkgcard')
        AND LOWER(identity_status) = 'active'
        AND business_effective_date >= DATE(2000,01,01)
        AND LOWER(JSON_VALUE(event_request_body, '$.meta.pcfcardtype')) in ('pc_mastercard_world_elite_fee', 'pc_mastercard_world_elite',
                                                                            'pc_mastercard_world', 'pc_mastercard_silver', 'pc_individual')
      ) ee_identity_subset_velvet
    WHERE
    ee_identity_subset_velvet.RANK_ = 1
  ) ee_id_velvet ON cnsld_latest.wallettransaction_walletid=ee_id_velvet.wallet_id
  INNER JOIN (
    SELECT
    ee_identity_subset_velvet.identity_id,
    ee_identity_subset_velvet.business_effective_ts,
    ee_identity_subset_velvet.identity_state,
    ee_identity_subset_velvet.identity_status,
    ee_identity_subset_velvet.wallet_id,
    ee_identity_subset_velvet.identity_type,
    ee_identity_subset_velvet.value,
    ee_identity_subset_velvet.event_request_body
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
      FROM
        `{table_ee_identity}`
      WHERE
        event_header_client_id NOT LIKE 'eestesting%'
        AND LOWER(identity_type) = 'ecommerce'
        AND LOWER(identity_status) = 'active'
        AND business_effective_date >= DATE(2000,01,01)
      ) ee_identity_subset_velvet
    WHERE
    ee_identity_subset_velvet.RANK_ = 1
  ) ee_id_ecommerce ON cnsld_latest.wallettransaction_identityid=ee_id_ecommerce.identity_id
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
  ) ee_id ON ee_id_velvet.wallet_id=ee_id.wallet_id
  INNER JOIN (
      SELECT
      *
      FROM
      (
        SELECT
          *,
          online_orders_items.sellerId AS seller_id,
          online_orders_pM.paymentMethodId AS payment_method_id,
          FORMAT_TIMESTAMP('{timestamp_format}', updatedTimestamp) AS updated_date,
          ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY updatedTimestamp) AS RANK_
        FROM
          `{table_changelog_latest}`, UNNEST(paymentMethods) AS online_orders_pM, UNNEST(orderItems) AS online_orders_items
        WHERE
          total IS NOT NULL
          AND updatedTimestamp IS NOT NULL
          AND walletId IS NOT NULL
          AND LOWER(state) = 'order_completed'
          AND LOWER(online_orders_pM.cardType) = 'mastercard'
          AND online_orders_pM.paymentMethodId is not NUll
          AND online_orders_pM.paymentMethodId <> ''
      ) ee_identity_subset
    WHERE
    ee_identity_subset.RANK_ = 1
    ) online_orders
  ON
    ee_id_velvet.wallet_id = online_orders.walletId
    AND {merchantstoreid_condition}
    AND {wallettransactionid_condition}
  INNER JOIN (
    SELECT
    *
    FROM
      `{table_payment_methods_raw_latest}` online_orders_payment_subset
    WHERE
      LOWER(JSON_VALUE(online_orders_payment_subset.data, '$.card_type')) = 'mastercard'
      AND LOWER(JSON_VALUE(online_orders_payment_subset.data, '$.payment_method_type')) IN ('apple-pay', 'google-pay','payment-card')
      AND (
        JSON_VALUE(online_orders_payment_subset.data, '$.first_6_digits') in ('516179', '518116', '522879', '518127', '533866')
        OR JSON_VALUE(online_orders_payment_subset.data, '$.first_6_digits') IS NULL
      )
  ) online_orders_payment
  ON
    online_orders.payment_method_id = JSON_VALUE(online_orders_payment.data, '$.payment_method_id')
    AND JSON_VALUE(ee_id_velvet.event_request_body, '$.meta.pcfcardlastfourdigits') = JSON_VALUE(online_orders_payment.data, '$.last_4_digits')
  WHERE {cnsld_trans_dt_condition}
  AND {updated_date_condition}
  AND LOWER(cnsld_latest.wallettransaction_status) = 'settled'
  AND LOWER(cnsld_latest.wallettransaction_type) in ('settle','return')
  AND LOWER(cnsld_latest.accounttransactiondetailsentity_accounttype) = 'points'
  AND LOWER(cnsld_latest.wallettransaction_location_storeparentid) = '{storeparentid_condition}'
  {reasoncode_condition}
  AND LOWER(online_orders.tenantType) = '{tenant_type_condition}'
  AND LOWER(cnsld_latest.event) = 'credit'
  GROUP BY pcf_customer_id, wallettransactionid, {tx_year_month} card_bin
),
online_bonus_velvet_refunds AS (
  SELECT
  SAFE_CAST(ee_id.value AS STRING) AS pcf_customer_id,
  cnsld_latest.wallettransaction_parentwallettransactionid as parentwallettransactionid,
  {tx_year_month_expr_1}
  SAFE_CAST(
      (
      CASE
        WHEN LOWER(ee_id_velvet.pcfcardtype) = 'pc_mastercard_world_elite_fee' THEN '516179'
        WHEN LOWER(ee_id_velvet.pcfcardtype) = 'pc_mastercard_world_elite' THEN '518116'
        WHEN LOWER(ee_id_velvet.pcfcardtype) = 'pc_mastercard_world' THEN '522879'
        WHEN LOWER(ee_id_velvet.pcfcardtype) = 'pc_mastercard_silver' THEN '518127'
        WHEN LOWER(ee_id_velvet.pcfcardtype) = 'pc_individual' THEN '533866'
      END
      ) AS INTEGER
  ) AS card_bin,
  SAFE_CAST(
    SUM(
      CASE
        WHEN LOWER(cnsld_latest.event) = 'debit' THEN cnsld_latest.value * -1
        WHEN LOWER(cnsld_latest.event) = 'credit' THEN cnsld_latest.value
      END
    )
    AS INTEGER
  ) AS points_earned
  FROM `{table_cnsld_latest}` cnsld_latest
  INNER JOIN (
      SELECT
      ee_identity_subset_velvet.identity_id,
      ee_identity_subset_velvet.business_effective_ts,
      ee_identity_subset_velvet.identity_state,
      ee_identity_subset_velvet.identity_status,
      ee_identity_subset_velvet.wallet_id,
      ee_identity_subset_velvet.identity_type,
      ee_identity_subset_velvet.value,
      ee_identity_subset_velvet.event_request_body,
      JSON_VALUE(ee_identity_subset_velvet.event_request_body, '$.meta.pcfcardtype') as pcfcardtype
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
        FROM
          `{table_ee_identity}`
        WHERE
          event_header_client_id NOT LIKE 'eestesting%'
          AND LOWER(identity_type) in ('pcfcreditcard', 'pcfbnkgcard')
          AND LOWER(identity_status) = 'active'
          AND business_effective_date >= DATE(2000,01,01)
          AND LOWER(JSON_VALUE(event_request_body, '$.meta.pcfcardtype')) in ('pc_mastercard_world_elite_fee', 'pc_mastercard_world_elite',
                                                                             'pc_mastercard_world', 'pc_mastercard_silver', 'pc_individual')
        ) ee_identity_subset_velvet
      WHERE
      ee_identity_subset_velvet.RANK_ = 1
    ) ee_id_velvet ON cnsld_latest.wallettransaction_walletid=ee_id_velvet.wallet_id
  INNER JOIN (
    SELECT
    ee_identity_subset_velvet.identity_id,
    ee_identity_subset_velvet.business_effective_ts,
    ee_identity_subset_velvet.identity_state,
    ee_identity_subset_velvet.identity_status,
    ee_identity_subset_velvet.wallet_id,
    ee_identity_subset_velvet.identity_type,
    ee_identity_subset_velvet.value,
    ee_identity_subset_velvet.event_request_body
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
      FROM
        `{table_ee_identity}`
      WHERE
        event_header_client_id NOT LIKE 'eestesting%'
        AND LOWER(identity_type) = 'ecommerce'
        AND LOWER(identity_status) = 'active'
        AND business_effective_date >= DATE(2000,01,01)
      ) ee_identity_subset_velvet
    WHERE
    ee_identity_subset_velvet.RANK_ = 1
  ) ee_id_ecommerce ON cnsld_latest.wallettransaction_identityid=ee_id_ecommerce.identity_id
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
  AND LOWER(cnsld_latest.wallettransaction_status) = 'settled'
  AND LOWER(cnsld_latest.wallettransaction_type) in ('settle','return')
  AND LOWER(cnsld_latest.accounttransactiondetailsentity_accounttype) = 'points'
  AND LOWER(cnsld_latest.wallettransaction_location_storeparentid) = '{storeparentid_condition}'
  {reasoncode_condition}
  AND LOWER(cnsld_latest.event) in ('debit','credit')
  AND cnsld_latest.wallettransaction_parentwallettransactionid != '0'
  GROUP BY pcf_customer_id, parentwallettransactionid, {tx_year_month} card_bin
)
SELECT
online_bonus_velvet_refunds.pcf_customer_id as pcf_customer_id,
{tx_year_month_expr_2}
online_bonus_velvet_refunds.card_bin as card_bin,
'{loyalty_partner}' AS loyalty_partner,
'{earning_category}' AS earning_category,
'REFUND' AS transaction_type,
'ONLINE' AS purchase_method,
SAFE_CAST(SUM(online_bonus_velvet_refunds.points_earned) AS INTEGER) AS points_earned,
CURRENT_DATETIME() AS create_dt,
'{create_user_id}' AS create_user_id,
CURRENT_DATETIME() AS update_dt,
'{update_user_id}' AS update_user_id
FROM online_bonus_velvet_refunds
INNER JOIN online_bonus ON online_bonus_velvet_refunds.parentwallettransactionid = online_bonus.wallettransactionid
GROUP BY pcf_customer_id, {tx_year_month} card_bin
HAVING points_earned <= -1;

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
WHEN MATCHED AND S.POINTS_EARNED < T.POINTS_EARNED THEN
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