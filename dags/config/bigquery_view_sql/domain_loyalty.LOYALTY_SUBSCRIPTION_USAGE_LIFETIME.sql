WITH loyalty_subscription_usage_lifetime AS (
  SELECT
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    SUBSCRIPTION_TYPE,
    SUM(USAGE_COUNT) AS total_usage_count
  FROM
    `pcb-{env}-landing.domain_loyalty.LOYALTY_SUBSCRIPTION_USAGE_LIFETIME`
  GROUP BY
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    SUBSCRIPTION_TYPE
),
loyalty_subscription_usage AS (
  SELECT
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    SUBSCRIPTION_TYPE,
    SUM(USAGE_COUNT) AS total_usage_count
  FROM
    `pcb-{env}-landing.domain_loyalty.LOYALTY_SUBSCRIPTION_USAGE`
  WHERE
    TX_YEAR = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
    AND TX_MONTH = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  GROUP BY
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    SUBSCRIPTION_TYPE
)
SELECT
  COALESCE(subscription_usage_lifetime.PCF_CUSTOMER_ID, subscription_usage_daily.PCF_CUSTOMER_ID) AS PCF_CUSTOMER_ID,
  COALESCE(subscription_usage_lifetime.CARD_BIN, subscription_usage_daily.CARD_BIN) AS CARD_BIN,
  COALESCE(subscription_usage_lifetime.LOYALTY_PARTNER, subscription_usage_daily.LOYALTY_PARTNER) AS LOYALTY_PARTNER,
  COALESCE(subscription_usage_lifetime.SUBSCRIPTION_TYPE, subscription_usage_daily.SUBSCRIPTION_TYPE) AS SUBSCRIPTION_TYPE,
  COALESCE(subscription_usage_lifetime.total_usage_count, 0) + COALESCE(subscription_usage_daily.total_usage_count, 0) AS USAGE_COUNT,
  SAFE_CAST(0 AS INTEGER) AS POINTS_EARNED,
  SAFE_CAST(0 AS INTEGER) AS MONETARY_VALUE,
  CURRENT_DATETIME() AS CREATE_DT,
  'INITIAL_LOAD' AS CREATE_USER_ID,
  CURRENT_DATETIME() AS UPDATE_DT,
  'MONTHLY_LOAD' AS UPDATE_USER_ID
FROM
  loyalty_subscription_usage_lifetime subscription_usage_lifetime
FULL OUTER JOIN
  loyalty_subscription_usage subscription_usage_daily
ON
  subscription_usage_lifetime.PCF_CUSTOMER_ID = subscription_usage_daily.PCF_CUSTOMER_ID
  AND subscription_usage_lifetime.CARD_BIN = subscription_usage_daily.CARD_BIN
  AND subscription_usage_lifetime.LOYALTY_PARTNER = subscription_usage_daily.LOYALTY_PARTNER
  AND subscription_usage_lifetime.SUBSCRIPTION_TYPE = subscription_usage_daily.SUBSCRIPTION_TYPE;