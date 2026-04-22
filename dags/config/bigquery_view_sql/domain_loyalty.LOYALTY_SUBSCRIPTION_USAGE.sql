SELECT
  loyalty_subscription_usage.PCF_CUSTOMER_ID,
  loyalty_subscription_usage.TX_YEAR,
  loyalty_subscription_usage.TX_MONTH,
  loyalty_subscription_usage.CARD_BIN,
  loyalty_subscription_usage.LOYALTY_PARTNER,
  loyalty_subscription_usage.SUBSCRIPTION_TYPE,
  loyalty_subscription_usage.USAGE_COUNT,
  SAFE_CAST(0 AS INTEGER) AS POINTS_EARNED,
  SAFE_CAST(0 AS INTEGER) AS MONETARY_VALUE,
  CURRENT_DATETIME() AS CREATE_DT,
  'DAILY_LOAD' AS CREATE_USER_ID,
  CURRENT_DATETIME() AS UPDATE_DT,
  'DAILY_LOAD' AS UPDATE_USER_ID
FROM `pcb-{env}-landing.domain_loyalty.LOYALTY_SUBSCRIPTION_USAGE` loyalty_subscription_usage
WHERE loyalty_subscription_usage.TX_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
AND loyalty_subscription_usage.TX_MONTH = EXTRACT(MONTH FROM CURRENT_DATE());