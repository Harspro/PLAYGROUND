-- Aggregation logic for Loyalty Points Earned.
CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
SELECT
  loyalty_points_earned.PCF_CUSTOMER_ID,
  loyalty_points_earned.TX_YEAR,
  loyalty_points_earned.TX_MONTH,
  loyalty_points_earned.CARD_BIN,
  CASE
    WHEN loyalty_points_earned.LOYALTY_PARTNER IN ('LCL', 'JF') THEN 'LCL'
    ELSE loyalty_points_earned.LOYALTY_PARTNER
  END AS LOYALTY_PARTNER,
  loyalty_points_earned.EARNING_CATEGORY,
  SUM(loyalty_points_earned.POINTS_EARNED) AS POINTS_EARNED,
  CURRENT_DATETIME() AS CREATE_DT,
  'DAILY_LOAD' AS CREATE_USER_ID,
  CURRENT_DATETIME() AS UPDATE_DT,
  'DAILY_LOAD' AS UPDATE_USER_ID
FROM `pcb-{env}-landing.domain_loyalty.LOYALTY_POINTS_EARNED` loyalty_points_earned
WHERE loyalty_points_earned.TX_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
AND loyalty_points_earned.TX_MONTH = EXTRACT(MONTH FROM CURRENT_DATE())
GROUP BY
  loyalty_points_earned.PCF_CUSTOMER_ID,
  loyalty_points_earned.TX_YEAR,
  loyalty_points_earned.TX_MONTH,
  loyalty_points_earned.CARD_BIN,
  LOYALTY_PARTNER,
  loyalty_points_earned.EARNING_CATEGORY;