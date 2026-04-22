-- Aggregation logic for Loyalty Points Earned LifeTime.
CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
WITH loyalty_points_earned_lifetime AS (
  SELECT
    PCF_CUSTOMER_ID,
    CARD_BIN,
    CASE
      WHEN LOYALTY_PARTNER IN ('LCL', 'JF') THEN 'LCL'
      ELSE LOYALTY_PARTNER
    END AS LOYALTY_PARTNER,
    EARNING_CATEGORY,
    SUM(POINTS_EARNED) AS total_points_earned
  FROM
    `pcb-{env}-landing.domain_loyalty.LOYALTY_POINTS_EARNED_LIFETIME`
  GROUP BY
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    EARNING_CATEGORY
),
loyalty_points_earned AS (
  SELECT
    PCF_CUSTOMER_ID,
    CARD_BIN,
    CASE
      WHEN LOYALTY_PARTNER IN ('LCL', 'JF') THEN 'LCL'
      ELSE LOYALTY_PARTNER
    END AS LOYALTY_PARTNER,
    EARNING_CATEGORY,
    SUM(POINTS_EARNED) AS total_points_earned
  FROM
    `pcb-{env}-landing.domain_loyalty.LOYALTY_POINTS_EARNED`
  WHERE
    TX_YEAR = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
    AND TX_MONTH = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  GROUP BY
    PCF_CUSTOMER_ID,
    CARD_BIN,
    LOYALTY_PARTNER,
    EARNING_CATEGORY
)
SELECT
  COALESCE(points_earned_lifetime.PCF_CUSTOMER_ID, points_earned_daily.PCF_CUSTOMER_ID) AS PCF_CUSTOMER_ID,
  COALESCE(points_earned_lifetime.CARD_BIN, points_earned_daily.CARD_BIN) AS CARD_BIN,
  COALESCE(points_earned_lifetime.LOYALTY_PARTNER, points_earned_daily.LOYALTY_PARTNER) AS LOYALTY_PARTNER,
  COALESCE(points_earned_lifetime.EARNING_CATEGORY, points_earned_daily.EARNING_CATEGORY) AS EARNING_CATEGORY,
  COALESCE(points_earned_lifetime.total_points_earned, 0) + COALESCE(points_earned_daily.total_points_earned, 0) AS POINTS_EARNED,
  CURRENT_DATETIME() AS CREATE_DT,
  'INITIAL_LOAD' AS CREATE_USER_ID,
  CURRENT_DATETIME() AS UPDATE_DT,
  'MONTHLY_LOAD' AS UPDATE_USER_ID
FROM
  loyalty_points_earned_lifetime points_earned_lifetime
FULL OUTER JOIN
  loyalty_points_earned points_earned_daily
ON
  points_earned_lifetime.PCF_CUSTOMER_ID = points_earned_daily.PCF_CUSTOMER_ID
  AND points_earned_lifetime.CARD_BIN = points_earned_daily.CARD_BIN
  AND points_earned_lifetime.LOYALTY_PARTNER = points_earned_daily.LOYALTY_PARTNER
  AND points_earned_lifetime.EARNING_CATEGORY = points_earned_daily.EARNING_CATEGORY;