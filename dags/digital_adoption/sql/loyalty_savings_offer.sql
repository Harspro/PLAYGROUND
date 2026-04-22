WITH
  MONEY_ACCOUNT AS(
  SELECT
    a.account_uid,
    a.account_no,
    b.customer_uid
  FROM
    `pcb-{env}-landing.domain_account_management.ACCOUNT` a
  JOIN
    `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER` b
  ON
    a.account_uid=b.account_uid
    AND b.account_customer_role_uid=1
    AND b.active_ind='Y'
  WHERE
    a.product_uid=7
    AND b.customer_uid NOT IN (9647425,
      9658200,
      8365290)),
  SAVINGS_ACCOUNT AS(
  SELECT
    CAST(accountId AS INT) AS account_uid,
    ledgerAccountId AS account_no,
    CAST(customerId AS INT) AS customer_uid,
    parentAccountId
  FROM
    `pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY accountId ORDER BY ingestion_timestamp DESC) = 1
    AND UPPER(productCode) = 'SV-ESA'
    AND (UPPER(activeInd) = 'Y'
      OR activeInd IS NULL)),
  DIRECT_DEPOSIT AS(
  SELECT
    b.account_no,
    SUM(a.amount) AS dd_amount
  FROM
    `pcb-{env}-landing.domain_payments.EFT_EXTORIGIN_REQ` a
  JOIN
    `pcb-{env}-landing.domain_payments.EFT_FULFILLMENT` b
  ON
    a.eft_fulfillment_uid=b.eft_fulfillment_uid
  WHERE
    CAST(a.cpa_transaction_type AS INTEGER) IN (200,
      201,
      202,
      203,
      204,
      205,
      206,
      207,
      208,
      209,
      210,
      211,
      212,
      213,
      214,
      215,
      216,
      217,
      218,
      219,
      220,
      221,
      222,
      223,
      224,
      225,
      226,
      227,
      228,
      229,
      230,
      231,
      232,
      233,
      234,
      235,
      236,
      237,
      238,
      239,
      272,
      310,
      311,
      313,
      315,
      316,
      318,
      603,
      606,
      612)
    AND DATE(a.fundsmove_processing_dt) BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY)
    AND CURRENT_DATE()
  GROUP BY
    account_no),
  DD_THRESHOLD AS(
  SELECT
    directDepositThreshold
  FROM
    `pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY ingestion_timestamp DESC) = 1
    AND offerActiveInd IS TRUE
    AND DATE_ADD(offerEndDate, INTERVAL enrolmentWindow + benefitPeriod DAY) >= CURRENT_DATE()),
  GOOD_STANDING_ACCOUNT AS(
  SELECT
    DISTINCT CAST(a.cifp_account_id5 AS INTEGER) AS cifp_account_id5
  FROM
    `pcb-{env}-landing.domain_account_management.CIF_ACCOUNT_CURR` a
  LEFT JOIN
    `pcb-{env}-landing.domain_account_management.AM08` b
  ON
    CAST(a.cifp_account_id5 AS INTEGER)=b.mast_account_id
  WHERE
    (a.am00_statc_closed IS NULL
      OR a.am00_statc_closed='')
    AND (a.am00_statc_chargeoff IS NULL
      OR am00_statc_chargeoff='')
    AND (a.am00_statc_credit_revoked IS NULL
      OR am00_statc_credit_revoked='')
    AND b.AM08_CHGOFF_LAG_DATE IS NULL
    AND (a.am00_statf_potential_purge IS NULL
      OR am00_statf_potential_purge='')
    AND (a.am00_statf_credit_counseling IS NULL
      OR am00_statf_credit_counseling='')
    AND (a.am00_bankruptcy_type IS NULL
      OR am00_bankruptcy_type='')
    AND (a.am00_statf_declined_reissue IS NULL
      OR am00_statf_declined_reissue='')
    AND (a.am00_statf_fraud IS NULL
      OR am00_statf_fraud=''))
SELECT
  MONEY_ACCOUNT.customer_uid AS customer_id,
  MONEY_ACCOUNT.account_uid AS pcma_account_id,
  SAVINGS_ACCOUNT.account_uid AS savings_account_id,
  CASE
    WHEN SAVINGS_ACCOUNT.account_uid IS NOT NULL THEN TRUE
    ELSE FALSE
END
  AS savings_account_ind,
  CASE
    WHEN dd_amount >= directDepositThreshold THEN TRUE
    ELSE FALSE
END
  AS direct_deposit_ind,
  FALSE AS large_inflow_ind,
  CURRENT_DATETIME('America/Toronto') AS rec_load_timestamp
FROM
  MONEY_ACCOUNT
INNER JOIN
  GOOD_STANDING_ACCOUNT
ON
  CAST(MONEY_ACCOUNT.account_no AS INTEGER) = GOOD_STANDING_ACCOUNT.cifp_account_id5
LEFT JOIN
  SAVINGS_ACCOUNT
ON
  MONEY_ACCOUNT.customer_uid=SAVINGS_ACCOUNT.customer_uid
  AND MONEY_ACCOUNT.account_uid=CAST(SAVINGS_ACCOUNT.parentAccountId AS INTEGER)
LEFT JOIN
  DIRECT_DEPOSIT
ON
  CAST(MONEY_ACCOUNT.account_no AS INTEGER) = CAST(DIRECT_DEPOSIT.account_no AS INTEGER)
CROSS JOIN
  DD_THRESHOLD;