CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.ACTIVE_CUSTOMER_BASE
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT CAST(a.mast_account_id AS INT64) AS account_no,
  a.account_uid,
  b.customer_uid,
  b.account_customer_uid,
  a.product_uid,
  a.open_dt AS account_open_dt
FROM
  `pcb-{env}-curated.domain_account_management.ACCOUNT` a
JOIN
  `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` b
ON
  a.account_uid=b.account_uid
  AND b.account_customer_role_uid=1
  AND b.active_ind='Y'
WHERE
  a.product_uid in ( 1,
    2,
    3,
    4,
    5,
    6,
    7,
    12)
  AND b.customer_uid NOT IN (9647425,9658200,8365290)
  AND DATE(a.open_dt) BETWEEN '{account_open_start_date}'
  AND '{account_open_end_date}';