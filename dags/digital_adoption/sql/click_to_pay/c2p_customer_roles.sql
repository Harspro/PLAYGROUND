CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT a.account_uid,
  CAST(a.mast_account_id AS INT64) AS account_no,
  a.product_uid,
  b.customer_uid,
  CASE
    WHEN b.account_customer_role_uid = 1 THEN 'PRIMARY-CARD-HOLDER'
    WHEN b.account_customer_role_uid = 2 THEN 'AUTHORIZED-USER'
    WHEN b.account_customer_role_uid = 3 THEN 'SECONDARY'
    WHEN b.account_customer_role_uid = 4 THEN 'ADDITIONAL-USER'
    WHEN b.account_customer_role_uid = 5 THEN 'POA/POE'
    WHEN b.account_customer_role_uid = 6 THEN 'FRAUD/PERPETRATOR'
END
  AS role_description,
  b.active_ind,
  a.open_dt AS account_open_dt
FROM
  `pcb-{env}-curated.domain_account_management.ACCOUNT` a
JOIN
  `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` b
ON
  a.account_uid=b.account_uid
WHERE
  a.product_uid IN (1,
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