MERGE INTO
  pcb-{env}-landing.domain_account_management.REF_CUSTOMER_OFFER_DETAIL AS trg
USING
  (
  SELECT
    1 AS offer_id,
    'Product Upgrade' AS offer_type,
    'Silver to World: >$15,000, Silver/World to World Elite: >$25,000' AS offer_desc,
    1 AS offer_priority,
    TRUE AS offer_active_ind,
    DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND) AS offer_created_dt,
    'IDL' AS offer_created_by,
    DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND) AS rec_load_timestamp ) AS src
ON
  trg.offer_id = src.offer_id
  WHEN NOT MATCHED
  THEN
INSERT
VALUES
  (src.offer_id,src.offer_type,src.offer_desc,src.offer_priority,src.offer_active_ind,src.offer_created_dt,src.offer_created_by,src.rec_load_timestamp);