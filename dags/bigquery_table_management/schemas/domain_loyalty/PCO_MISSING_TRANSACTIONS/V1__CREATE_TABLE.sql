CREATE TABLE IF NOT EXISTS `pcb-{env}-landing.domain_loyalty.PCO_MISSING_TRANSACTIONS` (
  wallet_id STRING,
  pcf_customer_id INT64,
  card_number STRING,
  transaction_date DATE,
  transaction_amount NUMERIC,
  transaction_time STRING,
  merchant_id STRING,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY transaction_date;