EXPORT DATA
  OPTIONS  (
    uri = 'gs://pcb-{env}-staging-extract/tsys-pcb-card-level-status/tsys-pcb-card-level-status-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
am01.mast_account_id,
CAST(am01.mast_customer_id AS string) AS mast_customer_id,
am0a.am0a_account_num AS card_number,
am0a_statc_card_watch AS status_am0a_statc_card_watch,
am0a_statc_fraud_victim AS status_am0a_statc_fraud_victim,
am0a_statc_sub_diff AS status_am0a_statc_sub_diff,
am0a_security_fraud_reason as status_am0a_security_fraud_reason,
FORMAT_DATETIME('%FT%H:%M:%E9S', DATETIME(TIMESTAMP(AM01.REC_LOAD_TIMESTAMP, 'America/Toronto'), 'UTC')) AS rec_load_timestamp
FROM
`pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM0A_REC` am0a
INNER JOIN
`pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM01_REC` am01
ON am0a.mast_account_id=am01.mast_account_id
WHERE am01.card_number = am0a.am0a_account_num
AND ((am01_account_rel_stat = 'A') OR (am01_account_rel_stat IS NULL) OR (TRIM(am01_account_rel_stat) = ''))
AND am0a.am00_application_suffix=0
AND am01.am00_application_suffix=0
AND am01_customer_type IN (0,2)
AND am01_account_num_transfer_to=""
AND am01_prim_card_id NOT IN ('9999','SSTF','MMHV','MMEL')
ORDER BY mast_account_id
{limit}
 )