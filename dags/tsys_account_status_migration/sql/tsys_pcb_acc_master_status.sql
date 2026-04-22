EXPORT DATA
  OPTIONS  (
    uri = 'gs://pcb-{env}-staging-extract/tsys-pcb-acc-master-status/tsys-pcb-acc-master-status-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
DISTINCT
am00.mast_account_id,
am00.am00_statf_addr_chng_this_cyc AS status_am00_statf_addr_chng_this_cyc,
am00.am00_statc_application AS status_am00_statc_application,
CAST(am00_bankruptcy_type AS string) AS status_am00_bankruptcy_type,
am00_statf_broken_promise AS status_am00_statf_broken_promise,
am00_statf_credit_counseling AS status_am00_statf_credit_counseling,
am00_statc_closed AS status_am00_statc_closed,
am00_statc_chargeoff AS  status_am00_statc_chargeoff,
am00_statc_credit_revoked AS status_am00_statc_credit_revoked,
am00_statc_client_defined AS status_am00_statc_client_defined,
am00_statf_fraud_auth_excl AS status_am00_statf_fraud_auth_excl,
am00_statc_foreclosure AS status_am00_statc_foreclosure,
am00_statf_fraud AS status_am00_statf_fraud,
am00_statc_first_use AS status_am00_statc_first_use,
am00_statf_inactive AS status_am00_statf_inactive,
am00_statc_legal AS status_am00_statc_legal,
am00_statf_xfer_another_lende AS status_am00_statf_xfer_another_lende,
am00_statc_current_overlimit AS status_am00_statc_current_overlimit,
am00_statf_potential_chargeoff AS status_am00_statf_potential_chargeoff,
am00_statc_current_past_due AS status_am00_statc_current_past_due,
am00_statf_product_change AS status_am00_statf_product_change,
am00_statf_potential_purge AS status_am00_statf_potential_purge,
am00_statf_refund_block AS status_am00_statf_refund_block,
am00_statf_declined_reissue AS status_am00_statf_declined_reissue,
am00_statc_return_mail AS status_am00_statc_return_mail,
am00_statf_refused_to_pay AS status_am00_statf_refused_to_pay,
am00_statf_recovery AS status_am00_statf_recovery,
am00_statf_skip_trace AS status_am00_statf_skip_trace,
am00_statc_tiered_auth AS status_am00_statc_tiered_auth,
am00_statf_clos_rvok_tdy AS status_am00_statf_clos_rvok_tdy,
am00_statc_watch AS status_am00_statc_watch,
am00_statf_crv as status_am00_statf_crv,
FORMAT_DATETIME('%FT%H:%M:%E9S', DATETIME(TIMESTAMP(AM00.REC_LOAD_TIMESTAMP, 'America/Toronto'), 'UTC')) AS rec_load_timestamp
FROM
`pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` am00
INNER JOIN
`pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM01_REC` am01
ON  am00.mast_account_id=am01.mast_account_id
WHERE  mast_account_suffix=0
AND am01.am00_application_suffix=0
AND ((am01_account_rel_stat = 'A') OR (am01_account_rel_stat IS NULL) OR (TRIM(am01_account_rel_stat) = ''))
AND am01_customer_type IN (0,2)
AND am01_account_num_transfer_to=""
AND am01_prim_card_id NOT IN ('9999','SSTF','MMHV','MMEL')
ORDER BY am00.mast_account_id
{limit}
);