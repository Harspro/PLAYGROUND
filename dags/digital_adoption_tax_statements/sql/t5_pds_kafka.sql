EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/t5_statement_PDS/statement_pds_t5_tax-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
    SAFE_CAST(T5Slip.sbmt_ref_id AS STRING) AS sbmt_ref_id,
    SAFE_CAST(CI1.CUSTOMER_UID AS STRING) AS stmt_id,
    "savings-statement-t5" as stmt_tp,
    FORMAT('{"snm":"%s","gvn_nm":"%s","init":"%s","sin":"%s","addr_l1_txt":"%s","addr_l2_txt":"%s","cty_nm":"%s","prov_cd":"%s","cntry_cd":"%s","pstl_cd":"%s","rcpnt_fi_acct_nbr":"%s","rpt_tcd":"%s","rcpnt_tcd":"%s","fgn_crcy_ind":"%s","cdn_int_amt":"%s","tx_yr":"%s","l1_nm":"%s","l2_nm":"%s","filr_addr_l1_txt":"%s","filr_addr_l2_txt":"%s","filr_cty_nm":"%s","filr_prov_cd":"%s","filr_cntry_cd":"%s","filr_pstl_cd":"%s","ts":"%s"}',
    COALESCE(SAFE_CAST(T5Slip.snm AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.gvn_nm AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.init AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.sin AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.addr_l1_txt AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.addr_l2_txt AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.cty_nm AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.prov_cd AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.cntry_cd AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.pstl_cd AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.rcpnt_fi_acct_nbr AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.rpt_tcd AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.rcpnt_tcd AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.fgn_crcy_ind AS STRING), ''),
    COALESCE(SAFE_CAST(T5Slip.cdn_int_amt AS STRING), ''),
    COALESCE(SAFE_CAST(TH.tx_yr AS STRING), ''),
    COALESCE(SAFE_CAST(TH.l1_nm AS STRING), ''),
    COALESCE(SAFE_CAST(TH.l2_nm AS STRING), ''),
    COALESCE(SAFE_CAST(TH.addr_l1_txt AS STRING), ''),
    COALESCE(SAFE_CAST(TH.addr_l2_txt AS STRING), ''),
    COALESCE(SAFE_CAST(TH.cty_nm AS STRING), ''),
    COALESCE(SAFE_CAST(TH.prov_cd AS STRING), ''),
    COALESCE(SAFE_CAST(TH.cntry_cd AS STRING), ''),
    COALESCE(SAFE_CAST(TH.pstl_cd AS STRING), ''),
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', CURRENT_TIMESTAMP(), 'America/Toronto')
    ) as statement_json
    FROM pcb-{env}-landing.domain_tax_slips.T5_TAX_SLIP T5Slip
        INNER JOIN pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_HEADER_REF TT
             ON T5Slip.sbmt_ref_id = TT.sbmt_ref_id
        INNER JOIN pcb-{env}-landing.domain_tax_slips.T5_TAX_TRAILER TH
             ON T5Slip.sbmt_ref_id = TH.sbmt_ref_id
        LEFT JOIN pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER CI1
            ON LPAD(T5Slip.rcpnt_fi_acct_nbr, 13, '0')  = CI1.CUSTOMER_IDENTIFIER_NO
            AND CI1.type = 'PCF-CUSTOMER-ID'
);