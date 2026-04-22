INSERT INTO `pcb-{env}-landing.domain_tax_slips.T5_TAX_VALIDATION_FAIL_RECORDS`
SELECT
   ES.snm,
   ES.gvn_nm,
   ES.addr_l1_txt,
   ES.cty_nm,
   ES.prov_cd,
   ES.cntry_cd,
   ES.pstl_cd,
   ES.rcpnt_fi_acct_nbr,
   ES.rpt_tcd,
   ES.rcpnt_tcd,
   ES.cdn_int_amt,
   ES.sbmt_ref_id,
   ES.sin,
   RT.l1_nm AS l1_nm_trailer,
   RT.addr_l1_txt AS addr_l1_txt_trailer,
   RT.cty_nm AS cty_nm_trailer,
   RT.prov_cd AS prov_cd_trailer,
   RT.cntry_cd AS cntry_cd_trailer,
   RT.pstl_cd AS pstl_cd_trailer,
   RT.tx_yr,
   current_datetime('America/Toronto') as create_date
FROM `pcb-{env}-processing.domain_tax_slips.T5_ENRICHED_TAX_SLIP` ES
INNER JOIN `pcb-{env}-landing.domain_tax_slips.T5_TAX_RAW_TRAILER` RT ON 1 = 1
WHERE ES.snm IS NULL
   OR ES.gvn_nm IS NULL
   OR ES.addr_l1_txt IS NULL
   OR ES.cty_nm IS NULL
   OR ES.prov_cd IS NULL
   OR ES.cntry_cd IS NULL
   OR ES.pstl_cd IS NULL
   OR ES.rcpnt_fi_acct_nbr IS NULL
   OR ES.rpt_tcd NOT IN ('O', 'A', 'C')
   OR ES.rcpnt_tcd != '1'
   OR CAST(ES.cdn_int_amt AS NUMERIC) < 50
   OR ES.sin IS NULL
   OR RT.l1_nm IS NULL
   OR RT.addr_l1_txt IS NULL
   OR RT.cty_nm IS NULL
   OR RT.prov_cd IS NULL
   OR RT.cntry_cd IS NULL
   OR RT.pstl_cd IS NULL
   OR RT.tx_yr IS NULL;
