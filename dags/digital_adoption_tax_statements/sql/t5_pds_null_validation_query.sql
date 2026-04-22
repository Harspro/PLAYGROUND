SELECT COUNT(*) AS count
FROM `pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_SLIP_EXTERNAL` ES
INNER JOIN `pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_TRAILER_EXTERNAL` RT ON 1 = 1
WHERE ES.snm IS NULL
   OR ES.gvn_nm IS NULL
   OR ES.addr_l1_txt IS NULL
   OR ES.cty_nm IS NULL
   OR ES.prov_cd IS NULL
   OR ES.cntry_cd IS NULL
   OR ES.pstl_cd IS NULL
   OR ES.rcpnt_fi_acct_nbr IS NULL
   OR ES.rpt_tcd NOT IN ('O', 'A', 'C')
   OR ES.rcpnt_tcd != 1
   OR RT.l1_nm IS NULL
   OR RT.addr_l1_txt IS NULL
   OR RT.cty_nm IS NULL
   OR RT.prov_cd IS NULL
   OR RT.cntry_cd IS NULL
   OR RT.pstl_cd IS NULL
   OR CAST(cdn_int_amt AS NUMERIC) < 50
   OR ES.sin IS NULL
   OR RT.tx_yr IS NULL
