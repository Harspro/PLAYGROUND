INSERT INTO `pcb-{env}-landing.domain_tax_slips.T5_TAX_VALIDATION_FAIL_RECORDS`
SELECT
   ES.snm,
   ES.gvn_nm,
   ES.addr_l1_txt,
   ES.cty_nm,
   ES.prov_cd,
   ES.cntry_cd,
   ES.pstl_cd,
   SAFE_CAST(ES.rcpnt_fi_acct_nbr AS String) AS rcpnt_fi_acct_nbr,
   SAFE_CAST(ES.rpt_tcd AS String) AS rpt_tcd ,
   SAFE_CAST(ES.rcpnt_tcd AS String) AS rcpnt_tcd,
   SAFE_CAST(ES.cdn_int_amt AS String) AS cdn_int_amt,
   SAFE_CAST(HE.sbmt_ref_id AS String) AS sbmt_ref_id,
   SAFE_CAST(ES.sin AS String) AS sin,
   RT.l1_nm AS l1_nm_trailer,
   RT.addr_l1_txt AS addr_l1_txt_trailer,
   RT.cty_nm AS cty_nm_trailer,
   RT.prov_cd AS prov_cd_trailer,
   RT.cntry_cd AS cntry_cd_trailer,
   RT.pstl_cd AS pstl_cd_trailer,
   SAFE_CAST(RT.tx_yr AS String) AS tx_yr,
   current_datetime('America/Toronto') as create_date
FROM `pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_SLIP_EXTERNAL` ES
LEFT JOIN `pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` CI1
ON LPAD(CAST(ES.rcpnt_fi_acct_nbr AS STRING), 13, '0') = CI1.CUSTOMER_IDENTIFIER_NO
AND CI1.type = 'PCF-CUSTOMER-ID'
INNER JOIN `pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_TRAILER_EXTERNAL` RT ON 1 = 1
INNER JOIN `pcb-{env}-processing.domain_tax_slips.T5_TAX_PDS_HEADER_EXTERNAL` HE ON 1 = 1
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
   OR CAST(ES.cdn_int_amt AS NUMERIC) < 50
   OR ES.sin IS NULL
   OR RT.l1_nm IS NULL
   OR RT.addr_l1_txt IS NULL
   OR RT.cty_nm IS NULL
   OR RT.prov_cd IS NULL
   OR RT.cntry_cd IS NULL
   OR RT.pstl_cd IS NULL
   OR RT.tx_yr IS NULL
   OR CI1.CUSTOMER_UID IS NULL;