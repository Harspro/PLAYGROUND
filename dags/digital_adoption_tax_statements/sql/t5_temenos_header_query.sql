INSERT INTO pcb-{env}-landing.domain_tax_slips.T5_TAX_HEADER
SELECT
    sbmt_ref_id,
    bn9,
    bn15,
    trust,
    nr4,
    RepID,
    summ_cnt,
    lang_cd,
    l1_nm,
    TransmitterCountryCode,
    cntc_nm,
    cntc_area_cd,
    cntc_phn_nbr,
    cntc_extn_nbr,
    cntc_email_area,
    sec_cntc_email_area,
    current_datetime('America/Toronto') AS create_date
FROM pcb-{env}-landing.domain_tax_slips.T5_TAX_RAW_HEADER;