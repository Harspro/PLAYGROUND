CREATE OR REPLACE TABLE pcb-{env}-processing.domain_tax_slips.RL3_TAX_SLIP
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 31 DAY)
)
AS

WITH RECURSIVE CUSTOMER_MERGE_CHAIN AS (
    SELECT 
        CAST(SurvivorCustomerId AS INT64) AS SurvivorCustomerId,
        CAST(VictimCustomerId AS INT64) AS VictimCustomerId,
        recordLoadTime AS MergeDate,
        1 AS depth
    FROM pcb-{env}-landing.domain_customer_management.CUSTOMER_MERGE_SPLIT

    UNION ALL

    SELECT
        CAST(CMS.SurvivorCustomerId AS INT64),
        CAST(CMC.VictimCustomerId AS INT64),
        CMC.MergeDate,
        CMC.depth + 1
    FROM CUSTOMER_MERGE_CHAIN CMC
    INNER JOIN pcb-{env}-landing.domain_customer_management.CUSTOMER_MERGE_SPLIT CMS
        ON CMC.SurvivorCustomerId = CAST(CMS.VictimCustomerId AS INT64)
    WHERE CMC.depth < 10
),

CUSTOMER_ADDRESS AS (
    SELECT
        C.CUSTOMER_UID,
        AC.PROVINCE_STATE,
        AC.UPDATE_DT
    FROM
        pcb-{env}-curated.domain_customer_management.CONTACT C
    INNER JOIN
        pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT AC
        ON C.CONTACT_UID = AC.CONTACT_UID
    WHERE
        C.TYPE = 'ADDRESS'
        AND C.CONTEXT = 'PRIMARY'

    UNION DISTINCT

    SELECT
        CH.CUSTOMER_UID,
        ACH.PROVINCE_STATE,
        ACH.UPDATE_DT
    FROM
        pcb-{env}-curated.domain_customer_management.CONTACT_HISTORY CH
    INNER JOIN
        pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT_HISTORY ACH
        ON CH.CONTACT_UID = ACH.CONTACT_UID
    WHERE
        CH.TYPE = 'ADDRESS'
        AND CH.CONTEXT = 'PRIMARY'
),

CUSTOMER_ADDRESS_HISTORY AS (
    SELECT
        CUSTOMER_UID,
        PROVINCE_STATE,
        UPDATE_DT AS EFFECTIVE_FROM,
        LEAD(UPDATE_DT) OVER (PARTITION BY CUSTOMER_UID ORDER BY UPDATE_DT) AS EFFECTIVE_TO
    FROM CUSTOMER_ADDRESS
),

CUSTOMER_ADDRESS_HISTORY_WITH_MERGES AS (
    -- Customer's own address history (survivor keeps their current address as-is)
    SELECT CUSTOMER_UID, PROVINCE_STATE, EFFECTIVE_FROM, EFFECTIVE_TO
    FROM CUSTOMER_ADDRESS_HISTORY

    UNION DISTINCT

    -- Inherited address history from merged victims
    -- If victim's address has no end date (EFFECTIVE_TO IS NULL), end it at the merge date
    SELECT
        CMC.SurvivorCustomerId AS CUSTOMER_UID,
        CAH.PROVINCE_STATE,
        CAH.EFFECTIVE_FROM,
        CASE
            WHEN CAH.EFFECTIVE_TO IS NULL THEN CAST(CMC.MergeDate AS DATETIME)
            ELSE CAH.EFFECTIVE_TO
        END AS EFFECTIVE_TO
    FROM CUSTOMER_MERGE_CHAIN CMC
    INNER JOIN CUSTOMER_ADDRESS_HISTORY CAH
        ON CMC.VictimCustomerId = CAH.CUSTOMER_UID
),

QUEBEC_CUSTOMER AS (
    SELECT
        DISTINCT CUSTOMER_UID
    FROM
        CUSTOMER_ADDRESS_HISTORY_WITH_MERGES
    WHERE
        PROVINCE_STATE = 'QC'
        AND EXTRACT(YEAR FROM EFFECTIVE_FROM) <= {year}
        AND (EFFECTIVE_TO IS NULL OR EXTRACT(YEAR FROM EFFECTIVE_TO) >= {year})
),

last_no_releve_number AS (
    SELECT 
        Annee, 
        MAX(NoReleve) AS NoReleve,
        MAX(NoRelevePDF) AS NoRelevePDF
    FROM pcb-{env}-landing.domain_tax_slips.RL3_TAX_SLIP
    WHERE Annee = {year}
    GROUP BY Annee
),

T5_SLIP AS (
    SELECT 
        TS.rcpnt_fi_acct_nbr,
        TS.sin,
        TS.snm,
        TS.gvn_nm,
        TS.init,
        TS.addr_l1_txt,
        TS.addr_l2_txt,
        TS.cty_nm,
        TS.prov_cd,
        TS.pstl_cd,
        TS.tx_elg_dvnd_pamt,
        TS.tx_dvnd_amt,
        TS.actl_dvnd_amt,
        TS.dvnd_tx_cr_amt,
        TS.cdn_int_amt,
        TS.oth_cdn_incamt,
        TS.fgn_incamt,
        TS.fgn_tx_pay_amt,
        TS.cdn_royl_amt,
        TS.cgain_dvnd_amt,
        TS.acr_annty_amt,
        TS.lk_nt_acr_intamt,
        TS.fgn_crcy_ind,
        TS.rpt_tcd,
        TS.sbmt_ref_id,
        TS.create_date,
        CAST(TT.tx_yr AS INTEGER) AS tx_yr,
        CI.CUSTOMER_UID
    FROM 
        pcb-{env}-landing.domain_tax_slips.T5_TAX_SLIP TS
    INNER JOIN
        pcb-{env}-landing.domain_tax_slips.T5_TAX_HEADER TH
    ON TS.sbmt_ref_id = TH.sbmt_ref_id
    INNER JOIN 
        pcb-{env}-landing.domain_tax_slips.T5_TAX_TRAILER TT 
    ON TS.sbmt_ref_id = TT.sbmt_ref_id
    INNER JOIN 
        pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER CI
    ON LPAD(TS.rcpnt_fi_acct_nbr, 13, '0') = CI.CUSTOMER_IDENTIFIER_NO
        AND CI.type = 'PCF-CUSTOMER-ID'
    LEFT JOIN pcb-{env}-landing.domain_tax_slips.RL3_TAX_HEADER RL3
    ON TS.sbmt_ref_id = RL3.SbmtRefId
    WHERE TT.tx_yr = CAST({year} AS STRING)
        AND RL3.SbmtRefId IS NULL
),

RL3_SLIP_OA AS (
SELECT
    tx_yr AS Annee,
    COALESCE(CAST(LEFT(LNRN.NoReleve, 8) AS INT64), {noreleve} - 1) + ROW_NUMBER() OVER(ORDER BY T5.create_date, T5.sbmt_ref_id, T5.sin, T5.rpt_tcd DESC) AS NoReleve,
    COALESCE(CAST(LEFT(LNRN.NoRelevePDF, 8) AS INT64), {norelevepdf} - 1) + ROW_NUMBER() OVER(ORDER BY T5.create_date, T5.sbmt_ref_id, T5.sin, T5.rpt_tcd DESC) AS NoRelevePDF,
    NULL AS NoReleveDerniereTrans,
    CASE WHEN T5.rpt_tcd IN ('O', 'A') THEN 1 ELSE NULL END AS BeneficiaireType,
    rcpnt_fi_acct_nbr AS BeneficiaireNo,
    sin AS PersonneNAS,
    snm AS PersonneNomFamille,
    gvn_nm AS PersonnePrenom,
    init AS PersonneInitiale,
    NULL AS RaisonSocialeAutreNoId,
    NULL AS RaisonSocialeNom1, --MISSING
    NULL AS RaisonSocialeNom2,
    addr_l1_txt AS AdresseLigne1,
    addr_l2_txt AS AdresseLigne2,
    cty_nm AS AdresseVille,
    prov_cd AS AdresseProvince,
    pstl_cd AS AdresseCodePostal,
    tx_elg_dvnd_pamt AS A1_DividendeDetermine,
    tx_dvnd_amt AS A2_DividendeOrdinaire,
    actl_dvnd_amt AS B_DividendeImposable,
    dvnd_tx_cr_amt AS C_CreditImpotDividende,
    cdn_int_amt AS D_InteretSourceCdn,
    oth_cdn_incamt AS E_AutreRevenuCdn,
    fgn_incamt AS F_RevenuBrutEtranger,
    fgn_tx_pay_amt AS G_ImpotEtranger,
    cdn_royl_amt AS H_RedevanceCdn,
    cgain_dvnd_amt AS I_DividendeGainCapital,
    acr_annty_amt AS J_RevenuAccumuleRente,
    lk_nt_acr_intamt AS K_InteretBilletsLies,
    fgn_crcy_ind AS DeviseEtrangere,
    NULL AS CodeRensCompl, --MISSING
    NULL AS DonneeRensCompl, --MISSING
    T5.CUSTOMER_UID AS StmtId,
    T5.rpt_tcd AS RptTcd,
    sbmt_ref_id AS SbmtRefId,
    current_datetime('America/Toronto') AS CreateDate
FROM T5_SLIP T5
INNER JOIN QUEBEC_CUSTOMER QC
    ON T5.CUSTOMER_UID = QC.CUSTOMER_UID
LEFT JOIN last_no_releve_number LNRN
    ON T5.tx_yr = LNRN.Annee
WHERE T5.rpt_tcd IN ('O','A')
),

RL3_SLIP_OA_MODULUS AS (
    SELECT
        Annee,
        LPAD(CONCAT(NoReleve, MOD(NoReleve, 7)), 9, '0') AS NoReleve,
        LPAD(CONCAT(NoRelevePDF, MOD(NoRelevePDF, 7)), 9, '0') AS NoRelevePDF,
        NoReleveDerniereTrans,
        BeneficiaireType,
        BeneficiaireNo,
        PersonneNAS,
        PersonneNomFamille,
        PersonnePrenom,
        PersonneInitiale,
        RaisonSocialeAutreNoId,
        RaisonSocialeNom1,
        RaisonSocialeNom2,
        AdresseLigne1,
        AdresseLigne2,
        AdresseVille,
        AdresseProvince,
        AdresseCodePostal,
        A1_DividendeDetermine,
        A2_DividendeOrdinaire,
        B_DividendeImposable,
        C_CreditImpotDividende,
        D_InteretSourceCdn,
        E_AutreRevenuCdn,
        F_RevenuBrutEtranger,
        G_ImpotEtranger,
        H_RedevanceCdn,
        I_DividendeGainCapital,
        J_RevenuAccumuleRente,
        K_InteretBilletsLies,
        DeviseEtrangere,
        CodeRensCompl,
        DonneeRensCompl,
        StmtId,
        RptTcd,
        SbmtRefId,
        CreateDate
    FROM RL3_SLIP_OA
),

sin_lag_no_releve_derniere_trans AS (
    SELECT 
        PersonneNAS, 
        SbmtRefId, 
        RptTcd, 
        CreateDate,
        CASE WHEN RptTcd = 'A' 
            THEN LAG(NoReleve) OVER (PARTITION BY PersonneNAS ORDER BY CreateDate, SbmtRefId, PersonneNAS, RptTcd DESC)
            ELSE NULL
        END AS NoReleveDerniereTrans
    FROM
        (
            SELECT PersonneNAS, SbmtRefId, RptTcd, CreateDate, NoReleve 
            FROM RL3_SLIP_OA_MODULUS
            UNION DISTINCT
            SELECT PersonneNAS, SbmtRefId, RptTcd, CreateDate, NoReleve 
            FROM pcb-{env}-landing.domain_tax_slips.RL3_TAX_SLIP
            WHERE RptTcd IN ('O','A') AND Annee = {year}
        )
),

sin_last_no_releve_number AS (
    SELECT 
        PersonneNAS, 
        MAX(NoReleve) AS NoReleve,
        MAX(NoRelevePDF) AS NoRelevePDF
    FROM
        (
            SELECT PersonneNAS, NoReleve, NoRelevePDF
            FROM RL3_SLIP_OA_MODULUS
            UNION DISTINCT
            SELECT PersonneNAS, NoReleve, NoRelevePDF
            FROM pcb-{env}-landing.domain_tax_slips.RL3_TAX_SLIP
            WHERE RptTcd IN ('O','A') AND Annee = {year}
        )
    GROUP BY PersonneNAS
),

RL3_SLIP_C AS (
SELECT
    tx_yr AS Annee,
    SLNRN.NoReleve,
    SLNRN.NoRelevePDF,
    CAST(NULL AS STRING) AS NoReleveDerniereTrans,
    1 AS BeneficiaireType,
    rcpnt_fi_acct_nbr AS BeneficiaireNo,
    sin AS PersonneNAS,
    snm AS PersonneNomFamille,
    gvn_nm AS PersonnePrenom,
    init AS PersonneInitiale,
    NULL AS RaisonSocialeAutreNoId,
    NULL AS RaisonSocialeNom1, --MISSING
    NULL AS RaisonSocialeNom2,
    addr_l1_txt AS AdresseLigne1,
    addr_l2_txt AS AdresseLigne2,
    cty_nm AS AdresseVille,
    prov_cd AS AdresseProvince,
    pstl_cd AS AdresseCodePostal,
    tx_elg_dvnd_pamt AS A1_DividendeDetermine,
    tx_dvnd_amt AS A2_DividendeOrdinaire,
    actl_dvnd_amt AS B_DividendeImposable,
    dvnd_tx_cr_amt AS C_CreditImpotDividende,
    cdn_int_amt AS D_InteretSourceCdn,
    oth_cdn_incamt AS E_AutreRevenuCdn,
    fgn_incamt AS F_RevenuBrutEtranger,
    fgn_tx_pay_amt AS G_ImpotEtranger,
    cdn_royl_amt AS H_RedevanceCdn,
    cgain_dvnd_amt AS I_DividendeGainCapital,
    acr_annty_amt AS J_RevenuAccumuleRente,
    lk_nt_acr_intamt AS K_InteretBilletsLies,
    fgn_crcy_ind AS DeviseEtrangere,
    NULL AS CodeRensCompl, --MISSING
    NULL AS DonneeRensCompl, --MISSING
    T5.CUSTOMER_UID AS StmtId,
    T5.rpt_tcd AS RptTcd,
    sbmt_ref_id AS SbmtRefId,
    current_datetime('America/Toronto') AS CreateDate
FROM T5_SLIP T5
INNER JOIN QUEBEC_CUSTOMER QC
    ON T5.CUSTOMER_UID = QC.CUSTOMER_UID
LEFT JOIN sin_last_no_releve_number SLNRN
    ON T5.sin = SLNRN.PersonneNAS
WHERE T5.rpt_tcd = 'C'
)

SELECT
    RL3_SLIP_OA_MODULUS.Annee,
    RL3_SLIP_OA_MODULUS.NoReleve,
    RL3_SLIP_OA_MODULUS.NoRelevePDF,
    SLNRDT.NoReleveDerniereTrans,
    RL3_SLIP_OA_MODULUS.BeneficiaireType,
    RL3_SLIP_OA_MODULUS.BeneficiaireNo,
    RL3_SLIP_OA_MODULUS.PersonneNAS,
    RL3_SLIP_OA_MODULUS.PersonneNomFamille,
    RL3_SLIP_OA_MODULUS.PersonnePrenom,
    RL3_SLIP_OA_MODULUS.PersonneInitiale,
    RL3_SLIP_OA_MODULUS.RaisonSocialeAutreNoId,
    RL3_SLIP_OA_MODULUS.RaisonSocialeNom1, --MISSING
    RL3_SLIP_OA_MODULUS.RaisonSocialeNom2,
    RL3_SLIP_OA_MODULUS.AdresseLigne1,
    RL3_SLIP_OA_MODULUS.AdresseLigne2,
    RL3_SLIP_OA_MODULUS.AdresseVille,
    RL3_SLIP_OA_MODULUS.AdresseProvince,
    RL3_SLIP_OA_MODULUS.AdresseCodePostal,
    RL3_SLIP_OA_MODULUS.A1_DividendeDetermine,
    RL3_SLIP_OA_MODULUS.A2_DividendeOrdinaire,
    RL3_SLIP_OA_MODULUS.B_DividendeImposable,
    RL3_SLIP_OA_MODULUS.C_CreditImpotDividende,
    RL3_SLIP_OA_MODULUS.D_InteretSourceCdn,
    RL3_SLIP_OA_MODULUS.E_AutreRevenuCdn,
    RL3_SLIP_OA_MODULUS.F_RevenuBrutEtranger,
    RL3_SLIP_OA_MODULUS.G_ImpotEtranger,
    RL3_SLIP_OA_MODULUS.H_RedevanceCdn,
    RL3_SLIP_OA_MODULUS.I_DividendeGainCapital,
    RL3_SLIP_OA_MODULUS.J_RevenuAccumuleRente,
    RL3_SLIP_OA_MODULUS.K_InteretBilletsLies,
    RL3_SLIP_OA_MODULUS.DeviseEtrangere,
    RL3_SLIP_OA_MODULUS.CodeRensCompl, --MISSING
    RL3_SLIP_OA_MODULUS.DonneeRensCompl, --MISSING
    RL3_SLIP_OA_MODULUS.StmtId,
    RL3_SLIP_OA_MODULUS.RptTcd,
    RL3_SLIP_OA_MODULUS.SbmtRefId,
    RL3_SLIP_OA_MODULUS.CreateDate
FROM RL3_SLIP_OA_MODULUS
INNER JOIN sin_lag_no_releve_derniere_trans SLNRDT
    ON RL3_SLIP_OA_MODULUS.SbmtRefId = SLNRDT.SbmtRefId
        AND RL3_SLIP_OA_MODULUS.PersonneNAS = SLNRDT.PersonneNAS
        AND RL3_SLIP_OA_MODULUS.RptTcd = SLNRDT.RptTcd
        AND RL3_SLIP_OA_MODULUS.CreateDate = SLNRDT.CreateDate
UNION ALL
SELECT
    RL3_SLIP_C.Annee,
    RL3_SLIP_C.NoReleve,
    RL3_SLIP_C.NoRelevePDF,
    RL3_SLIP_C.NoReleveDerniereTrans,
    RL3_SLIP_C.BeneficiaireType,
    RL3_SLIP_C.BeneficiaireNo,
    RL3_SLIP_C.PersonneNAS,
    RL3_SLIP_C.PersonneNomFamille,
    RL3_SLIP_C.PersonnePrenom,
    RL3_SLIP_C.PersonneInitiale,
    RL3_SLIP_C.RaisonSocialeAutreNoId,
    RL3_SLIP_C.RaisonSocialeNom1, --MISSING
    RL3_SLIP_C.RaisonSocialeNom2,
    RL3_SLIP_C.AdresseLigne1,
    RL3_SLIP_C.AdresseLigne2,
    RL3_SLIP_C.AdresseVille,
    RL3_SLIP_C.AdresseProvince,
    RL3_SLIP_C.AdresseCodePostal,
    RL3_SLIP_C.A1_DividendeDetermine,
    RL3_SLIP_C.A2_DividendeOrdinaire,
    RL3_SLIP_C.B_DividendeImposable,
    RL3_SLIP_C.C_CreditImpotDividende,
    RL3_SLIP_C.D_InteretSourceCdn,
    RL3_SLIP_C.E_AutreRevenuCdn,
    RL3_SLIP_C.F_RevenuBrutEtranger,
    RL3_SLIP_C.G_ImpotEtranger,
    RL3_SLIP_C.H_RedevanceCdn,
    RL3_SLIP_C.I_DividendeGainCapital,
    RL3_SLIP_C.J_RevenuAccumuleRente,
    RL3_SLIP_C.K_InteretBilletsLies,
    RL3_SLIP_C.DeviseEtrangere,
    RL3_SLIP_C.CodeRensCompl, --MISSING
    RL3_SLIP_C.DonneeRensCompl, --MISSING
    RL3_SLIP_C.StmtId,
    RL3_SLIP_C.RptTcd,
    RL3_SLIP_C.SbmtRefId,
    RL3_SLIP_C.CreateDate
FROM RL3_SLIP_C
ORDER BY NoReleve;
