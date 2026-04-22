CREATE OR REPLACE TABLE pcb-{env}-processing.domain_tax_slips.RL3_TAX_HEADER
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 31 DAY)
)
AS
WITH unprocessed_sbt_ref_type AS (
    SELECT
        T5S.sbmt_ref_id,
        T5S.rpt_tcd
    FROM pcb-{env}-landing.domain_tax_slips.T5_TAX_TRAILER T5T
    INNER JOIN pcb-{env}-landing.domain_tax_slips.T5_TAX_SLIP T5S
        ON T5T.sbmt_ref_id = T5S.sbmt_ref_id
    LEFT JOIN pcb-{env}-landing.domain_tax_slips.RL3_TAX_HEADER RL3H
        ON T5T.sbmt_ref_id = RL3H.SbmtRefId
    WHERE T5T.tx_yr = CAST({year} AS STRING)
        AND RL3H.SbmtRefId IS NULL
    GROUP BY T5S.sbmt_ref_id, T5S.rpt_tcd
),

sbt_ref_count AS (
SELECT
    SbmtRefId,
    COUNT(1) AS NbReleves
FROM pcb-{env}-processing.domain_tax_slips.RL3_TAX_SLIP
GROUP BY SbmtRefId
)

SELECT
    T5T.tx_yr AS Annee,
    CASE
        WHEN USRT.rpt_tcd = 'O' THEN 1
        WHEN USRT.rpt_tcd = 'A' THEN 4
        WHEN USRT.rpt_tcd = 'C' THEN 6
        ELSE 0
    END AS TypeEnvoi,
    CASE WHEN '{env}' = 'prod'
        THEN 'NP052112'
        ELSE 'NP088880'
    END AS PreparateurNo,
    2 AS PreparateurType,
    T5H.l1_nm AS PreparateurNom1,
    NULL AS PreparateurNom2,
    T5T.addr_l1_txt AS PreparateurAdresseLigne1,
    T5T.addr_l2_txt AS PreparateurAdresseLigne2,
    T5T.cty_nm AS PreparateurAdresseVille,
    T5T.prov_cd AS PreparateurAdresseProvince,
    T5T.pstl_cd AS PreparateurAdresseCodePostal,
    NULL AS InformatiqueNom,
    NULL AS InformatiqueIndRegional,
    NULL AS InformatiqueTel,
    NULL AS InformatiquePosteTel,
    NULL AS InformatiqueLangue,
    T5H.cntc_nm AS ComptabiliteNom,
    CAST(T5H.cntc_area_cd AS INTEGER) AS ComptabiliteIndRegional,
    T5H.cntc_phn_nbr AS ComptabiliteTel,
    T5H.cntc_extn_nbr AS ComptabilitePosteTel,
    T5H.lang_cd AS ComptabiliteLangue,
    CONCAT('RQ-', SUBSTR('{year}', 3, 2), '-03-069') AS NoCertification,
    NULL AS NomLogiciel,
    NULL AS VersionLogiciel,
    NULL AS CourrielResponsable,
    'A' AS CourrielLangue,
    '0000000000001DC1' AS IdPartenaireReleves,
    '0000000000003964' AS IdProduitReleves,
    CASE WHEN '{env}' = 'prod'
        THEN NULL
        ELSE
            CASE USRT.rpt_tcd
                WHEN 'O' THEN 'RAD_{year}_03_001'
                WHEN 'A' THEN 'RAD_{year}_03_002'
                WHEN 'C' THEN 'RAD_{year}_03_003'
                ELSE 'FAIL'
            END
    END AS NoCasEssai,
    USRT.sbmt_ref_id AS SbmtRefId,
    CURRENT_DATETIME('America/Toronto') AS CreateDate
FROM unprocessed_sbt_ref_type USRT
INNER JOIN pcb-{env}-landing.domain_tax_slips.T5_TAX_HEADER T5H
    ON USRT.sbmt_ref_id = T5H.sbmt_ref_id
INNER JOIN pcb-{env}-landing.domain_tax_slips.T5_TAX_TRAILER T5T
    ON T5H.sbmt_ref_id = T5T.sbmt_ref_id
INNER JOIN sbt_ref_count SRC
    ON T5H.sbmt_ref_id = SRC.SbmtRefId
LEFT JOIN pcb-{env}-landing.domain_tax_slips.RL3_TAX_HEADER RL3H
    ON T5H.sbmt_ref_id = RL3H.SbmtRefId
WHERE T5T.tx_yr = CAST({year} AS STRING)
    AND RL3H.SbmtRefId IS NULL;