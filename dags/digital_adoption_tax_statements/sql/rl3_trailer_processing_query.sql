CREATE OR REPLACE TABLE pcb-{env}-processing.domain_tax_slips.RL3_TAX_TRAILER
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 31 DAY)
)
AS
WITH sbt_ref_count AS (
    SELECT
        SbmtRefId,
        COUNT(1) AS NbReleves
    FROM pcb-{env}-processing.domain_tax_slips.RL3_TAX_SLIP
    GROUP BY SbmtRefId
)

SELECT
    T5T.tx_yr AS Annee,
    SRC.NbReleves,
    '0000000000' AS NoId,
    'RS' AS TypeDossier,
    '0000' AS NoDossier,
    NULL AS NEQ,
    NULL AS NoSuccursale,
    T5H.l1_nm AS Nom,
    T5T.addr_l1_txt AS AdresseLigne1,
    T5T.addr_l2_txt AS AdresseLigne2,
    T5T.cty_nm AS AdresseVille,
    T5T.prov_cd AS AdresseProvince,
    T5T.pstl_cd AS AdresseCodePostal,
    T5T.sbmt_ref_id AS SbmtRefId,
    current_datetime('America/Toronto') AS CreateDate
FROM pcb-{env}-landing.domain_tax_slips.T5_TAX_TRAILER T5T
INNER JOIN pcb-{env}-landing.domain_tax_slips.T5_TAX_HEADER T5H
    ON T5T.sbmt_ref_id = T5H.sbmt_ref_id
INNER JOIN sbt_ref_count SRC
    ON T5T.sbmt_ref_id = SRC.SbmtRefId
LEFT JOIN pcb-{env}-landing.domain_tax_slips.RL3_TAX_TRAILER RL3T
    ON T5T.sbmt_ref_id = RL3T.SbmtRefId
WHERE T5T.tx_yr = CAST({year} AS STRING)
        AND RL3T.SbmtRefId IS NULL;