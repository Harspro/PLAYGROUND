INSERT INTO pcb-{env}-landing.domain_tax_slips.RL3_TAX_TRAILER (
    Annee,
    NbReleves,
    NoId,
    TypeDossier,
    NoDossier,
    NEQ,
    NoSuccursale,
    Nom,
    AdresseLigne1,
    AdresseLigne2,
    AdresseVille,
    AdresseProvince,
    AdresseCodePostal,
    SbmtRefId,
    CreateDate
)

WITH sbmt_ref_id_processed AS (
  SELECT DISTINCT SbmtRefId FROM pcb-{env}-landing.domain_tax_slips.RL3_TAX_TRAILER
)

SELECT 
    CAST(RL3_TAX_TRAILER.Annee AS INTEGER) AS Annee,
    CAST(RL3_TAX_TRAILER.NbReleves AS STRING) AS NbReleves,
    CAST(RL3_TAX_TRAILER.NoId AS STRING) AS NoId,
    CAST(RL3_TAX_TRAILER.TypeDossier AS STRING) AS TypeDossier,
    CAST(RL3_TAX_TRAILER.NoDossier AS STRING) AS NoDossier,
    CAST(RL3_TAX_TRAILER.NEQ AS INTEGER) AS NEQ,
    CAST(RL3_TAX_TRAILER.NoSuccursale AS STRING) AS NoSuccursale,
    CAST(RL3_TAX_TRAILER.Nom AS STRING) AS Nom,
    CAST(RL3_TAX_TRAILER.AdresseLigne1 AS STRING) AS AdresseLigne1,
    CAST(RL3_TAX_TRAILER.AdresseLigne2 AS STRING) AS AdresseLigne2,
    CAST(RL3_TAX_TRAILER.AdresseVille AS STRING) AS AdresseVille,
    CAST(RL3_TAX_TRAILER.AdresseProvince AS STRING) AS AdresseProvince,
    CAST(RL3_TAX_TRAILER.AdresseCodePostal AS STRING) AS AdresseCodePostal,
    CAST(RL3_TAX_TRAILER.SbmtRefId AS STRING) AS SbmtRefId,
    CAST(RL3_TAX_TRAILER.CreateDate AS DATETIME) AS CreateDate
FROM pcb-{env}-processing.domain_tax_slips.RL3_TAX_TRAILER
LEFT JOIN sbmt_ref_id_processed PROCESSED
ON RL3_TAX_TRAILER.SbmtRefId = PROCESSED.SbmtRefId
WHERE PROCESSED.SbmtRefId IS NULL
