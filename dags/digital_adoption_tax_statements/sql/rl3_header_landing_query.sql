INSERT INTO pcb-{env}-landing.domain_tax_slips.RL3_TAX_HEADER (
    Annee,
    TypeEnvoi,
    PreparateurNo,
    PreparateurType,
    PreparateurNom1,
    PreparateurNom2,
    PreparateurAdresseLigne1,
    PreparateurAdresseLigne2,
    PreparateurAdresseVille,
    PreparateurAdresseProvince,
    PreparateurAdresseCodePostal,
    InformatiqueNom,
    InformatiqueIndRegional,
    InformatiqueTel,
    InformatiquePosteTel,
    InformatiqueLangue,
    ComptabiliteNom,
    ComptabiliteIndRegional,
    ComptabiliteTel,
    ComptabilitePosteTel,
    ComptabiliteLangue,
    NoCertification,
    NomLogiciel,
    VersionLogiciel,
    CourrielResponsable,
    CourrielLangue,
    IdPartenaireReleves,
    IdProduitReleves,
    NoCasEssai,
    SbmtRefId,
    CreateDate
)

WITH sbmt_ref_id_processed AS (
  SELECT DISTINCT SbmtRefId FROM pcb-{env}-landing.domain_tax_slips.RL3_TAX_HEADER
)

SELECT
    CAST(RL3_TAX_HEADER.Annee AS INTEGER) AS Annee,
    CAST(RL3_TAX_HEADER.TypeEnvoi AS INTEGER) AS TypeEnvoi,
    CAST(RL3_TAX_HEADER.PreparateurNo AS STRING) AS PreparateurNo,
    CAST(RL3_TAX_HEADER.PreparateurType AS INTEGER) AS PreparateurType,
    CAST(RL3_TAX_HEADER.PreparateurNom1 AS STRING) AS PreparateurNom1,
    CAST(RL3_TAX_HEADER.PreparateurNom2 AS STRING) AS PreparateurNom2,
    CAST(RL3_TAX_HEADER.PreparateurAdresseLigne1 AS STRING) AS PreparateurAdresseLigne1,
    CAST(RL3_TAX_HEADER.PreparateurAdresseLigne2 AS STRING) AS PreparateurAdresseLigne2,
    CAST(RL3_TAX_HEADER.PreparateurAdresseVille AS STRING) AS PreparateurAdresseVille,
    CAST(RL3_TAX_HEADER.PreparateurAdresseProvince AS STRING) AS PreparateurAdresseProvince,
    CAST(RL3_TAX_HEADER.PreparateurAdresseCodePostal AS STRING) AS PreparateurAdresseCodePostal,
    CAST(RL3_TAX_HEADER.InformatiqueNom AS STRING) AS InformatiqueNom,
    CAST(RL3_TAX_HEADER.InformatiqueIndRegional AS INTEGER) AS InformatiqueIndRegional,
    CAST(RL3_TAX_HEADER.InformatiqueTel AS STRING) AS InformatiqueTel,
    CAST(RL3_TAX_HEADER.InformatiquePosteTel AS STRING) AS InformatiquePosteTel,
    CAST(RL3_TAX_HEADER.InformatiqueLangue AS STRING) AS InformatiqueLangue,
    CAST(RL3_TAX_HEADER.ComptabiliteNom AS STRING) AS ComptabiliteNom,
    CAST(RL3_TAX_HEADER.ComptabiliteIndRegional AS INTEGER) AS ComptabiliteIndRegional,
    CAST(RL3_TAX_HEADER.ComptabiliteTel AS STRING) AS ComptabiliteTel,
    CAST(RL3_TAX_HEADER.ComptabilitePosteTel AS STRING) AS ComptabilitePosteTel,
    CAST(RL3_TAX_HEADER.ComptabiliteLangue AS STRING) AS ComptabiliteLangue,
    CAST(RL3_TAX_HEADER.NoCertification AS STRING) AS NoCertification,
    CAST(RL3_TAX_HEADER.NomLogiciel AS STRING) AS NomLogiciel,
    CAST(RL3_TAX_HEADER.VersionLogiciel AS STRING) AS VersionLogiciel,
    CAST(RL3_TAX_HEADER.CourrielResponsable AS STRING) AS CourrielResponsable,
    CAST(RL3_TAX_HEADER.CourrielLangue AS STRING) AS CourrielLangue,
    CAST(RL3_TAX_HEADER.IdPartenaireReleves AS STRING) AS IdPartenaireReleves,
    CAST(RL3_TAX_HEADER.IdProduitReleves AS STRING) AS IdProduitReleves,
    CAST(RL3_TAX_HEADER.NoCasEssai AS STRING) AS NoCasEssai,
    CAST(RL3_TAX_HEADER.SbmtRefId AS STRING) AS SbmtRefId,
    CAST(RL3_TAX_HEADER.CreateDate AS DATETIME) AS CreateDate
FROM pcb-{env}-processing.domain_tax_slips.RL3_TAX_HEADER
LEFT JOIN sbmt_ref_id_processed PROCESSED
ON RL3_TAX_HEADER.SbmtRefId = PROCESSED.SbmtRefId
WHERE PROCESSED.SbmtRefId IS NULL
