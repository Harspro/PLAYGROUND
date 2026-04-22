EXPORT DATA
    OPTIONS (
        uri = 'gs://pcb-{env}-staging-extract/rl3_statement/rl3_statement-*.parquet',
        format = 'PARQUET',
        overwrite = true
    )
AS
(
    SELECT
        SAFE_CAST(h.SbmtRefId AS STRING) AS sourceReferenceId,
        SAFE_CAST(s.StmtId AS STRING) AS statementId,
        "savings-statement-rl3" AS statementType,
        FORMAT('{"annee":"%s","typeEnvoi":"%s","preparateurNo":"%s","preparateurType":"%s","type":"%s","nom2":"%s","rue2":"%s","app2":"%s","ville2":"%s","province2":"%s","cp2":"%s","noCertification":"%s","idPartenaireReleves":"%s","idProduitReleves":"%s","noReleve":"%s","noRelevePDF":"%s","noDernier":"%s","beneficiaireNo":"%s","nas":"%s","nom1":"%s","prenom1":"%s","initiale1":"%s","rue1":"%s","app1":"%s","ville1":"%s","province1":"%s","cp1":"%s","caseD":"%s","codeDevise":"%s","gererRensCompl1":"%s","infoRenseignement1":"%s","codeReleve":"%s","seq":"%s","numIncr":"%s","seqPreuve":"%s","createDate":"%s"}',
        COALESCE(SAFE_CAST(h.Annee AS STRING), ''),
        COALESCE(SAFE_CAST(h.TypeEnvoi AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurNo AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurType AS STRING), ''),
        COALESCE(SAFE_CAST(s.BeneficiaireType AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurNom1 AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurAdresseLigne1 AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurAdresseLigne2 AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurAdresseVille AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurAdresseProvince AS STRING), ''),
        COALESCE(SAFE_CAST(h.PreparateurAdresseCodePostal AS STRING), ''),
        COALESCE(SAFE_CAST(h.NoCertification AS STRING), ''),
        COALESCE(SAFE_CAST(h.IdPartenaireReleves AS STRING), ''),
        COALESCE(SAFE_CAST(h.IdProduitReleves AS STRING), ''),
        COALESCE(SAFE_CAST(s.NoReleve AS STRING), ''),
        COALESCE(SAFE_CAST(s.NoRelevePDF AS STRING), ''),
        COALESCE(SAFE_CAST(s.NoReleveDerniereTrans AS STRING), ''),
        COALESCE(SAFE_CAST(s.BeneficiaireNo AS STRING), ''),
        COALESCE(SAFE_CAST(s.PersonneNAS AS STRING), ''),
        COALESCE(SAFE_CAST(s.PersonneNomFamille AS STRING), ''),
        COALESCE(SAFE_CAST(s.PersonnePrenom AS STRING), ''),
        COALESCE(SAFE_CAST(s.PersonneInitiale AS STRING), ''),
        COALESCE(SAFE_CAST(s.AdresseLigne1 AS STRING), ''),
        COALESCE(SAFE_CAST(s.AdresseLigne2 AS STRING), ''),
        COALESCE(SAFE_CAST(s.AdresseVille AS STRING), ''),
        COALESCE(SAFE_CAST(s.AdresseProvince AS STRING), ''),
        COALESCE(SAFE_CAST(s.AdresseCodePostal AS STRING), ''),
        COALESCE(SAFE_CAST(s.D_InteretSourceCdn AS STRING), ''),
        COALESCE(SAFE_CAST(s.DeviseEtrangere AS STRING), ''),
        COALESCE(SAFE_CAST(s.CodeRensCompl AS STRING), ''),
        COALESCE(SAFE_CAST(s.DonneeRensCompl AS STRING), ''),
        COALESCE(SAFE_CAST(CASE s.RptTcd WHEN 'O' THEN 'R' WHEN 'C' THEN 'D' ELSE s.RptTcd END AS STRING), ''),
        COALESCE(SAFE_CAST(LEFT(CAST(s.NoReleve AS STRING),5) AS STRING), ''),
        COALESCE(SAFE_CAST(SUBSTRING(CAST(s.NoReleve AS STRING),6, 3) AS STRING), ''),
        COALESCE(SAFE_CAST(SUBSTRING(CAST(s.NoReleve AS STRING),9, 1) AS STRING), ''),
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', CURRENT_TIMESTAMP(), 'America/Toronto')
        ) AS statementJson
    FROM `pcb-{env}-processing.domain_tax_slips.RL3_TAX_SLIP` s
    JOIN `pcb-{env}-processing.domain_tax_slips.RL3_TAX_HEADER` h
    ON s.SbmtRefId = h.SbmtRefId
    ORDER BY NoReleve
)