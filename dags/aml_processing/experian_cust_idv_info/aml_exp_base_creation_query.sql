CREATE OR REPLACE TABLE `pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_BASE`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
AS
SELECT 
  experian_base.APP_NUMBER,
  experian_base.MCCARD_NUMBER_FDR       AS ACCESS_MEDIUM_NO,
  experian_base.ID1TYPE                 AS ID_TYPE,
  experian_base.ID1SERIAL               AS ID_NUMBER,
  ref_experian_id_place.PROVINCE        AS ID_STATE,
  ref_experian_id_place.COUNTRY_CODE_2  AS ID_COUNTRY,
  experian_base.ID1ISSUEDT              AS ID_ISSUE_DATE,
  experian_base.ID1EXPIRY               AS ID_EXPIRY_DATE,
  CASE
    WHEN experian_base.APP_SOURCE_CODE IN ('DML','INT','OBT','PIA','STR','TEL','UND','CPP','TBE','')
    THEN 'NFTF'
    WHEN experian_base.APP_SOURCE_CODE IN ('QMM','EVT','TBA','CAS','CRD','GAS','EMP')
    THEN 'FTFP'
    WHEN experian_base.APP_SOURCE_CODE IN ('TAB','SDI','TBC','TBG')
    THEN 'FTFT'
    WHEN experian_base.APP_SOURCE_CODE IN ('IMM','DSF','ACT','JWR','TBB','PAV') AND SUBSTR(experian_base.IMAGE_ID , 1 , 3) = 'PCA'
    THEN 'FTFP'
    WHEN experian_base.APP_SOURCE_CODE IN ('IMM','DSF','ACT','JWR','TBB','PAV') AND SUBSTR(experian_base.IMAGE_ID , 1 , 3) != 'PCA'
    THEN 'FTFT'
    ELSE 'NFTF'
  END                                   AS IDV_METHOD,
  'EXPERIAN'                            AS IDV_DECISION      
FROM 
  `pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.EXPERIAN_BASE` AS experian_base
  LEFT OUTER JOIN `pcb-{DEPLOY_ENV}-landing.domain_aml.REF_EXPERIAN_ID_PLACE` AS ref_experian_id_place
    ON experian_base.ID1PLACE = ref_experian_id_place.ID1PLACE
WHERE
  experian_base.APP_STATUS = 'APPROVED'
  AND experian_base.MCCARD_NUMBER_FDR IS NOT NULL
  AND experian_base.ID1SERIAL IS NOT NULL