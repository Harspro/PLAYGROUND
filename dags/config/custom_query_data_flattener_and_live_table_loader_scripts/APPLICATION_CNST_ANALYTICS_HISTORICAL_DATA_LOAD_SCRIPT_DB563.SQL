INSERT INTO pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS (
  cnstEventSource, cnstEventSourceDesc,
  cnstEventId, eventType, eventTypeDesc,
  eventDt, sourceDt, sourceOrg, cnstType,
  cnstTypeDesc, cnstTypeCode, cnstTypeVer,
  cnstCategory, cnstLangCode, sourceOrgDesc,
  mktgGrp, mktgGrpDesc, externalIdTypeCode,
  externalIdType, externalIdValue,
  evidenceId, pcfCustId, applicationId,
  signaturePathOriginal, signaturePathCurrent,
  signatureFile, documentStorageLink,
  channel, faceToFaceInd, legacyDataSource
)
SELECT
  SAFE_CAST(cnst.cnstEventSource AS STRING) AS cnstEventSource,
  cnst.cnstEventSourceDesc,
  SAFE_CAST(cnst.cnstEventId AS STRING) AS cnstEventId,
  SAFE_CAST(cnst.eventType AS STRING) AS eventType,
  cnst.eventTypeDesc,
  cnst.rec_create_tms,
  cnst.rec_create_tms,
  SAFE_CAST(cnst.sourceOrg AS STRING) AS sourceOrg,
  SAFE_CAST(cnst.cnstType AS STRING) AS cnstType,
  cnst.cnstTypeDesc,
  cnst.cnstTypeCode,
  cnst.cnstTypeVer,
  cnst.cnstCategory,
  cnst.cnstLangCode,
  cnst.sourceOrgDesc,
  SAFE_CAST(cnst.mktgGrp AS STRING) AS mktgGrp,
  cnst.mktgGrpDesc,
  SAFE_CAST(cnst.externalIdTypeCode AS STRING) AS externalIdTypeCode,
  cnst.externalIdType,
  cnst.external_Id_Value,
  cnst.evidenceId,
  cif.PCF_CUST_ID as pcfCustId,
  SAFE_CAST(applicationId AS STRING) AS applicationId,
  cnst.signaturePathOriginal,
  cnst.signaturePathCurrent,
  cnst.signatureFile,
  cnst.documentStorageLink,
  cnst.channel,
  cnst.faceToFaceInd,
  'PR563'
FROM
  pcb-{env}-processing.domain_consent.CNST_EVENTS_ANALYTICS_DBEXT cnst
  LEFT JOIN
     (
     SELECT DISTINCT CARD.CIFP_ACCOUNT_ID6 AS MAST_ACCOUNT_ID,
     CARD.CIFP_PCF_CUST_ID AS PCF_CUST_ID,
     ADM.PC_MC_CUST_APPL_ID AS LCL_APPLICATION_ID,
     FROM pcb-{env}-curated.domain_account_management.CIF_CARD_CURR AS CARD
     JOIN pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR ACC
        ON CARD.CIFP_ACCOUNT_ID6 = ACC.CIFP_ACCOUNT_ID5
     JOIN pcb-{env}-curated.domain_customer_acquisition.ADM_RT_RESPONSE ADM
        ON SAFE_CAST(ADM.APA_APP_NUM AS STRING) = ACC.CIFP_CUSTOM_DATA_81
     WHERE
        CARD.CIFP_PCF_CUST_ID IS NOT NULL
        AND TRIM(CARD.CIFP_PCF_CUST_ID) != ''
        AND ( CARD.CIFP_TRANS_TO_ACCT_NO IS NULL OR TRIM(CARD.CIFP_TRANS_TO_ACCT_NO) = '')
        AND CARD.CIFP_CUSTOMER_TYPE = 0
        AND (CARD.cifp_relationship_stat IS NULL OR TRIM(CARD.CIFP_TRANS_TO_ACCT_NO) = '' OR CARD.cifp_relationship_stat = 'A')
     ) cif
  ON cif.LCL_APPLICATION_ID = cnst.applicationId;
