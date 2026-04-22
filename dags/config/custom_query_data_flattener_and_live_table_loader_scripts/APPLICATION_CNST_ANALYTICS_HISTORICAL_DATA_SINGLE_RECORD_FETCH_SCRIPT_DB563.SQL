SELECT
  DISTINCT PR.SOURCE_CD AS cnstEventSource,
  PR.SOURCE_CD AS cnstEventSourceDesc,
  null AS cnstEventId,
  AOP.PROD_DESC AS eventType,
  AOP.PROD_DESC AS eventTypeDesc,
  PR.APP_TYPE AS app_type,
  PR.APPL_TMS AS rec_create_tms,
  PR.APPL_TMS AS rec_update_tms,
  PR.STORE_ID AS sourceOrg,
  null AS cnstType,
  null AS cnstTypeCode,
  CASE
   WHEN  AOPX.PROD_ID = 1 THEN 'Account Balance Protection'
   WHEN  AOPX.PROD_ID = 14 THEN 'Account Balance Protection Job Loss'
   WHEN  AOPX.PROD_ID = 15 THEN 'Credit Alert Plus'
   WHEN  AOPX.PROD_ID = 16 THEN 'Account Balance Protection Spousal'
   WHEN  AOPX.PROD_ID IS NULL OR TRIM(AOPX.PROD_ID) = '' THEN 'PCMC APPLICATION SUBMISSION' END AS cnstTypeDesc,
  CASE
    WHEN  AOPX.PROD_ID = 1 THEN 'ABP_STD'
    WHEN  AOPX.PROD_ID = 14 THEN 'ABP_JOBLOSS'
    WHEN  AOPX.PROD_ID = 15 THEN 'CR_ALERT_PLUS'
    WHEN  AOPX.PROD_ID = 16 THEN 'ABP_SPOUSAL'
    WHEN  AOPX.PROD_ID IS NULL OR TRIM(AOPX.PROD_ID) = '' THEN 'PCMC_APP_SUB' END AS cnstCategory,
  AOPX.PROD_ID,
  null AS cnstTypeVer,
  PR.LANG_CD AS cnstLangCode,
  PR.STORE_ID AS sourceOrgDesc,
  null AS mktgGrp,
  null AS mktgGrpDesc,
  null AS externalIdTypeCode,
  null AS externalIdType,
  null AS external_Id_Value,
  null AS evidenceId,
  null AS pcfCustId,
  PR.PC_MC_CUST_APPL_ID AS applicationId,
  PS.IMG_PATH AS signaturePathOriginal,
  CASE WHEN PS.IMG_PATH IS NOT NULL THEN '/Consent/' || TO_CHAR(PS.IMG_RECIEVED_DATE, 'yyyy/mm')|| '/' || SUBSTR(
    PS.IMG_PATH,
    INSTR(PS.IMG_PATH, '/',-1)+ 1,
    LENGTH(PS.IMG_PATH) - INSTR(PS.IMG_PATH, '/',-1)
  ) ELSE NULL END AS signaturePathCurrent,
  SUBSTR(
    PS.IMG_PATH,
    INSTR(PS.IMG_PATH, '/',-1)+ 1,
    LENGTH(PS.IMG_PATH) - INSTR(PS.IMG_PATH, '/',-1)
  ) AS signatureFile,
  null AS documentStorageLink,
  CASE WHEN aac.VENDOR_DESC IS NOT NULL
  AND aac.CHANNEL_DESC is Not NULL THEN aac.VENDOR_DESC || ' --> ' || aac.CHANNEL_DESC ELSE NULL END AS channel,
  TDC.FTF_IND AS faceToFaceInd
FROM
  PCMC.PCMC_REGISTRATION PR
  LEFT JOIN PCMC.TU_DIRECTORY_CHECK TDC ON PR.SOURCE_CD = TDC.SOURCE_CD
  LEFT JOIN PCMC.ADM_INST_CARD_INFO AICI ON PR.PC_MC_CUST_APPL_ID = AICI.PC_MC_CUST_APPL_ID
  LEFT JOIN PCMC.APP_OP_PROD_XREF AOPX ON PR.PC_MC_CUST_APPL_ID = AOPX.PC_MC_CUST_APPL_ID
  LEFT JOIN PCMC.APP_OPTIONAL_PROD AOP ON AOPX.PROD_ID = AOP.PROD_ID
  LEFT JOIN PCMC.APP_ADJUDICATION_CTL AAC ON PR.SOURCE_CD = AAC.SOURCE_CD
  AND PR.APP_TYPE = AAC.APP_TYPE
  LEFT JOIN PCMC.PCMC_SIGNATURE PS ON PR.PC_MC_CUST_APPL_ID = PS.PC_MC_CUST_APPL_ID
WHERE PR.REC_CREATE_TMS between TO_DATE('2015-02-02 00:00:00', 'yyyy-mm-dd HH24:MI:SS') AND TO_DATE('2018-11-01 00:00:00', 'yyyy-mm-dd HH24:MI:SS')
AND PR.PC_MC_CUST_APPL_ID = {{dag_run.conf.get('single_entry_fetch_id')}}
