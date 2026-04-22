CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
) AS
SELECT cnstEventSource AS cnst_event_source,
        cnstEventSourceDesc AS cnst_event_source_desc,
        cnstEventId AS cnst_event_id,
        eventType AS event_type,
        eventTypeDesc AS event_type_desc,
        CAST(eventDt as DATE) AS event_dt,
        CAST(sourceDt as DATE) AS source_dt,
        sourceOrg AS source_org,
        cnstType AS cnst_type,
        cnstTypeDesc AS cnst_type_desc,
        cnstTypeCode AS cnst_type_code,
        cnstTypeVer AS cnst_type_ver,
        cnstCategory AS cnst_category,
        cnstLangCode AS cnst_lang_code,
        sourceOrgDesc AS source_org_desc,
        mktgGrp AS mktg_grp,
        mktgGrpDesc AS mktg_grp_desc,
        externalIdTypeCode AS external_id_type_code,
        externalIdType AS external_id_type,
        externalIdValue AS external_id_value,
        evidenceId AS evidence_id,
        pcfCustId AS pcf_cust_id,
        applicationId AS application_id,
        signaturePathOriginal AS signature_path_original,
        signaturePathCurrent AS signature_path_current,
        signatureFile AS signature_file,
        documentStorageLink AS document_storage_link,
        channel,
        faceToFaceInd AS face_to_face_ind,
        legacyDataSource AS legacy_data_source
FROM `{source_table_id}`
LIMIT 0;
INSERT INTO `{staging_table_id}`
(cnst_event_source, cnst_event_source_desc, cnst_event_id, event_type, event_type_desc, event_dt, source_dt, source_org, cnst_type, cnst_type_desc, cnst_type_code, cnst_type_ver, cnst_category, cnst_lang_code, source_org_desc, mktg_grp, mktg_grp_desc, external_id_type_code, external_id_type, external_id_value, evidence_id, pcf_cust_id, application_id, signature_path_original, signature_path_current, signature_file, document_storage_link, channel, face_to_face_ind, legacy_data_source)
SELECT cnstEventSource, cnstEventSourceDesc, cnstEventId, eventType, eventTypeDesc, DATE(eventDt) as eventDt, DATE(sourceDt) as sourceDt, sourceOrg, cnstType, cnstTypeDesc, cnstTypeCode, cnstTypeVer, cnstCategory, cnstLangCode, sourceOrgDesc, mktgGrp, mktgGrpDesc, externalIdTypeCode, externalIdType, externalIdValue, evidenceId, pcfCustId, applicationId, signaturePathOriginal, signaturePathCurrent, signatureFile, documentStorageLink, channel, faceToFaceInd, legacyDataSource
FROM `{source_table_id}`
WHERE legacyDataSource = 'PR053';
