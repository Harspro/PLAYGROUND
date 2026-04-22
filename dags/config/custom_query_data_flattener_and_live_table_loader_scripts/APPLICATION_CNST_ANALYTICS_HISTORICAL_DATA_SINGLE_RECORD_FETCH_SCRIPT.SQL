select ce.cnst_event_id                                          as cnstEventId,
       ce.evidence_id                                            as evidenceId,
       ce.rec_create_tms,
       ct.cnst_type_id                                           as cnstType,
       ct.cnst_type_code                                         as cnstTypeCode,
       ct.cnst_text                                              as cnstTypeDesc,
       ct.cnst_code                                              as cnstCategory,
       et.EVENT_TYPE_ID                                          as eventType,
       et.EVENT_TYPE                                             as eventTypeDesc,
       ct.cnst_code_ver                                          as cnstTypeVer,
       ct.lang_cd                                                as cnstLangCode,
       sc.source_id                                              as cnstEventSource,
       sc.data_source                                            as cnstEventSourceDesc,
       sco.source_org_id                                         as sourceOrg,
       sco.source_org_desc                                       as sourceOrgDesc,
       (select ext_cust_type
        from dnscnst.cnst_acct_cust_id_xref cacix
                 inner join dnscnst.ext_cust_id_type ecit on cacix.ext_cust_id_type_cd = ecit.ext_cust_id_type_cd
        where ce.cnst_acct_id = cacix.cnst_acct_id
          and cacix.ext_cust_id_type_cd in (28, 29)
        order by ct.rec_create_tms desc fetch first 1 rows only) as externalIdType,
       (select ext_cust_id_type_cd
        from dnscnst.cnst_acct_cust_id_xref cacix
        where ce.cnst_acct_id = cacix.cnst_acct_id
          and cacix.ext_cust_id_type_cd in (28, 29)
        order by ct.rec_create_tms desc fetch first 1 rows only) as externalIdTypeCode,
       (select ext_cust_id
        from dnscnst.cnst_acct_cust_id_xref cacix
        where ce.cnst_acct_id = cacix.cnst_acct_id
          and cacix.ext_cust_id_type_cd in (28, 29)
        order by ct.rec_create_tms desc fetch first 1 rows only) as external_Id_Value,
       mg.mktg_grp_id                                            as mktgGrp,
       mg.mktg_grp_desc                                          as mktgGrpDesc,
       (select ext_cust_id
        from dnscnst.cnst_acct_cust_id_xref cacix
        where ce.cnst_acct_id = cacix.cnst_acct_id
          and cacix.ext_cust_id_type_cd = 2
        order by ct.rec_create_tms desc fetch first 1 rows only) as pcfCustId,
       (select ext_cust_id
        from dnscnst.cnst_acct_cust_id_xref cacix
        where ce.cnst_acct_id = cacix.cnst_acct_id
          and cacix.ext_cust_id_type_cd = 4
        order by ct.rec_create_tms desc fetch first 1 rows only) as applicationId,
       null                                                      as signaturePathOriginal,
       null                                                      as signaturePathCurrent,
       cs.signature_id                                           as signatureFile,
       null                                                      as documentStorageLink,
       'N/A'                                                     as channel,
       null                                                      as faceToFaceInd
from dnscnst.cnst_event ce
 left join dnscnst.cnst_type ct on ce.cnst_type_id = ct.cnst_type_id
 left join DNSCNST.EVENT_TYPE et on ce.EVENT_TYPE_ID = et.EVENT_TYPE_ID
 left join dnscnst.source sc on ce.source_id = sc.source_id
 left join dnscnst.source_org sco on ce.source_org_id = sco.source_org_id
 left join dnscnst.cnst_type_mktg_grp_xref ctmgrf on ce.cnst_type_id = ctmgrf.cnst_type_id
 left join dnscnst.mktg_grp mg on ctmgrf.mktg_grp_id = mg.mktg_grp_id
 left join dnscnst.cnst_signature cs on cs.cnst_event_id = ce.cnst_event_id
where ce.cnst_event_id = {{dag_run.conf.get('single_entry_fetch_id')}}
