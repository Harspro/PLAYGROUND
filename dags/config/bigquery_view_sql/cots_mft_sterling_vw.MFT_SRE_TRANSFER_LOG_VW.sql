SELECT
  df.DATA_FLOW_ID as DATA_FLOW_ID,
  datetime(df.CREATE_TIME,"America/Toronto") as CREATE_TIME,
  fd.FILENAME as FILE_NAME,
  ms_cdr.operation as MS_OPERATION,
  fa.PROD_ORG_NAME as MFT_SOURCE,
  fa.MAILBOX_PATH as MFT_SOURCE_LOCATION,
  fd.MAILBOX_PATH as MFT_TARGET_LOCATION,
  fd.STATE as MFT_STATE,
  ms_cdr.source_node as MS_SOURCE_NODE,
  ms_cdr.source_location as MS_SOURCE_LOCATION,
  ms_cdr.target_node as MS_TARGET_NODE,
  ms_cdr.target_location as MS_TARGET_LOCATION,
  ms_cdr.status as MS_DELIVERY_STATUS,
  df.ROOT_DOC_SIZE as ROOT_DOC_SIZE,
  ms_cdr.session_id as SESSION_ID
FROM
  `pcb-{env}-curated.cots_mft_sterling_vw.MFT_DATA_FLOW_VW` df
  JOIN `pcb-{env}-curated.cots_mft_sterling_vw.MFT_FG_DELIVERY_VW` fd ON fd.DATA_FLOW_ID = df.DATA_FLOW_ID
  JOIN `pcb-{env}-curated.cots_mft_sterling_vw.MFT_FG_ARRIVEDFILE_VW` fa ON fa.DATA_FLOW_ID = df.DATA_FLOW_ID
  LEFT JOIN `pcb-{env}-curated.cots_mft_sterling_vw.MFT_MS_CDRS_VW` as ms_cdr ON df.root_doc_name = ms_cdr.file_name
WHERE
  fd.createts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 50 HOUR)
GROUP BY
  df.DATA_FLOW_ID,
  df.CREATE_TIME,
  fd.FILENAME,
  ms_cdr.operation,
  fa.PROD_ORG_NAME,
  fa.MAILBOX_PATH,
  fd.MAILBOX_PATH,
  fd.STATE,
  ms_cdr.source_node,
  ms_cdr.source_location,
  ms_cdr.target_node,
  ms_cdr.target_location,
  ms_cdr.status,
  df.ROOT_DOC_SIZE,
  ms_cdr.session_id
ORDER BY
  MAX(fd.createts) DESC;