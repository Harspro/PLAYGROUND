SELECT
  sa40.* REPLACE(TBAL_DESC AS SA40_TBAL_DESCRIPTION),
  DATETIME(CURRENT_TIMESTAMP(), "America/Toronto") AS REC_LOAD_TIMESTAMP
FROM
  `pcb-{env}-processing.domain_account_management.SA40{file_type}_DBEXT` sa40
INNER JOIN
  `pcb-{env}-processing.domain_account_management.SA10{file_type}_DBEXT` sa10
ON
  sa10.PARENT_CARD_NUM = sa40.PARENT_CARD_NUM
LEFT JOIN
  pcb-{env}-landing.domain_account_management.STMT_VI70_STMTCNTRLOPT_LOOKUP LKP
ON
  LKP.TBAL_CODE = SA40.SA40_TBAL_CODE
  AND LKP.LANG_CODE = sa10.SA10_LANG_CODE_BEG_CY