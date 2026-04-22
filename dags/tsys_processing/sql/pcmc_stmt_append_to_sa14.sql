SELECT
  *,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP
FROM
(SELECT
  * EXCEPT (Is_Ins_Active, SA10_RET_INS_CLAIM_STAT, LANG_CODE_BEG_CY)
FROM pcb-{env}-processing.domain_account_management.SA14_MID{file_type}
WHERE Is_Ins_Active = '0'
UNION ALL
SELECT
  * EXCEPT (Is_Ins_Active, SA10_RET_INS_CLAIM_STAT, LANG_CODE_BEG_CY)
FROM pcb-{env}-processing.domain_account_management.SA14_PSP{file_type})