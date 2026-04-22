CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_ind_unpivot` AS
SELECT 
    applicationid, 
    cnstcategory 
    FROM 
    (
      SELECT 
        loblawappid as applicationid, 
        acct_bal_prot_ins_ind, 
        bal_transfer_ind, 
        job_loss_ins_ind,
        authorized_user_ind 
      FROM 
        `pcb-{env}-landing.domain_customer_acquisition.EXPERIAN_BASE` 
      WHERE loblawappid IS NOT NULL
    )
    UNPIVOT (
        VALUE FOR cnstcategory IN (acct_bal_prot_ins_ind AS 'ABP_STD', bal_transfer_ind AS 'BAL_TRANSFER', job_loss_ins_ind AS 'ABP_JOBLOSS', authorized_user_ind AS 'ADD_AUTH_USER')
    )
  WHERE VALUE = 'Y'

UNION ALL

SELECT 
    loblawappid AS applicationid, 
    'PCMC_APP_SUB' AS cnstcategory 
FROM 
  `pcb-{env}-landing.domain_customer_acquisition.EXPERIAN_BASE` 
WHERE 
  loblawappid IS NOT NULL