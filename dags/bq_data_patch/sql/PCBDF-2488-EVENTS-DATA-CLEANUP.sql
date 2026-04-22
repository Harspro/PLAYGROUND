DELETE
FROM
  pcb-{env}-landing.domain_account_management.EVENTS
WHERE
    FILE_CREATE_DT >= '2024-10-25';
