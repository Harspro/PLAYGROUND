SELECT * EXCEPT(ROW_NUM)
    FROM
    (
      SELECT
        CONTACT_HISTORY_UID,
        CONTACT_UID,
        CUSTOMER_UID,
        TYPE,
        CONTEXT,
        CREATE_DT,
        CREATE_USER_ID,
        CREATE_FUNCTION_NAME,
        UPDATE_DT,
        UPDATE_USER_ID,
        UPDATE_FUNCTION_NAME,
        VALIDATE_IND,
        OVERRIDE_IND,
        ACTION,
        ACTION_DT,
        MESSAGE_ID,
        ROW_NUMBER() OVER(PARTITION BY CONTACT_HISTORY_UID ORDER BY ACTION_DT DESC) AS ROW_NUM
      FROM `pcb-{env}-landing.domain_customer_management.CONTACT_HISTORY`
    )
    WHERE
    ROW_NUM = 1