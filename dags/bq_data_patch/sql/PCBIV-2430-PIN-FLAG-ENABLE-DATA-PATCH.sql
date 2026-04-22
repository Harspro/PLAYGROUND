WITH Chip_Sig AS (
    SELECT
        CIFP_CRV_STATUS,
        CIFP_CRV_DATE,
        CIFP_PRIM_CARD_ID,
        CIFP_ACCOUNT_NUM,
        CIFP_ACCOUNT_ID6
    FROM `pcb-{env}-curated.domain_account_management.CIF_CARD_CURR`
    WHERE CIFP_PRIM_CARD_ID LIKE 'S%'
      AND CIFP_PRIM_CARD_ID NOT IN ('MCET', 'MMST', 'MCEF', '9999', 'MMHV', 'MCPP', 'MCSD', 'PHTE', 'SSTF')
),

account_customer_uid AS (
    SELECT
        s.CIFP_PRIM_CARD_ID,
        s.CIFP_CRV_STATUS,
        s.CIFP_CRV_DATE,
        CONCAT(
            SUBSTRING(s.CIFP_ACCOUNT_NUM, 1, 6),
            REPEAT('*', 6),
            SUBSTRING(s.CIFP_ACCOUNT_NUM, 13)
        ) AS masked_card,
        ac.ACCOUNT_CUSTOMER_UID,
        ac.PIN_FLAG_ENABLED
    FROM Chip_Sig s
    JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` am
        ON s.CIFP_ACCOUNT_NUM = am.CARD_NUMBER
    JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac
        ON ac.ACCOUNT_CUSTOMER_UID = am.ACCOUNT_CUSTOMER_UID
    WHERE am.DEACTIVATED_DT IS NULL
      AND ac.PIN_FLAG_ENABLED = 'N'
)

UPDATE `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER`
SET PIN_FLAG_ENABLED = 'Y'
WHERE ACCOUNT_CUSTOMER_UID IN (
    SELECT acu.ACCOUNT_CUSTOMER_UID
    FROM account_customer_uid acu
)
