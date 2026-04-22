WITH EVENT_DATA AS (
  SELECT *,
    LAST_VALUE(productName IGNORE NULLS)
      OVER (
        PARTITION BY ledgerAccountId
        ORDER BY transactDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS filled_productName,
    LAST_VALUE(productLine IGNORE NULLS)
      OVER (
        PARTITION BY ledgerAccountId
        ORDER BY transactDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS filled_productLine,
    LAST_VALUE(productGroup IGNORE NULLS)
      OVER (
        PARTITION BY ledgerAccountId
        ORDER BY transactDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS filled_productGroup,
    LAST_VALUE(ledgerCustomerId IGNORE NULLS)
      OVER (
        PARTITION BY ledgerAccountId
        ORDER BY transactDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS filled_ledgerCustomerId,
    LAST_VALUE(customerType IGNORE NULLS)
      OVER (
        PARTITION BY ledgerAccountId
        ORDER BY transactDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS filled_customerType
  FROM pcb-{env}-landing.domain_account_management.TRANSACT_ACCOUNT
)

SELECT
  eventSpecificationVersion AS EVENT_SPECIFICATION_VERSION,
  eventType AS EVENT_TYPE,
  eventId AS EVENT_ID,
  eventSource AS EVENT_SOURCE,
  eventSubject AS EVENT_SUBJECT,
  DATETIME(TIMESTAMP(transactDateTime), 'America/Toronto') AS TRANSACT_DATE_TIME,
  DATETIME(TIMESTAMP(eventDateTime), 'America/Toronto') AS EVENT_DATE_TIME,
  ledgerAccountId AS ACCOUNT_ID,
  arrangementId AS ARRANGEMENT_ID,
  filled_productName AS PRODUCT_NAME,
  filled_productLine AS PRODUCT_LINE,
  filled_productGroup AS PRODUCT_GROUP,
  status AS STATUS,
  customerId AS CUSTOMER_ID,
  filled_ledgerCustomerId AS LEDGER_CUSTOMER_ID,
  filled_customerType AS CUSTOMER_TYPE,
  closureReason AS CLOSURE_REASON,
  closureNotes AS CLOSURE_NOTES,
  closureDate AS CLOSURE_DATE,
  statusPriorToClosure AS STATUS_PRIOR_TO_CLOSURE,
  ARRAY(
    SELECT AS STRUCT
      rateName AS RATE_NAME,
      rateDescription AS RATE_DESCRIPTION,
      ARRAY(
        SELECT AS STRUCT
          effectiveDate AS EFFECTIVE_DATE,
          rateTierType AS RATE_TIER_TYPE,
          ARRAY(
            SELECT AS STRUCT
              fixedRate AS FIXED_RATE,
              floatingIndex AS FLOATING_INDEX,
              ARRAY(
                SELECT AS STRUCT
                  marginType AS MARGIN_TYPE,
                  marginOperand AS MARGIN_OPERAND,
                  marginRate AS MARGIN_RATE
                FROM UNNEST(margins)
              ) AS MARGINS,
              tierAmount AS TIER_AMOUNT,
              tierPercent AS TIER_PERCENT,
              effectiveRate AS EFFECTIVE_RATE,
              linkedRateIndicator AS LINKED_RATE_INDICATOR
            FROM UNNEST(tierDetails)
          ) AS TIER_DETAILS
        FROM UNNEST(interestConditions)
      ) AS INTEREST_CONDITIONS
    FROM UNNEST(interests)
  ) AS INTEREST,
  postingDate AS POSTING_DATE,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS INGESTION_TIMESTAMP
FROM
  EVENT_DATA
WHERE
  NOT (eventType = 'accounts.pcma-savings-account.updateInterest.interestUpdated'
    AND (interests IS NULL
      OR ARRAY_LENGTH(interests) = 0));