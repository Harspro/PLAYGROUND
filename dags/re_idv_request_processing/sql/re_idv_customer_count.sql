WITH re_idv_abb_customer AS (
  SELECT DISTINCT CUSTOMER_UID
  FROM (
    SELECT
      CUSTOMER_UID,
      IFNULL(EMAIL_SENT_COUNTER, 0) AS EMAIL_SENT_COUNTER,
      RE_IDV_STATUS,
      STATUS_CHANGE_TIMESTAMP,
      SOURCE,
      ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_UID
        ORDER BY STATUS_CHANGE_TIMESTAMP DESC
      ) AS rn
    FROM `pcb-{env}-landing.domain_customer_acquisition.RE_IDV_REQUIRED_CUSTOMER`
  )
  WHERE rn = 1
    AND (
      UPPER(RE_IDV_STATUS) = 'NEW'
      OR (
        UPPER(RE_IDV_STATUS) = 'PENDING'
        AND EMAIL_SENT_COUNTER < {max_reminder_count}
        AND (
            STATUS_CHANGE_TIMESTAMP IS NULL
            OR STATUS_CHANGE_TIMESTAMP <= CURRENT_TIMESTAMP() - INTERVAL {min_hours_between_reminders} HOUR
         )
      )
    )
)

SELECT COUNT(CUSTOMER_UID) AS customer_count
FROM re_idv_abb_customer;
