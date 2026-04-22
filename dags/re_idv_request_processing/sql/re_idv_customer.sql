EXPORT DATA
OPTIONS (
  uri = 'gs://pcb-{env}-staging-extract/re_idv_request/re-idv-request-customer-*.parquet',
  format = 'PARQUET',
  overwrite = true
)
AS
WITH latest_records AS (
  SELECT
    CUSTOMER_UID,
    GENERATE_UUID() AS UUID,
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

SELECT
  CUSTOMER_UID,
  UUID,
  EMAIL_SENT_COUNTER,
  SOURCE
FROM latest_records
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
  );
