EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/eft-manual-limits/eft-manual-limits-*.parquet',
    format = 'PARQUET',
    OVERWRITE = true
  )
    AS
  (
   SELECT
    GENERATE_UUID() AS eventId,
    SAFE_CAST(customerId AS STRING) AS customerId,
    fundsMoveType,
    periodType,
    SAFE_CAST(NULL AS STRING) AS minLimit,
    SAFE_CAST(maxLimit AS STRING) AS maxLimit,
    FORMAT_DATETIME("%Y-%m-%dT%H:%M:%E6S",CURRENT_DATETIME("America/Toronto")) AS requestDate,
    SAFE_CAST(NULL AS BOOL) AS removeLimit,
    'MANUAL' AS reason
  FROM (
    SELECT
      customerId,
      DAILY,
      MONTHLY,
      fundsMoveType
    FROM
      pcb-{env}-landing.domain_movemoney_technical.EFT_LIMITS_MANUAL
    WHERE
      FILE_NAME = "<<FILENAME>>") SRC
UNPIVOT( maxLimit FOR periodType IN (DAILY, MONTHLY))
  UNION ALL
  SELECT
    GENERATE_UUID() AS eventId,
    SAFE_CAST(customerId AS STRING) AS customerId,
    fundsMoveType,
    'PER-REQUEST' AS periodType,
    SAFE_CAST(NULL AS STRING) AS minLimit,
    SAFE_CAST(DAILY AS STRING) AS maxLimit,
    FORMAT_DATETIME("%Y-%m-%dT%H:%M:%E6S",CURRENT_DATETIME("America/Toronto")) AS requestDate,
    SAFE_CAST(NULL AS BOOL) AS removeLimit,
    'MANUAL' AS reason
  FROM
    pcb-{env}-landing.domain_movemoney_technical.EFT_LIMITS_MANUAL
  WHERE
    FILE_NAME = "<<FILENAME>>"
  );