-- CRI Model BQ Task 3: Populate wrk lookup TCAT/TCODE (Deloitte: 2. Working Tables/1. populate wrk lookup tcat tcode fees interest.sql)
-- Table created via schema: jira_ticket_37_cri_wrk_lookup_tcat_tcode_fees_interest_table.md
-- Single lookup: Classification = Fee | Interest | Principal | Payment | Exclude. Replaces separate fee/interest and principal/payment lists.
-- Append-only: each run inserts a new batch with RecLoadTimestamp and JobId.
-- Parameters: {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION`
  (
    Tcat,
    Tcode,
    Classification,
    Name,
    Reason,
    EffectiveStartDate,
    EffectiveEndDate,
    Active,
    RecLoadTimestamp,
    JobId )
SELECT
  CAST(Tcat AS STRING) AS Tcat,
  CAST(Tcode AS STRING) AS Tcode,
  Classification,
  Name,
  Reason,
  FORMAT_DATE('%Y-%m-%d', EffectiveStartDate) AS EffectiveStartDate,
  FORMAT_DATE('%Y-%m-%d', EffectiveEndDate) AS EffectiveEndDate,
  Active,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM
  UNNEST(
    [
      STRUCT(
        54 AS Tcat,
        485 AS Tcode,
        'Fee' AS Classification,
        'ADJ Balance Transfer Fee' AS Name,
        'Adjustment' AS Reason,
        DATE('1900-01-01') AS EffectiveStartDate,
        DATE('2999-12-31') AS EffectiveEndDate),
      STRUCT(
        10,
        354,
        'Fee',
        'ADJ Cash Equivalent Fee',
        'Adjustment',
        DATE('1900-01-01'),
        DATE('2999-12-31')),
      STRUCT(
        1,
        479,
        'Interest',
        'Interest Adjustment',
        'Adjustment',
        DATE('1900-01-01'),
        DATE('2999-12-31')),
      -- Fee Adjustment: TCAT 0069, TCODE 7297; GL 0282; EN: FEE ADJUSTMENT, FR: ADJUSTEMENT DE FRAIS
      STRUCT(
        69 AS Tcat,
        7297 AS Tcode,
        'Fee' AS Classification,
        'FEE ADJUSTMENT' AS Name,
        '0282' AS Reason,
        DATE('1900-01-01') AS EffectiveStartDate,
        DATE('2999-12-31') AS EffectiveEndDate)]);