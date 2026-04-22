CREATE OR REPLACE FUNCTION `pcb-{env}-landing.domain_scoring.get_report_month`(
  report_year STRING,
  report_month STRING)
RETURNS
  STRUCT<
    report_month DATE,
    month_list ARRAY<STRING>>
AS (
  (
    SELECT AS STRUCT
      current_month AS report_month,

      -- last 12 months from AM08 up to current_month
      (
        SELECT
          ARRAY_AGG(
            FORMAT_DATE('%Y-%m-%d', file_create_dt)
            ORDER BY file_create_dt DESC)
        FROM
          (
            SELECT DISTINCT DATE_TRUNC(file_create_dt, MONTH) AS file_create_dt
            FROM `pcb-{env}-curated.domain_account_management.AM08`
            WHERE
              DATE_TRUNC(file_create_dt, MONTH)
              <= DATE_TRUNC(current_month, MONTH)
            ORDER BY file_create_dt DESC
            LIMIT 13
          )
      ) AS month_list
    FROM
      (
        SELECT
          CASE
            WHEN
              report_year IS NULL
              OR report_year = ''
              OR report_month IS NULL
              OR report_month = ''
              THEN
                (
                  SELECT MAX(file_create_dt) AS file_create_dt
                  FROM `pcb-{env}-curated.domain_account_management.AM08`
                )
            ELSE
              (
                SELECT max(file_create_dt) AS file_create_dt
                FROM `pcb-{env}-curated.domain_account_management.AM08`
                WHERE
                  DATE_TRUNC(file_create_dt, MONTH)
                  <= DATE(CONCAT(report_year, '-', report_month, '-01'))
              )
            END AS current_month
      )
  )
);
