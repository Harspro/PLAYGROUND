CREATE OR REPLACE TABLE `{{ curated.FORECASTED_BURNDOWN }}` AS
WITH initiative AS (
    SELECT
        issue_key,
        forecasted_burndown_rate,
        forecasted_completion_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        forecasted_completion_date IS NOT NULL
        AND forecasted_burndown_rate IS NOT NULL
),

epic AS (
    SELECT
        issue_key,
        parent_key
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE parent_key IN (SELECT issue_key FROM initiative)
),

current_dag_run_timestamp AS (
    SELECT
        TIMESTAMP(DATETIME(TIMESTAMP(MAX(dag_run_time)), "America/Toronto"))
            AS dag_run_time
    FROM `{{ landing.DAG_HISTORY }}`
),

burndown AS (
    SELECT
        issue_key,
        event_time,
        burndown_weeks
    FROM `{{ curated.BURNDOWN_EVENT }}`
),

latest_burndown AS (
    SELECT
        epic.parent_key,
        SUM(burn.burndown_weeks) AS remaining_weeks
    FROM epic
    INNER JOIN burndown AS burn
        ON epic.issue_key = burn.issue_key
    GROUP BY epic.parent_key
    HAVING remaining_weeks > 0
),

forecasted_burndown_array AS (
    SELECT
        init.issue_key,
        lb.remaining_weeks,
        GENERATE_TIMESTAMP_ARRAY(
            cdrt.dag_run_time,
            LEAST(
                init.forecasted_completion_date,
                TIMESTAMP_ADD(
                    cdrt.dag_run_time,
                    INTERVAL 365 DAY
                )
            ),
            INTERVAL 1 DAY
        ) AS date_range
    FROM initiative AS init
    INNER JOIN latest_burndown AS lb
        ON init.issue_key = lb.parent_key,
        current_dag_run_timestamp AS cdrt
),

forecasted_burndown_unnested AS (
    SELECT
        fba.issue_key,
        event_time,
        remaining_weeks,
        ROW_NUMBER() OVER (
            PARTITION BY fba.issue_key
            ORDER BY event_time
        ) - 1 AS date_index
    FROM forecasted_burndown_array AS fba,
        UNNEST(fba.date_range) AS event_time
),

forecasted_burndown AS (
    SELECT
        init.issue_key,
        fbu.event_time,
        init.forecasted_completion_date,
        CASE
            WHEN
                fbu.remaining_weeks
                < init.forecasted_burndown_rate * fbu.date_index
                THEN 0
            ELSE
                fbu.remaining_weeks
                - init.forecasted_burndown_rate * fbu.date_index
        END AS remaining_weeks
    FROM initiative AS init
    INNER JOIN forecasted_burndown_unnested AS fbu
        ON init.issue_key = fbu.issue_key
    WHERE (
        DATE(fbu.event_time) = DATE(init.forecasted_completion_date)
        OR MOD(fbu.date_index, 7) = 0
    )
)

SELECT
    issue_key,
    event_time,
    ROUND(remaining_weeks - COALESCE(LAG(remaining_weeks) OVER (
        PARTITION BY issue_key
        ORDER BY event_time
    ), remaining_weeks), 2) AS burndown_weeks,
    ROUND(remaining_weeks, 2) AS remaining_weeks
FROM forecasted_burndown;
