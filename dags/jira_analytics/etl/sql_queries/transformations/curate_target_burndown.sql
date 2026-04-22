CREATE OR REPLACE TABLE `{{ curated.TARGET_BURNDOWN }}` AS
WITH issues AS (
    SELECT
        issue_key,
        total_size,
        TIMESTAMP(DATE(target_start_date, "America/Toronto"))
            AS target_start_date,
        TIMESTAMP(DATE(target_completion_date, "America/Toronto"))
            AS target_completion_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "initiative"
        AND total_size > 0
        AND target_start_date IS NOT NULL
        AND target_completion_date IS NOT NULL
        AND target_start_date != target_completion_date
),

target_burndown AS (
    SELECT
        issue_key,
        total_size
        / TIMESTAMP_DIFF(target_completion_date, target_start_date, DAY)
            AS target_burndown_rate,
        GENERATE_TIMESTAMP_ARRAY(
            target_start_date, target_completion_date, INTERVAL 1 DAY
        ) AS date_range
    FROM issues
),

target_burndown_unnested AS (
    SELECT
        tb.issue_key,
        tb.target_burndown_rate,
        event_time,
        ROW_NUMBER() OVER (
            PARTITION BY tb.issue_key
            ORDER BY event_time
        ) - 1 AS date_index
    FROM target_burndown AS tb,
        UNNEST(tb.date_range) AS event_time
)

SELECT
    issues.issue_key,
    tbu.event_time,
    COALESCE(ROUND(tbu.target_burndown_rate * (LAG(tbu.date_index) OVER (
        PARTITION BY issues.issue_key
        ORDER BY tbu.event_time
    ) - tbu.date_index), 2), 0) AS burndown_weeks,
    ROUND(
        issues.total_size
        - tbu.target_burndown_rate * tbu.date_index, 2
    ) AS remaining_weeks
FROM issues
INNER JOIN target_burndown_unnested AS tbu
    ON issues.issue_key = tbu.issue_key
WHERE
    tbu.event_time = issues.target_completion_date
    OR MOD(tbu.date_index, 7) = 0
ORDER BY 1 DESC, 2;
