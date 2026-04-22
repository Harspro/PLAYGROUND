CREATE OR REPLACE TABLE `{{ curated.LOOKER_INITIATIVE_BURNDOWN_CHART }}` AS
WITH init AS (
    SELECT
        issue_key,
        issue_summary,
        status,
        target_start_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE issue_type = "initiative"
),

epic AS (
    SELECT
        parent_key,
        issue_key,
        issue_summary
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE issue_type = "epic"
),

burndown AS (
    SELECT
        issue_key,
        event_type,
        event_time,
        burndown_weeks
    FROM `{{ curated.BURNDOWN_EVENT }}`
),

target_burndown AS (
    SELECT
        issue_key AS initiative_key,
        "" AS epic_key,
        event_time,
        "target_burndown" AS burndown_type,
        "" AS event_type,
        burndown_weeks,
        remaining_weeks
    FROM `{{ curated.TARGET_BURNDOWN }}`
),

forecasted_burndown AS (
    SELECT
        issue_key AS initiative_key,
        "" AS epic_key,
        event_time,
        "forecasted_burndown" AS burndown_type,
        "" AS event_type,
        burndown_weeks,
        remaining_weeks
    FROM `{{ curated.FORECASTED_BURNDOWN }}`
),

current_dag_run_timestamp AS (
    SELECT
        TIMESTAMP(
            DATETIME(
                TIMESTAMP_SUB(TIMESTAMP(MAX(dag_run_time)), INTERVAL 1 SECOND),
                "America/Toronto"
            )
        )
            AS dag_run_time
    FROM `{{ landing.DAG_HISTORY }}`
),

grouped_epic_burndown_event AS (
    SELECT
        init.issue_key AS initiative_key,
        CASE
            WHEN burn.event_time > init.target_start_date THEN burn.issue_key
            ELSE epic.parent_key
        END AS epic_key,
        CASE
            WHEN burn.event_time > init.target_start_date THEN burn.event_type
            ELSE "START"
        END AS event_type,
        CASE
            WHEN burn.event_time > init.target_start_date
                THEN burn.event_time
            ELSE init.target_start_date
        END AS event_time,
        SUM(burn.burndown_weeks) AS burndown_weeks
    FROM burndown AS burn
    INNER JOIN epic
        ON burn.issue_key = epic.issue_key
    INNER JOIN init
        ON epic.parent_key = init.issue_key
    GROUP BY 1, 2, 3, 4
),

real_burndown AS (
    SELECT
        initiative_key,
        epic_key,
        "burndown" AS burndown_type,
        event_type,
        burndown_weeks,
        TIMESTAMP(DATETIME(event_time, "America/Toronto")) AS event_time,
        SUM(burndown_weeks) OVER (
            PARTITION BY initiative_key
            ORDER BY event_time
        ) AS remaining_weeks
    FROM grouped_epic_burndown_event
),

current_date_indicator AS (
    SELECT
        gebe.initiative_key,
        "" AS epic_key,
        cdrt.dag_run_time,
        "burndown" AS burndown_type,
        "CURRENT_TIME_INDICATOR" AS event_type,
        0 AS burndown_weeks,
        SUM(gebe.burndown_weeks) AS remaining_weeks
    FROM grouped_epic_burndown_event AS gebe
    INNER JOIN init
        ON gebe.initiative_key = init.issue_key,
        current_dag_run_timestamp AS cdrt
    WHERE init.status NOT IN ("Done", "Closed")
    GROUP BY 1, 2, 3, 4, 5, 6
),

combined_burndown AS (
    SELECT
        initiative_key,
        epic_key,
        event_time,
        burndown_type,
        event_type,
        burndown_weeks,
        remaining_weeks
    FROM real_burndown
    UNION ALL
    SELECT *
    FROM current_date_indicator
    UNION ALL
    SELECT *
    FROM target_burndown
    UNION ALL
    SELECT *
    FROM forecasted_burndown
)

SELECT
    cb.initiative_key,
    init.issue_summary AS initiative_summary,
    cb.epic_key,
    cb.event_time,
    cb.burndown_type,
    cb.event_type,
    cb.burndown_weeks,
    cb.remaining_weeks
FROM combined_burndown AS cb
INNER JOIN init
    ON cb.initiative_key = init.issue_key
