CREATE OR REPLACE TABLE `{{ curated.LOOKER_EPIC_BURNDOWN_EVENTS }}` AS
WITH init AS (
    SELECT
        issue_key,
        issue_summary
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
)

SELECT
    epic.parent_key AS initiative_key,
    init.issue_summary AS initiative_summary,
    epic.issue_key AS epic_key,
    epic.issue_summary AS epic_summary,
    burn.event_type,
    burn.event_time,
    burn.burndown_weeks,
    SUM(burn.burndown_weeks) OVER (
        PARTITION BY epic.parent_key
        ORDER BY burn.event_time
    ) AS remaining_weeks
FROM burndown AS burn
INNER JOIN epic
    ON burn.issue_key = epic.issue_key
INNER JOIN init
    ON epic.parent_key = init.issue_key;
