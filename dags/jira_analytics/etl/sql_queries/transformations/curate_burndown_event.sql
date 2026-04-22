INSERT `{{ curated.BURNDOWN_EVENT }}` (
    issue_key, event_time, event_type, burndown_weeks
)
SELECT
    issue_key,
    final_close_date AS event_time,
    "BURNDOWN" AS event_type,
    (CASE issue_type
        WHEN "epic" THEN mean_size
        WHEN "story" THEN story_point
    END) * -1.0 AS burndown_weeks
FROM `{{ curated.UNIQUE_ISSUES }}`
WHERE
    issue_type IN ("epic", "story")
    AND resolution NOT IN ("Won't Do", "Duplicate", "Cancelled by Business")
    AND final_close_date IS NOT NULL

UNION ALL

(
    WITH epics AS (
        SELECT issue_key
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE
            issue_type = "epic"
            AND resolution NOT IN (
                "Won't Do", "Duplicate", "Cancelled by Business"
            )
    )

    SELECT
        issue_key,
        event_time,
        CASE
            WHEN from_t_shirt_size IS NULL THEN "NEW"
            WHEN to_mean_size - from_mean_size > 0 THEN "INCREASE"
            WHEN to_mean_size - from_mean_size < 0 THEN "DECREASE"
            ELSE ""
        END AS event_type,
        to_mean_size - from_mean_size AS burndown_weeks
    FROM `{{ curated.T_SHIRT_SIZE_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM epics)
)

UNION ALL

(
    WITH stories AS (
        SELECT issue_key
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE
            issue_type = "story"
            AND resolution NOT IN (
                "Won't Do", "Duplicate", "Cancelled by Business"
            )
    )

    SELECT
        issue_key,
        event_time,
        CASE
            WHEN from_story_point IS NULL THEN "NEW"
            WHEN to_story_point - from_story_point > 0 THEN "INCREASE"
            WHEN to_story_point - from_story_point < 0 THEN "DECREASE"
            ELSE ""
        END AS event_type,
        to_story_point - from_story_point AS burndown_weeks
    FROM `{{ curated.STORY_POINT_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM stories)
);
