CREATE OR REPLACE TABLE `{{ curated.LOOKER_CUMULATIVE_FLOW }}` AS
WITH initiatives AS (
    SELECT
        issue_key,
        issue_summary,
        target_start_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "initiative"
        AND target_start_date IS NOT NULL
),

epics AS (
    SELECT
        parent_key AS initiative_key,
        issue_key AS epic_key
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "epic"
        AND parent_key IN (SELECT issue_key FROM initiatives)
        AND resolution NOT IN (
            "Won't Do",
            "Duplicate",
            "Cancelled by Business"
        )
),

burndown AS (
    SELECT
        issue_key,
        event_time
    FROM `{{ curated.BURNDOWN_EVENT }}`
    WHERE issue_key IN (SELECT epic_key FROM epics)
),

status_changes AS (
    SELECT
        issue_key,
        event_time,
        to_status
    FROM `{{ curated.STATUS_CHANGELOG }}`
    WHERE issue_key IN (SELECT epic_key FROM epics)
),

epic_status_changes AS (
    SELECT
        epics.initiative_key,
        epics.epic_key,
        sc.event_time,
        sc.to_status
    FROM status_changes AS sc
    INNER JOIN epics
        ON sc.issue_key = epics.epic_key
),

end_date_candidates AS (
    SELECT
        issue_key AS initiative_key,
        MAX(
            event_time
        ) AS event_time
    FROM `{{ curated.TARGET_BURNDOWN }}`
    WHERE issue_key IN (SELECT issue_key FROM initiatives)
    GROUP BY issue_key
    UNION ALL
    SELECT
        issue_key AS initiative_key,
        MAX(
            event_time
        ) AS event_time
    FROM `{{ curated.FORECASTED_BURNDOWN }}`
    WHERE issue_key IN (SELECT issue_key FROM initiatives)
    GROUP BY issue_key
    UNION ALL
    SELECT
        initiative_key,
        MAX(
            event_time
        ) AS event_time
    FROM epic_status_changes
    GROUP BY initiative_key
    UNION ALL
    SELECT
        epics.initiative_key,
        MAX(
            burn.event_time
        ) AS event_time
    FROM burndown AS burn
    INNER JOIN epics
        ON burn.issue_key = epics.epic_key
    GROUP BY epics.initiative_key
),

end_date AS (
    SELECT
        initiative_key,
        MAX(
            event_time
        ) AS event_time
    FROM end_date_candidates
    GROUP BY initiative_key
),

date_range AS (
    SELECT
        init.issue_key,
        GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP(
                DATE(
                    init.target_start_date
                )
            ), TIMESTAMP(
                DATE(
                    ed.event_time
                )
            ), INTERVAL 7 DAY
        ) AS ts_array
    FROM initiatives AS init
    INNER JOIN end_date AS ed
        ON init.issue_key = ed.initiative_key
),

cfd_date AS (
    SELECT
        date_range.issue_key,
        event_time
    FROM date_range, UNNEST(date_range.ts_array) AS event_time
),

cfd_status AS (
    SELECT
        esc.initiative_key,
        cd.event_time,
        esc.epic_key,
        MAX_BY(
            esc.to_status, esc.event_time
        ) AS to_status
    FROM cfd_date AS cd
    INNER JOIN epic_status_changes AS esc
        ON
            cd.issue_key = esc.initiative_key
            AND TIMESTAMP(DATE(esc.event_time)) <= cd.event_time
    GROUP BY
        esc.initiative_key,
        cd.event_time,
        esc.epic_key
),

unique_status AS (
    SELECT
        initiative_key,
        to_status AS status
    FROM cfd_status
    GROUP BY initiative_key, status
)

SELECT
    cd.issue_key AS initiative_key,
    init.issue_summary AS initiative_summary,
    cd.event_time,
    us.status,
    COUNT(DISTINCT cs.epic_key) AS epic_count
FROM cfd_date AS cd
INNER JOIN initiatives AS init
    ON cd.issue_key = init.issue_key
INNER JOIN unique_status AS us
    ON cd.issue_key = us.initiative_key
LEFT JOIN cfd_status AS cs
    ON
        cd.issue_key = cs.initiative_key
        AND cd.event_time = cs.event_time
        AND us.status = cs.to_status
GROUP BY
    cd.issue_key,
    init.issue_summary,
    cd.event_time,
    us.status;
