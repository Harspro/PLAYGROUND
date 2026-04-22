CREATE OR REPLACE TABLE `{{ curated.LOOKER_STORY_POINT_BURNDOWN_CHART }}` AS
WITH initiative AS (
    SELECT
        issue_key,
        issue_summary,
        target_start_date,
        final_close_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "initiative"
        AND target_start_date IS NOT NULL
),

epic AS (
    SELECT
        issue_key,
        parent_key
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "epic"
        AND parent_key IN (SELECT issue_key FROM initiative)
        AND resolution NOT IN ("Won't Do", "Duplicate", "Cancelled by Business")
),

story AS (
    SELECT
        issue_key,
        parent_key
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "story"
        AND parent_key IN (SELECT issue_key FROM epic)
        AND resolution NOT IN ("Won't Do", "Duplicate", "Cancelled by Business")
),

epic_status_changelog AS (
    SELECT
        issue_key AS epic_key,
        event_time,
        to_status
    FROM `{{ curated.STATUS_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM epic)
),

story_status_changelog AS (
    SELECT
        issue_key AS story_key,
        event_time,
        to_status
    FROM `{{ curated.STATUS_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM story)
),

epic_t_shirt_size_changelog AS (
    SELECT
        issue_key AS epic_key,
        event_time,
        to_mean_size
    FROM `{{ curated.T_SHIRT_SIZE_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM epic)
),

story_point_changelog AS (
    SELECT
        issue_key AS story_key,
        event_time,
        to_story_point
    FROM `{{ curated.STORY_POINT_CHANGELOG }}`
    WHERE issue_key IN (SELECT issue_key FROM story)
),

init_epic_story_combined AS (
    SELECT
        init.issue_key AS initiative_key,
        init.issue_summary AS initiative_summary,
        epic.issue_key AS epic_key,
        story.issue_key AS story_key,
        init.target_start_date AS initiative_start_date,
        init.final_close_date AS initiative_close_date
    FROM initiative AS init
    INNER JOIN epic
        ON init.issue_key = epic.parent_key
    LEFT JOIN story
        ON epic.issue_key = story.parent_key
),

date_array AS (
    SELECT
        initiative_key,
        initiative_summary,
        epic_key,
        story_key,
        GENERATE_TIMESTAMP_ARRAY(
            initiative_start_date,
            COALESCE(initiative_close_date, CAST(CURRENT_DATE() AS TIMESTAMP)),
            INTERVAL 7
            DAY
        ) AS dates
    FROM init_epic_story_combined
),

unnested_date_array AS (
    SELECT
        da.initiative_key,
        da.initiative_summary,
        da.epic_key,
        da.story_key,
        event_time
    FROM date_array AS da,
        UNNEST(da.dates) AS event_time
),

story_point_history AS (
    SELECT
        uda.initiative_key,
        uda.epic_key,
        uda.story_key,
        uda.event_time,
        MAX_BY(spc.to_story_point, spc.event_time) AS latest_story_point
    FROM unnested_date_array AS uda
    INNER JOIN story_point_changelog AS spc
        ON
            uda.story_key = spc.story_key
            AND uda.event_time >= spc.event_time
    GROUP BY 1, 2, 3, 4
),

story_status_history AS (
    SELECT
        uda.initiative_key,
        uda.epic_key,
        uda.story_key,
        uda.event_time,
        MAX_BY(ssc.to_status, ssc.event_time) AS latest_story_status
    FROM unnested_date_array AS uda
    INNER JOIN story_status_changelog AS ssc
        ON
            uda.story_key = ssc.story_key
            AND uda.event_time >= ssc.event_time
    GROUP BY 1, 2, 3, 4
),

epic_t_shirt_size_history AS (
    SELECT
        uda.initiative_key,
        uda.epic_key,
        uda.event_time,
        MAX_BY(etssc.to_mean_size, etssc.event_time) AS latest_mean_size
    FROM unnested_date_array AS uda
    INNER JOIN epic_t_shirt_size_changelog AS etssc
        ON
            uda.epic_key = etssc.epic_key
            AND uda.event_time >= etssc.event_time
    GROUP BY 1, 2, 3
),

epic_status_history AS (
    SELECT
        uda.initiative_key,
        uda.initiative_summary,
        uda.epic_key,
        uda.event_time,
        MAX_BY(esc.to_status, esc.event_time) AS latest_epic_status
    FROM unnested_date_array AS uda
    INNER JOIN epic_status_changelog AS esc
        ON
            uda.epic_key = esc.epic_key
            AND uda.event_time >= esc.event_time
    GROUP BY 1, 2, 3, 4
),

epic_burndown_by_story_point AS (
    SELECT
        esh.initiative_key,
        esh.initiative_summary,
        esh.epic_key,
        esh.event_time,
        esh.latest_epic_status,
        etssh.latest_mean_size,
        SUM(sph.latest_story_point) AS total_story_point,
        SUM(CASE
            WHEN ssh.latest_story_status = "Done"
                THEN sph.latest_story_point
            ELSE 0
        END) AS burned_story_point
    FROM epic_status_history AS esh
    INNER JOIN epic_t_shirt_size_history AS etssh
        ON
            esh.initiative_key = etssh.initiative_key
            AND esh.epic_key = etssh.epic_key
            AND esh.event_time = etssh.event_time
    LEFT JOIN story_point_history AS sph
        ON
            esh.initiative_key = sph.initiative_key
            AND esh.epic_key = sph.epic_key
            AND esh.event_time = sph.event_time
    LEFT JOIN story_status_history AS ssh
        ON
            sph.initiative_key = ssh.initiative_key
            AND sph.epic_key = ssh.epic_key
            AND sph.story_key = ssh.story_key
            AND sph.event_time = ssh.event_time
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    initiative_key,
    initiative_summary,
    event_time,
    SUM(latest_mean_size) AS total_mean_size,
    ROUND(SUM(CASE
        WHEN latest_epic_status IN ("Done", "Closed") THEN latest_mean_size
        WHEN
            total_story_point > 0
            THEN latest_mean_size * burned_story_point / total_story_point
        ELSE 0
    END), 1) AS burned_mean_size
FROM epic_burndown_by_story_point
GROUP BY 1, 2, 3
