CREATE OR REPLACE TABLE `{{ curated.LOOKER_ACTIVE_SPRINT_ALLOCATION }}` AS

-- Calculate total story points per outcome_team and sprint_number for active states
WITH total_story_points AS (
    SELECT
        outcome_team,
        sprint_number,
        SUM(COALESCE(story_points, 0)) AS total_points
    FROM
        `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
    WHERE
        state = 'active'
    GROUP BY
        outcome_team,
        sprint_number
),

-- Calculate initiative-specific story points per outcome_team and sprint_number for active states
initiative_story_points AS (
    SELECT
        outcome_team,
        sprint_number,
        initiative_key,
        initiative_summary,
        SUM(COALESCE(story_points, 0)) AS initiative_points
    FROM
        `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
    WHERE
        state = 'active'
    GROUP BY
        outcome_team,
        sprint_number,
        initiative_key,
        initiative_summary
),

-- Calculate allocation percentages by joining initiative and total story points
allocation AS (
    SELECT
        isp.outcome_team,
        isp.initiative_key,
        isp.initiative_summary,
        isp.initiative_points,
        SAFE_DIVIDE(isp.initiative_points, tsp.total_points)
            AS allocation_percentage,
        isp.sprint_number
    FROM
        initiative_story_points AS isp
    INNER JOIN
        total_story_points AS tsp
        ON
            isp.outcome_team = tsp.outcome_team
            AND isp.sprint_number = tsp.sprint_number
)

SELECT
    allo.outcome_team,
    allo.initiative_key,
    allo.initiative_summary,
    allo.allocation_percentage,
    allo.sprint_number,
    CASE
        -- Scenario 1: UAT Bug or Prod Bug without an Initiative Key
        WHEN
            allo.initiative_summary IN ('UAT bug', 'Prod Bug')
            AND allo.initiative_key IS NULL THEN allo.initiative_summary

        -- Scenario 2: Any record with a non-NULL Initiative Key
        WHEN
            allo.initiative_key IS NOT NULL
            THEN CONCAT(allo.initiative_key, ': ', allo.initiative_summary)

        -- Scenario 3: All other cases
        ELSE 'Unassigned'
    END AS initiative_category
FROM
    allocation AS allo
WHERE
    allo.initiative_points > 0;
