CREATE OR REPLACE TABLE `{{ curated.LOOKER_COMBINED_INITIATIVE_ALLOCATION }}` AS

-- Select the top 3 most recent sprints
WITH unique_sprint_outcomes AS (
    SELECT DISTINCT
        sprint_year,
        sprint_number,
        outcome_team
    FROM
        `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
),

ranked_sprint_teams AS (
    SELECT
        sprint_year,
        sprint_number,
        outcome_team,
        ROW_NUMBER()
            OVER (
                PARTITION BY outcome_team
                ORDER BY sprint_year DESC, sprint_number DESC
            )
            AS row_num
    FROM unique_sprint_outcomes
),

three_recent_sprints AS (
    SELECT
        sprint_year,
        sprint_number,
        outcome_team
    FROM ranked_sprint_teams
    WHERE row_num <= 3
),

story_allocation AS (
    SELECT
        otsa.outcome_team,
        otsa.pod_name,
        otsa.sprint_number,
        otsa.sprint_year,
        otsa.story_points,
        otsa.initiative_key,
        otsa.initiative_summary,
        otsa.initiative_type
    FROM
        `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}` AS otsa
),

-- Calculate total story points per outcome_team, and sprint_number
total_story_points AS (
    SELECT
        sa.outcome_team,
        sa.sprint_number,
        SUM(COALESCE(sa.story_points, 0)) AS total_points
    FROM
        story_allocation AS sa
    INNER JOIN
        three_recent_sprints AS trs
        ON
            sa.sprint_year = trs.sprint_year
            AND sa.sprint_number = trs.sprint_number
            AND sa.outcome_team = trs.outcome_team
    GROUP BY
        sa.outcome_team,
        sa.sprint_number
),

-- Calculate initiative-specific story points per outcome_team, and sprint_number
initiative_story_points AS (
    SELECT
        sa.outcome_team,
        sa.pod_name,
        sa.sprint_number,
        sa.sprint_year,
        sa.initiative_key,
        sa.initiative_summary,
        sa.initiative_type,
        SUM(COALESCE(sa.story_points, 0)) AS initiative_points
    FROM
        story_allocation AS sa
    INNER JOIN
        three_recent_sprints AS trs
        ON
            sa.sprint_year = trs.sprint_year
            AND sa.sprint_number = trs.sprint_number
            AND sa.outcome_team = trs.outcome_team
    GROUP BY
        sa.outcome_team,
        sa.pod_name,
        sa.sprint_number,
        sa.sprint_year,
        sa.initiative_key,
        sa.initiative_summary,
        sa.initiative_type
),

-- Calculate allocation percentages
allocation AS (
    SELECT
        isp.outcome_team,
        isp.pod_name,
        isp.sprint_number,
        isp.sprint_year,
        isp.initiative_key,
        isp.initiative_summary,
        isp.initiative_type,
        isp.initiative_points,
        tsp.total_points,
        SAFE_DIVIDE(isp.initiative_points, tsp.total_points)
            AS allocation_percentage
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
    allo.pod_name,
    allo.sprint_number,
    allo.sprint_year,
    allo.initiative_key,
    allo.initiative_summary,
    CASE
        -- Scenario 1: Technical Allocation based on specific Initiative Keys or Prod Bug
        WHEN
            allo.initiative_key IN (
                'PCBINIT-127', 'PCBINIT-1311', 'PCBINIT-1542'
            )
            OR allo.initiative_summary = 'Prod Bug' THEN 'Technical Allocation'

        -- Scenario 2: Existing Initiative Type if not null
        WHEN allo.initiative_type IS NOT NULL THEN allo.initiative_type

        -- Scenario 3: All other cases
        ELSE 'None'
    END AS initiative_type,
    allo.allocation_percentage,
    CASE
        -- Scenario 1: UAT Bug or Prod Bug without an Initiative Key
        WHEN
            allo.initiative_summary IN ('Prod Bug')
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
