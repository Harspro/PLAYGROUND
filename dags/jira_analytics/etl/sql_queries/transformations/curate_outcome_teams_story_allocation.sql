-- Insert Outcome Teams, Initiatives, Epics, and Stories into STORY_ALLOCATION
INSERT INTO `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
(
    outcome_team,
    pod_name,
    initiative_summary,
    initiative_key,
    initiative_type,
    story_key,
    epic_key,
    story_points,
    sprint_name,
    sprint_number,
    sprint_year,
    state
)
-- Step 1: Set up story - epic - initiative relations
WITH initiative AS (
    SELECT
        issue_key AS initiative_key,
        issue_summary AS initiative_summary,
        initiative_category AS initiative_type
    FROM
        `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = 'initiative'
),

epic AS (
    SELECT
        issue_key AS epic_key,
        parent_key AS initiative_key
    FROM
        `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = 'epic'
        AND parent_key IN (SELECT initiative_key FROM initiative)
),

story AS (
    SELECT
        issue_key AS story_key,
        issue_summary AS story_summary,
        parent_key AS epic_key
    FROM
        `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = 'story'
),

-- Step 1a: Combine issues from closed and active sprints
combined_issues AS (
    SELECT
        iics.issue_key,
        iics.summary,
        iics.story_points,
        iics.sprint_id,
        iics.issue_type
    FROM
        `{{ landing.ISSUES_IN_CLOSED_SPRINTS }}` AS iics
    UNION ALL
    SELECT
        iias.issue_key,
        iias.summary,
        iias.story_points,
        iias.sprint_id,
        iias.issue_type
    FROM
        `{{ landing.ISSUES_IN_ACTIVE_SPRINTS }}` AS iias
),

base_data AS (
    SELECT
        ci.issue_key,
        ci.summary,
        ci.story_points,
        ci.issue_type,
        stor.epic_key,
        ep.initiative_key,
        init.initiative_summary,
        init.initiative_type,
        ci.sprint_id
    FROM
        combined_issues AS ci
    LEFT JOIN
        story AS stor
        ON ci.issue_key = stor.story_key
    LEFT JOIN
        epic AS ep
        ON stor.epic_key = ep.epic_key
    LEFT JOIN
        initiative AS init
        ON ep.initiative_key = init.initiative_key
),

-- Step 2: Compile sprint and team relation
sprint_board AS (
    SELECT
        sp.id AS sprint_id,
        sp.name AS sprint_name,
        sp.state,
        sp.board_id
    FROM
        `{{ landing.SPRINT }}` AS sp
    RIGHT JOIN
        `{{ landing.OUTCOME_TEAM }}` AS ot
        ON sp.board_id = ot.board_id
    WHERE
        -- current year
        sp.name LIKE CONCAT(
            '%S', CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING), '%'
        )
        OR
        -- past year
        sp.name LIKE CONCAT(
            '%-%S', CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING), '%'
        )
),

team_data AS (
    SELECT
        ot.board_id,
        ot.project_name
    FROM
        `{{ landing.OUTCOME_TEAM }}` AS ot
)

SELECT DISTINCT
    td.project_name AS outcome_team,
    SPLIT(sb.sprint_name, '-')[OFFSET(0)] AS pod_name,
    COALESCE(bd.initiative_summary, bd.issue_type) AS initiative_summary,
    bd.initiative_key,
    bd.initiative_type,
    bd.issue_key AS story_key,
    bd.epic_key,
    bd.story_points,
    sb.sprint_name,
    SAFE_CAST(SPLIT(sb.sprint_name, '-')[OFFSET(2)] AS INT64) AS sprint_number,
    TRIM(SPLIT(sb.sprint_name, '-')[OFFSET(1)]) AS sprint_year,
    sb.state
FROM
    base_data AS bd
INNER JOIN
    sprint_board AS sb
    ON bd.sprint_id = sb.sprint_id
LEFT JOIN
    team_data AS td
    ON sb.board_id = td.board_id;
