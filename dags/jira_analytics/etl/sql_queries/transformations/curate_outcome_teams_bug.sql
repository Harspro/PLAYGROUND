-- Create and load OUTCOME_TEAMS_BUG
CREATE OR REPLACE TABLE `{{ curated.OUTCOME_TEAMS_BUG }}` AS

WITH bugs AS (
    SELECT
        issue_key,
        SPLIT(issue_key, '-')[OFFSET(0)] AS outcome_team_key,
        team_name,
        sprint_name,
        SUBSTR(priority, 3) AS severity,
        (CASE
            WHEN env_type IN (NULL, 'Dev', 'QA', '') THEN 'UAT'
            ELSE env_type
        END) AS env_type,
        state,
        story_points,
        created_date
    FROM
        `{{ landing.BUG }}`

),

team_data AS (
    SELECT
        project_name,
        project
    FROM
        `{{ landing.OUTCOME_TEAM }}`
)

SELECT DISTINCT
    bg.issue_key,
    td.project_name AS outcome_team,
    bg.team_name,
    bg.sprint_name,
    bg.severity,
    bg.env_type,
    bg.state,
    bg.story_points,
    bg.created_date
FROM
    bugs AS bg
LEFT JOIN
    team_data AS td
    ON bg.outcome_team_key = td.project;
