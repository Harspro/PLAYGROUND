INSERT INTO `{{ curated.LOOKER_OUTCOME_TEAMS_COMPLETED_STORY_POINTS }}` (
    outcome_team,
    pod,
    sprint_number,
    sprint_name,
    story_points
)

-- Select the top 16 most recent sprints
WITH ranked_sprints AS (
    SELECT DISTINCT
        sprint_year,
        sprint_number
    FROM
        `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
    ORDER BY
        sprint_year DESC,
        sprint_number DESC
    LIMIT 16
)

SELECT
    outcome_team,
    SPLIT(sprint_name, '-')[0] AS pod,
    CONCAT(SPLIT(sprint_name, '-')[1], '-', SPLIT(sprint_name, '-')[2])
        AS sprint_number,
    sprint_name,
    SUM(story_points) AS story_points
FROM `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}` AS otsa
INNER JOIN ranked_sprints AS rs
    ON
        otsa.sprint_year = rs.sprint_year
        AND otsa.sprint_number = rs.sprint_number
WHERE state = 'closed'
GROUP BY sprint_name, outcome_team
