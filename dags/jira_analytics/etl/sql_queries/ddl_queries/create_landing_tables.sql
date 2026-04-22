CREATE TABLE IF NOT EXISTS `{{ landing.INITIATIVE }}`
(
    issue_key STRING,
    summary STRING,
    status STRING,
    query_time TIMESTAMP,
    created_date TIMESTAMP,
    t_shirt_size STRING,
    due_date TIMESTAMP,
    start_date TIMESTAMP,
    initiative_category STRING,
    initiative_type STRING,
    overall_status STRING
);

CREATE TABLE IF NOT EXISTS `{{ landing.EPIC }}`
(
    issue_key STRING,
    summary STRING,
    status STRING,
    query_time TIMESTAMP,
    t_shirt_size STRING,
    initiative_key STRING,
    resolution STRING,
    created_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{ landing.STORY }}`
(
    issue_key STRING,
    summary STRING,
    status STRING,
    query_time TIMESTAMP,
    epic_key STRING,
    story_point FLOAT64,
    resolution STRING,
    created_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{ landing.CHANGELOG_EVENT }}`
(
    issue_key STRING,
    event_time TIMESTAMP,
    field STRING,
    field_type STRING,
    field_id STRING,
    from_string STRING,
    to_string STRING,
    query_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{ landing.CHANGELOG_COUNT }}`
(
    issue_key STRING,
    count INT64,
    query_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{ landing.OUTCOME_TEAM }}`
(
    project_key STRING,
    project_name STRING,
    team_name STRING,
    board_id INT64
);

CREATE TABLE IF NOT EXISTS `{{ landing.SPRINT }}`
(
    id INT64,
    name STRING,
    state STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    board_id INT64
);

CREATE TABLE IF NOT EXISTS `{{ landing.ISSUES_IN_ACTIVE_SPRINTS }}`
(
    issue_key STRING,
    summary STRING,
    story_points FLOAT64,
    sprint_id INT64,
    issue_type STRING
);

CREATE TABLE IF NOT EXISTS `{{ landing.ISSUES_IN_CLOSED_SPRINTS }}`
(
    issue_key STRING,
    summary STRING,
    story_points FLOAT64,
    sprint_id INT64,
    issue_type STRING
);

CREATE TABLE IF NOT EXISTS `{{ landing.BUG }}`
(
    issue_key STRING,
    summary STRING,
    state STRING,
    parent_key STRING,
    story_points FLOAT64,
    sprint_name STRING,
    env_type STRING,
    team_name STRING,
    priority STRING,
    created_date TIMESTAMP
);
