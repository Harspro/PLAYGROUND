CREATE OR REPLACE TABLE `{{ curated.UNIQUE_ISSUES }}`
(
    issue_key STRING,
    parent_key STRING,
    issue_type STRING,
    issue_summary STRING,
    status STRING,
    resolution STRING,
    t_shirt_size STRING,
    initiative_category STRING,
    initiative_type STRING,
    overall_status STRING,
    story_point FLOAT64,
    start_date TIMESTAMP,
    due_date TIMESTAMP,
    created_date TIMESTAMP,
    first_open_date TIMESTAMP,
    final_close_date TIMESTAMP,
    target_start_date TIMESTAMP,
    target_completion_date TIMESTAMP,
    forecasted_completion_date TIMESTAMP,
    forecasted_burndown_rate FLOAT64,
    forecasted_slack FLOAT64,
    lead_time FLOAT64,
    total_size FLOAT64,
    mean_size FLOAT64,
    total_completed_size FLOAT64
);

CREATE OR REPLACE TABLE `{{ curated.STATUS_CHANGELOG }}`
(
    issue_key STRING,
    event_time TIMESTAMP,
    from_status STRING,
    to_status STRING
);

CREATE OR REPLACE TABLE `{{ curated.T_SHIRT_SIZE_CHANGELOG }}`
(
    issue_key STRING,
    event_time TIMESTAMP,
    from_t_shirt_size STRING,
    to_t_shirt_size STRING,
    from_mean_size FLOAT64,
    to_mean_size FLOAT64
);

CREATE OR REPLACE TABLE `{{ curated.STORY_POINT_CHANGELOG }}`
(
    issue_key STRING,
    event_time TIMESTAMP,
    from_story_point FLOAT64,
    to_story_point FLOAT64
);

CREATE OR REPLACE TABLE `{{ curated.BURNDOWN_EVENT }}`
(
    issue_key STRING,
    event_time TIMESTAMP,
    event_type STRING,
    burndown_weeks FLOAT64
);

CREATE OR REPLACE TABLE `{{ curated.OUTCOME_TEAMS_STORY_ALLOCATION }}`
(
    outcome_team STRING,
    pod_name STRING,
    initiative_summary STRING,
    initiative_key STRING,
    initiative_type STRING,
    story_key STRING,
    epic_key STRING,
    story_points FLOAT64,
    sprint_name STRING,
    sprint_number INT64,
    sprint_year STRING,
    state STRING
);

CREATE OR REPLACE TABLE `{{ curated.LOOKER_OUTCOME_TEAMS_COMPLETED_STORY_POINTS }}`
(
    outcome_team STRING,
    pod STRING,
    sprint_name STRING,
    story_points FLOAT64,
    sprint_number STRING
);

CREATE OR REPLACE TABLE `{{ curated.LOOKER_ACTIVE_SPRINT_ALLOCATION }}`
(
    outcome_team STRING,
    initiative_key STRING,
    initiative_summary STRING,
    allocation_percentage FLOAT64,
    sprint_number INT64,
    initiative_category STRING
);

CREATE OR REPLACE TABLE `{{ curated.LOOKER_COMBINED_INITIATIVE_ALLOCATION }}`
(
    outcome_team STRING,
    pod_name STRING,
    sprint_number INT64,
    sprint_year STRING,
    initiative_key STRING,
    initiative_summary STRING,
    initiative_type STRING,
    allocation_percentage FLOAT64,
    initiative_category STRING
);

CREATE OR REPLACE TABLE `{{ curated.OUTCOME_TEAMS_BUG }}`
(
    issue_key STRING,
    outcome_team STRING,
    pod_name STRING,
    sprint_name STRING,
    priority STRING,
    env_type STRING,
    story_points FLOAT64,
    state STRING,
    created_date TIMESTAMP
);

CREATE OR REPLACE TABLE `{{ curated.TABLEAU_STORY_ALLOCATION }}`
(
    outcome_team STRING,
    pod_name STRING,
    initiative_summary STRING,
    initiative_key STRING,
    initiative_type STRING,
    story_key STRING,
    story_summary STRING,
    issue_type STRING,
    epic_key STRING,
    epic_summary STRING,
    story_points FLOAT64,
    sprint_name STRING,
    sprint_number INT64,
    sprint_year STRING,
    state STRING
);
