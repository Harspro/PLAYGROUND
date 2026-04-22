INSERT `{{ curated.UNIQUE_ISSUES }}` (
    issue_key,
    parent_key,
    issue_type,
    issue_summary,
    status,
    resolution,
    t_shirt_size,
    initiative_category,
    initiative_type,
    overall_status,
    story_point,
    start_date,
    due_date,
    created_date
)
WITH union_issues AS (
    SELECT
        issue_key,
        NULL AS parent_key,
        "initiative" AS issue_type,
        summary AS issue_summary,
        status,
        NULL AS resolution,
        t_shirt_size,
        initiative_category,
        initiative_type,
        overall_status,
        NULL AS story_point,
        start_date,
        due_date,
        created_date,
        query_time
    FROM `{{ landing.INITIATIVE }}`

    UNION ALL

    SELECT
        issue_key,
        initiative_key AS parent_key,
        "epic" AS issue_type,
        summary AS issue_summary,
        status,
        resolution,
        t_shirt_size,
        NULL AS initiative_category,
        NULL AS initiative_type,
        NULL AS overall_status,
        NULL AS story_point,
        NULL AS start_date,
        NULL AS due_date,
        created_date,
        query_time
    FROM `{{ landing.EPIC }}`

    UNION ALL

    SELECT
        issue_key,
        epic_key AS parent_key,
        "story" AS issue_type,
        summary AS issue_summary,
        status,
        resolution,
        NULL AS t_shirt_size,
        NULL AS initiative_category,
        NULL AS initiative_type,
        NULL AS overall_status,
        story_point,
        NULL AS start_date,
        NULL AS due_date,
        created_date,
        query_time
    FROM `{{ landing.STORY }}`
),

row_num_issues AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY issue_key ORDER BY query_time DESC)
            AS row_num
    FROM union_issues
)

SELECT
    issue_key,
    parent_key,
    issue_type,
    issue_summary,
    status,
    resolution,
    t_shirt_size,
    initiative_category,
    initiative_type,
    overall_status,
    story_point,
    start_date,
    due_date,
    created_date
FROM row_num_issues
WHERE row_num = 1;
