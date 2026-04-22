-- View to get the latest record for each PRMI ticket
-- This deduplicates records by taking the most recent query_time for each issue_key
-- Use this view for reporting/analytics instead of the raw table

CREATE OR REPLACE VIEW `{project_id}.domain_project_artifacts.PRMI_TICKET_LATEST` AS
SELECT
    issue_key,
    summary,
    status,
    created_date,
    priority_id,
    priority_name,
    impacted_capabilities,
    impacted_platforms,
    incident_end_date,
    query_time
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY issue_key ORDER BY query_time DESC) as rn
    FROM `{project_id}.domain_project_artifacts.PRMI_TICKET`
)
WHERE rn = 1;
