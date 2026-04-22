# PRMI Jira Tickets DAG

This DAG extracts PRMI (Production Risk Management Incident) tickets from Jira and loads them into BigQuery for analytics.

## Overview

The PRMI Jira Tickets DAG extracts the following fields from PRMI tickets:
- Issue Key
- Summary
- Status
- Created Date
- Priority (ID and Name)
- Impacted Capabilities (customfield_13289)
- Impacted Platforms (customfield_15144)
- Incident End Date

## Structure

```
prmi_jira_tickets/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── config.py              # Configuration settings
│   └── terminus_table_config.py  # BigQuery table configurations
├── etl/
│   ├── __init__.py
│   ├── extract.py             # Jira extraction logic
│   └── load.py                # BigQuery loading logic
├── model/
│   ├── jira/
│   │   ├── issue/
│   │   │   ├── fields/        # Jira field models
│   │   │   └── issue.py       # Jira issue models
│   │   └── response.py        # Jira API response models
│   └── terminus/
│       └── prmi_ticket.py     # BigQuery landing zone model
├── utils/
│   ├── __init__.py
│   └── utils.py               # Utility functions
├── prmi_jira_tickets_dag.py   # Main DAG file
└── README.md                  # This file
```

## Configuration

### Jira Configuration

The DAG uses the same Jira credentials as the `jira_analytics` DAG:
- URL: `https://pc-technology.atlassian.net`
- Username: `svc_DevOpsTools@pcbank.ca`
- Password: Retrieved from Vault at `/secret/data/jira-analytics` (key: `api_password`)

### Date Range

By default, the DAG extracts PRMI tickets from the last 30 days. This can be configured in `config/config.py`:
- `DEFAULT_DAYS_BACK`: Number of days to look back (default: 30)

### JQL Query

The DAG uses a delta window on `updated`:
```
project = PRMI AND updated >= "{start_date}" AND updated <= "{end_date}"
```

## BigQuery Tables

### Landing Zone

- **PRMI_TICKET**: Contains the extracted PRMI ticket data
- **DAG_HISTORY**: Tracks successful DAG run times

### Table Schema (PRMI_TICKET)

- `issue_key` (STRING): Jira issue key (e.g., PRMI-123)
- `summary` (STRING): Issue summary/title
- `status` (STRING): Current status
- `created_date` (TIMESTAMP): Issue creation date
- `priority_id` (STRING): Priority ID (1 for P1, 2 for P2, etc.)
- `priority_name` (STRING): Priority name
- `impacted_capabilities` (STRING): Comma-separated list of impacted capabilities
- `impacted_platforms` (STRING): Comma-separated list of impacted platforms
- `incident_end_date` (TIMESTAMP): Incident end date (if available)
- `query_time` (TIMESTAMP): When the data was extracted

## DAG Schedule

The DAG runs every 2 minutes by default (`*/2 * * * *`). This can be changed in `config/config.py`:
- `UPDATE_CRON_SCHEDULE`: Cron expression for DAG schedule

## Incremental Updates

The DAG uses incremental updates based on the `updated` field in Jira. This means:
- **New tickets**: Will be captured when they are created or updated
- **Updated tickets**: Will be captured when any field is updated (including missing fields being populated)
-- The DAG tracks the last successful run time and only fetches tickets updated since then
-- If there is no prior successful run, it defaults to the last `DEFAULT_DAYS_BACK` days

### Update Strategy

The DAG uses **MERGE** operations in BigQuery to:
- **UPDATE existing records** when a PRMI ticket already exists (matched by `issue_key`)
- **INSERT new records** when a PRMI ticket doesn't exist yet

This ensures:
- Each PRMI ticket has only **one record** in the table (the latest version)
- Fields are automatically updated when tickets are modified in Jira
- Missing fields are populated when they become available
- No duplicate records are created

## Dependencies

The DAG uses the following utilities:
- `util.bq_utils`: BigQuery operations
- `util.vault_util`: Vault credential retrieval
- `dag_factory.terminus_dag_factory`: DAG tagging

## Custom Fields

The DAG extracts the following Jira custom fields:
- `customfield_13289`: Impacted Capabilities (multi-select)
- `customfield_15144`: Impacted Platforms (multi-select)

**Note:** The Incident End Date field name in JQL is `"Incident End Date[Date]"`. The exact custom field ID may need to be verified in your Jira instance.

## Troubleshooting

### No tickets found

1. Check the date range in the JQL query
2. Verify that PRMI tickets exist with priority >= 2-High
3. Check Jira API credentials

### Missing custom field data

1. Verify the custom field IDs in your Jira instance
2. Check if the fields are populated in the PRMI tickets
3. Review the extraction logs for field mapping errors

### Connection errors

1. Verify Jira API credentials
2. Check network connectivity to Jira
3. Review Jira API rate limits

## Next Steps

1. **Deploy the DAG**: The DAG can be deployed to Airflow
2. **Create BigQuery tables**: Ensure the landing zone tables are created before running the DAG

