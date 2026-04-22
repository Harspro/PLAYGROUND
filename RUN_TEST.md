# How to Run the PRMI Test Script

## Quick Start

1. **Activate the virtual environment** (created in the project root):
   ```bash
   source venv/bin/activate
   ```

2. **Set your Jira API token**:
   ```bash
   export JIRA_PASSWORD='your-api-token-here'
   ```

3. **Run the test script**:
   ```bash
   python3 dags/prmi_jira_tickets/test_extract_to_csv_new_fields.py
   ```

## Complete Example

```bash
# Navigate to project directory (if not already there)
cd /Users/amiclem/pcb-data-foundation-airflow-dags

# Activate virtual environment
source venv/bin/activate

# Set Jira API token (replace with your actual token)
export JIRA_PASSWORD='ATATT3xFfGF0...'

# Run the test script
python3 dags/prmi_jira_tickets/test_extract_to_csv_new_fields.py
```

## Expected Output

You should see:
```
================================================================================
PRMI Ticket Extraction Test - New Query and Fields
================================================================================
Using JQL query: project = PRMI AND updatedDate >= -7d ORDER BY updatedDate DESC
Extracting tickets updated in the last 7 days...
Extracted 15 PRMI tickets (total available: 15)
✅ CSV file created: /Users/amiclem/pcb-data-foundation-airflow-dags/dags/prmi_jira_tickets/prmi_tickets_new_fields.csv
```

## Finding Your CSV File

The CSV will be created at:
```
/Users/amiclem/pcb-data-foundation-airflow-dags/dags/prmi_jira_tickets/prmi_tickets_new_fields.csv
```

## Getting a Jira API Token

If you don't have a Jira API token:
1. Go to: https://id.atlassian.com/manage-profile/security/api-tokens
2. Click "Create API token"
3. Copy the token and use it in the `export JIRA_PASSWORD='...'` command

## Note

- The virtual environment (`venv`) is already created in the project root
- You need to activate it each time you open a new terminal session
- The `venv` folder is in `.gitignore` so it won't be committed to git


