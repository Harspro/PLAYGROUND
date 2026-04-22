from datetime import timedelta

DAG_DEFAULT_ARGS = {
    "owner": "team-vortex",
    'capability': 'Consent',
    'severity': 'P4',
    'sub_capability': 'Consent',
    'business_impact': 'Failure to load historical consents',
    'customer_impact': 'None',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "tags": ["consents"],
    "region": "northamerica-northeast1"
}
