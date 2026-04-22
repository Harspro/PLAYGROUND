"""
The class defines methods related to extraction of data from JIRA Cloud.
"""

import logging
from datetime import datetime, timedelta
from time import time, sleep
from requests.exceptions import ConnectionError
from typing import Dict, Union, Optional, List, Type, Tuple
from dateutil.parser import parse

from atlassian import Jira
from dacite import from_dict, Config as DaciteConfig
from dateutil.parser import isoparse
from google.cloud.bigquery import Client
from google.cloud.exceptions import NotFound
from pandas import isna, to_datetime

from prmi_jira_tickets.config import PRMIJiraTicketsConfig as AppConfig
from prmi_jira_tickets.model.jira.response import PRMITicketResponse
from prmi_jira_tickets.model.terminus.prmi_ticket import PRMITicket as TerminusPRMITicket
from prmi_jira_tickets.utils import as_naive_utc


def _fetch_attr(attrs: List[str], obj) -> Dict:
    """Helper function to fetch attributes from an object."""
    d = dict()
    for attr in attrs:
        if not attr.startswith('__'):
            if attr in dir(obj):
                d[attr] = getattr(obj, attr)
    return d


def _extract_array_field(field_value) -> Optional[str]:
    """
    Helper function to extract array field values as comma-separated string.

    :param field_value: The field value (can be list, dict with 'value', or None)
    :return: Comma-separated string or None
    """
    if field_value is None:
        return None
    if isinstance(field_value, list):
        # Handle list of objects with 'value' attribute or simple strings
        values = []
        for item in field_value:
            if isinstance(item, dict) and 'value' in item:
                values.append(str(item['value']))
            elif hasattr(item, 'value'):
                values.append(str(item.value))
            else:
                values.append(str(item))
        return ', '.join(values) if values else None
    return str(field_value)


CRITICAL_DATETIME_FIELDS = {
    "incident_start_date",
    "incident_end_date",
    "issue_reported_date_time",
}


def _parse_datetime_field(field_value, issue_key: str, field_name: str) -> Optional[datetime]:
    """
    Helper function to parse datetime fields safely.

    :param field_value: The field value to parse
    :param issue_key: The issue key for logging
    :param field_name: The field name for logging
    :return: Parsed datetime or None
    """
    if field_value is None:
        return None
    try:
        if isinstance(field_value, datetime):
            return as_naive_utc(field_value)
        return as_naive_utc(parse(str(field_value)))
    except Exception as e:
        try:
            fallback_value = to_datetime(field_value, errors="coerce")
            if not isna(fallback_value):
                return as_naive_utc(fallback_value.to_pydatetime())
        except Exception:
            fallback_value = None

        if field_name in CRITICAL_DATETIME_FIELDS:
            logging.error(
                "Failed to parse %s for %s: %s (raw=%s).",
                field_name,
                issue_key,
                e,
                field_value,
            )
            return None

        logging.warning(
            "Failed to parse %s for %s: %s (raw=%s).",
            field_name,
            issue_key,
            e,
            field_value,
        )
        return None


def _extract_string_field(field_value) -> Optional[str]:
    """
    Helper function to extract string field values.

    :param field_value: The field value
    :return: String value or None
    """
    if field_value is None:
        return None
    if isinstance(field_value, dict):
        for key in ("value", "displayName", "name", "id"):
            if key in field_value and field_value[key] is not None:
                return str(field_value[key])
        return str(field_value) if field_value else None
    if hasattr(field_value, 'value'):
        return str(field_value.value)
    if hasattr(field_value, 'displayName'):
        return str(field_value.displayName)
    if hasattr(field_value, 'name'):
        return str(field_value.name)
    return str(field_value) if field_value else None


def _get_field(fields_dict: dict, field_name: str):
    if not isinstance(fields_dict, dict):
        return None
    return fields_dict.get(field_name)


def transform_prmi_ticket_dict_to_terminus(issue_dict: dict) -> TerminusPRMITicket:
    """
    Transform a raw Jira issue dict to a Terminus PRMI ticket.
    """
    query_time = as_naive_utc(datetime.now())
    fields_dict = issue_dict.get("fields", {}) if isinstance(issue_dict, dict) else {}

    issue_key = issue_dict.get("key") if isinstance(issue_dict, dict) else None
    summary = _get_field(fields_dict, "summary") or ""
    status = _extract_string_field(_get_field(fields_dict, "status")) or ""

    priority = _get_field(fields_dict, "priority")
    if isinstance(priority, dict):
        priority_id = priority.get("id") or ""
        priority_name = priority.get("name") or ""
    else:
        priority_id = _extract_string_field(priority) or ""
        priority_name = _extract_string_field(priority) or ""

    created_raw = _get_field(fields_dict, "created")
    created_date = as_naive_utc(parse(str(created_raw))) if created_raw else None

    assignee = _extract_string_field(_get_field(fields_dict, "assignee"))
    description = _get_field(fields_dict, "description")
    issuetype = _extract_string_field(_get_field(fields_dict, "issuetype"))
    reporter = _extract_string_field(_get_field(fields_dict, "reporter"))

    impacted_capabilities = _extract_array_field(_get_field(fields_dict, "customfield_13289"))
    impacted_platforms = _extract_array_field(_get_field(fields_dict, "customfield_15144"))

    # Parse datetime fields
    incident_start_date = _parse_datetime_field(_get_field(fields_dict, "customfield_13174"), issue_key, "incident_start_date")
    incident_end_date = _parse_datetime_field(_get_field(fields_dict, "customfield_13175"), issue_key, "incident_end_date")
    issue_actual_start = _parse_datetime_field(_get_field(fields_dict, "customfield_13243"), issue_key, "issue_actual_start")
    end_of_issue_resolution = _parse_datetime_field(_get_field(fields_dict, "customfield_13244"), issue_key, "end_of_issue_resolution")
    end_of_customer_impact_recovery = _parse_datetime_field(_get_field(fields_dict, "customfield_13245"), issue_key, "end_of_customer_impact_recovery")
    issue_response_time = _parse_datetime_field(_get_field(fields_dict, "customfield_13376"), issue_key, "issue_response_time")
    issue_reported_date_time = _parse_datetime_field(_get_field(fields_dict, "customfield_13586"), issue_key, "issue_reported_date_time")
    banner_start_time = _parse_datetime_field(_get_field(fields_dict, "customfield_14682"), issue_key, "banner_start_time")
    banner_end_time = _parse_datetime_field(_get_field(fields_dict, "customfield_14683"), issue_key, "banner_end_time")

    # Extract string fields
    time_in_status = _extract_string_field(_get_field(fields_dict, "customfield_10101"))
    time_to_resolution = _extract_string_field(_get_field(fields_dict, "customfield_11000"))
    impact_description = _extract_string_field(_get_field(fields_dict, "customfield_12911"))
    resolution_detail = _extract_string_field(_get_field(fields_dict, "customfield_12974"))
    duration = _extract_string_field(_get_field(fields_dict, "customfield_13122"))
    incident_id = _extract_string_field(_get_field(fields_dict, "customfield_13123"))
    affected_systems = _extract_string_field(_get_field(fields_dict, "customfield_13173"))
    mi_root_cause = _extract_string_field(_get_field(fields_dict, "customfield_13246"))
    corrective_action = _extract_string_field(_get_field(fields_dict, "customfield_13247"))
    incident_type = _extract_string_field(_get_field(fields_dict, "customfield_13248"))
    snow_problem_id = _extract_string_field(_get_field(fields_dict, "customfield_13266"))
    snow_change_id = _extract_string_field(_get_field(fields_dict, "customfield_13267"))
    sla_breached = _extract_string_field(_get_field(fields_dict, "customfield_13337"))
    sre_task_required = _extract_string_field(_get_field(fields_dict, "customfield_13470"))
    detection = _extract_string_field(_get_field(fields_dict, "customfield_13589"))
    effort_spent_by_support_team = _extract_string_field(_get_field(fields_dict, "customfield_13648"))
    update_field = _extract_string_field(_get_field(fields_dict, "customfield_14681"))
    platforms_task_required = _extract_string_field(_get_field(fields_dict, "customfield_16246"))
    engineering_task_required = _extract_string_field(_get_field(fields_dict, "customfield_16247"))

    # Extract array fields (convert to comma-separated strings)
    systemic_problem = _extract_array_field(_get_field(fields_dict, "customfield_13250"))
    never_call = _extract_array_field(_get_field(fields_dict, "customfield_13251"))
    caused_by_change = _extract_array_field(_get_field(fields_dict, "customfield_13268"))
    pending_rca = _extract_array_field(_get_field(fields_dict, "customfield_13285"))
    within_control = _extract_array_field(_get_field(fields_dict, "customfield_13286"))
    application_channel = _extract_array_field(_get_field(fields_dict, "customfield_13295"))
    orm_reporting = _extract_array_field(_get_field(fields_dict, "customfield_13302"))
    impacted_vendor = _extract_array_field(_get_field(fields_dict, "customfield_13340"))
    caused_by_vendor = _extract_array_field(_get_field(fields_dict, "customfield_13377"))
    incident_cause = _extract_array_field(_get_field(fields_dict, "customfield_15078"))

    return TerminusPRMITicket(
        issue_key=issue_key or "",
        summary=summary,
        status=status,
        created_date=created_date,
        priority_id=priority_id,
        priority_name=priority_name,
        query_time=query_time,
        assignee=assignee,
        description=description,
        issuetype=issuetype,
        reporter=reporter,
        impacted_capabilities=impacted_capabilities,
        impacted_platforms=impacted_platforms,
        time_in_status=time_in_status,
        time_to_resolution=time_to_resolution,
        impact_description=impact_description,
        resolution_detail=resolution_detail,
        duration=duration,
        incident_id=incident_id,
        affected_systems=affected_systems,
        incident_start_date=incident_start_date,
        incident_end_date=incident_end_date,
        issue_actual_start=issue_actual_start,
        end_of_issue_resolution=end_of_issue_resolution,
        end_of_customer_impact_recovery=end_of_customer_impact_recovery,
        mi_root_cause=mi_root_cause,
        corrective_action=corrective_action,
        incident_type=incident_type,
        systemic_problem=systemic_problem,
        never_call=never_call,
        snow_problem_id=snow_problem_id,
        snow_change_id=snow_change_id,
        caused_by_change=caused_by_change,
        pending_rca=pending_rca,
        within_control=within_control,
        application_channel=application_channel,
        orm_reporting=orm_reporting,
        sla_breached=sla_breached,
        impacted_vendor=impacted_vendor,
        issue_response_time=issue_response_time,
        caused_by_vendor=caused_by_vendor,
        sre_task_required=sre_task_required,
        issue_reported_date_time=issue_reported_date_time,
        detection=detection,
        effort_spent_by_support_team=effort_spent_by_support_team,
        update_field=update_field,
        banner_start_time=banner_start_time,
        banner_end_time=banner_end_time,
        incident_cause=incident_cause,
        platforms_task_required=platforms_task_required,
        engineering_task_required=engineering_task_required
    )


def transform_prmi_ticket_to_terminus(jira_issue) -> TerminusPRMITicket:
    """
    Transform a Jira PRMI ticket issue to a Terminus PRMI ticket.

    :param jira_issue: The Jira issue object (from PRMITicketResponse)
    :return: TerminusPRMITicket object
    """
    query_time = as_naive_utc(datetime.now())

    # Extract impacted capabilities
    impacted_capabilities = None
    if jira_issue.impacted_capabilities:
        impacted_capabilities = ', '.join(jira_issue.impacted_capabilities)

    # Extract impacted platforms
    impacted_platforms = None
    if jira_issue.impacted_platforms:
        impacted_platforms = ', '.join(jira_issue.impacted_platforms)

    # Parse created date
    created_date = as_naive_utc(parse(jira_issue.created_date))

    # Extract standard fields from fields object
    issue_fields_obj = jira_issue.fields if hasattr(jira_issue, 'fields') else None
    assignee = _extract_string_field(getattr(issue_fields_obj, 'assignee', None))
    description = getattr(issue_fields_obj, 'description', None)
    issuetype = _extract_string_field(getattr(issue_fields_obj, 'issuetype', None))
    reporter = _extract_string_field(getattr(issue_fields_obj, 'reporter', None))

    # Parse datetime fields
    incident_start_date = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13174', None),
        jira_issue.issue_key, 'incident_start_date'
    )
    incident_end_date = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13175', None),
        jira_issue.issue_key, 'incident_end_date'
    )
    issue_actual_start = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13243', None),
        jira_issue.issue_key, 'issue_actual_start'
    )
    end_of_issue_resolution = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13244', None),
        jira_issue.issue_key, 'end_of_issue_resolution'
    )
    end_of_customer_impact_recovery = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13245', None),
        jira_issue.issue_key, 'end_of_customer_impact_recovery'
    )
    issue_response_time = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13376', None),
        jira_issue.issue_key, 'issue_response_time'
    )
    issue_reported_date_time = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_13586', None),
        jira_issue.issue_key, 'issue_reported_date_time'
    )
    banner_start_time = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_14682', None),
        jira_issue.issue_key, 'banner_start_time'
    )
    banner_end_time = _parse_datetime_field(
        getattr(issue_fields_obj, 'customfield_14683', None),
        jira_issue.issue_key, 'banner_end_time'
    )

    # Extract string fields
    time_in_status = _extract_string_field(getattr(issue_fields_obj, 'customfield_10101', None))
    time_to_resolution = _extract_string_field(getattr(issue_fields_obj, 'customfield_11000', None))
    impact_description = _extract_string_field(getattr(issue_fields_obj, 'customfield_12911', None))
    resolution_detail = _extract_string_field(getattr(issue_fields_obj, 'customfield_12974', None))
    duration = _extract_string_field(getattr(issue_fields_obj, 'customfield_13122', None))
    incident_id = _extract_string_field(getattr(issue_fields_obj, 'customfield_13123', None))
    affected_systems = _extract_string_field(getattr(issue_fields_obj, 'customfield_13173', None))
    mi_root_cause = _extract_string_field(getattr(issue_fields_obj, 'customfield_13246', None))
    corrective_action = _extract_string_field(getattr(issue_fields_obj, 'customfield_13247', None))
    incident_type = _extract_string_field(getattr(issue_fields_obj, 'customfield_13248', None))
    snow_problem_id = _extract_string_field(getattr(issue_fields_obj, 'customfield_13266', None))
    snow_change_id = _extract_string_field(getattr(issue_fields_obj, 'customfield_13267', None))
    sla_breached = _extract_string_field(getattr(issue_fields_obj, 'customfield_13337', None))
    sre_task_required = _extract_string_field(getattr(issue_fields_obj, 'customfield_13470', None))
    detection = _extract_string_field(getattr(issue_fields_obj, 'customfield_13589', None))
    effort_spent_by_support_team = _extract_string_field(getattr(issue_fields_obj, 'customfield_13648', None))
    update_field = _extract_string_field(getattr(issue_fields_obj, 'customfield_14681', None))
    platforms_task_required = _extract_string_field(getattr(issue_fields_obj, 'customfield_16246', None))
    engineering_task_required = _extract_string_field(getattr(issue_fields_obj, 'customfield_16247', None))

    # Extract array fields (convert to comma-separated strings)
    systemic_problem = _extract_array_field(getattr(issue_fields_obj, 'customfield_13250', None))
    never_call = _extract_array_field(getattr(issue_fields_obj, 'customfield_13251', None))
    caused_by_change = _extract_array_field(getattr(issue_fields_obj, 'customfield_13268', None))
    pending_rca = _extract_array_field(getattr(issue_fields_obj, 'customfield_13285', None))
    within_control = _extract_array_field(getattr(issue_fields_obj, 'customfield_13286', None))
    application_channel = _extract_array_field(getattr(issue_fields_obj, 'customfield_13295', None))
    orm_reporting = _extract_array_field(getattr(issue_fields_obj, 'customfield_13302', None))
    impacted_vendor = _extract_array_field(getattr(issue_fields_obj, 'customfield_13340', None))
    caused_by_vendor = _extract_array_field(getattr(issue_fields_obj, 'customfield_13377', None))
    incident_cause = _extract_array_field(getattr(issue_fields_obj, 'customfield_15078', None))

    return TerminusPRMITicket(
        issue_key=jira_issue.issue_key,
        summary=jira_issue.summary,
        status=jira_issue.status,
        created_date=created_date,
        priority_id=jira_issue.priority_id,
        priority_name=jira_issue.priority_name,
        query_time=query_time,
        assignee=assignee,
        description=description,
        issuetype=issuetype,
        reporter=reporter,
        impacted_capabilities=impacted_capabilities,
        impacted_platforms=impacted_platforms,
        time_in_status=time_in_status,
        time_to_resolution=time_to_resolution,
        impact_description=impact_description,
        resolution_detail=resolution_detail,
        duration=duration,
        incident_id=incident_id,
        affected_systems=affected_systems,
        incident_start_date=incident_start_date,
        incident_end_date=incident_end_date,
        issue_actual_start=issue_actual_start,
        end_of_issue_resolution=end_of_issue_resolution,
        end_of_customer_impact_recovery=end_of_customer_impact_recovery,
        mi_root_cause=mi_root_cause,
        corrective_action=corrective_action,
        incident_type=incident_type,
        systemic_problem=systemic_problem,
        never_call=never_call,
        snow_problem_id=snow_problem_id,
        snow_change_id=snow_change_id,
        caused_by_change=caused_by_change,
        pending_rca=pending_rca,
        within_control=within_control,
        application_channel=application_channel,
        orm_reporting=orm_reporting,
        sla_breached=sla_breached,
        impacted_vendor=impacted_vendor,
        issue_response_time=issue_response_time,
        caused_by_vendor=caused_by_vendor,
        sre_task_required=sre_task_required,
        issue_reported_date_time=issue_reported_date_time,
        detection=detection,
        effort_spent_by_support_team=effort_spent_by_support_team,
        update_field=update_field,
        banner_start_time=banner_start_time,
        banner_end_time=banner_end_time,
        incident_cause=incident_cause,
        platforms_task_required=platforms_task_required,
        engineering_task_required=engineering_task_required
    )


def extract_prmi_tickets_from_jira_cloud(
    jira_config: Dict[str, str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Union[float, int] = float("Inf"),
    use_created_date: bool = False,
    jql_override: Optional[str] = None
) -> Tuple[List[TerminusPRMITicket], int]:
    """
    This function executes the JQL query for PRMI tickets, extracts the required fields,
    and returns them as a list of TerminusPRMITicket objects.

    :param jira_config: The configuration for Jira Cloud client in Python dictionary format.
    :param start_date: The start date for filtering by Incident End Date (YYYY-MM-DD format).
    :param end_date: The end date for filtering by Incident End Date (YYYY-MM-DD format).
    :param limit: The maximum number of records to return.
    :return: A tuple of (list of PRMI tickets, total count).
    """
    # Create a Jira client for fetching issues.
    jira_client = Jira(**jira_config)

    # Build the JQL query for PRMI tickets
    # For regular DAG runs, use updated within the delta window
    # For one-time full loads, use created date with date range
    if jql_override:
        jql_query = jql_override
    elif use_created_date and start_date and end_date:
        # Use created date for full/historical loads when explicitly requested
        jql_query = f'project = PRMI AND created >= "{start_date}" AND created <= "{end_date}" ORDER BY created DESC'
    elif use_created_date and end_date:
        # If only end_date is provided, get tickets created up to that date
        jql_query = f'project = PRMI AND created <= "{end_date}" ORDER BY created DESC'
    elif start_date and end_date:
        # Regular DAG run: Use updated within the delta window
        jql_query = (
            f'project = PRMI AND updated >= "{start_date}" '
            f'AND updated <= "{end_date}" ORDER BY updated DESC'
        )
    else:
        # Fallback: last 7 days
        jql_query = "project = PRMI AND updated >= -7d ORDER BY updated DESC"

    # Fetch the data from JIRA cloud
    total = 0
    responses: List[PRMITicketResponse] = []
    batch_size = int(limit) if limit < float("inf") else 1000

    # Get the fields we need to extract
    # Standard fields
    issue_fields = [
        'summary',
        'status',
        'created',
        'priority',
        'assignee',
        'attachment',
        'components',
        'description',
        'issuelinks',
        'issuetype',
        'labels',
        'parent',
        'reporter',
    ]
    # Add custom fields explicitly
    issue_fields.extend([
        'customfield_10101',  # [CHART] Time in Status
        'customfield_11000',  # Time to resolution
        'customfield_12911',  # Impact Description
        'customfield_12974',  # Resolution Detail
        'customfield_13122',  # Duration
        'customfield_13123',  # Incident ID
        'customfield_13173',  # Affected Systems
        'customfield_13174',  # Incident Start Date
        'customfield_13175',  # Incident End Date
        'customfield_13243',  # Issue Actual Start
        'customfield_13244',  # End of Issue - No More Work Required (Resolution)
        'customfield_13245',  # End of Customer Impact (Recovery)
        'customfield_13246',  # MI Root Cause
        'customfield_13247',  # Corrective Action
        'customfield_13248',  # Incident Type
        'customfield_13250',  # Systemic Problem
        'customfield_13251',  # NeverCall
        'customfield_13266',  # SNOW Problem ID
        'customfield_13267',  # SNOW Change ID
        'customfield_13268',  # Caused by Change
        'customfield_13285',  # Pending RCA
        'customfield_13286',  # Within Control
        'customfield_13289',  # Impacted Capability
        'customfield_13295',  # Application Channel
        'customfield_13302',  # ORM Reporting
        'customfield_13337',  # SLA Breached
        'customfield_13340',  # Impacted Vendor
        'customfield_13376',  # Issue Response Time
        'customfield_13377',  # Caused by Vendor
        'customfield_13470',  # SRE Task Required
        'customfield_13586',  # Issue Reported Date and Time
        'customfield_13589',  # Detection
        'customfield_13648',  # Effort spent by Support team
        'customfield_14681',  # Update
        'customfield_14682',  # Banner Start Time
        'customfield_14683',  # Banner End Time
        'customfield_15078',  # Incident Cause
        'customfield_15144',  # Impacted Platform
        'customfield_16246',  # Platforms Task Required
        'customfield_16247',  # Engineering Task Required
    ])

    logging.info(f"Extracting issue fields: {str(issue_fields)}")
    logging.info(f"Executing JQL: \n{jql_query}")

    is_last_page = False
    page_size = 100
    jira_data = None
    next_page_token = None
    logging.info(f"batch_size is {batch_size}")

    # Retry configuration for ConnectionError
    max_retries = 5
    retry_count = 0

    while True:
        start_time = time()
        try:
            jira_data = jira_client.enhanced_jql(
                jql=jql_query,
                fields=issue_fields,
                nextPageToken=next_page_token,
                limit=page_size,
                expand="changelog"
            )
            # Reset retry count on successful request
            retry_count = 0
        except ConnectionError as e:
            retry_count += 1
            if retry_count >= max_retries:
                logging.error(f"Max retries ({max_retries}) reached for connection error: {e}")
                raise
            logging.warning(f"Connection Error (attempt {retry_count}/{max_retries}) - {e}, Failed Time - {start_time}, Duration - {time() - start_time:.3f}s")
            sleep(1)
            continue

        # Validate that jira_data contains 'issues' key
        if jira_data is None or 'issues' not in jira_data:
            logging.error(f"Invalid response from Jira: missing 'issues' key. Response: {jira_data}")
            break

        logging.info(f"Extracted {len(jira_data['issues'])} issues from Jira Cloud in {time() - start_time:.3f}s.")
        start_time = time()

        try:
            response = from_dict(PRMITicketResponse, jira_data, config=DaciteConfig(check_types=False))
            responses.append(response)
            issue_count = len(response.issues)
            logging.info(f"Formatted {issue_count} issue records extracted in {time() - start_time:.3f}s.")
        except Exception as e:
            logging.error(f"Error parsing response: {e}")
            logging.error(f"Response data: {jira_data}")
            raw_issues = jira_data.get("issues", [])
            responses.append({"issues": raw_issues})
            issue_count = len(raw_issues)
            logging.info(f"Using raw issues fallback: {issue_count} issue records.")

        total += issue_count
        # Prevent negative page_size and handle edge case where total exceeds limit
        page_size = max(0, min(page_size, limit - total))
        if page_size <= 0:
            logging.info(f"Page size is {page_size}, stopping pagination. Total: {total}, Limit: {limit}")
            break

        is_last_page = jira_data.get("isLast", None)
        if not is_last_page:
            next_page_token = jira_data.get("nextPageToken", None)
        else:
            next_page_token = None

        if total >= limit or is_last_page is True:
            logging.info(f"Total is : {total}, Limit is : {limit}, is_last_page is {is_last_page}")
            break

    if len(responses) == 0:
        return [], total
    last_issues = responses[-1].issues if hasattr(responses[-1], "issues") else responses[-1].get("issues", [])
    if len(last_issues) == 0:
        return [], total

    # Transform issues to Terminus format
    start_time = time()
    all_tickets = []
    for response in responses:
        issues = response.issues if hasattr(response, "issues") else response.get("issues", [])
        for issue in issues:
            try:
                if isinstance(issue, dict):
                    terminus_ticket = transform_prmi_ticket_dict_to_terminus(issue)
                else:
                    terminus_ticket = transform_prmi_ticket_to_terminus(issue)
                all_tickets.append(terminus_ticket)
            except Exception as e:
                issue_key = (
                    issue.get("key") if isinstance(issue, dict)
                    else issue.issue_key if hasattr(issue, "issue_key")
                    else "unknown"
                )
                logging.error(f"Error transforming issue {issue_key}: {e}")
                logging.exception("Full error details:")
                continue

    logging.info(
        f"Transformed {len(all_tickets)} PRMI tickets to Terminus format "
        f"in {time() - start_time:.3f}s."
    )

    return all_tickets, total


def extract_last_successful_dag_run_time() -> Optional[datetime]:
    """
    This function retrieves the time of the last successful DAG run.

    :returns: The time stamp of the last successful DAG run. If there is not a successful
        DAG run before, this function will return None.
    """
    try:
        table_id = AppConfig.LANDING_TABLES.DAG_HISTORY
        client = Client()
        run_history = client.query(f"SELECT MAX(dag_run_time) AS dag_run_time FROM {table_id}") \
            .result() \
            .to_dataframe()
        run_history = run_history \
            .assign(dag_run_time=lambda x: to_datetime(x.dag_run_time, utc=True).astype("datetime64[ns, US/Eastern]"))
        last_run_time = run_history["dag_run_time"].max()
        return None if isna(last_run_time) else last_run_time
    except NotFound:
        return None
