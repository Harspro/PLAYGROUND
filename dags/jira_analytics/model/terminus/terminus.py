from dataclasses import fields
from datetime import datetime
from typing import List, Type, Dict, Any

import pendulum

from jira_analytics.config import JiraAnalyticsConfig as Config
from jira_analytics.model.jira.response import IssueResponse, ChangelogResponse
from jira_analytics.model.terminus.changelog_count import ChangelogCount
from jira_analytics.model.terminus.changelog_event import ChangelogEvent
from jira_analytics.model.terminus.issue import Issue
from jira_analytics.utils import as_naive_utc


def _fetch_attr(attrs: List[str], obj) -> Dict[str, Any]:
    d = dict()
    for attr in attrs:
        if not attr.startswith('__'):
            if attr in dir(obj):
                d[attr] = getattr(obj, attr)
    return d


def transform_issue_to_terminus(responses: List[IssueResponse],
                                terminus_class: Type[Issue]) -> (List[Issue], List[ChangelogCount]):
    query_time = as_naive_utc(datetime.now(tz=pendulum.timezone('America/Toronto')))
    all_issues = []
    all_changelog_counts = []
    for response in responses:
        for issue in response.issues:
            # Parse each issue from the response chunk
            all_issues.append(terminus_class(
                query_time=query_time,
                **_fetch_attr([f.name for f in fields(terminus_class)], issue))
            )

            # Parse latest total count of changelog under each issue
            all_changelog_counts.append(ChangelogCount(
                issue_key=issue.issue_key,
                count=issue.changelog.total,
                query_time=query_time)
            )

    return all_issues, all_changelog_counts


def transform_changelog_to_terminus(responses: List[ChangelogResponse],
                                    terminus_class: Type[Issue],
                                    issue_key: str) -> List[ChangelogEvent]:
    changelog_fields = Config.CHANGELOG_FIELDS[terminus_class.__name__]
    query_time = as_naive_utc(datetime.now(tz=pendulum.timezone('America/Toronto')))
    all_events = []
    for response in responses:
        for event in response.values:
            for item in event.items:
                # Match fields that may not have an ID by checking only field and fieldtype
                match_fields = [item.field, item.fieldtype]
                match_fields_with_id = [item.field, item.fieldtype, item.fieldId] if item.fieldId else match_fields

                if match_fields in changelog_fields or match_fields_with_id in changelog_fields:
                    changelog_event_data = {
                        **_fetch_attr(['event_time'], event),
                        **_fetch_attr([f.name for f in fields(ChangelogEvent)], item)
                    }

                    # Handle the case where fieldId might be missing and map to field_id
                    changelog_event_data['field_id'] = getattr(item, 'fieldId', None)

                    all_events.append(ChangelogEvent(
                        query_time=query_time,
                        issue_key=issue_key,
                        **changelog_event_data
                    ))
    return all_events
