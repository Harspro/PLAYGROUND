"""
This module defines the Response and related dataclasses.
"""

from dataclasses import dataclass
from typing import List

from prmi_jira_tickets.model.jira.issue.issue import PRMITicket


@dataclass(frozen=True)
class Response:
    """
    This class defines the data structure received in the response from Jira cloud.
    """
    # Boolean indicating whether this response is the last page
    isLast: bool


@dataclass(frozen=True)
class IssueResponse(Response):
    """
    This class extends the Response class for when the responses are issues.
    """
    # The list of Jira Issues.
    issues: List[PRMITicket]


@dataclass(frozen=True)
class PRMITicketResponse(IssueResponse):
    """
    This class extends the IssueResponse class for when the issues are PRMI tickets.
    """
    # The list of PRMI Tickets.
    issues: List[PRMITicket]
