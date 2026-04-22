"""
Utility functions for PRMI Jira Tickets DAG.
"""

from datetime import datetime, timezone


def as_naive_utc(dt: datetime) -> datetime:
    """
    Convert a datetime to naive UTC datetime.

    :param dt: The datetime to convert
    :return: Naive UTC datetime
    """
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)
