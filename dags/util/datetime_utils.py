from datetime import datetime
import pytz

DATE_PATTERN_TZ_NAME = '%Y-%m-%dT%H:%M:%S-%f %Z'

TORONTO_TZ = pytz.timezone('America/Toronto')


def current_datetime_toronto():
    """ Current datetime in Toronto"""
    return datetime.now(tz=TORONTO_TZ)


def current_datetime_toronto_str():
    """ Current datetime in Toronto"""
    return datetime.now(tz=TORONTO_TZ).strftime(DATE_PATTERN_TZ_NAME)
