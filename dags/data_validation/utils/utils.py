import logging
from datetime import timedelta
from util.constants import SCHEDULE_INTERVAL
from vendor_holiday.util.utils import is_holiday


def get_schedule_interval(deploy_env: str, config: dict):
    """
    This function gets the schedule interval from the config. The user may or may not provide a schedule
    interval, and therefore this case is accounted for in the code. Furthermore, we have the ability to provide
    different schedules based on the deployment environment ('dev', 'uat', or 'prod').

    :param config: The config for this job in the YAML file.
    :deploy_env: The environment for this job.
    """
    schedule_interval = config.get(SCHEDULE_INTERVAL, None)
    if schedule_interval:
        schedule_interval = schedule_interval.get(deploy_env, None)
    return schedule_interval


def check_holiday(vendor, prefix_date, hours_delta, holiday_check_delta=0) -> bool:
    date = prefix_date.date()
    if vendor is not None:
        if holiday_check_delta:
            date -= timedelta(days=holiday_check_delta)

        if hours_delta:
            last_checked_date = prefix_date.date()
            for i in range(1, hours_delta + 1):
                prev_hour = prefix_date - timedelta(hours=i)
                prev_date = prev_hour.date()

                # Check if new date due to time subtraction overlaps into the previous day
                if prev_date != last_checked_date:
                    last_checked_date = prev_date
                    if is_holiday(prev_date, vendor):
                        logging.info(f"{prev_date} was a holiday for vendor {vendor}. No file is expected.")
                        return True
                    else:
                        logging.info(
                            f"File expected but not present and {prev_date} is not a holiday for vendor {vendor}.")
                        return False

        # Check if the current date is a holiday
        if is_holiday(date, vendor):
            logging.info(f"{date} is a holiday for vendor {vendor}. No file is expected.")
            return True

    return False
