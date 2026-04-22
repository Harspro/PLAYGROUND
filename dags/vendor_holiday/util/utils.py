from google.cloud import bigquery

from util.constants import DOMAIN_TECHNICAL_DATASET_ID, GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from util.datetime_utils import current_datetime_toronto
from util.miscutils import read_variable_or_file
from vendor_holiday.util.constants import VENDOR_HOLIDAY_DATES_TABLE_NAME, HOLIDAY_DATE_COLUMN_NAME


def is_today_a_holiday_for_a_vendor(
        vendor: str
) -> bool:
    """
    This function queries the vendor holiday table to check whether today is a holiday or not.

    :param vendor: The name of the vendor to check for in the holiday table.
    :return: A boolean indicating if today is a holiday or not
    """
    return is_holiday(current_datetime_toronto().date(), vendor)


def get_holidays(vendor):
    env = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
    get_vendor_holidays_string = f"""
    SELECT {HOLIDAY_DATE_COLUMN_NAME}
    FROM `pcb-{env}-landing.{DOMAIN_TECHNICAL_DATASET_ID}.{VENDOR_HOLIDAY_DATES_TABLE_NAME}`
    WHERE Vendor = '{vendor}'
    """
    holiday_dates_df = bigquery.Client().query(get_vendor_holidays_string).result().to_dataframe()
    return holiday_dates_df


def is_holiday(date, vendor) -> bool:
    holiday_dates_df = get_holidays(vendor)
    return date in holiday_dates_df[HOLIDAY_DATE_COLUMN_NAME].values
