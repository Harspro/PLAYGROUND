from typing import Final

from airflow import settings

VENDOR_HOLIDAY_DATES_TABLE_DDL_PATH: Final = \
    f'{settings.DAGS_FOLDER}/vendor_holiday/sql/create_vendor_holiday_dates_table.sql'

VENDOR_HOLIDAY_DATES_CSV_PATH: Final = f'{settings.DAGS_FOLDER}/vendor_holiday/resources/vendor_holiday_dates.csv'
VENDOR_HOLIDAY_DATES_TABLE_NAME: Final = "VENDOR_HOLIDAY_DATES"
HOLIDAY_DATE_COLUMN_NAME: Final = "Holiday_Date"
