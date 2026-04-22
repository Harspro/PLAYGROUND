"""
This class defines methods related to validating whether a file exists for file loading pathways.
"""
import logging
from airflow.exceptions import AirflowException
from datetime import timedelta
from data_validation.base_validator import BaseValidator
from util.constants import GCP_CONFIG, SOURCE_BUCKET, DEPLOY_ENV_STORAGE_SUFFIX, FOLDER_NAME, FILE_PREFIX, DAYS_DELTA, HOURS_DELTA, HOLIDAY_CHECK_DELTA
from util.datetime_utils import current_datetime_toronto
from util.gcs_utils import list_blobs_with_prefix
from util.miscutils import read_variable_or_file
from data_validation.utils.utils import check_holiday


class FileValidator(BaseValidator):
    def check_file_exist(self, config, prefix_date, source_bucket, hourly_check: bool = False) -> int:
        if hourly_check:
            file_prefix_search_str = f"{config.get(FOLDER_NAME)}/" \
                                     f"{config.get(FILE_PREFIX)}{prefix_date.strftime('%Y%m%d%H')}"
        else:
            file_prefix_search_str = f"{config.get(FOLDER_NAME)}/" \
                                     f"{config.get(FILE_PREFIX)}{prefix_date.strftime('%Y%m%d')}"
        logging.info(f"file prefix checked : {file_prefix_search_str}")
        items = list_blobs_with_prefix(bucket_name=source_bucket, prefix=file_prefix_search_str)
        file_count = len(items)
        logging.info(f"number of files returned : {file_count}")

        return file_count

    def validate(
            self,
            config: dict
    ) -> None:
        """
        This function checks whether the file with the file_prefix exists in the bucket provided in the config. If the
        file does not exist, it checks whether today is a holiday for the vendor or not. If it is a holiday, then no
        file is expected and the function returns. If it isn't then an exception is raised. If the file does exist then
        the function also returns. Multiple files with the same prefix (or today's date) also raises an exception.

        :param config: The config for this job in the YAML file.
        """
        prefix_date = current_datetime_toronto()
        days_delta = config.get(DAYS_DELTA)
        hours_delta = config.get(HOURS_DELTA)
        total_count = 0
        hourly_check = False
        deploy_env_storage_suffix = read_variable_or_file(GCP_CONFIG).get(DEPLOY_ENV_STORAGE_SUFFIX)
        source_bucket = f"{config.get(SOURCE_BUCKET)}{deploy_env_storage_suffix}"
        check_schedules = []

        if days_delta:
            for i in range(0, days_delta + 1):  # including current date
                check_schedules.append(prefix_date - timedelta(days=i))
        elif hours_delta:
            hourly_check = True
            for i in range(1, hours_delta + 1):  # excluding current_hour
                check_schedules.append(prefix_date - timedelta(hours=i))
        else:
            check_schedules.append(prefix_date)

        # File check
        for schedule in check_schedules:
            total_count += self.check_file_exist(config, schedule, source_bucket, hourly_check=hourly_check)

        if total_count == 0:
            vendor = config.get('vendor')
            holiday_check_delta = config.get(HOLIDAY_CHECK_DELTA)
            is_holiday = check_holiday(vendor, prefix_date, hours_delta, holiday_check_delta)
            if is_holiday:
                logging.info("No file found due to vendor holiday")
                return
            else:
                raise AirflowException(" [WARNING] No file found today and no vendor holiday.")

        # When days_delta or hours_delta is set, multiple files are expected (one per day/hour checked)
        # Only fail on multiple files if checking a single date/hour
        expected_file_count = len(check_schedules)
        logging.info(f"Total files found: {total_count}, Expected file count (based on schedules checked): {expected_file_count}")

        if not days_delta and not hours_delta and total_count > 1:
            raise AirflowException(f" [WARNING] Multiple files found: file count is {total_count}")
        elif (days_delta or hours_delta) and total_count > expected_file_count:
            raise AirflowException(f" [WARNING] Too many files found: file count is {total_count}, expected at most {expected_file_count}")

        logging.info("File validation passed.")
