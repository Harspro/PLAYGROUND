import unittest
from unittest.mock import patch
from datetime import datetime
from data_validation.utils.utils import get_schedule_interval, check_holiday
from util.constants import SCHEDULE_INTERVAL


class TestUtils(unittest.TestCase):
    def test_get_schedule_interval_with_provided_interval(self):
        config = {
            SCHEDULE_INTERVAL: {
                'dev': '0 12 * * *',
                'uat': '0 13 * * *',
                'prod': '0 14 * * *'
            }
        }
        deploy_env = 'dev'

        result = get_schedule_interval(deploy_env, config)
        self.assertEqual(result, '0 12 * * *')

    def test_get_schedule_interval_without_provided_interval(self):
        config = {}
        deploy_env = 'dev'

        result = get_schedule_interval(deploy_env, config)
        self.assertIsNone(result)

    @patch('data_validation.utils.utils.is_holiday')
    def test_check_holiday_vendor_holiday_today(self, mock_is_holiday):
        mock_is_holiday.return_value = True  # Mocking today as a holiday

        vendor = 'test_vendor'
        prefix_date = datetime(2024, 1, 1)
        hours_delta = 0

        result = check_holiday(vendor, prefix_date, hours_delta)
        self.assertTrue(result)

    @patch('data_validation.utils.utils.is_holiday')
    def test_check_holiday_no_holiday(self, mock_is_holiday):
        mock_is_holiday.return_value = False

        vendor = 'test_vendor'
        prefix_date = datetime(2024, 1, 1)
        hours_delta = 0

        result = check_holiday(vendor, prefix_date, hours_delta)
        self.assertFalse(result)

    @patch('data_validation.utils.utils.is_holiday')
    def test_check_holiday_with_hours_delta_causing_date_change(self, mock_is_holiday):
        def test_is_holiday(date, vendor):
            if date == datetime(2023, 12, 31).date():
                return True
            return False

        mock_is_holiday.side_effect = test_is_holiday

        vendor = 'test_vendor'
        prefix_date = datetime(2024, 1, 1, 1, 0)  # 1 AM on January 1, 2024
        hours_delta = 3  # Check 3 hours ago, changes date to Dec 31, 2023

        result = check_holiday(vendor, prefix_date, hours_delta)
        self.assertTrue(result)

    @patch('data_validation.utils.utils.is_holiday')
    def test_check_holiday_with_hours_delta_causing_date_change_today_holiday(self, mock_is_holiday):
        # Today is holiday, but previous day is not holiday, so if file does not exist should raise exception
        def test_is_holiday(date, vendor):
            if date == datetime(2024, 1, 1).date():
                return True
            return False

        mock_is_holiday.side_effect = test_is_holiday

        vendor = 'test_vendor'
        prefix_date = datetime(2024, 1, 1, 1, 0)  # 1 AM on January 1, 2024
        hours_delta = 3  # Check 3 hours ago, changes date to Dec 31, 2023

        result = check_holiday(vendor, prefix_date, hours_delta)
        self.assertFalse(result)
