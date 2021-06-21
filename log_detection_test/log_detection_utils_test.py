from log_detection_utils import utils
from unittest import TestCase, main
from datetime import datetime


class UtilsTestCase(TestCase):
    def test_get_date_str(self):
        good_cases = [
            ('2021', (datetime.strptime('2021', '%Y'), '%Y')),
            ('2021-03-05', (datetime.strptime('2021-03-05', '%Y-%m-%d'), '%Y-%m-%d'))
        ]
        for expected, (date_time, date_format) in good_cases:
            with self.subTest((date_time, date_format)):
                self.assertEqual(expected, utils.get_datetime_to_str(date_time=date_time, time_format=date_format))


if __name__ == '__main__':
    main()
