import unittest

from bizdays import Calendar


def test_new_holiday_brazil():
    cal = Calendar.load('B3')
    assert cal.isbizday('2022-11-20') == False
    assert cal.isbizday('2024-11-20') == False
    assert cal.isbizday('2025-11-20') == False


if __name__ == '__main__':
    unittest.main(verbosity=1)
