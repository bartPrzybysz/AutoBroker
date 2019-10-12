import unittest
import math
from datetime import datetime, timedelta

import pandas as pd

import AutoBroker

AutoBroker.logging.info('Unit Testing')


def get_sample_data():
    sample_data = pd.read_csv('test/SampleData.csv', index_col=0)
    sample_data.index = pd.to_datetime(sample_data.index)
    return sample_data


def get_expected_results():
    expected_results = pd.read_csv('test/ExpectedResults.csv', index_col=0)
    return expected_results


def get_weekly_data(daily_data):
    daily_data = daily_data.iloc[::-1]

    cur_weekday = daily_data.index[-1].weekday()  # last weekday in datapull
    weekday_dates = list(filter(lambda d: d.weekday() ==
                                cur_weekday, daily_data.index))[-53:]
    historical_data = daily_data.loc[weekday_dates]

    for ticker, data in historical_data.items():
        for date, value in data.items():
            while math.isnan(value):
                previous_date = date - timedelta(days=1)
                value = daily_data[ticker][previous_date]
                historical_data[ticker][date] = value

    return historical_data


class TestSharpe(unittest.TestCase):
    def test_unadjusted_sharpes(self):
        sample_data = get_sample_data()
        historical_data = get_weekly_data(sample_data)

        unadjusted_sharpes = AutoBroker.sharpe_ratios(historical_data)

        expected_results = get_expected_results()

        tolerance = 0.04

        for ticker, sharpe in unadjusted_sharpes.items():
            expected = float(expected_results.loc['Unadjusted Sharpe', ticker])
            self.assertTrue(
                expected - tolerance <= sharpe <= expected + tolerance,
                msg=(f'{ticker} sharpe: {sharpe} expected: {expected} '
                     f'difference: {abs(sharpe - expected)}'))


if __name__ == '__main__':
    unittest.main()
