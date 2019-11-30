import unittest
import math
from datetime import datetime, timedelta

import pandas as pd

from autobroker import AutoBroker


def get_sample_data():
    sample_data = pd.read_csv('tests/resources/SampleData.csv', index_col=0)
    sample_data.index = pd.to_datetime(sample_data.index)
    return sample_data


def get_expected_results():
    expected_results = pd.read_csv('tests/resources/ExpectedResults.csv', index_col=0)
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
    expected_results = get_expected_results()
    weekly_data = get_weekly_data(get_sample_data())

    def test_unadjusted_sharpes(self):
        unadjusted_sharpes = AutoBroker.sharpe_ratios(self.weekly_data)

        tolerance = 0.03

        for ticker, sharpe in unadjusted_sharpes.items():
            expected = float(
                self.expected_results.loc['Unadjusted Sharpe', ticker])

            self.assertTrue(
                expected - tolerance <= sharpe <= expected + tolerance,
                msg=(f'{ticker} sharpe: {sharpe} expected: {expected} '
                     f'difference: {abs(sharpe - expected)}'))

    def test_target_share(self):
        AutoBroker.settings = {'max_portfolio_size': 13}
        AutoBroker.historical_data = self.weekly_data
        AutoBroker.sharpe_ratios()
        AutoBroker.target_portfolio()

        target_shares = AutoBroker.portfolio['Target (%)']
        expected_shares = self.expected_results.loc['Target Share']

        tolerance = 1

        for ticker in list(target_shares.index):
            share = float(target_shares[ticker])
            expected = float(expected_shares[ticker].replace('%', ''))

            self.assertTrue(
                expected - tolerance <= share <= expected + tolerance,
                msg=(f'{ticker} share: {share} expected: {expected}'
                     f'difference: {abs(expected - share)}'))


if __name__ == '__main__':
    unittest.main()
