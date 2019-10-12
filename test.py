import unittest

import pandas as pd

import AutoBroker


def get_sample_data():
    return pd.read_csv('test/SampleData.csv', index_col=0, delimiter='\t')


class TestSharpe(unittest.TestCase):
    def test_unadjusted_sharpes(self):
        sample_data = pd.read_csv(
            'test/SampleData.csv', index_col=0, delimiter='\t')
        self.assertIsNotNone(sample_data)


if __name__ == '__main__':
    unittest.main()
