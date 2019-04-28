import json
from ib_insync import *
import pandas as pd
from typing import Set, Dict

SETTINGS_PATH = 'settings\\settings.json'
TICKERS_PATH = 'settings\\tickers.xlsx'
CACHE_DIR = 'cache'

tickers = set()

try:
    settings = json.load(open(SETTINGS_PATH, 'r'))
except Exception as e:
    print('Loading settigs failed')
    print(e)

try:
    ib = IB()
    ib.connect(settings['TWS_ip'], settings['TWS_port'], settings['TWS_id'])
except Exception as e:
    print('Connecting to TWS failed')
    print(e)


def get_tickers(path : str = TICKERS_PATH) -> Set[str]:
    """
    Get ticker symbols from an excel sheet. Return ticker symbols as a set
    of strings

    path -- path to excel sheet
    """
    sheet_data = pd.read_excel(path, skipna=True)

    ticker_series = sheet_data.iloc[:, 0].dropna()

    global tickers
    tickers = set(ticker_series)

    return tickers