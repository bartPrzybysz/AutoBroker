import json
from ib_insync import *

SETTINGS_PATH = 'settings\settings.json'
TICKERS_PATH = 'settings\\tickers.xlsx'
CACHE_DIR = 'cache'

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
