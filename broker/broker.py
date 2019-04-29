import json
from ib_insync import *
import pandas as pd
from pandas_datareader import data as web
from typing import Set, Dict
from datetime import datetime, timedelta

SETTINGS_PATH = 'settings\\settings.json'
TICKERS_PATH = 'settings\\tickers.xlsx'
CACHE_DIR = 'cache'


# extend json.JSONEncoder to handle pandas dataframes
# Credit: https://stackoverflow.com/questions/33061302/dictionary-of-panda-dataframe-to-json
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)


tickers = set()
historical_data = dict()
portfolio_value = float()
account = str()
portfolio = pd.DataFrame(columns=[
    'Price', 'Sharpe (unadjusted)', 'Sharpe (adjusted)',
    'Actual (cnt)', 'Actual ($)', 'Actual (%)',
    'Target (cnt)', 'Target ($)', 'Target (%)'
])
contracts = dict()
sell_orders = list()
buy_orders = list()

try:
    settings = json.load(open(SETTINGS_PATH, 'r'))
    account = settings['TWS_account']
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

    # if tickers are not in portfolio, add them
    global portfolio
    missing_tickers = list(tickers - set(portfolio.index))
    portfolio = portfolio.reindex(portfolio.index.union(missing_tickers))

    return tickers


def get_historical_data(symbols: Set[str] = None) -> Dict[str, pd.DataFrame]:
    """ 
    Pull weekly historical data for all ticker symbols going back 52 
    weeks from current date.
    Return data in a dict of ticker symbols mapped to pandas DataFrame 
    objects. DataFrames are indexed 1 to 52 with 52 being the most 
    recent week, and contain the following columns: open, high, low, 
    close and volume.

    symbols -- Set of ticker symbols (example: {'TSLA', 'MSFT'})
    """
    if symbols is None:
        global tickers
        symbols = tickers
    
    # Set start_date to Monday of 52 weeks ago, end_date to last friday
    cur_weekday = datetime.now().weekday()
    start_date = datetime.now() - timedelta(weeks=52, days=cur_weekday)
    end_date = datetime.now() - timedelta(days=3+cur_weekday)

    # Dates of monday-friday of each week going back 52 weeks
    week_dates = list()
    for i in range(52):
        monday = datetime.now() - timedelta(weeks=52-i, days=cur_weekday)
        week = tuple((monday + timedelta(days=j)).strftime('%Y-%m-%d')
                     for j in range(5))
        week_dates.append(week)

    # Read cache file
    with open(CACHE_DIR + '/historical_data.json', 'r') as historical_cache:
        cached_data = json.load(historical_cache)

        for ticker, data in cached_data.items():
            cached_data[ticker] = pd.read_json(data)

    # Container for weekly historical data
    global historical_data
    historical_data = {
        ticker: pd.DataFrame(
            columns=['weekof', 'open', 'high', 'low', 'close', 'volume']
        )
        for ticker in symbols
    }

    # if tickers are missing in cached data, get iex data
    missing_tickers = symbols - set(cached_data.keys())
    
    # Pulling data for one ticker is annoyingly slightly different
    # from pulling data for multiple tickers
    if len(missing_tickers) == 1:
        data_pull = web.DataReader(
            list(missing_tickers)[0], 'iex', start_date, end_date)
        
        for i, week in enumerate(week_dates):
            # Get historical data rows for each day of the week 
            # (if there is data for that day)
            week_series = [
                data_pull.loc[day, :]
                for day in week
                if day in data_pull.index
            ]

            # Populate weekly data for each ticker
            for ticker in missing_tickers:
                # Place weekly average for each column indexed by week 
                # number
                historical_data[ticker].loc[i + 1] = [
                    # Week date is first day of week
                    week[0],

                    # Week open is open of first day
                    week_series[0].loc['open'],

                    # Week high is highest high of the week
                    max(day.loc['high'] for day in week_series),

                    # Week low is lowest low of the week
                    min(day.loc['low'] for day in week_series),

                    # Week close is close of last day
                    week_series[-1].loc['close'],

                    # Week volume is last volume of week
                    week_series[-1].loc['volume']
                ]

    elif len(missing_tickers) > 1:
        data_pull = web.DataReader(
            list(missing_tickers), 'iex', start_date, end_date)
    
        for i, week in enumerate(week_dates):
            # Get historical data rows for each day of the week 
            # (if there is data for that day)
            week_series = [
                data_pull.loc[day, :]
                for day in week
                if day in data_pull.index
            ]

            # Populate weekly data for each ticker
            for ticker in missing_tickers:
                # Place weekly average for each column indexed by week 
                # number
                historical_data[ticker].loc[i + 1] = [
                    # Week date is first day of week
                    week[0],

                    # Week open is open of first day
                    week_series[0].loc['open'].loc[ticker],

                    # Week high is highest high of the week
                    max(day.loc['high'].loc[ticker] for day in week_series),

                    # Week low is lowest low of the week
                    min(day.loc['low'].loc[ticker] for day in week_series),

                    # Week close is close of last day
                    week_series[-1].loc['close'].loc[ticker],

                    # Week volume is last volume of week
                    week_series[-1].loc['volume'].loc[ticker]
                ]
    
    # Get historical data from cached data
    for ticker in {t for t, d in historical_data.items() if d.empty}:
        df = cached_data[ticker]

        for i, week in enumerate(week_dates):
            week_from_cache = df.loc[df['weekof'].isin(week)]

            # if there is no data for that week get it from iex
            if week_from_cache.empty:
                data_pull = web.DataReader(ticker, 'iex', week[0], week[-1])

                week_series = [
                    data_pull.loc[day, :]
                    for day in week
                    if day in data_pull.index
                ]

                historical_data[ticker].loc[i + 1] = [
                    # Week date is first day of week
                    week[0],

                    # Week open is open of first day
                    week_series[0].loc['open'],

                    # Week high is highest high of the week
                    max(day.loc['high'] for day in week_series),

                    # Week low is lowest low of the week
                    min(day.loc['low'] for day in week_series),

                    # Week close is close of last day
                    week_series[-1].loc['close'],

                    # Week volume is last volume of week
                    week_series[-1].loc['volume']
                ]
            else:
                # Use cached data
                historical_data[ticker].loc[i + 1] = week_from_cache.iloc[0]

    # Save historical data to cache
    with open(CACHE_DIR + '/historical_data.json', 'w') as historical_cache:
        json.dump(historical_data, historical_cache, cls=JSONEncoder)

    return historical_data


def _sharpe_single(weekly_change: pd.DataFrame, weeks: int = 52) -> float:
    """
    Internal hepler function

    Calculate sharpe ratio of specified data over specifed number of 
    weeks. Return numeric value.

    weekly_change --  pandas DataFrame containing column 'change'
    weeks -- number of weeks to account in sharpe ratio
    """

    # Get change data of weeks in question
    total_weeks = weekly_change.iloc[:, 0].count()
    change = weekly_change.loc[total_weeks-weeks : total_weeks, 'change']

    # Calculate average change
    average = change.mean(skipna=True)

    # Calculate standard deviation
    deviation = change.std(skipna=True)

    return average / deviation


def _weekly_change(weekly_data: Dict[str, pd.DataFrame]) \
        -> Dict[str, pd.DataFrame]:
    """
    Internal helper function

    Calculate week to week change of close values in weekly data.
    Return data in a dict of ticker symbols mapped to pandas DataFrames.
    DataFrames contain close and change columns where close is week 
    close and change is percent change from previous week

    weekly_data -- dict of ticker symbols mapped to pandas DataFrames 
                   which must contain a close column
    """

    # Get ticker symbols
    tickers = set(weekly_data.keys())

    weekly_change = dict()

    for ticker in tickers:
        # Get close column
        close = weekly_data[ticker].loc[:, 'close']
        # Calculate weekly change
        change = close.pct_change()

        # Combine close and change into single data frame
        df = pd.DataFrame(close)
        df.loc[:, 'change'] = change

        # Assign dataframe to ticker symbol
        weekly_change[ticker] = df

    return weekly_change


def _sharpe_single(weekly_change: pd.DataFrame, weeks: int = 52) -> float:
    """
    Internal helper function

    Calculate sharpe ratio of specified data over specifed number of 
    weeks. Return numeric value.

    weekly_change --  pandas DataFrame containing column 'change'
    weeks -- number of weeks to account in sharpe ratio
    """

    # Get change data of weeks in question
    total_weeks = weekly_change.iloc[:, 0].count()
    change = weekly_change.loc[total_weeks-weeks : total_weeks, 'change']

    # Calculate average change
    average = change.mean(skipna=True)

    # Calculate standard deviation
    deviation = change.std(skipna=True)

    return average / deviation


def sharpe_ratios(weekly_data: Dict[str, pd.DataFrame] = None) \
        -> Dict[str, float]:
    """
    Calculate average sharpe ratio for each ticker.
    Average sharpe ratio is the averege of sharpe ratios calculated over 
    52, 26 and 13 weeks.
    Return dict of ticker symbols mapped to average sharpe values.

    weekly_data -- dict of ticker symbols mapped to pandas DataFrames 
                   which must contain a close column
    """

    if weekly_data is None:
        global historical_data
        weekly_data = historical_data
    
    # Get weekly change
    change = _weekly_change(weekly_data)

    # Get ticker symbols
    tickers = set(change.keys())

    sharpes = dict()

    # if tickers are not in portfolio, add them
    global portfolio
    missing_tickers = list(tickers - set(portfolio.index))
    portfolio = portfolio.reindex(portfolio.index.union(missing_tickers))

    for ticker in tickers:
        data = change[ticker]

        # Calculate sharpe ratios for ticker
        sharpe_52 = _sharpe_single(data, 52)
        sharpe_26 = _sharpe_single(data, 26)
        sharpe_13 = _sharpe_single(data, 13)

        average = (sharpe_52 + sharpe_26 + sharpe_13) / 3
        adjusted = average ** 1.5 if average > 0.2 else 0

        sharpes[ticker] = average

        # Update portfolio
        portfolio.loc[ticker]['Sharpe (unadjusted)'] = average
        portfolio.loc[ticker]['Sharpe (adjusted)'] = adjusted

    return sharpes


def get_prices(symbols: Set[str] = None) -> Dict[str, float]:
    """
    Get current price of each ticker. Return set of ticker symbols 
    mapped to a float value (USD).

    symbols -- set of ticker symbols
    """
    if symbols is None:
        global tickers
        symbols = tickers
    
    global portfolio
    prices = dict()

    # Get the latest sell price for each ticker
    prices_pull = web.DataReader(list(symbols), 'iex-last')
    
    for index, row in prices_pull.iterrows():
        portfolio.loc[row['symbol']]['Price'] = row['price']
        prices[row['symbol']] = row['price']
    
    return prices


def actual_portfolio():
    """
    Get data on actual positions from TWS, store in portfolio dataframe
    """

    # if account not set, pick active account
    global account
    if account == 'auto':
        account_value = [v for v in ib.accountValues() 
                        if v.tag == 'NetLiquidation'][0]
        account = account_value.account
    else:
        account_value = [v for v in ib.accountValues(account) 
                        if v.tag == 'NetLiquidation'][0]
    
    global portfolio_value
    portfolio_value = float(account_value.value)

    global contracts
    global portfolio

    for position in ib.positions(account):
        ticker = position.contract.symbol
        count = position.position
        price = position.avgCost
        value = price * count

        if ticker not in portfolio.index:
            portfolio.loc[ticker] = None

        contracts[ticker] = position.contract
        portfolio.loc[ticker]['Actual (cnt)'] = count
        portfolio.loc[ticker]['Price'] = price
        portfolio.loc[ticker]['Actual ($)'] = value
        portfolio.loc[ticker]['Actual (%)'] = (value / portfolio_value) * 100
    
    # Fill blank values with zeros
    portfolio['Actual (cnt)'].fillna(0, inplace=True)
    portfolio['Actual ($)'].fillna(0, inplace=True)
    portfolio['Actual (%)'].fillna(0, inplace=True)


def target_portfolio():
    """
    Calculate target portfolio, store in portfolio dataframe
    """

    # Sort portfolio by sharpe ratio
    global portfolio
    portfolio = portfolio.sort_values(by=['Sharpe (unadjusted)'], 
                                      ascending=False)

    # Adjust sharpe ratio to zero for tickers that will not be used
    # (only leave max number of top tickers)
    global settings
    max_size = settings['max_portfolio_size']
    portfolio.iloc[max_size:, 2] = 0
    
    sum_sharpe = portfolio.loc[:,'Sharpe (adjusted)'].sum()

    global portfolio_value

    # Populate portfolio dataframe
    for ticker, row in portfolio.iterrows():
        target_percentage = row['Sharpe (adjusted)'] / sum_sharpe
        target_value = target_percentage * portfolio_value
        target_cnt = target_value / row['Price']

        portfolio.loc[ticker, 'Target (%)'] = target_percentage * 100
        portfolio.loc[ticker, 'Target ($)'] = target_value
        portfolio.loc[ticker, 'Target (cnt)'] = target_cnt


def generate_sell_orders():
    """
    Doc here
    """
    
    global sell_orders

    for ticker, row in portfolio.iterrows():
        if row['Actual (%)'] - row['Target (%)'] > 2:
            contract = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(contract)
            
            if row['Target (cnt)'] == 0:
                number = row['Actual (cnt)']
            else:
                number = row['Actual (cnt)'] - row['Target (cnt)']
                number = number - (number % 25) # Round down

            order = Order(action='SELL', orderType='MIDPRICE', totalQuantity=int(number))

            sell_orders.append((contract, order))
    
    return sell_orders


def execute_sell_orders():
    global sell_orders

    trades = list()

    for order in sell_orders:
        trades.append(ib.placeOrder(*order))
    
    return trades