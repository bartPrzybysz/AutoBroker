from typing import Set, Dict, List
from datetime import datetime, timedelta
import logging
import math
import json
import time
import pytz

from ib_insync import *
import pandas as pd


SETTINGS_PATH = 'settings\\settings.json'
TICKERS_PATH = 'settings\\tickers.xlsx'
LOG_DIR = 'log\\'

ib = IB()
settings = dict()
contracts = dict()
historical_data = pd.DataFrame()
portfolio_value = float()
portfolio = pd.DataFrame(columns=[
    'Price', 'Sharpe (unadjusted)', 'Sharpe (adjusted)',
    'Actual (cnt)', 'Actual ($)', 'Actual (%)',
    'Target (cnt)', 'Target ($)', 'Target (%)'
])
sell_orders = list()
buy_orders = list()

# logger
timestr = time.strftime("%Y-%m-%d_%H-%M-%S")
log_path = LOG_DIR + timestr + '.log'
logging.basicConfig(
    filename=log_path,
    filemode='a+',
    level=logging.INFO,
    format='%(asctime)s|%(levelname)s|%(message)s'
)
logging.getLogger().addHandler(logging.StreamHandler())


def load_settings():
    """ Load settings from settings file and store in global settings """

    global settings

    logging.info('Loading settings')

    try:
        with open(SETTINGS_PATH, 'r') as file:
            settings = json.load(file)

    except Exception as e:
        logging.error('Loading settings failed ' + str(e))


def connect():
    """ Connect to TWS with provided credentials """

    logging.info('Connecting to TWS')

    try:
        global ib
        ib = IB()
        ib.connect(settings['TWS_ip'],
                   settings['TWS_port'], settings['TWS_id'])
    except Exception as e:
        logging.error('Connecting to tws failed ' + str(e))


def get_tickers(path: str = TICKERS_PATH) -> Set[str]:
    """
    Get ticker symbols from an excel sheet. Return ticker symbols as a set
    of strings. Also stores Contract objects in global contracts

    path -- path to excel sheet
    """

    sheet_data = pd.read_excel(path, skipna=True, header=None)

    ticker_series = sheet_data.iloc[:, 0].dropna()

    tickers = set(ticker_series)

    # If tickers are not in portfolio, add them
    global portfolio
    missing_tickers = list(tickers - set(portfolio.index))
    portfolio = portfolio.reindex(portfolio.index.union(missing_tickers))

    logging.info(f'Ticker list: {tickers}')

    # Generate all contracts
    global contracts
    for ticker in tickers:
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        contracts[ticker] = contract

    return tickers


def get_historical_data(cont: Dict[str, Contract] = None) -> pd.DataFrame:
    """
    Get weekly historical data for all contracts going back 53 weeks.

    Sore result in DataFrame which is returned and stored in global
    historical_data. The index of this dataframe is a string
    representation of a datetime.date object, the column names are the
    ticker symbols and the data is the close value of the ticker as a
    float.

    cont -- dict of ticker symbols mapped to their contract objects
    """

    if cont is None:
        global contracts
    else:
        contracts = cont

    global historical_data

    logging.info('getting historical data')

    start = time.time()

    data_pull = pd.DataFrame()

    # Get historical data from ib api
    # There's room for optimization here. We need 53 weeks of data but
    # all duration strings greater than a year must be defined in terms
    # of years. A '1 Y' durationStr only gives 52 weeks of data not the
    # needed 53, so we request '2 Y' of data and trim what we don't
    # need. If execution time is ever an issue this could be optimized
    # by requesting first '1 Y' of data then '1 W' of data seperately
    for ticker in contracts.keys():
        bars = ib.reqHistoricalData(
            contract=contracts[ticker],
            endDateTime='',
            durationStr='2 Y',
            barSizeSetting='1 day',
            whatToShow='ADJUSTED_LAST',
            useRTH=True
        )

        for bar in bars:
            data_pull.loc[bar.date, ticker] = bar.close

    end = time.time()
    logging.info(f'Data pull took {end-start} seconds')

    cur_weekday = data_pull.index[-1].weekday()  # last weekday in datapull
    weekday_dates = list(filter(lambda d: d.weekday() ==
                                cur_weekday, data_pull.index))[-53:]
    historical_data = data_pull.loc[weekday_dates]

    for ticker, data in historical_data.items():
        for date, value in data.items():
            while math.isnan(value):
                previous_date = date - timedelta(days=1)
                value = data_pull[ticker][previous_date]
                historical_data[ticker][date] = value

    return historical_data


def sharpe_single(ticker_change: pd.Series, weeks: int = 52) -> float:
    """
    Get the sharpe ratio of a single ticker going over a certain number
    of weeks

    ticker_change -- a Series object containting change percentages
    weeks -- number of weeks to take into account, most recent weeks
             will be uesed
    """

    total_weeks = len(ticker_change.index)
    change = ticker_change[total_weeks-weeks: total_weeks]

    average = change.mean(skipna=True)

    standard_deviation = change.std(skipna=True)

    return average / standard_deviation


def sharpe_ratios(weekly_data: pd.DataFrame = None) -> Dict[str, float]:
    """
    Calculate average sharpe ratio for each ticker.

    Average sharpe ratio is the averege of sharpe ratios calculated over
    52, 26 and 13 weeks. Results will also be stored in global portfolio.
    Return dict of ticker symbols mapped to average sharpe values.

    weekly_data -- A pandas dataframe following the same format as
                   global historical data
    """

    if weekly_data is None:
        global historical_data
        weekly_data = historical_data

    weekly_change = historical_data.pct_change()

    tickers = set(weekly_change.columns)

    sharpes = dict()

    # if tickers are not in portfolio, add them
    global portfolio
    missing_tickers = list(tickers - set(portfolio.index))
    portfolio = portfolio.reindex(portfolio.index.union(missing_tickers))

    for ticker in tickers:
        ticker_change = weekly_change[ticker]

        # Calculate sharpe ratios for ticker
        sharpe_52 = sharpe_single(ticker_change, 52)
        sharpe_26 = sharpe_single(ticker_change, 26)
        sharpe_13 = sharpe_single(ticker_change, 13)

        average = (sharpe_52 + sharpe_26 + sharpe_13) / 3
        adjusted = average ** 1.5 if average > 0.2 else 0

        sharpes[ticker] = average

        # Update portfolio
        portfolio.loc[ticker]['Sharpe (unadjusted)'] = average
        portfolio.loc[ticker]['Sharpe (adjusted)'] = adjusted

    return sharpes


def get_prices(cont: Dict[str, Contract] = None) -> Dict[str, float]:
    """
    Get current price of each ticker. Return set of ticker symbols
    mapped to a float value (USD). Results will also be stored in global
    portfolio.

    symbols -- set of ticker symbols
    """
    if cont is None:
        global contracts
    else:
        contracts = cont

    logging.info('Requesting current ticker prices')

    global portfolio
    prices = dict()

    ib.reqTickers(*list(contracts.values()))

    for symbol, contract in contracts.items():
        ticker = ib.ticker(contract)

        portfolio.loc[symbol, 'Price'] = ticker.close
        prices[symbol] = ticker.close

    return prices


def actual_portfolio():
    """
    Get data on actual positions from TWS, store in portfolio dataframe
    """

    logging.info('Requesting current portfolio details')

    # if account not set, pick active account
    global settings
    account = settings['TWS_account']

    if not account:
        account_value = [v for v in ib.accountValues()
                         if v.tag == 'NetLiquidation'][0]
        account = account_value.account
    else:
        account_value = [v for v in ib.accountValues(account)
                         if v.tag is 'NetLiquidation'][0]

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
        portfolio.loc[ticker]['Actual (cnt)'] = round(count, 2)
        portfolio.loc[ticker]['Price'] = price
        portfolio.loc[ticker]['Actual ($)'] = round(value, 2)
        portfolio.loc[ticker]['Actual (%)'] = (value / portfolio_value) * 100

    # Fill blank values with zeros
    portfolio['Actual (cnt)'].fillna(0, inplace=True)
    portfolio['Actual ($)'].fillna(0, inplace=True)
    portfolio['Actual (%)'].fillna(0, inplace=True)


def target_portfolio():
    """
    Calculate target portfolio, store in portfolio dataframe
    """

    logging.info('Generating target portfolio')

    # Sort portfolio by sharpe ratio
    global portfolio
    portfolio = portfolio.sort_values(by=['Sharpe (unadjusted)'],
                                      ascending=False)

    # Adjust sharpe ratio to zero for tickers that will not be used
    # (only leave max number of top tickers)
    global settings
    max_size = settings['max_portfolio_size']
    portfolio.iloc[max_size:, 2] = 0

    sum_sharpe = portfolio.loc[:, 'Sharpe (adjusted)'].sum()

    global portfolio_value

    # If target is over 25%, store here by how much
    target_excess = 0

    # Populate portfolio dataframe
    for ticker, row in portfolio.iterrows():
        target_percentage = row['Sharpe (adjusted)'] / sum_sharpe
        target_percentage = target_percentage + target_excess

        if target_percentage > 25:
            target_excess = target_percentage - 25
            target_percentage = 25

        target_value = target_percentage * portfolio_value
        if row['Price']:
            target_cnt = target_value / row['Price']
        else:
            logging.error(f'{ticker} excluded from target portfolio')
            target_cnt = None

        portfolio.loc[ticker, 'Target (%)'] = target_percentage * 100
        portfolio.loc[ticker, 'Target ($)'] = target_value
        portfolio.loc[ticker, 'Target (cnt)'] = target_cnt

    logging.info(f'Total portfolio value: {portfolio_value}')
    logging.info('Portfolio:\n' + str(portfolio))


def generate_sell_orders():
    """
    Generate sell orders of type specified by setting
    'primary_sell_type'. Return list of sell orders and store in global
    sell_orders
    """

    logging.info('Generating sell orders')

    global sell_orders

    global settings
    r = settings['round_quantities_to']
    primary_sell_type = settings['primary_sell_type']

    for ticker, row in portfolio.iterrows():

        # Only generate sell order if difference between actual and
        # target portfolio is more than 2%
        if row['Actual (%)'] - row['Target (%)'] > 2:
            contract = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(contract)

            if row['Target (cnt)'] == 0:
                number = row['Actual (cnt)']
            else:
                number = row['Actual (cnt)'] - row['Target (cnt)']
                number = number + (r - (number % r))  # Round up

            # If we want to sell all of the  holdings
            if number > row['Actual (cnt)']:
                number = row['Actual (cnt)']

            order = Order(action='SELL', orderType=primary_sell_type,
                          totalQuantity=int(number))

            sell_orders.append((contract, order))

    return sell_orders


def trades_complete(trades: List[Trade]) -> bool:
    """
    Internal helper function.

    Check if all trades in list are cokmplete. Return true or false

    trades -- list of Trade objects
    """

    for trade in trades:
        if not trade.isDone():
            return False

    return True


def execute_sell_orders():
    """
    Execute all sell orders in global sell_orders. Wait for one of the
    followint conditions:
    1) All trades completed successfully
    2) Time specified by sell_wait_duration settings expires
    3) Current time exceeds sell_wait_until setting

    If all trades completed successfully return list of Trade objects.

    If either time constraint exceeded, cancel all unfulfilled orders
    and resubmit them as an order of the type specified by the
    auxiliary_sell_type setting. Remove cancelled orders from global
    sell_orders and replace them with the new orders. Wait until new
    orders are complete (with no time constraint) and return list of
    Trade objects
    """

    logging.info('Executing sell orders')

    global sell_orders

    global settings
    auxiliary_sell_type = settings['auxiliary_sell_type']
    timezone = pytz.timezone(settings['timezone'])
    sell_wait_duration = settings['sell_wait_duration']
    sell_wait_until = settings['sell_wait_until']

    for order in sell_orders:
        ticker = order[0].symbol
        amount = order[1].totalQuantity
        logging.info(f'selling {amount} shares of {ticker}')

    trades = [ib.placeOrder(*order) for order in sell_orders]

    submit_time = datetime.now(timezone)
    cutoff = submit_time + timedelta(days=1)

    # Modify cutoff time based on settings
    if sell_wait_duration:
        hours, minutes = sell_wait_duration.split(':')
        hours, minutes = int(hours), int(minutes)
        cutoff = submit_time + timedelta(hours=hours, minutes=minutes)

    if sell_wait_until:
        hour, minute = sell_wait_until.split(':')
        hour, minute = int(hour), int(minute)
        specified_time = datetime.now(timezone).replace(hour=hour,
                                                        minute=minute)

        if specified_time < cutoff:
            cutoff = specified_time

    logging.info('Waiting until ' + str(cutoff))

    status = 'WAIT'  # Status can be 'WAIT', 'COMPLETE' or 'REVISE'

    while status == 'WAIT':
        ib.reqAllOpenOrders()

        if trades_complete(trades):
            status = 'COMPLETE'
            break

        if datetime.now(timezone) >= cutoff:
            status = 'REVISE'
            break

        time.sleep(.5)

    # If cutoff time was reached and orders are still incomplete
    if status == 'REVISE':
        incomplete_trades = [t for t in trades if not t.isDone()]

        new_trades = list()

        for trade in incomplete_trades:
            contract = trade.contract
            order = Order(action='SELL', orderType=auxiliary_sell_type,
                          totalQuantity=trade.remaining())

            logging.info(f'resubmitting sell order for {contract.symbol}')

            # Cancel incomplete oreders and remove them from trades
            ib.cancelOrder(trade.order)
            trades.remove(trade)

            # Create new orders of auxiliary sell type
            new_trade = ib.placeOrder(contract, order)
            new_trades.append(new_trade)
            trades.append(new_trade)

        status = 'WAIT'

    # Wait for new orders to complete
    while status == 'WAIT':
        ib.reqAllOpenOrders()

        if trades_complete(new_trades):
            status = 'COMPLETE'
            break

        time.sleep(.5)

    return trades


def generate_buy_orders():
    """
    Generate buy orders of type specified by setting
    'primary_buy_type'. Return list of buy orders and store in global
    buy_orders
    """

    logging.info('Generating buy orders')

    global buy_orders

    global settings
    r = settings['round_quantities_to']
    primary_buy_type = settings['primary_buy_type']

    for ticker, row in portfolio.iterrows():
        if row['Target (%)'] - row['Actual (%)'] > 2:
            contract = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(contract)

            number = row['Target (cnt)'] - row['Actual (cnt)']
            number = number - (number % r)  # Round down

            order = Order(action='BUY', orderType=primary_buy_type,
                          totalQuantity=int(number))

            buy_orders.append((contract, order))

    return buy_orders


def execute_buy_orders():
    """
    Execute all sell orders in global buy_orders. Wait for one of the
    following conditions:
    1) All trades completed successfully
    2) Time specified by buy_wait_duration setting expires
    3) Current time exceeds buy_wait_until setting

    If all trades completed successfully return list of Trade objects.

    If either time constraint exceeded, cancel all unfulfilled orders
    and resubmit them as an order of the type specified by the
    auxiliary_buy_type setting. Remove cancelled orders from global
    buy_orders and replace them with the new orders. Wait until new
    orders are complete (with no time constraint) and return list of
    Trade objects
    """
    global buy_orders

    global settings
    auxiliary_buy_type = settings['auxiliary_buy_type']
    timezone = pytz.timezone(settings['timezone'])
    buy_wait_duration = settings['buy_wait_duration']
    buy_wait_until = settings['buy_wait_until']

    for order in buy_orders:
        ticker = order[0].symbol
        amount = order[1].totalQuantity
        logging.info(f'buying {amount} shares of {ticker}')

    trades = [ib.placeOrder(*order) for order in buy_orders]

    submit_time = datetime.now(timezone)
    cutoff = submit_time + timedelta(days=1)

    if buy_wait_duration:
        hours, minutes = buy_wait_duration.split(':')
        hours, minutes = int(hours), int(minutes)
        cutoff = submit_time + timedelta(hours=hours, minutes=minutes)

    if buy_wait_until:
        hour, minute = buy_wait_until.split(':')
        hour, minute = int(hour), int(minute)
        specified_time = datetime.now(timezone).replace(hour=hour,
                                                        minute=minute)

        if specified_time < cutoff:
            cutoff = specified_time

    logging.info('Waiting until ' + str(cutoff))

    ib.openTrades()

    status = 'WAIT'  # Status can be 'WAIT', 'COMPLETE' or 'REVISE'

    while status == 'WAIT':
        ib.reqAllOpenOrders()

        if trades_complete(trades):
            status = 'COMPLETE'
            break

        if datetime.now(timezone) >= cutoff:
            status = 'REVISE'
            break

        time.sleep(.5)

    if status == 'REVISE':
        incomplete_trades = [t for t in trades if not t.isDone()]

        new_trades = list()

        for trade in incomplete_trades:
            contract = trade.contract
            order = Order(action='BUY', orderType=auxiliary_buy_type,
                          totalQuantity=trade.remaining())

            ib.cancelOrder(trade.order)
            trades.remove(trade)

            logging.info(f'resubmitting sell order for {contract.symbol}')

            new_trade = ib.placeOrder(contract, order)
            new_trades.append(new_trade)
            trades.append(new_trade)

    return trades


def run():
    """ Perform the whole process """

    load_settings()
    connect()
    get_tickers()

    get_historical_data()

    start = time.time()
    sharpe_ratios()
    get_prices()
    actual_portfolio()
    target_portfolio()
    end = time.time()

    logging.info(f'Analyzing data took {end - start} seconds')

    generate_sell_orders()
    execute_sell_orders()
    generate_buy_orders()
    execute_buy_orders()


if __name__ == '__main__':
    run()
