import broker.broker as Broker
from time import time 

print('\nAnalyzing Data. This may take a minute.')
start = time()

Broker.get_tickers()
Broker.get_historical_data()
Broker.sharpe_ratios()

end = time()
print(f'Done analyzing data. Process took {end - start} seconds\n')

print('Generating Portfolio\n')

Broker.get_prices()
Broker.actual_portfolio()
Broker.target_portfolio()

print('Total portfolio value: ' + str(Broker.portfolio_value))
Broker.log_file.write('Total portfolio value: ' + str(Broker.portfolio_value) + '\n')
print(Broker.portfolio)
Broker.log_file.write('\n' + str(Broker.portfolio) + '\n\n')

print('\nWorking on sell orders...\n')
Broker.generate_sell_orders()
Broker.execute_sell_orders()

print('Working on buy orders...\n')
Broker.generate_buy_orders()
Broker.execute_buy_orders()

print("Done\n")