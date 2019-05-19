import broker.broker as Broker

Broker.get_tickers()
Broker.get_historical_data()
Broker.sharpe_ratios()
Broker.get_prices()
Broker.actual_portfolio()
Broker.target_portfolio()
Broker.generate_sell_orders()
Broker.execute_sell_orders()
Broker.generate_buy_orders()
Broker.execute_buy_orders()

print(Broker.portfolio)

print("yeet")