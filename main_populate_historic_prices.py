from backtest import Backtest

backtest = Backtest()
backtest.populate_historic_prices()
backtest.calculate_stock_return()