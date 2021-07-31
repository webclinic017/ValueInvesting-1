
import logging
from backtest import Backtest

logging.getLogger().setLevel(logging.INFO)



def main(country):
    backtest = Backtest()
    list_of_stocks = backtest.stock_db.get("""SELECT *  FROM COMPANyinfo where EXCHANGE = '%s'""" % country)
    for stock in list_of_stocks:
        backtest.run_ulcer_index(stock[2])

main('US')