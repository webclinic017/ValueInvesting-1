from stock_database import StockDatabase
from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
import logging
from datetime import datetime
from api import *

API_PERIOD = 60
API_CALL = 50


class MarketData:
    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        self.db = StockDatabase()
        self.db.create_historic_price_table()
        logging.getLogger().setLevel(logging.INFO)

    def set_logging_config(self,version = False):
        if not version:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')

    def insert_historic_prices(self, exchange):
        logging.info("Getting historical prices for stocks as per exchange:%s" %exchange)
        list_of_stocks = self.db.get_stocks_per_exchange(exchange)
        for stock in list_of_stocks:
            ticker = stock[2]
            if self.pricing_data_available(ticker):
                logging.info("Pricing data for %s is available. Skipping to next symbol" %ticker)
            else:
                historic_prices = self.make_api_call_alpha_vantage(ticker)
                if "Time Series (Daily)" in historic_prices:
                    list_of_prices = historic_prices["Time Series (Daily)"]
                    for key , value in list_of_prices.items():
                        eod_date = int(key.replace('-',''))
                        open = float(value["1. open"])
                        high = float(value["2. high"])
                        low = float(value["3. low"])
                        close = float(value["4. close"])
                        adjusted_close = float(value["5. adjusted close"])
                        volume = float(value["6. volume"])
                        price = (eod_date,ticker,open,high,low,close,adjusted_close,volume)
                        self.db.insert_historic_price(price)
                else:
                    logging.info("No pricing data for for symbol: %s" %ticker)

    def get_historical_prices(self, ticker, last_published_price_date):
        limit = last_published_price_date[0:4] + '-' + last_published_price_date[4:6] + '-' + last_published_price_date[6:8]
        historic_prices = self.make_api_call_alpha_vantage(ticker)
        if "Time Series (Daily)" in historic_prices:
            list_of_prices = historic_prices["Time Series (Daily)"]
            for key, value in list_of_prices.items():
                if key == limit:
                    logging.info("Price exist for ticker for %s on %s" % (ticker, key))
                    break
                else:
                    eod_date = int(key.replace('-', ''))
                    open = float(value["1. open"])
                    high = float(value["2. high"])
                    low = float(value["3. low"])
                    close = float(value["4. close"])
                    adjusted_close = float(value["5. adjusted close"])
                    volume = float(value["6. volume"])
                    price = (eod_date, ticker, open, high, low, close, adjusted_close, volume)
                    self.db.insert_historic_price(price)


    def get_historical_prices_v2(self, ticker):
        limit = datetime.strptime('2010-01-01','%Y-%m-%d')
        historic_prices = self.make_api_call_alpha_vantage(ticker)
        if "Time Series (Daily)" in historic_prices:
            list_of_prices = historic_prices["Time Series (Daily)"]
            for key, value in list_of_prices.items():
                date = datetime.strptime(key,'%Y-%m-%d')
                if date <= limit:
                    break
                else:
                    eod_date = int(key.replace('-', ''))
                    open = float(value["1. open"])
                    high = float(value["2. high"])
                    low = float(value["3. low"])
                    close = float(value["4. close"])
                    adjusted_close = float(value["5. adjusted close"])
                    volume = float(value["6. volume"])
                    price = (eod_date, ticker, open, high, low, close, adjusted_close, volume)
                    self.db.insert_historic_price(price)

    @sleep_and_retry
    @limits(calls=30, period=60)
    def make_api_call_alpha_vantage(self, ticker):
        """
        API call to get all stocks trading in an exchange
        :param url:
        :return:
        """
        logging.info("API call for symbol:%s" % ticker)
        response = requests.get(
            alpha_vantage_time_series
            % (ticker))
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    def pricing_data_available(self,ticker):
        prices = self.db.get_historical_prices_per_ticker(ticker)
        if len(prices) > 0:
            return True
        else:
            return False


# market_date = MarketData()
# market_date.insert_historic_prices("US")
