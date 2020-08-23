from stock_database import StockDatabase
from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
import logging

API_PERIOD = 60
API_CALL = 50


class MarketData:
    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        self.db = StockDatabase()
        self.db.create_historic_price_table()
        self.alpha_vantage_api_key = r'U3XAKFVJ5I3WW3YH'
        logging.getLogger().setLevel(logging.INFO)

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
            'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&outputsize=full&apikey=%s'
            % (ticker, self.alpha_vantage_api_key))
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
