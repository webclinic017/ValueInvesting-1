from datetime import datetime
import dateutil.relativedelta
import requests
import logging
import math
import trading_calendars as tc
import pandas as pd
import pytz
from stock_database import StockDatabase
from ratelimit import limits, sleep_and_retry
from api import *

class UlcerIndex:

    def __init__(self):
        self.api_query = alpha_vantage_time_series
        self.stock_db = StockDatabase()
        self.last_date = datetime(2001, 1, 1)
        self.today_date = datetime.now()
        logging.getLogger().setLevel(logging.INFO)

    def set_logging_config(self,version = False):
        if not version:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')

    @sleep_and_retry
    @limits(calls=10, period=60)
    def make_api_call_alpha_vantage(self, ticker):
        """
        API call to get all stocks trading in an exchange
        :param url:
        :return:
        """
        response = requests.get(self.api_query % ticker)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()


    def get_ttm_index(self,ticker):
        try:
            prices = list(self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"].items())
            date_today = self.today_date.strftime("%Y-%m-%d")
            start_index_3_mths = self.get_trading_days_between_two_dates((self.today_date - dateutil.relativedelta.relativedelta(months=3)).strftime("%Y-%m-%d"),date_today)
            start_index_6_mths = self.get_trading_days_between_two_dates((self.today_date - dateutil.relativedelta.relativedelta(months=6)).strftime("%Y-%m-%d"),date_today)
            start_index_12_mths = self.get_trading_days_between_two_dates((self.today_date - dateutil.relativedelta.relativedelta(months=12)).strftime("%Y-%m-%d"),date_today)
            three_mth_index = self.calculate_index(start_index_3_mths,prices)
            six_mth_index = self.calculate_index(start_index_6_mths, prices)
            tweleve_mth_idx = self.calculate_index(start_index_12_mths, prices)
            current_year = int(self.today_date.strftime("%Y"))
            self.stock_db.insert_into_ulcer_table(ticker, three_mth_index, six_mth_index, tweleve_mth_idx, current_year)
            logging.info("Calculated ulcer rindex for %s" % ticker)
        except Exception as e:
            logging.error(e)

    def calculate_index(self,starting_index,list_of_prices):
        days_of_period = 0
        max_value = 0
        sumSq = 0.0
        while starting_index >= 0:
            price_obj = list_of_prices[starting_index]
            price = float(price_obj[1]["5. adjusted close"])
            if price > max_value:
                max_value = price
            else:
                ulcer = ((price / max_value) - 1.0) * 100
                sumSq = sumSq + (ulcer * ulcer)

            starting_index -= 1
            days_of_period += 1
        return math.sqrt(sumSq / days_of_period)



    def one_year_ulcer_index(self,ticker):
        try:
            days_of_period = 1
            prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
            list_of_prices = list(prices.items())
            max_value = 0
            sumSq = 0.0
            start_year = datetime(2007,1,1)
            trading_days_index = self.get_starting_index(list(prices.keys()), start_year)
            current_year = self.get_current_year(list(prices.keys()),start_year)
            last_trading_date_of_year = self.get_last_trading_date_of_year(current_year)
            while trading_days_index >= 0:
                price_obj = list_of_prices[trading_days_index]
                price = float(price_obj[1]["5. adjusted close"])
                if price > max_value:
                    max_value = price
                else:
                    ulcer = ((price / max_value) -1.0) * 100
                    sumSq = sumSq + (ulcer * ulcer)

                current_date = datetime.strptime(price_obj[0], "%Y-%m-%d").date()

                if current_date == last_trading_date_of_year:
                    ulcer_index = math.sqrt(sumSq / days_of_period)
                    self.stock_db.insert_into_ulcer_table(ticker,ulcer_index,current_year)
                    logging.info("Ulcer index:%s\nTicker:%s\nYear:%s\n" % (str(ulcer_index), ticker, str(current_year)))
                    current_year += 1
                    last_trading_date_of_year = self.get_last_trading_date_of_year(current_year)
                    sumSq = 0
                    max_value = 0
                    days_of_period = 0

                if trading_days_index != 0:
                    days_of_period += 1
                trading_days_index -= 1

            ulcer_index = math.sqrt(sumSq / days_of_period)
            self.stock_db.insert_into_ulcer_table(ticker, ulcer_index, current_year)
            logging.info("Ulcer index:%s\nTicker:%s\nYear:%s\n" % (str(ulcer_index),ticker,str(current_year)))
        except Exception as e:
            logging.error(e.__str__())

    def get_last_trading_date_of_year(self,current_year):
        xnys = tc.get_calendar("XNYS")
        start_year = datetime(current_year,1,1).strftime("%Y-%m-%d")
        end_year = datetime(current_year,12,31).strftime("%Y-%m-%d")
        pns = xnys.sessions_in_range(pd.Timestamp(start_year, tz=pytz.UTC), pd.Timestamp(end_year, tz=pytz.UTC))
        return datetime.fromisoformat(str(pns[pns.size -1])).date()

    def get_current_year(self,list_of_dates,start_date):
        ticker_start_dates = datetime.strptime(list_of_dates[len(list_of_dates) - 1], "%Y-%m-%d")
        if ticker_start_dates < start_date:
           return start_date.year
        else:
           return  ticker_start_dates.year

    def get_starting_index(self,list_of_dates,start_date):
        xnys = tc.get_calendar("XNYS")
        end_year = datetime.strptime(list_of_dates[0], "%Y-%m-%d")
        ticker_start_dates = datetime.strptime(list_of_dates[len(list_of_dates) - 1], "%Y-%m-%d")
        if ticker_start_dates < start_date:
            pns = xnys.sessions_in_range(pd.Timestamp(start_date.strftime("%Y-%m-%d"), tz=pytz.UTC), pd.Timestamp(end_year.strftime("%Y-%m-%d"), tz=pytz.UTC))
        else:
            ticker_start_dates = ticker_start_dates.strftime("%Y-%m-%d")
            pns = xnys.sessions_in_range(pd.Timestamp(ticker_start_dates, tz=pytz.UTC), pd.Timestamp(end_year.strftime("%Y-%m-%d"), tz=pytz.UTC))

        return pns.size - 1

    def get_trading_days_between_two_dates(self, start_date, end_date):
        xnys = tc.get_calendar("XNYS")
        pns = xnys.sessions_in_range(pd.Timestamp(start_date, tz=pytz.UTC),
                                     pd.Timestamp(end_date, tz=pytz.UTC))
        return pns.size -1

#
# market_data = UlcerIndex()
# market_data.get_ttm_index("AAPL")