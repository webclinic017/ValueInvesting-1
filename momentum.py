from datetime import datetime
import datetime
import dateutil.relativedelta
import requests
import logging
import pytz
import collections
import pandas as pd
import trading_calendars as tc
from stock_database import StockDatabase
from ratelimit import limits, RateLimitException, sleep_and_retry
from api import *


class Momentum:

    def __init__(self):
        self.today_date = self.set_working_date()
        self.api_query = alpha_vantage_api_query
        self.stock_db = StockDatabase()
        self.last_date = datetime.datetime(2001, 1, 1)
        logging.getLogger().setLevel(logging.INFO)

    def set_logging_config(self,version = False):
        if not version:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')

    def set_working_date(self):
        today_date = datetime.datetime.today().weekday() + 1
        if today_date > 5:
            delta = 7 - today_date
            return datetime.datetime.today() - dateutil.relativedelta.relativedelta(days=delta)
        else:
            return datetime.datetime.today()

    def get_6_month_return(self, stock):
        prices = self.make_api_call_alpha_vantage(stock)["Time Series (Daily)"]
        date_6_months_ago = (self.today_date - dateutil.relativedelta.relativedelta(months=6))
        date_now = self.get_valid_date(self.today_date, prices)
        date_six_mth = self.get_valid_date(date_6_months_ago, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_six_mths = float(prices[date_six_mth]["5. adjusted close"])
        return ((current_price / price_six_mths) - 1) * 100

    def get_12_month_return(self, stock):
        prices = self.make_api_call_alpha_vantage(stock)
        date_12_months_ago = (self.today_date - dateutil.relativedelta.relativedelta(months=12))
        date_now = self.get_valid_date(self.today_date, prices)
        date_twelve_mth = self.get_valid_date(date_12_months_ago, prices)
        current_price = float(prices["Time Series (Daily)"][date_now]["5. adjusted close"])
        price_twelve_mths = float(prices["Time Series (Daily)"][date_twelve_mth]["5. adjusted close"])
        return ((current_price / price_twelve_mths) - 1) * 100

    def get_3_month_return(self, ticker):
        prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
        date_3_months_ago = (self.today_date - dateutil.relativedelta.relativedelta(months=3))
        date_now = self.get_valid_date(self.today_date, prices)
        date_3_mth = self.get_valid_date(date_3_months_ago, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_3_mths = float(prices[date_3_mth]["5. adjusted close"])
        return ((current_price / price_3_mths) - 1) * 100

    def annual_get_3_month_return(self,year,prices):
        first_date_of_year = datetime.datetime(year, 1, 1)
        date_3_months_ago = self.get_valid_date(first_date_of_year - dateutil.relativedelta.relativedelta(months=3),prices)
        date_now = self.get_valid_date(first_date_of_year, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_3_mths = float(prices[date_3_months_ago]["5. adjusted close"])
        return ((current_price / price_3_mths) - 1) * 100

    def annual_get_6_month_return(self,year,prices):
        first_date_of_year = datetime.datetime(year, 1, 1)
        date_6_months_ago = self.get_valid_date(first_date_of_year - dateutil.relativedelta.relativedelta(months=6),prices)
        date_now = self.get_valid_date(first_date_of_year, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_6_mths = float(prices[date_6_months_ago]["5. adjusted close"])
        return ((current_price / price_6_mths) - 1) * 100

    def annual_get_total_return(self,year,prices):
        first_date_of_year = datetime.datetime(year, 1, 1)
        next_year = datetime.datetime(year,12,31)
        price_12_mths_ago =self.annual_get_valid_date(first_date_of_year,prices)
        if next_year >= self.last_date:
            date_now = self.last_date.strftime('%Y-%m-%d')
        else:
            date_now = self.annual_get_valid_date(next_year, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_12_mths = float(prices[price_12_mths_ago]["5. adjusted close"])
        return ((current_price  - price_12_mths) / price_12_mths) * 100

    def store_annual_price_momentum(self, ticker):
        logging.info("Populating price momentum for %s" % ticker)
        prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
        limit = 2001
        start_year = int(list(prices.keys())[0][:4])
        last_price = int(list(prices.keys())[-1][:4])
        self.last_date = datetime.datetime.strptime(list(prices.keys())[-1], '%Y-%m-%d')
        if limit < last_price:
            limit = last_price
        try:
            while start_year > limit:
                three_month_momemtum = self.annual_get_3_month_return(start_year,prices)
                six_month_momemtum = self.annual_get_6_month_return(start_year,prices)
                statement =(start_year,ticker,three_month_momemtum,six_month_momemtum)
                self.stock_db.insert_price_momemtum(statement,True)
                logging.info("Populated for year %s" % str(start_year))
                start_year -= 1
        except Exception as e:
            logging.info(e.__str__())

    def calculate_current_momemtum(self,ticker):
        try:
            price_6_months = self.get_6_month_return(ticker)
            price_3_month = self.get_3_month_return(ticker)
            statement = (ticker, price_3_month, price_6_months)
            self.stock_db.insert_price_momemtum(statement)
        except Exception  as e :
            logging.error(e.__str__())


    def store_stock_return(self,ticker,country):
        prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
        limit = 2003
        last_date  = int(list(prices.keys())[0][:4])
        start_year = int(list(prices.keys())[-1][:4])
        self.last_date = datetime.datetime.strptime(list(prices.keys())[0], '%Y-%m-%d')
        if start_year > limit:
            limit = start_year

        while limit <= last_date:
            try:
                ror =  self.annual_get_total_return(limit,prices)
                statement = (ticker,limit,ror,country)
                self.stock_db.insert_into_stock_return_table(statement)
                logging.info("Populated\nticker:%s\nRate of Return:%s\nYear:%s" % (ticker,str(ror),str(limit)))
                limit +=1
            except Exception as e:
                logging.info(e.__str__())

    def store_stock_return_per_year(self,ticker,country,year):
        prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
        limit = year
        last_date  = int(list(prices.keys())[0][:4])
        start_year = int(list(prices.keys())[-1][:4])
        self.last_date = datetime.datetime.strptime(list(prices.keys())[0], '%Y-%m-%d')
        if start_year > limit:
            limit = start_year

        while limit <= last_date:
            try:
                ror =  self.annual_get_total_return(limit,prices)
                statement = (ticker,limit,ror,country)
                self.stock_db.insert_into_stock_return_table(statement)
                logging.info("Populated\nticker:%s\nRate of Return:%s\nYear:%s" % (ticker,str(ror),str(limit)))
                limit +=1
            except Exception as e:
                logging.info(e.__str__())


    def calculate_mtd_return(self,ticker):
        start_date_of_month = datetime.datetime.today().replace(day=1).strftime("%Y-%m-%d")
        today_date = datetime.datetime.today().strftime("%Y-%m-%d")
        dates = self.get_trading_days_between_two_dates(start_date_of_month, today_date)
        for date in dates:
            print(date)


    def get_trading_days_between_two_dates(self, start_date, end_date):
        xnys = tc.get_calendar("XNYS")
        return xnys.sessions_in_range(pd.Timestamp(start_date, tz=pytz.UTC),
                                     pd.Timestamp(end_date, tz=pytz.UTC))

    def get_valid_date(self, date,prices):
        curr_date = date
        while not self.date_valid(prices, curr_date):
            curr_date = curr_date - dateutil.relativedelta.relativedelta(days=1)
        return curr_date.strftime("%Y-%m-%d")

    def annual_get_valid_date(self, date,prices):
        curr_date = date
        while not self.annual_date_valid(prices, curr_date):
            curr_date = curr_date + dateutil.relativedelta.relativedelta(days=1)
        return curr_date.strftime("%Y-%m-%d")

    def annual_date_valid(self, prices, date):
        str_date = date.strftime("%Y-%m-%d")
        if date > self.last_date:
            raise Exception("index out of bound")
        else:
            if str_date in prices:
                return True
            else:
                return False

    def date_valid(self, prices, date):
        str_date = date.strftime("%Y-%m-%d")
        if date < self.last_date:
            raise Exception("index out of bound")
        else:
            if str_date in prices:
                return True
            else:
                return False

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

#
pm = Momentum()
pm.calculate_mtd_return('SCCO')