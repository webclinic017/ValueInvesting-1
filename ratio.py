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


class Ratio:

    def __init__(self):
        self.f_key_metric_api = f_key_metric_api
        self.f_balance_sheet_api =  f_balance_sheet_api
        self.f_income_statement_api = f_income_statement_api
        self.f_cashflow_api =  f_cashflow_api
        self.annual_ratio = f_annual_ratio
        self.ttm_ratio = f_ttm_ratio
        self.stock_db = StockDatabase()
        logging.getLogger().setLevel(logging.INFO)

    def get_annual_ratio(self,ticker):
        try:
            result =  self.make_api_call_alpha_vantage(self.annual_ratio % ticker)
            if len(result) < 10:
                index = len(result)
            else:
                index = 10
            i = 0
            while(i < index):
                year = datetime.strptime(result[i]['date'],'%Y-%m-%d').year
                roe = result[i]["returnOnEquity"]
                fair_value = result[i]["priceFairValue"]
                div_yield = result[i]["dividendYield"]
                fcf_per_share = result[i]["freeCashFlowPerShare"]
                self.stock_db.insert_into_ratio(ticker, roe, fair_value, div_yield, fcf_per_share, year)
                i += 1
                logging.info("Inserted Ratio for %s at year %s" % (ticker,year))
            self.get_ttm_ratio(ticker)
        except Exception as e:
            logging.info(e.__str__())

    def get_ttm_ratio(self,ticker):
        try:
            result =  self.make_api_call_alpha_vantage(self.ttm_ratio % ticker)
            year = datetime.now().year
            roe = result[0]["returnOnEquityTTM"]
            fair_value = result[0]["priceFairValueTTM"]
            div_yield = result[0]["dividendYieldTTM"]
            fcf_per_share = result[0]["freeCashFlowPerShareTTM"]
            self.stock_db.insert_into_ratio(ticker,roe,fair_value,div_yield,fcf_per_share,year)
            logging.info("Inserted ttm Ratio for %s" % (ticker))
        except Exception as e:
            logging.info(e.__str__())


    @sleep_and_retry
    @limits(calls=10, period=60)
    def make_api_call_alpha_vantage(self, request):
        """
        API call to get all stocks trading in an exchange
        :param url:
        :return:
        """
        response = requests.get(request)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()


# peg = Ratio()
# list_of_stocks = peg.stock_db.get_stocks_per_exchange('US', False)
# for ticker in list_of_stocks:
#     try:
#         peg.get_annual_ratio(ticker[2])
#     except Exception as e:
#         logging.info(e.__str__())
