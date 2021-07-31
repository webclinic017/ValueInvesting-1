import logging
import sys
from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
from  stock_database import StockDatabase
import logging
from api import *

class Zscore :

    def __init__(self):
        self.balance_sheet_api = alpha_vantage_balance_sheet_api
        self.income_statement = alpha_vantage_income_statement
        self.cashflow = alpha_vantage_cashflow
        self.company_overview = alpha_vantage_company_overview
        self.f_key_metric_api =  f_key_metric_api
        self.f_balance_sheet_api = f_balance_sheet_api
        self.f_income_statement_api = f_income_statement_api
        self.f_cashflow_api = f_cashflow_api
        self.stock_db = StockDatabase()
        self.manufacturing_cofficient = [1.2,1.4,3.3,0.6,1.0]
        self.non_manufacturing_cofficient = [6.56,3.26,6.72,1.05]
        logging.getLogger().setLevel(logging.INFO)

    def set_logging_config(self, version=False):
        if not version:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')

    def main(self):
        if sys.argv[2] == 'TEST':
            self.set_logging_config(True)
            self.stock_db.set_logging_config(True)
            self.stock_db.create_test_tables()
            self.screen_stocks(True)
        else:
            self.set_logging_config()
            self.stock_db.set_logging_config()
            self.screen_stocks()

    def set_exchange(self):
        self.exchange = sys.argv[1]

    def screen_stocks(self, version=False):
        logging.info("Screening stock using Piotroski F score")
        list_of_stock = self.stock_db.get_stocks_per_exchange(self.exchange, version)
        for stock in list_of_stock:
            self.get_score(stock[2], version)

    def store_annual_z_score(self, ticker, income_statement, balance_sheet, cashflow_statement, key_metric, version=False):
        logging.info("Populating Zscore for %s" % ticker)
        index = 0
        if self.annual_statement_has_no_record(balance_sheet, cashflow_statement, income_statement,key_metric):
            return

        if len(income_statement) > 19 and len(balance_sheet) > 19 and len(cashflow_statement) > 19 and len(key_metric) > 19:
            limit = 19
        else:
            if len(income_statement) % 2 != 0:
                limit = len(income_statement) - 1
            else:
                limit = len(income_statement)
        while index < limit:
            try:
                year = balance_sheet[index]["date"][:4]
                year = int(year)
                logging.info("Calculating score for %s" % ticker)
                self.annual_reset_metric()
                self.annual_set_total_asset(balance_sheet,index)
                a = 1.2 * (self.annual_get_working_capital(balance_sheet,index) / self.annual_total_asset)
                b = 1.4 * (self.annual_retained_earnings(balance_sheet,index) / self.annual_total_asset)
                c = 3.3 * (self.annual_get_ebit(income_statement,index) / self.annual_total_asset)
                d = 0.6 * (self.annual_get_market_cap(key_metric,index) / self.annual_total_liability(balance_sheet,index))
                e = 1.0 * (self.annual_get_revenue(income_statement,index) / self.annual_total_asset)
                z_score = a + b + c + d + e
            except Exception as e:
                logging.error(e.__str__())
                self.stock_db.insert_into_zscore_table(ticker, 0, test=True,year=year)
                index += 1

            self.stock_db.insert_into_zscore_table(ticker, z_score, test=True,year=year)
            logging.info("Z score:%s\nYear:%s\n Ticker:%s\n" % (str(z_score), str(year), ticker))
            index += 1

        return z_score

    def get_score(self, ticker,balance_sheet, income_statement):
        try:
            logging.info("Calculating score for %s" % ticker)
            self.reset_metric()
            self.set_total_asset(balance_sheet)
            a = 1.2 * (self.get_working_capital(balance_sheet) / self.ttm_total_asset)
            b = 1.4 * (self.get_retained_earnings(balance_sheet) / self.ttm_total_asset)
            c = 3.3 * (self.get_ttm_ebit(income_statement) / self.ttm_total_asset)
            d = 0.6 * (self.get_market_cap(ticker) / self.get_total_liability(balance_sheet))
            e = 1.0 * (self.get_ttm_revenue(income_statement) / self.ttm_total_asset)
            z_score = a + b + c + d + e
            print(z_score)
        except Exception as e:
            logging.error(e.__str__())
            self.stock_db.insert_into_zscore_table(ticker, 0, False)
            return 0
        self.stock_db.insert_into_zscore_table(ticker, z_score, False)
        return z_score

    def get_working_capital(self, balance_sheet):
        """
        Calculates the working capital of a company
        Formula: total_current_asset - total_current_liability
        :param balance_sheet:
        :return:
        """
        total_current_asset = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentAssets"])
        total_current_lia = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentLiabilities"])
        return total_current_asset - total_current_lia

    def annual_get_working_capital(self, balance_sheet,index):
        """
        Calculates the working capital of a company
        Formula: total_current_asset - total_current_liability
        :param balance_sheet:
        :return:
        """
        total_current_asset = self.get_value(balance_sheet[index]["totalCurrentAssets"])
        total_current_lia = self.get_value(balance_sheet[index]["totalCurrentLiabilities"])
        return total_current_asset - total_current_lia

    def get_value(self, query):
        value = query
        if value == "None":
            return 0
        else:
            return float(value)

    def set_total_asset(self, balance_sheet):
        self.ttm_total_asset = self.get_value(balance_sheet["quarterlyReports"][0]["totalAssets"])

    def annual_set_total_asset(self, balance_sheet,index):
        self.annual_total_asset = self.get_value(balance_sheet[index]["totalAssets"])

    def get_retained_earnings(self,balance_sheet):
        return self.get_value(balance_sheet["quarterlyReports"][0]["retainedEarnings"])

    def annual_retained_earnings(self, balance_sheet,index):
        return self.get_value(balance_sheet[index]["retainedEarnings"])

    def get_market_cap(self,ticker):
        company_info = self.make_api_call_alpha_vantage(self.company_overview % ticker)
        return self.get_value(company_info["MarketCapitalization"])

    def annual_get_market_cap(self,key_metric,index):
        return self.get_value(key_metric[index]["marketCap"])

    def get_total_liability(self,balance_sheet):
        return self.get_value(balance_sheet["quarterlyReports"][0]["totalLiabilities"])

    def annual_total_liability(self,balance_sheet,index):
        return self.get_value(balance_sheet[index]["totalLiabilities"])

    def get_ttm_ebit(self,income_statement):
        ttm = 0
        period = 0
        while period < 4:
            ttm += self.get_value(income_statement["quarterlyReports"][period]["ebit"])
            period +=1
        return ttm

    def annual_get_ebit(self, income_statement ,index):
        depreciationAndAmortization = self.get_value(income_statement[index]["depreciationAndAmortization"])
        ebitda = self.get_value(income_statement[index]["ebitda"])
        return ebitda - depreciationAndAmortization

    def get_ttm_revenue(self,income_statement):
        revenue = 0
        period = 0
        while period < 4:
            revenue += self.get_value(income_statement["quarterlyReports"][period]["totalRevenue"])
            period +=1
        return revenue

    def annual_get_revenue(self,income_statement,index):
        return self.get_value(income_statement[index]["revenue"])

    @sleep_and_retry
    @limits(calls=30, period=60)
    def make_api_call_alpha_vantage(self, api_call):
        """
        API call to get all stocks trading in an exchange
        :param url:
        :return:
        """
        response = requests.get(api_call)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    def reset_metric(self):
        self.ttm_total_asset = 0

    def annual_reset_metric(self):
        self.annual_total_asset = 0

    def statement_has_no_record(self, balance_sheet, income_statement):
        if len(balance_sheet["quarterlyReports"]) == 0 or len(income_statement["quarterlyReports"]) == 0:
            return True
        else:
            return False
    def annual_statement_has_no_record(self, balance_sheet, income_statement, cashflow,key_metric):
        if len(balance_sheet) == 0 or len(income_statement) == 0 or \
                len(cashflow) == 0 or len(key_metric) == 0:
            return True
        else:
            return False


# zscore = Zscore()
#
# balance_sheet = zscore.make_api_call_alpha_vantage(zscore.f_balance_sheet_api % 'AAPL')
# income_statement = zscore.make_api_call_alpha_vantage(zscore.f_income_statement_api % 'AAPL')
# cashflow = zscore.make_api_call_alpha_vantage(zscore.f_cashflow_api % 'AAPL')
# key_metric = zscore.make_api_call_alpha_vantage(zscore.f_key_metric_api % 'AAPL')
#
# zscore.screen_stocks_backtest('AAPL',income_statement,balance_sheet,cashflow,key_metric)

