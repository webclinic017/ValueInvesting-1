from value_investing import ValueInvesting
from Piotroski import PiotroskiScore
from momentum import Momentum
from z_score import Zscore
from stock_database import StockDatabase
from sql_queries import *
import sys
import requests
import logging
import pandas as pd
import matplotlib as mp
import matplotlib.pyplot as plt
from ratelimit import limits, RateLimitException, sleep_and_retry
from market_data import MarketData
from ulcer_index import UlcerIndex
from datetime import datetime
import dateutil.relativedelta
from peg import PEG
from ratio import Ratio

import time



class Backtest:
    def __init__(self):
        self.value_investinig = ValueInvesting()
        self.piotroski = PiotroskiScore()
        self.momentum = Momentum()
        self.z_score = Zscore()
        self.stock_db = StockDatabase()
        self.exchange = sys.argv[1]
        self.f_balance_sheet_api = r'https://financialmodelingprep.com/api/v3/balance-sheet-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_income_statement_api = r'https://financialmodelingprep.com/api/v3/income-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_cashflow_api = r'https://financialmodelingprep.com/api/v3/cash-flow-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_key_statistic = r'https://financialmodelingprep.com/api/v3/key-metrics/%s?limit=40&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_balance_sheet_as_reported = r'https://financialmodelingprep.com/api/v3/balance-sheet-statement-as-reported/%s?limit=20&apikey=4bb56bf385e5ed64f869e94649147090'
        self.alpha_balance_sheet_api = r'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.alpha_income_statement_api = r'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.alpha_cashflow_api = r'https://www.alphavantage.co/query?function=CASH_FLOW&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.market_date = MarketData()
        self.ulcer_index = UlcerIndex()
        self.ratio = Ratio()
        self.peg = PEG()
        logging.getLogger().setLevel(logging.INFO)

    def delete_all_table(self):
        self.stock_db.delete_all_records("ZscoreTest")
        self.stock_db.delete_all_records("PriceMomemtumTest")
        self.stock_db.delete_all_records("RankedResultTest")
        self.stock_db.delete_all_records("PiotroskiFscoreTest")

    def populateData(self):
        self.delete_all_table()
        list_of_stocks = self.stock_db.get_stocks_per_exchange(self.exchange, False)
        for ticker in list_of_stocks:
            try:
                logging.info("ticker to be populated:%s\n\n" %ticker[2])
                balance_sheet = self.make_api_call_alpha_vantage(self.f_balance_sheet_api % ticker[2])
                income_statement = self.make_api_call_alpha_vantage(self.f_income_statement_api % ticker[2])
                cashflow_statement = self.make_api_call_alpha_vantage(self.f_cashflow_api % ticker[2])
                key_statistic = self.make_api_call_alpha_vantage(self.f_key_statistic % ticker[2])
                self.statement_is_empty(balance_sheet,income_statement,cashflow_statement,key_statistic)
                self.piotroski.store_annual_piotroski_score(ticker[2], income_statement, balance_sheet, cashflow_statement)
                self.z_score.store_annual_z_score(ticker[2], income_statement, balance_sheet, cashflow_statement, key_statistic, True)
                self.peg.get_ttm_peg(ticker)
                self.ratio.get_ttm_ratio(ticker)
                self.momentum.store_annual_price_momentum(ticker[2])
                self.value_investinig.store_annual_mf(ticker[2], balance_sheet, key_statistic, income_statement)
            except Exception as e:
                logging.error(e.__str__())

    def run_strategy(self,ticker):
        try:
            logging.info("ticker to be populated:%s\n\n" %ticker)
            balance_sheet = self.make_api_call_alpha_vantage(self.alpha_balance_sheet_api % ticker)
            income_statement = self.make_api_call_alpha_vantage(self.alpha_income_statement_api % ticker)
            cashflow_statement = self.make_api_call_alpha_vantage(self.alpha_cashflow_api % ticker)
            key_statistic = self.make_api_call_alpha_vantage(self.f_key_statistic % ticker)
            self.statement_is_empty(balance_sheet,income_statement,cashflow_statement,key_statistic)
            self.piotroski.get_score(ticker, balance_sheet, income_statement, cashflow_statement)
            self.z_score.get_score(ticker, balance_sheet, income_statement)
            self.momentum.calculate_current_momemtum(ticker)
            self.ulcer_index.get_ttm_index(ticker)
        except Exception as e:
            logging.error(e.__str__())

    def run_ulcer_index(self,ticker):
        self.ulcer_index.get_ttm_index(ticker)

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

    def statement_is_empty(self,balance_sheet,income_statement,cashflow_statement,key_statistic):
        if not balance_sheet or not income_statement or not cashflow_statement or not key_statistic:
            raise Exception("Statements are empty")
        else:
            pass

    def rank_result(self):
        start_year = 2002;
        end_year = 2020
        while start_year <= end_year:
            logging.info("Ranking result for %s" % start_year)
            list_of_result = self.stock_db.get_ranked_result(start_year,True)
            for result in list_of_result:
                self.value_investinig.earning_yield_rank[result[0]] = float(result[1])
                self.value_investinig.roc_rank[result[0]] = float(result[2])

            self.value_investinig.rank_earning_yield()
            self.value_investinig.rank_roc()
            self.update_rank(start_year)
            self.reset_rank()
            start_year += 1


    def update_rank(self,year):
        logging.info("Storing MF rank results for year %s" % year)
        for ticker, value in self.value_investinig.result.items():
            final_rank = value["earning_rank"] + value["roc_rank"]
            statement = prod_update_rank_table % (value["earning_rank"] , value["roc_rank"] , final_rank,ticker, year )
            self.stock_db.update_ranked_result(statement,True)
            logging.info("ticker:%s\nyear:%s\nfinal_result:%s\n" % (ticker,str(year),str(final_rank)))

    def reset_rank(self):
        self.value_investinig.earning_yield_rank = dict()
        self.value_investinig.roc_rank = dict()
        self.value_investinig.result = dict()

    def populate_portfolio_as_per_strategy(self,strategy,strat_sql_query,year):
            logging.info("Populating portfolio for %s" % year)
            portfolio = self.stock_db.get_port_strat(strat_sql_query)
            for stock in portfolio:
                statement = (stock[0],strategy,year,self.exchange)
                self.stock_db.insert_into_portfolio(statement)
            logging.info("Populated portfolio for %s" % year)


    def populate_stock_returns(self,year):
        list_of_stocks = self.stock_db.get_stocks_per_exchange(self.exchange, False)
        for ticker in list_of_stocks:
            try:
                logging.info("Populating rate of return for stock %s" % ticker[2])
                self.momentum.store_stock_return_per_year(ticker[2],self.exchange,year)
            except Exception as e:
                logging.info(e.__str__())

    def populate_mtd_return_for_stoc(self):
        list_of_stocks = self.stock_db.get_stocks_per_exchange(self.exchange, False)
        for ticker in list_of_stocks:
            try:
                logging.info("Populating rate of return for stock %s" % ticker[2])
                self.momentum.store_stock_return_per_year(ticker[2], self.exchange, year)
            except Exception as e:
                logging.info(e.__str__())


    def get_roi(self, strategy, year):
        starting_val = 1000000
        equally_weighted_invested = starting_val / 20
        end_of_portfolio = 0
        statement = get_portfolio_with_returns % (self.exchange, year, year, strategy)
        list_of_stocks_inportfolio = self.stock_db.get(statement)
        for ticker in list_of_stocks_inportfolio:
            end_of_portfolio+= equally_weighted_invested * ( 1 + (ticker[1] / 100))

        roi = ((end_of_portfolio - starting_val) / starting_val) * 100
        statement = (year, strategy, roi)
        self.stock_db.insert_into_portfolio_return(statement)

    def visualise_result_portfolio(self,strategy):
        conn = self.stock_db.create_connection()
        result = pd.read_sql(get_roi_per_potfrolio % strategy, conn)
        sp500_returns = pd.read_sql(get_sp500_returns,conn)
        list_of_year = result['year'].tolist()
        dates = pd.to_datetime(pd.Series(list_of_year), format="%Y")
        sp500_returns['year'] = dates
        result['year'] = dates
        result.set_index('year', inplace=True)
        sp500_returns.set_index('year', inplace=True)
        mp.rcParams['figure.figsize'] = (10, 8)  # Increases the Plot Size
        sp_plot = sp500_returns['ROI'].plot()
        result['ROI'].plot(sp_plot=sp_plot)
        plt.xlabel("Years")
        plt.ylabel("Return on Investment(%)")
        plt.show()

    def measure_ulcer_index(self):
        pass

    # def populate_historic_prices(self):
    #     #     list_of_stocks_a_l = self.stock_db.get(get_last_prices_interval % ('A','LA'))
    #     #     list_of_stocks_l_p = self.stock_db.get(get_last_prices_interval % ('LA','PA'))
    #     #     list_of_stocks_p_z = self.stock_db.get(get_last_prices_interval % ('PA', 'ZZZZ'))
    #     #
    #     #     for stock in list_of_stocks_a_l:
    #     #         self.market_date.get_historical_prices(stock[0],str(stock[1]))
    #     #     time.sleep(100)
    #     #     for stock in list_of_stocks_l_p:
    #     #         self.market_date.get_historical_prices(stock[0], str(stock[1]))
    #     #     time.sleep(100)
    #     #     for stock in list_of_stocks_p_z:
    #     #         self.market_date.get_historical_prices(stock[0], str(stock[1]))

    def populate_historic_prices(self):
        list_of_stocks_a_l = self.stock_db.get(get_last_prices_interval)
        for stock in list_of_stocks_a_l:
            self.market_date.get_historical_prices(stock[0], str(stock[1]))



    def calculate_stock_return(self):
        start_of_year = "%s0101" % str(datetime.now().year)
        curr_date = datetime.now()
        result = None
        while not result:
            date_str = curr_date.strftime('%Y%m%d')
            result = self.stock_db.get(get_min_max_price % (start_of_year,self.exchange,date_str))
            curr_date = curr_date - dateutil.relativedelta.relativedelta(days=1)

        logging.info("Found result")
        for stock in result:
            min_price = stock[3]
            max_price = stock[4]
            price_diff = ((max_price - min_price) / min_price) * 100
            statement = [stock[0],str(datetime.now().year),price_diff]
            self.stock_db.insert_into_stock_return_table(statement)

    def get_rest_of_prices(self):
        sql = """
        SELECT ticker from companyInfo where ticker > 'TGI'
        """
        list_of_stocks = self.stock_db.get(sql)
        for stock in list_of_stocks:
            self.market_date.get_historical_prices_v2(stock[0])

    def calculate_ulcer_index_per_ticker(self,ticker):
        sql = """
        SELECT ticker from companyInfo where exchange = '%s' 
        """ % ticker
        list_of_stocks = self.stock_db.get(sql)
        for stock in list_of_stocks:
            self.ulcer_index.one_year_ulcer_index(stock[0])

    def remove_duplicate(self):
        list_of_stocks = self.stock_db.get_stocks_per_exchange(self.exchange, False)
        sql_delete = """
        DELETE FROM STOCKRETURN WHERE ID=%s
        """
        for stock in list_of_stocks:
            sql = "select id,ticker from stockreturn where ticker = '%s' and year = 2020" % stock[2]
            result = self.stock_db.get(sql)
            if len(result) == 2:
                if result[0][0] < result[1][0]:
                    self.stock_db.execute_command(sql_delete % result[0][0])
                    logging.info("DELETED DUPLICATE PRICE for %s" % stock[2])
                else:
                    logging.info("DELETED DUPLICATE PRICE for %s" % stock[2])
                    self.stock_db.execute_command(sql_delete % result[0][1])


# backtest = Backtest()

