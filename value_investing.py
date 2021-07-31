import json
import time
from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
from datetime import date, timedelta, datetime
import logging
import sys
from sql_queries import *
from stock_database import StockDatabase
from momentum import Momentum
from Piotroski import PiotroskiScore
from api import *

API_PERIOD = 60
API_CALL = 50


class ValueInvesting:
    def __init__(self):
        self.stocks = list()
        self.result = dict()
        self.url_key_statistics = yahoo_url_key_statistics
        self.url_balance_sheet = yahoo_url_balance_sheet
        self.url_income_statement = yahoo_url_income_statement
        self.url_market_quote = yahoo_url_market_quote
        self.headers = {
            'x-rapidapi-host': "yahoo-finance15.p.rapidapi.com",
            'x-rapidapi-key': "e4477794c6msha9ee95cdc33a2b4p15d8ccjsn4f61b37a7247"
        }
        self.earning_yield_rank = dict()
        self.roc_rank = dict()
        self.final_rank = list()
        self.api_period = 60
        self.api_call = 5
        self.result_file = r".\ranked_companies_mining_us_stocks.json"
        self.finhub_api_key = r'brmngnnrh5re15om7qd0'
        self.small_cap_companies_result = list()
        self.medium_cap_companies_result = list()
        self.large_cap_companies_result = list()
        self.currency_quote = self.get_fx_for_curr()
        self.stock_db = StockDatabase()
        self.rundate = int(datetime.now().strftime('%Y%m%d'))
        self.set_exchange()
        logging.getLogger().setLevel(logging.INFO)
        self.momentum_price = Momentum()
        self.piotroski_score = PiotroskiScore()
        self.final_rank_by_momemtum = list()
        self.six_month_price_index = dict()
        self.three_month_price_index = dict()
        self.twelve_month_price_index = dict()
        self.final_rank_with_momentum = list()
        self.piotroski_screen_stock = dict()

        self.f_balance_sheet_api = r'https://financialmodelingprep.com/api/v3/balance-sheet-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_income_statement_api = r'https://financialmodelingprep.com/api/v3/income-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_cashflow_api = r'https://financialmodelingprep.com/api/v3/cash-flow-statement/%s?limit=120&apikey=4bb56bf385e5ed64f869e94649147090'
        self.f_key_statistic = r'https://financialmodelingprep.com/api/v3/key-metrics/%s?limit=40&apikey=4bb56bf385e5ed64f869e94649147090'

    def main(self):
        self.apply_magic_formula()
        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.save_result()

    def rank_results(self):
        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.save_result()


    def set_exchange(self):
        self.exchange = sys.argv[1]


    def run(self):
        if sys.argv[2] == 'TEST':
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
            self.test_run()
        else:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
            self.main()

    def screen_stock(self, version=False):
        logging.info("Screening stock using Piotroski F score")
        list_of_stock = self.stock_db.get_stocks_per_exchange(self.exchange, version)
        stock_list = list()
        for stock in list_of_stock:
            score = self.piotroski_score.get_score(stock[2])
            self.piotroski_screen_stock[stock[2]] = score
            if score > 6:
                logging.info("%s has a score of %s. Adding stock" % (stock[2], str(score)))
                stock_list.append(stock)
        return stock_list

    def test_run(self):
        self.stock_db.create_test_tables()
        self.apply_magic_formula(True)
        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.sort_by_cap()
        self.save_result(True)
        self.write_result_to_file(True)


    def if_exchange_in_db(self):
        statement = """
                    SELECT * FROM companyInfo i where i.exchange=%s
                    """ % self.exchange
        result = self.stock_db.execute_command(statement)
        if len(result) > 0:
            return True
        else:
            return False

    def rank_earning_yield(self):
        """
        ranks companies via the earning yield
        :return:
        """
        logging.info("Ranking companies via earning yield")
        i = 0
        ranked_earning_yield = sorted(self.earning_yield_rank.items(), key=lambda x: (x[1]), reverse=True)
        size = len(ranked_earning_yield)
        while i < size:
            company = ranked_earning_yield[i][0]
            self.result[company]["earning_rank"] =  i + 1
            i += 1

    def rank_roc(self):
        """
        ranks companies via the return on capital
        :return:
        """
        logging.info("Ranking companies via ROC")
        i = 0
        ranked_roc = sorted(self.roc_rank.items(), key=lambda x: (x[1]), reverse=True)
        size = len(ranked_roc)
        while i < size:
            company = ranked_roc[i][0]
            self.result[company]["roc_rank"] = i + 1
            i += 1

    def rank_final(self):
        """
        As per the magic formula, the final rank is the summation of the roc and earning yield rank
        :return:
        """
        i = 0
        final_rank_list = dict()
        for key, value in self.result.items():
            final_rank = value["earning_rank"] + value["roc_rank"]
            final_rank_list[key] = final_rank
            final_rank_temp = sorted(final_rank_list.items(), key=lambda x: (x[1]))

        size = len(final_rank_temp)
        while i < size:
            company = final_rank_temp[i][0]
            company_dict = self.result[company]
            company_dict["final_rank"] = final_rank_temp[i][1]
            self.final_rank.append(company_dict)
            i += 1

    def apply_magic_formula(self, version=False):
        list_of_stocks = self.stock_db.get_stocks_per_exchange(self.exchange, version)
        if len(list_of_stocks) == 0:
            self.get_stock_via_api()
        logging.info("Applying Formula on Stock")
        for stock in list_of_stocks:
            logging.info("MF to be applied to %s" %stock[2])
            self.calculate_mf_formula(stock[2])

    def calculate_mf_formula(self, symbol):
        """
        For each stock, get financial statements and calculates the return on Capital and earning yield
        :param company:
        :return:
        """
        try:
            key_statistic = self.make_api_call_yahoo_finance(self.url_key_statistics % symbol)
            balance_sheet = self.make_api_call_yahoo_finance(self.url_balance_sheet % symbol)
            income_statement = self.make_api_call_yahoo_finance(self.url_income_statement % symbol)
            market_quote = self.make_api_call_yahoo_finance(self.url_market_quote % symbol)
            if self.financial_statement_is_old(balance_sheet, income_statement) or self.stock_is_illquid(market_quote):
                return

            ebit = self.get_ttm_ebit(income_statement)
            total_fixed_asset = self.get_total_fixed_asset(balance_sheet)
            working_capital = self.get_working_capital(balance_sheet)
            enterprise_value = self.get_enterprise_value(key_statistic)
            roc = round((ebit / (total_fixed_asset + working_capital)) * 100, 2)
            earning_yield = round((ebit / enterprise_value) * 100, 2)
            self.earning_yield_rank[symbol] = earning_yield
            earning_str = str(earning_yield) + r'%'
            roc_str = str(roc) + r'%'
            self.roc_rank[symbol] = roc

            self.result[symbol] = {"symbol": symbol, "ebit": ebit,
                                   "total_fixed_asset": total_fixed_asset
                , "working_capital": working_capital, "enterprise_value": enterprise_value,
                                   "earning_yield": earning_str,
                                   "roc": roc_str}
            statement = [symbol, ebit, total_fixed_asset, working_capital,
                        enterprise_value, earning_yield, roc]

            self.stock_db.insert_into_ranked_result(statement)
            logging.info("Applied formula for %s" % symbol)
        except Exception as e:
            logging.error(e.__str__())
            pass
    def get_result_from_db(self):
        statement = """SELECT r.ticker,r.EBIT,r.TotalFixedAsset,r.WorkingCapital,r.EnterpriseValue,r.EarningYield,r.ReturnOnCapital from rankedResult r
inner join companyInfo cp on cp.ticker = r.ticker
where cp.exchange = '%s'""" % self.exchange
        list_of_result = self.stock_db.get(statement)
        for result in list_of_result:
            self.result[result[0]] = {"symbol": result[0], "ebit": result[1],
                                   "total_fixed_asset": result[2]
                , "working_capital": result[3], "enterprise_value": result[4],
                                   "earning_yield": result[5],
                                   "roc": result[6]}
            self.earning_yield_rank[result[0]] = result[5]
            self.roc_rank[result[0]] = result[6]
            logging.info("Extracted ranked result for  %s" %result[0] )

        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.save_result()



    def annual_statement_has_no_record(self, balance_sheet, income_statement, key_metric):
        if len(balance_sheet) == 0 or len(income_statement) == 0 or len(key_metric) == 0:
            return True
        else:
            return False

    def store_annual_mf(self, ticker, balance_sheet, key_statistic, income_statement):
        """
        For each stock, get financial statements and calculates the return on Capital and earning yield
        :param company:
        :return:
        """
        index = 0
        if self.annual_statement_has_no_record(balance_sheet, income_statement, key_statistic):
            return

        if len(income_statement) > 19 and len(balance_sheet) > 19 and len(key_statistic) > 19:
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
                symbol = ticker
                ebit = self.annual_get_ebit(income_statement,index)
                total_fixed_asset = self.annual_get_total_fixed_asset(balance_sheet,index)
                working_capital = self.annual_get_working_capital(balance_sheet,index)
                enterprise_value = self.annual_get_enterprise_value(key_statistic,index)
                roc = round((ebit / (total_fixed_asset + working_capital)) * 100, 2)
                earning_yield = round((ebit / enterprise_value) * 100, 2)
                self.earning_yield_rank[symbol] = earning_yield
                self.roc_rank[symbol] = roc
                statement = (year,ticker,ebit,total_fixed_asset,working_capital,enterprise_value,earning_yield,roc)
                self.stock_db.insert_into_ranked_result(statement,test=True)
                logging.info("Applied formula for %s for year %s" % (ticker,year))
                index += 1
            except Exception as e:
                logging.error(e.__str__())
                index += 1

    def get_ttm_ebit(self, income_statement):
        """
        Gets trailing twelve month earning before interest and tax
        :param income_statement:
        :return:
        """
        ebit = 0
        quarterly_list = income_statement["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]
        if len(quarterly_list) != 4:
            raise Exception
        for quarterly_report in quarterly_list:
            ebit += quarterly_report["ebit"]["raw"]
        return ebit

    def annual_get_ebit(self, income_statement ,index):
        depreciationAndAmortization = self.get_value(income_statement[index]["depreciationAndAmortization"])
        ebitda = self.get_value(income_statement[index]["ebitda"])
        return ebitda - depreciationAndAmortization

    def get_total_fixed_asset(self, balance_sheet):
        """
        Utility method to return total fixed asset from balance sheet for a company
        :param balance_sheet:
        :return:
        """
        return balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["propertyPlantEquipment"][
            "raw"]

    def annual_get_total_fixed_asset(self, balance_sheet,index):
        """
        Utility method to return total fixed asset from balance sheet for a company
        :param balance_sheet:
        :return:
        """
        return self.get_value(balance_sheet[index]["propertyPlantEquipmentNet"])

    def get_value(self, query):
        value = query
        if value == "None":
            return 0
        else:
            return float(value)

    def get_working_capital(self, balance_sheet):
        """
        Calculates the working capital of a company
        Formula: total_current_asset - total_current_liability
        :param balance_sheet:
        :return:
        """
        total_current_asset = \
            balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["totalCurrentAssets"]["raw"]
        total_current_liability = \
            balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["totalCurrentLiabilities"]["raw"]
        return total_current_asset - total_current_liability

    def annual_get_working_capital(self, balance_sheet,index):
        """
        Calculates the working capital of a company
        Formula: total_current_asset - total_current_liability
        :param balance_sheet:
        :return:
        """
        total_current_asset = self.get_value(balance_sheet[index]["totalCurrentAssets"])
        total_current_liability = self.get_value(balance_sheet[index]["totalCurrentLiabilities"])
        return total_current_asset - total_current_liability

    def get_enterprise_value(self, key_statistic):
        """
        Utility method to get current enterprise value for a company
        :param key_statistic:
        :return:
        """
        return key_statistic["defaultKeyStatistics"]["enterpriseValue"]["raw"]

    def annual_get_enterprise_value(self, key_statistic,index):
        """
        Utility method to get current enterprise value for a company
        :param key_statistic:
        :return:
        """
        return key_statistic[index]["enterpriseValue"]

    def get_stock_via_api(self):
        """
        gets all stocks from an exchange using an API
        :param exchange:
        :return:
        """
        logging.info('Grabbing full ist of stocks from Exchange.\n Exchange code:"%s' % self.exchange)
        list_of_stocks = self.get_stocks_from_exchange(self.exchange)
        iter_onj = iter(list_of_stocks)
        while True:
            try:
                element = next(iter_onj)
                self.add_company(element, self.exchange)
            except StopIteration:
                break

    def get_stocks_from_exchange(self, exchange):
        return requests.get(
            'https://finnhub.io/api/v1/stock/symbol?exchange=%s&token=%s' % (exchange, self.finhub_api_key)).json()

    def add_company(self, company, exchange):
        """
        For any exchange, exclude stocks based on certain criterias such as industry or deposit receipt company
        :param company:
        :return:
        """
        try:
            symbol = company['symbol']
            company_info = self.make_api_call_finhub(
                'https://finnhub.io/api/v1/stock/profile2?symbol=%s&token=%s' % (symbol, self.finhub_api_key))
            if not company_info:
                pass
            else:
                if company_info["finnhubIndustry"] == 'Utilities' or company_info[
                    "finnhubIndustry"] == 'Financial Services' \
                        or 'ADR' in company_info["name"]:
                    return
                else:
                    company = {"description": company["description"], "ticker": company_info['ticker'],
                               "marketCapitalization":
                                   company_info["marketCapitalization"], "industry": company_info["finnhubIndustry"]}

                    stock = (company_info["name"], company_info['ticker'], company["description"],
                             company_info["finnhubIndustry"], company_info["marketCapitalization"], exchange)
                    self.stock_db.add_stock_to_db(stock)
                    self.stocks.append(company)

        except Exception as e:
            time.sleep(5)

    @sleep_and_retry
    @limits(calls=60, period=60)
    def make_api_call_finhub(self, url):
        """
        API call to get all stocks trading in an exchange
        :param url:
        :return:
        """
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    @sleep_and_retry
    @limits(calls=API_CALL, period=API_PERIOD)
    def make_api_call_yahoo_finance(self, url):
        """
        API call to get data such as financial statements or key statistics about a company via
        yahoo finance
        :param url:
        :return:
        """
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    def sort_by_cap(self):
        """
        Divide ranked companies into market capitilisation
        :return:
        """
        logging.info("Sort Rank by Market Capitalisation")
        for element in self.final_rank:
            if element["marketCapitilisation"] <= 2000:
                self.small_cap_companies_result.append(element)
            elif element["marketCapitilisation"] <= 10000:
                self.medium_cap_companies_result.append(element)
            else:
                self.large_cap_companies_result.append(element)

    def write_result_to_file(self, test=False):
        """
        save results into a file
        :return:
        """
        logging.info("Writing result to file")
        if not test:
            self.write_file(r"C:\Users\user\Google Drive\value_investing\prod\small_cap_ranked_stocks.json",
                            self.small_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\prod\mid_cap_ranked_stocks.json",
                            self.medium_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\prod\large_cap_ranked_stocks.json",
                            self.large_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\prod\final_ranked_stocks.json",
                            self.final_rank)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\prod\final_ranked_with_momentum_stocks.json",
                            self.final_rank_with_momentum)
        else:
            self.write_file(r"C:\Users\user\Google Drive\value_investing\test\small_cap_ranked_stocks.json",
                            self.small_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\test\mid_cap_ranked_stocks.json",
                            self.medium_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\test\large_cap_ranked_stocks.json",
                            self.large_cap_companies_result)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\test\final_ranked_stocks.json",
                            self.final_rank)
            self.write_file(r"C:\Users\user\Google Drive\value_investing\test\final_ranked_with_momentum_stocks.json",
                            self.final_rank_with_momentum)

    def save_result(self, version=False):
        try:
            logging.info("Saving Ranking data to DB")
            for stock in self.final_rank:
                if not version:
                    result = (stock['symbol'],
                              stock['earning_rank'],
                              stock['roc_rank'],
                              stock['final_rank'])
                    self.stock_db.update_ranking_result(result, False)
                else:
                    result = (stock['symbol'], stock['ebit'], stock['total_fixed_asset'], stock['working_capital'],
                              stock['enterprise_value'], stock['earning_yield'], stock['roc'], stock['earning_rank'],
                              stock['roc_rank'],
                              stock['final_rank'])

                    self.stock_db.update_ranking_result(result, True)
        except Exception as e:
            print(e)

    def write_file(self, file, ranked_stock):
        with open(file, 'w') as f:
            json.dump(ranked_stock, f, indent=4)

    def date_is_old(self, epoch):
        """
        Checks if company has not published a financial statement for more than six months.
        :param epoch:
        :return:
        """
        six_months_from_now = (datetime.today() - timedelta(weeks=24)).timestamp()
        if epoch < six_months_from_now:
            return True
        else:
            return False

    def financial_statement_is_old(self, balance_sheet, income_statement):
        """
        Checks if company has not published a financial statement for more than six months.
        Such companies are to be excluded from being ranked
        :param balance_sheet:
        :param income_statement:
        :return:
        """
        first_income_statment_date = \
            income_statement["incomeStatementHistoryQuarterly"]["incomeStatementHistory"][0]["endDate"]["raw"]
        first_balance_sheet_date = \
            balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["endDate"]["raw"]

        if self.date_is_old(first_balance_sheet_date) or self.date_is_old(first_income_statment_date):
            return True
        else:
            return False

    def stock_is_illquid(self, market_quote):
        """
        Checks the liquidity of a stocks by using finding the US Dollar value of the average trading volume.
        The aim is to exclude illiquid stocks
        :param market_quote:
        :return:
        """
        currency = market_quote[0]["currency"]
        avg_trading_vol = market_quote[0]["averageDailyVolume3Month"]
        stock_price = market_quote[0]["regularMarketPreviousClose"]
        fx = self.get_fx(currency)
        avg_trading_val = avg_trading_vol * stock_price * fx
        if avg_trading_val < 100000:
            return True
        else:
            return False

    def get_fx(self, currency):
        """
        returns fx for a given currency pair with usd being the target currency
        :param currency:
        :return:
        """
        if currency != "USD":
            currency_par_key = ("%susd:cur" % currency).lower()
            return float(self.currency_quote['result'][currency_par_key]["last"])
        else:
            return 1

    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_fx_for_curr(self):
        """
        API call to get yesterday's close of all FX exchange
        :return:
        """
        url = "https://bloomberg-market-and-financial-news.p.rapidapi.com/market/get-cross-currencies"
        querystring = {
            "id": "aed%2Caud%2Cbrl%2Ccad%2Cchf%2Ccnh%2Ccny%2Ccop%2Cczk%2Cdkk%2Ceur%2Cgbp%2Chkd%2Chuf%2Cidr%2Cils%2Cinr%2Cjpy%2Ckrw%2Cmxn%2Cmyr%2Cnok%2Cnzd%2Cphp%2Cpln%2Crub%2Csek%2Csgd%2Cthb%2Ctry%2Ctwd%2Cusd%2Czar"}
        headers = {
            'x-rapidapi-host': "bloomberg-market-and-financial-news.p.rapidapi.com",
            'x-rapidapi-key': "e4477794c6msha9ee95cdc33a2b4p15d8ccjsn4f61b37a7247"
        }

        return requests.request("GET", url, headers=headers, params=querystring).json()

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

#
# valueinvesting = ValueInvesting()
# balance_sheet = valueinvesting.make_api_call_alpha_vantage(valueinvesting.f_balance_sheet_api % 'AAPL')
# key_statistic = valueinvesting.make_api_call_alpha_vantage(valueinvesting.f_key_statistic % 'AAPL')
# income_statement = valueinvesting.make_api_call_alpha_vantage(valueinvesting.f_income_statement_api % 'AAPL')
# cashflow = valueinvesting.make_api_call_alpha_vantage(valueinvesting.f_cashflow_api % 'AAPL')
# valueinvesting.annual_get_market_data('AAPL',balance_sheet,key_statistic,income_statement)
