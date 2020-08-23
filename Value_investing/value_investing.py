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

API_PERIOD = 60
API_CALL = 50


class ValueInvesting:
    def __init__(self):
        self.stocks = list()
        self.result = dict()
        self.url_key_statistics = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/%s/default-key-statistics"
        self.url_balance_sheet = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/%s/balance-sheet"
        self.url_income_statement = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/%s/income-statement"
        self.url_market_quote = r"https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/%s"
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

    def main(self):
        self.apply_magic_formula()
        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.sort_by_cap()
        self.save_result()
        self.write_result_to_file()

    def set_exchange(self):
        self.exchange = sys.argv[1]

    def run(self):
        if sys.argv[2] == 'TEST':
            self.test_run()
        else:
            self.main()

    def screen_stock(self, version=False):
        list_of_stock = self.stock_db.get_stocks_per_exchange(self.exchange, version)
        stock_list = list()
        for stock in list_of_stock:
            score = self.piotroski_score.calculate_score(stock[2])
            if score > 6:
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
            self.result[company]["earning_rank"] = i + 1
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
        j = 0
        k = 0
        final_rank_list = dict()
        final_list_with_momentum = dict()
        for key, value in self.result.items():
            final_rank = value["earning_rank"] + value["roc_rank"]
            final_rank_list[key] = final_rank
            final_rank_temp = sorted(final_rank_list.items(), key=lambda x: (x[1]))

        while j < 10:
            six_month = self.result[final_rank_temp[j][0]]["price_6_months_index"]
            final_list_with_momentum[final_rank_temp[j][0]] = six_month
            final_temp_six_mths = sorted(final_list_with_momentum.items(), key=lambda x: (x[1]),reverse=True)
            j += 1

        size = len(final_rank_temp)
        while i < size:
            company = final_rank_temp[i][0]
            company_dict = self.result[company]
            company_dict["final_rank"] = final_rank_temp[i][1]
            self.final_rank.append(company_dict)
            i += 1

        momentum_size = len(final_temp_six_mths)
        while k < momentum_size:
            company = final_temp_six_mths[k][0]
            company_dict = self.result[company]
            company_dict["final_rank"] = final_temp_six_mths[k][1]
            self.final_rank_with_momentum.append(company_dict)
            k += 1


    def apply_magic_formula(self, version=False):
        logging.info("Applying Formula on Stock")
        list_of_stocks = self.screen_stock(version)
        if len(list_of_stocks) == 0:
            self.get_stock_via_api()
        for stock in list_of_stocks:
            dict_stock = {'name': stock[1], 'ticker': stock[2], 'description': stock[3], 'industry': stock[4],
                          'marketCapitalization': stock[5]}
            self.get_market_data(dict_stock)

    def get_market_data(self, company):
        """
        For each stock, get financial statements and calculates the return on Capital and earning yield
        :param company:
        :return:
        """
        try:
            symbol = company['ticker']
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
            momentum_3_months = self.momentum_price.get_3_month_return(symbol)
            momentum_6_months = self.momentum_price.get_6_month_return(symbol)
            self.earning_yield_rank[symbol] = earning_yield
            self.six_month_price_index[symbol] = momentum_6_months
            self.three_month_price_index[symbol] = momentum_3_months
            earning_str = str(earning_yield) + r'%'
            roc_str = str(roc) + r'%'
            self.roc_rank[symbol] = roc

            self.result[symbol] = {"name": company["description"], "symbol": symbol,
                                   "marketCapitilisation": company["marketCapitalization"],
                                   "industry": company["industry"], "ebit": ebit,
                                   "total_fixed_asset": total_fixed_asset
                , "working_capital": working_capital, "enterprise_value": enterprise_value,
                                   "earning_yield": earning_str,
                                   "roc": roc_str, "price_3_months_index": momentum_3_months,
                                   "price_6_months_index": momentum_6_months}
        except Exception as e:
            pass

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

    def get_total_fixed_asset(self, balance_sheet):
        """
        Utility method to return total fixed asset from balance sheet for a company
        :param balance_sheet:
        :return:
        """
        return balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["propertyPlantEquipment"][
            "raw"]

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

    def get_enterprise_value(self, key_statistic):
        """
        Utility method to get current enterprise value for a company
        :param key_statistic:
        :return:
        """
        return key_statistic["defaultKeyStatistics"]["enterpriseValue"]["raw"]

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
            for stock in self.final_rank:
                result = (
                    self.rundate, stock['symbol'], stock['ebit'], stock['total_fixed_asset'], stock['working_capital'],
                    stock['enterprise_value'], stock['earning_yield'], stock['roc'], stock['earning_rank'],
                    stock['roc_rank'],
                    stock['final_rank'])
                self.stock_db.insert_into_ranked_result(result, version)
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


valueinvesting = ValueInvesting()
valueinvesting.run()
