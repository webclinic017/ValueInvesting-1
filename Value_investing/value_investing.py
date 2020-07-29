import requests
import symbol
import json
import time
from backoff import on_exception, expo
from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
from datetime import date, timedelta, datetime
import logging

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

    def main(self):
        self.get_ranking_data()
        self.rank_earning_yield()
        self.rank_roc()
        self.rank_final()
        self.sort_by_cap()
        self.save_result()

    def rank_earning_yield(self):
        logging.info("Ranking companies via earning yield")
        i = 0
        ranked_earning_yield = sorted(self.earning_yield_rank.items(), key=lambda x: (x[1]), reverse=True)
        size = len(ranked_earning_yield)
        while i < size:
            company = ranked_earning_yield[i][0]
            self.result[company]["earning_rank"] = i + 1
            i += 1

    def rank_roc(self):
        logging.info("Ranking companies via ROC")
        i = 0
        ranked_roc = sorted(self.roc_rank.items(), key=lambda x: (x[1]), reverse=True)
        size = len(ranked_roc)
        while i < size:
            company = ranked_roc[i][0]
            self.result[company]["roc_rank"] = i + 1
            i += 1

    def rank_final(self):
        final_rank_list = dict()
        for key, value in self.result.items():
            final_rank = value["earning_rank"] + value["roc_rank"]
            final_rank_list[key] = final_rank
            final_rank_temp = sorted(final_rank_list.items(), key=lambda x: (x[1]))

        i = 0
        size = len(final_rank_temp)
        while i < size:
            company = final_rank_temp[i][0]
            company_dict = self.result[company]
            company_dict["final_rank"] = final_rank_temp[i][1]
            self.final_rank.append(company_dict)
            i += 1
        print(self.final_rank)

    def get_ranking_data(self):
        logging.info("Extracting company's financial")
        iter_list = iter(self.stocks)
        while True:
            try:
                iter_obj = next(iter_list)
                self.get_market_data(iter_obj)
            except StopIteration:
                break

    def get_market_data(self, company):
        try:
            symbol = company['ticker']
            key_statistic = self.make_api_call_yahoo_finance(self.url_key_statistics % symbol)
            balance_sheet = self.make_api_call_yahoo_finance(self.url_balance_sheet % symbol)
            income_statement = self.make_api_call_yahoo_finance(self.url_income_statement % symbol)
            market_quote = self.make_api_call_yahoo_finance(self.url_market_quote % symbol)
            if self.financial_statement_is_old(balance_sheet,income_statement) or self.stock_is_illquid(market_quote):
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
            self.result[symbol] = {"name": company["description"], "symbol": symbol,
                                   "marketCapitilisation": company["marketCapitalization"],
                                   "industry": company["industry"], "ebit": ebit,
                                   "total_fixed_asset": total_fixed_asset
                , "working_capital": working_capital, "enterprise_value": enterprise_value,
                                   "earning_yield": earning_str,
                                   "roc": roc_str}
        except Exception as e:
            pass

    def get_ttm_ebit(self, income_statement):
        ebit = 0
        quarterly_list = income_statement["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]
        if len(quarterly_list) != 4:
            raise Exception
        for quarterly_report in quarterly_list:
            ebit += quarterly_report["ebit"]["raw"]
        return ebit

    def get_total_fixed_asset(self, balance_sheet):
        return balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["propertyPlantEquipment"][
            "raw"]

    def get_working_capital(self, balance_sheet):
        total_current_asset = \
            balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["totalCurrentAssets"]["raw"]
        total_current_liability = \
            balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["totalCurrentLiabilities"]["raw"]
        return total_current_asset - total_current_liability

    def get_enterprise_value(self, key_statistic):
        return key_statistic["defaultKeyStatistics"]["enterpriseValue"]["raw"]

    def get_stock_list(self, list_of_stocks):
        iter_onj = iter(list_of_stocks)
        while True:
            try:
                element = next(iter_onj)
                self.add_company(element)
            except StopIteration:
                break


    def get_stock_via_api(self, exchange):
        logging.info('Grabbing full ist of stocks from Exchange.\n Exchange code:"%s' % exchange)
        list_of_stocks = self.get_stocks_from_exchange(exchange)
        self.get_stock_list(list_of_stocks)



    def get_stock_via_file(self, file):
        logging.info('Grabbing full ist of stocks from file.\n File name:"%s' % file)
        with open(file, 'r') as f:
            list_of_stocks = json.load(f)
            self.get_stock_list(list_of_stocks)


    def get_stocks_from_exchange(self, exchange):
        return requests.get(
            'https://finnhub.io/api/v1/stock/symbol?exchange=%s&token=%s' % (exchange, self.finhub_api_key)).json()

    def add_company(self, element):
        try:
            symbol = element['symbol']
            company_info = self.make_api_call_finhub(
                'https://finnhub.io/api/v1/stock/profile2?symbol=%s&token=%s' % (symbol,self.finhub_api_key))
            if not company_info:
                pass
            else:
                if company_info["finnhubIndustry"] == 'Utilities' or company_info["finnhubIndustry"] == 'Financial Services' \
                        or 'ADR' in company_info["name"]:
                    return
                else:
                    company = {"description": element["description"], "ticker": element['symbol'],
                               "marketCapitalization":
                                   company_info["marketCapitalization"], "industry": company_info["finnhubIndustry"]}
                    self.stocks.append(company)
        except Exception as e:
            time.sleep(5)

    @sleep_and_retry
    @limits(calls=60, period=60)
    def make_api_call_finhub(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    @sleep_and_retry
    @limits(calls=API_CALL, period=API_PERIOD)
    def make_api_call_yahoo_finance(self, url):
        response = requests.request("GET", url, headers=self.headers)
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response.json()

    def sort_by_cap(self):
        logging.info("Sort Rank by Market Capitalisation")
        for element in self.final_rank:
            if element["marketCapitilisation"] <= 2000:
                self.small_cap_companies_result.append(element)
            elif element["marketCapitilisation"] <= 10000:
                self.medium_cap_companies_result.append(element)
            else:
                self.large_cap_companies_result.append(element)


    def save_result(self):
        logging.info("Writing result to file")
        self.write_file(r"C:\Users\user\Google Drive\value_investing\small_cap_ranked_stocks.json",self.small_cap_companies_result)
        self.write_file(r"C:\Users\user\Google Drive\value_investing\mid_cap_ranked_stocks.json", self.medium_cap_companies_result)
        self.write_file(r"C:\Users\user\Google Drive\value_investing\large_cap_ranked_stocks.json", self.large_cap_companies_result)
        self.write_file(r"C:\Users\user\Google Drive\value_investing\final_ranked_stocks.json", self.final_rank)

    def write_file(self, file,ranked_stock):
        with open(file, 'w') as f:
            json.dump(ranked_stock, f, indent=4)


    def date_is_old(self, epoch):
        six_months_from_now = (datetime.today() - timedelta(weeks=24)).timestamp()
        if epoch < six_months_from_now:
            return True
        else:
            return False

    def financial_statement_is_old(self, balance_sheet,income_statement):
        first_income_statment_date = income_statement["incomeStatementHistoryQuarterly"]["incomeStatementHistory"][0]["endDate"]["raw"]
        first_balance_sheet_date = balance_sheet["balanceSheetHistoryQuarterly"]["balanceSheetStatements"][0]["endDate"]["raw"]

        if self.date_is_old(first_balance_sheet_date) or self.date_is_old(first_income_statment_date):
            return True
        else:
            return False

    def stock_is_illquid(self,market_quote):
        currency = market_quote[0]["currency"]
        avg_trading_vol = market_quote[0]["averageDailyVolume3Month"]
        stock_price = market_quote[0]["regularMarketPreviousClose"]
        fx = self.get_fx(currency)
        avg_trading_val = avg_trading_vol * stock_price * fx
        if avg_trading_val < 100000:
            return True
        else:
            return False


    def get_fx(self,currency):
        if currency != "USD":
            currency_par_key = ("%susd:cur" % currency).lower()
            return float(self.currency_quote['result'][currency_par_key]["last"])
        else:
            return 1


    @sleep_and_retry
    @limits(calls=5, period=60)
    def get_fx_for_curr(self):
        url = "https://bloomberg-market-and-financial-news.p.rapidapi.com/market/get-cross-currencies"
        querystring = {
            "id": "aed%2Caud%2Cbrl%2Ccad%2Cchf%2Ccnh%2Ccny%2Ccop%2Cczk%2Cdkk%2Ceur%2Cgbp%2Chkd%2Chuf%2Cidr%2Cils%2Cinr%2Cjpy%2Ckrw%2Cmxn%2Cmyr%2Cnok%2Cnzd%2Cphp%2Cpln%2Crub%2Csek%2Csgd%2Cthb%2Ctry%2Ctwd%2Cusd%2Czar"}
        headers = {
            'x-rapidapi-host': "bloomberg-market-and-financial-news.p.rapidapi.com",
            'x-rapidapi-key': "e4477794c6msha9ee95cdc33a2b4p15d8ccjsn4f61b37a7247"
        }

        return requests.request("GET", url, headers=headers, params=querystring).json()


valueinvesting = ValueInvesting()
valueinvesting.get_stock_via_api("L")
valueinvesting.main()