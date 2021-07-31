from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
import logging
from stock_database import StockDatabase
import sys
from api import *


class PiotroskiScore:
    def __init__(self):
        self.balance_sheet_api = alpha_vantage_balance_sheet_api
        self.income_statement = alpha_vantage_income_statement
        self.cashflow = alpha_vantage_cashflow
        self.back_test_balance_sheet = list()
        self.back_test_income_statement = list()
        self.back_test_cashflow_statment = list()

        self.ttm_net_income = 0
        self.ttm_net_income_last_year = 0
        self.ttm_roa = 0
        self.ttm_roa_last_year = 0
        self.ttm_total_asset = 0
        self.ttm_avg_total_asset = 0
        self.ttm_avg_total_asset_ly = 0
        self.ttm_total_asset_last_year = 0
        self.ttm_operating_cash_flow = 0
        self.ttm_operating_last_year = 0
        self.ttm_curr_ass = 0
        self.ttm_curr_ass_last_year = 0
        self.ttm_curr_lbt = 0
        self.ttm_curr_lbt_last_year = 0
        self.ttm_gross_profit = 0
        self.ttm_gross_profit_ly = 0
        self.ttm_total_revenue = 0
        self.ttm_total_revenue_ly = 0

        self.annual_net_income = 0
        self.annual_net_income_last_year = 0
        self.annual_roa = 0
        self.annual_roa_last_year = 0
        self.annual_avg_total_asset = 0
        self.annual_avg_total_asset_ly = 0
        self.annual__total_asset = 0
        self.annual_total_asset_last_year = 0
        self.annual_operating_cash_flow = 0
        self.annual_operating_last_year = 0
        self.annual_curr_ass = 0
        self.annual_curr_ass_last_year = 0
        self.annual_curr_lbt = 0
        self.annual_curr_lbt_last_year = 0
        self.annual_gross_profit = 0
        self.annual_gross_profit_ly = 0
        self.annual_total_revenue = 0
        self.annual_total_revenue_ly = 0

        self.f_balance_sheet_api = f_balance_sheet_api
        self.f_income_statement_api = f_income_statement_api
        self.f_cashflow_api = f_cashflow_api

        self.stock_db = StockDatabase()
        self.set_exchange()
        logging.getLogger().setLevel(logging.INFO)

    def reset_metric(self):
        self.ttm_net_income = 0
        self.ttm_net_income_last_year = 0
        self.ttm_roa = 0
        self.ttm_roa_last_year = 0
        self.ttm_avg_total_asset = 0
        self.ttm_avg_total_asset_ly = 0
        self.ttm_total_asset = 0
        self.ttm_total_asset_last_year = 0
        self.ttm_operating_cash_flow = 0
        self.ttm_operating_last_year = 0
        self.ttm_curr_ass = 0
        self.ttm_curr_ass_last_year = 0
        self.ttm_curr_lbt = 0
        self.ttm_curr_lbt_last_year = 0
        self.ttm_gross_profit = 0
        self.ttm_gross_profit_ly = 0
        self.ttm_total_revenue = 0
        self.ttm_total_revenue_ly = 0

    def reset_annual_metric(self):
        self.annual_net_income = 0
        self.annual_net_income_last_year = 0
        self.annual_roa = 0
        self.annual_roa_last_year = 0
        self.annual_avg_total_asset = 0
        self.annual_avg_total_asset_ly = 0
        self.annual__total_asset = 0
        self.annual_total_asset_last_year = 0
        self.annual_operating_cash_flow = 0
        self.annual_operating_last_year = 0
        self.annual_curr_ass = 0
        self.annual_curr_ass_last_year = 0
        self.annual_curr_lbt = 0
        self.annual_curr_lbt_last_year = 0
        self.annual_gross_profit = 0
        self.annual_gross_profit_ly = 0
        self.annual_total_revenue = 0
        self.annual_total_revenue_ly = 0

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

    def store_annual_piotroski_score(self, ticker, income_statement, balance_sheet, cashflow_statement, version=False):
        logging.info("Populating  Piotroski F score data for %s" % ticker)
        index = 0
        if self.annual_statement_has_no_record(balance_sheet, cashflow_statement, income_statement):
            return

        if len(income_statement) > 19 and len(balance_sheet) > 19 and len(cashflow_statement) > 19:
            limit = 18
        else:
            if len(income_statement) % 2 != 0:
                limit = len(income_statement) - 1
            else:
                limit = len(income_statement)
        alpha_balance_sheet = self.make_api_call_alpha_vantage(self.balance_sheet_api % ticker)["annualReports"]
        while index < limit:
            try:
                year = balance_sheet[index]["date"][:4]
                year = int(year)
                self.reset_annual_metric()
                self.set_annual_total_asset(balance_sheet, index)
                score = self.annual_is_return_on_asset_positive(income_statement, index) + \
                        self.annual_is_operating_cash_flow_positive(cashflow_statement,
                                                                    index) + self.annual_delta_roa_positive(
                    income_statement, index) \
                        + self.annual_cfo_greater_than_roa() + self.annual_decrease_long_term_debt(balance_sheet, index) \
                        + self.annual_is_current_ratio_positive(balance_sheet, index) + self.annual_shares_outstanding(
                    balance_sheet, index,alpha_balance_sheet) \
                        + self.annual_get_gross_margin(income_statement, index) + self.annual_asset_turnover(
                    balance_sheet, index)
            except Exception as e:
                logging.error(e.__str__())
                self.stock_db.insert_score_into_piotroski_table(ticker, 0, test=True, year=year)
                index += 1
                continue

            self.stock_db.insert_score_into_piotroski_table(ticker, score, test=True, year=year)
            logging.info("Piotrosci F score:%s\nYear:%s\n Ticker:%s\n" % (str(score), str(year) ,ticker))
            index += 1

    def get_score(self, ticker, balance_sheet,income_statement,cashflow):
        try:
            logging.info("Calculating score for %s" % ticker)
            self.reset_metric()
            self.set_total_asset(balance_sheet)
            self.set_avg_total_asset(balance_sheet)
            score = self.is_return_on_asset_positive(income_statement) + \
                    self.is_operating_cash_flow_positive(cashflow) + self.delta_roa_positive(income_statement) \
                    + self.cfo_greater_than_roa() + self.decrease_long_term_debt(balance_sheet) \
                    + self.is_current_ratio_positive(balance_sheet) + self.shares_outstanding(balance_sheet) \
                    + self.get_gross_margin(income_statement) + self.asset_turnover(balance_sheet)

        except Exception as e:
            logging.error(e.__str__())
            self.stock_db.insert_score_into_piotroski_table(ticker, 0, False)
            return 0
        self.stock_db.insert_score_into_piotroski_table(ticker, score, False)
        return score

    def statement_has_no_record(self, balance_sheet, income_statement, cashflow):
        if len(balance_sheet["quarterlyReports"]) == 0 or len(income_statement["quarterlyReports"]) == 0 or \
                len(cashflow["quarterlyReports"]) == 0:
            return True
        else:
            return False

    def annual_statement_has_no_record(self, balance_sheet, income_statement, cashflow):
        if len(balance_sheet) == 0 or len(income_statement) == 0 or \
                len(cashflow) == 0:
            return True
        else:
            return False

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

    def set_avg_total_asset(self, balance_sheet):
        iterator = 0
        while (iterator < 5):
            self.ttm_avg_total_asset += self.get_value(balance_sheet["quarterlyReports"][iterator]["totalAssets"])
            iterator += 1

        iterator = 4
        while (iterator < 9):
            self.ttm_avg_total_asset_ly += self.get_value(balance_sheet["quarterlyReports"][iterator]["totalAssets"])
            iterator += 1

        self.ttm_avg_total_asset = self.ttm_avg_total_asset / 5
        self.ttm_avg_total_asset_ly = self.ttm_avg_total_asset_ly / 5

    def set_total_asset(self, balance_sheet):
        self.ttm_total_asset = self.get_value(balance_sheet["quarterlyReports"][4]["totalAssets"])
        self.ttm_total_asset_last_year = self.get_value(balance_sheet["quarterlyReports"][8]["totalAssets"])

    def set_annual_total_asset(self, balance_sheet, index):
        self.annual_total_asset = self.get_value(balance_sheet[index]["totalAssets"])
        self.annual_total_asset_last_year = self.get_value(balance_sheet[index + 1]["totalAssets"])

    def is_return_on_asset_positive(self, income):
        iterator = 0
        while (iterator < 4):
            self.ttm_net_income += self.get_value(income["quarterlyReports"][iterator]["netIncome"])
            iterator += 1
        self.ttm_roa = float(self.ttm_net_income / self.ttm_total_asset)
        return int(self.ttm_roa > 0.0)

    def annual_is_return_on_asset_positive(self, income, index):
        self.annual_net_income = self.get_value(income[index]["netIncome"])
        self.annual_roa = float(self.annual_net_income / self.annual_total_asset)
        return int(self.annual_roa > 0.0)

    def is_operating_cash_flow_positive(self, cash_flow):
        iterator = 0
        while (iterator < 4):
            self.ttm_operating_cash_flow += self.get_value(cash_flow["quarterlyReports"][iterator]["operatingCashflow"])
            iterator += 1
        return int(self.ttm_operating_cash_flow > 0.0)

    def annual_is_operating_cash_flow_positive(self, cash_flow, index):
        self.annual_operating_cash_flow = self.get_value(cash_flow[index]["operatingCashFlow"])
        return int(self.annual_operating_cash_flow > 0.0)

    def delta_roa_positive(self, income):
        net_income = 0
        iterator = 4
        quarterly_reports = income["quarterlyReports"]
        while (iterator < 8):
            net_income += self.get_value(quarterly_reports[iterator]["netIncome"])
            iterator += 1
        self.ttm_roa_last_year = float(net_income / self.ttm_total_asset_last_year)
        delta = self.ttm_roa - self.ttm_roa_last_year
        return int(delta > 0.0)

    def annual_delta_roa_positive(self, income, index):
        self.annual_net_income_last_year = income[index + 1]["netIncome"]
        self.annual_roa_last_year = float(self.annual_net_income_last_year / self.annual_total_asset_last_year)
        delta = self.annual_roa - self.annual_roa_last_year
        return int(delta > 0.0)

    def cfo_greater_than_roa(self):
        ttm_cfo = float(self.ttm_operating_cash_flow / self.ttm_total_asset)
        delta = ttm_cfo - self.ttm_roa
        return int(delta > 0.0)

    def annual_cfo_greater_than_roa(self):
        annual_cfo = float(self.annual_operating_cash_flow / self.annual_total_asset)
        delta = annual_cfo - self.annual_roa
        return int(delta > 0.0)

    def is_current_ratio_positive(self, balance_sheet):
        self.ttm_curr_ass = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentAssets"])
        self.ttm_curr_lbt = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentLiabilities"])
        current_ratio = self.get_value(self.ttm_curr_ass / self.ttm_curr_lbt)
        self.ttm_curr_ass_last_year = self.get_value(balance_sheet["quarterlyReports"][4]["totalCurrentAssets"])
        self.ttm_curr_lbt_last_year = self.get_value(balance_sheet["quarterlyReports"][4]["totalCurrentLiabilities"])
        previous_ratio = float(self.ttm_curr_ass_last_year / self.ttm_curr_lbt_last_year)
        delta = current_ratio - previous_ratio
        return int(delta > 0)

    def annual_is_current_ratio_positive(self, balance_sheet, index):
        self.annual_curr_ass = self.get_value(balance_sheet[index]["totalCurrentAssets"])
        self.annual_curr_lbt = self.get_value(balance_sheet[index]["totalCurrentLiabilities"])
        current_ratio = self.get_value(self.annual_curr_ass / self.annual_curr_lbt)
        self.annual_curr_ass_last_year = self.get_value(balance_sheet[index + 1]["totalCurrentAssets"])
        self.annual_curr_lbt_last_year = self.get_value(balance_sheet[index + 1]["totalCurrentLiabilities"])
        previous_ratio = float(self.annual_curr_ass_last_year / self.annual_curr_lbt_last_year)
        delta = current_ratio - previous_ratio
        return int(delta > 0)

    def decrease_long_term_debt(self, balance_sheet):
        curr_ltd = self.get_value(balance_sheet["quarterlyReports"][0]["longTermDebt"]) / self.ttm_avg_total_asset
        las_ltd = self.get_value(balance_sheet["quarterlyReports"][4]["longTermDebt"]) / self.ttm_avg_total_asset_ly
        delta = curr_ltd - las_ltd
        return int(delta < 0.0)

    def annual_decrease_long_term_debt(self, balance_sheet, index):
        curr_ltd = self.get_value(balance_sheet[index]["longTermDebt"]) / self.annual_total_asset
        las_ltd = self.get_value(balance_sheet[index + 1]["longTermDebt"]) / self.annual_total_asset_last_year
        delta = curr_ltd - las_ltd
        return int(delta < 0.0)

    def get_gross_margin(self, income_statement):
        iterator = 0;
        while (iterator <= 3):
            self.ttm_gross_profit += self.get_value(income_statement["quarterlyReports"][iterator]["grossProfit"])
            self.ttm_total_revenue += self.get_value(income_statement["quarterlyReports"][iterator]["totalRevenue"])
            iterator += 1

        ttm_gross_margin_ratio = self.ttm_gross_profit / self.ttm_total_revenue

        while (iterator <= 7):
            self.ttm_gross_profit_ly += self.get_value(income_statement["quarterlyReports"][iterator]["grossProfit"])
            self.ttm_total_revenue_ly += self.get_value(income_statement["quarterlyReports"][iterator]["totalRevenue"])
            iterator += 1
        gross_margin_ratio = ttm_gross_margin_ratio - (self.ttm_gross_profit_ly / self.ttm_total_revenue_ly)

        return int(gross_margin_ratio > 0.0)

    def annual_get_gross_margin(self, income_statement, index):
        self.annual_gross_profit = self.get_value(income_statement[index]["grossProfit"])
        self.annual_total_revenue = self.get_value(income_statement[index]["revenue"])
        annual_gross_margin_ratio = self.annual_gross_profit / self.annual_total_revenue
        self.annual_gross_profit_ly = self.get_value(income_statement[index + 1]["grossProfit"])
        self.annual_total_revenue_ly = self.get_value(income_statement[index + 1]["revenue"])

        gross_margin_ratio = annual_gross_margin_ratio - (self.annual_gross_profit_ly / self.annual_total_revenue_ly)

        return int(gross_margin_ratio > 0.0)

    def asset_turnover(self, balance_sheet):
        ttm_asset = (self.get_value(balance_sheet["quarterlyReports"][0]["totalAssets"]) + self.get_value(
            balance_sheet["quarterlyReports"][4][
                "totalAssets"])) / 2
        ttm_asset_ly = (self.get_value(balance_sheet["quarterlyReports"][4]["totalAssets"]) + self.get_value(
            balance_sheet["quarterlyReports"][8][
                "totalAssets"])) / 2
        asset_turnover_ttm = float(self.ttm_total_revenue / ttm_asset)

        asset_turnover_ttm_ly = float(self.ttm_total_revenue_ly / ttm_asset_ly)
        delta_asset_turnover = asset_turnover_ttm - asset_turnover_ttm_ly
        return int(delta_asset_turnover > 0.0)

    def annual_asset_turnover(self, balance_sheet, index):
        annual_asset = self.get_value(balance_sheet[index]["totalAssets"])
        annual_asset_ly = self.get_value(balance_sheet[index + 1]["totalAssets"])
        asset_turnover_annual = float(self.annual_total_revenue / annual_asset)

        asset_turnover_annual_ly = float(self.annual_total_revenue_ly / annual_asset_ly)
        delta_asset_turnover = asset_turnover_annual - asset_turnover_annual_ly
        return int(delta_asset_turnover > 0.0)

    def get_value(self, query):
        value = query
        if value == "None":
            return 0
        else:
            return float(value)

    def shares_outstanding(self, balance_sheet):
        shares_outstanding_curr_quarter = self.get_value(
            balance_sheet["quarterlyReports"][0]["commonStockSharesOutstanding"])
        shares_outstanding_lq = self.get_value(balance_sheet["quarterlyReports"][4]["commonStockSharesOutstanding"])
        return int(int(shares_outstanding_curr_quarter) < int(shares_outstanding_lq))

    def annual_shares_outstanding(self, balance_sheet, index,alpha_balance_sheet):
        if index < len(alpha_balance_sheet) and index + 1 < len(alpha_balance_sheet):
            shares_outstanding_curr_quarter = self.get_value(
                alpha_balance_sheet[index]["commonStockSharesOutstanding"])
            shares_outstanding_lq = self.get_value(alpha_balance_sheet[index + 1]["commonStockSharesOutstanding"])
        else:
            shares_outstanding_curr_quarter = self.get_value(
                balance_sheet[index]["commonStock"])
            shares_outstanding_lq = self.get_value(balance_sheet[index + 1]["commonStock"])
        return int(int(shares_outstanding_curr_quarter) < int(shares_outstanding_lq))
#
# piotroski = PiotroskiScore()
# balance_sheet = piotroski.make_api_call_alpha_vantage(piotroski.f_balance_sheet_api % 'AMZN')
# income_statement = piotroski.make_api_call_alpha_vantage(piotroski.f_income_statement_api % 'AMZN')
# cashflow = piotroski.make_api_call_alpha_vantage(piotroski.f_cashflow_api % 'AMZN')
# piotroski.screen_stocks_backtest('AMZN',income_statement,balance_sheet,cashflow)
