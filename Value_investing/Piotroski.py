from ratelimit import limits, RateLimitException, sleep_and_retry
import requests
import logging


class PiotroskiScore:
    def __init__(self):
        self.api_key = r'U3XAKFVJ5I3WW3YH'
        self.balance_sheet_api = r'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.income_statement = r'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.cashflow = r'https://www.alphavantage.co/query?function=CASH_FLOW&symbol=%s&apikey=U3XAKFVJ5I3WW3YH'
        self.ttm_net_income = 0
        self.ttm_net_income_last_year = 0
        self.ttm_roa = 0
        self.ttm_roa_last_year = 0
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

    def reset_metric(self):
        self.ttm_net_income = 0
        self.ttm_net_income_last_year = 0
        self.ttm_roa = 0
        self.ttm_roa_last_year = 0
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

    def calculate_score(self, ticker):
        try:
            self.reset_metric()
            balance_sheet = self.make_api_call_alpha_vantage(self.balance_sheet_api % ticker)
            income_statement = self.make_api_call_alpha_vantage(self.income_statement % ticker)
            cashflow = self.make_api_call_alpha_vantage(self.cashflow % ticker)
            if self.statement_has_no_record(balance_sheet,cashflow,income_statement):
                return 0
            else:
                score = self.is_return_on_asset_positive(balance_sheet , income_statement) + \
                        self.is_operating_cash_flow_positive(cashflow) + self.delta_roa_positive(balance_sheet,income_statement) \
                        + self.cfo_greater_than_roa() + self.decrease_long_term_debt(balance_sheet) \
                        + self.is_current_ratio_positive(balance_sheet) + self.shares_outstanding(balance_sheet) \
                        + self.get_gross_margin(income_statement) + self.asset_turnover(balance_sheet)
        except Exception as e:
            return 0

        return score

    def statement_has_no_record(self,balance_sheet,income_statement,cashflow):
        if len(balance_sheet["quarterlyReports"]) == 0 or len(income_statement["quarterlyReports"]) == 0 or \
                len(cashflow["quarterlyReports"]) == 0 :
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

    def is_return_on_asset_positive(self, balance_sheet, income):
        iterator = 0
        while (iterator < 4):
            self.ttm_net_income += self.get_value(income["quarterlyReports"][iterator]["netIncome"])
            self.ttm_total_asset += self.get_value(balance_sheet["quarterlyReports"][iterator]["totalAssets"])
            iterator += 1
        self.ttm_total_asset = self.ttm_total_asset / 4
        self.ttm_roa = float(self.ttm_net_income / self.ttm_total_asset)
        return int(self.ttm_roa > 0.0)

    def is_operating_cash_flow_positive(self, cash_flow):
        iterator = 0
        while (iterator < 4):
            self.ttm_operating_cash_flow += self.get_value(cash_flow["quarterlyReports"][iterator]["operatingCashflow"])
            iterator += 1
        return int(self.ttm_operating_cash_flow > 0.0)

    def delta_roa_positive(self, balance_sheet, income):
        net_income = 0
        iterator = 4
        quarterly_reports = income["quarterlyReports"]
        while (iterator < 8):
            net_income += self.get_value(quarterly_reports[iterator]["netIncome"])
            self.ttm_total_asset_last_year += self.get_value(balance_sheet["quarterlyReports"][iterator]["totalAssets"])
            iterator += 1
        self.ttm_total_asset_last_year = self.ttm_total_asset_last_year / 4
        self.ttm_roa_last_year = float(net_income / self.ttm_total_asset_last_year)
        delta = self.ttm_roa - self.ttm_roa_last_year
        return int(delta > 0.0)

    def cfo_greater_than_roa(self):
        ttm_cfo = float(self.ttm_operating_cash_flow / self.ttm_total_asset)
        delta = ttm_cfo - self.ttm_roa
        return int(delta > 0.0)

    def is_current_ratio_positive(self, balance_sheet):
        self.ttm_curr_ass = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentAssets"])
        self.ttm_curr_lbt = self.get_value(balance_sheet["quarterlyReports"][0]["totalCurrentLiabilities"])
        current_ratio = self.get_value(self.ttm_curr_ass / self.ttm_curr_lbt)
        self.ttm_curr_ass_last_year = self.get_value(balance_sheet["quarterlyReports"][1]["totalCurrentAssets"])
        self.ttm_curr_lbt_last_year = self.get_value(balance_sheet["quarterlyReports"][1]["totalCurrentLiabilities"])
        previous_ratio = float(self.ttm_curr_ass_last_year / self.ttm_curr_lbt_last_year)
        delta = current_ratio - previous_ratio
        return int(delta > 0)

    def decrease_long_term_debt(self, balance_sheet):
        curr_ltd = self.get_value(balance_sheet["quarterlyReports"][0]["longTermDebt"])
        las_ltd = self.get_value(balance_sheet["quarterlyReports"][1]["longTermDebt"])
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

    def asset_turnover(self, balance_sheet):
        ttm_asset = (self.get_value(balance_sheet["quarterlyReports"][0]["totalAssets"]) + self.get_value(balance_sheet["quarterlyReports"][3][
            "totalAssets"])) / 2
        ttm_asset_ly = (self.get_value(balance_sheet["quarterlyReports"][4]["totalAssets"]) +self.get_value(balance_sheet["quarterlyReports"][7][
            "totalAssets"])) / 2
        asset_turnover_ttm = float(self.ttm_total_revenue / ttm_asset)

        asset_turnover_ttm_ly = float(self.ttm_total_revenue_ly / ttm_asset_ly)
        delta_asset_turnover = asset_turnover_ttm - asset_turnover_ttm_ly
        return int(delta_asset_turnover > 0.0)

    def get_value(self, query):
        value = query
        if value == "None":
            return 0
        else:
            return float(value)

    def shares_outstanding(self, balance_sheet):
        shares_outstanding_curr_quarter = self.get_value(balance_sheet["quarterlyReports"][0]["commonStockSharesOutstanding"])
        shares_outstanding_lq = self.get_value(balance_sheet["quarterlyReports"][1]["commonStockSharesOutstanding"])
        return int(int(shares_outstanding_curr_quarter) < int(shares_outstanding_lq))


# piotroski = PiotroskiScore()
# score = piotroski.calculate_score("ACA")
# print(score)