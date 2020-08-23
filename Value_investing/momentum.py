import datetime
import datetime
import dateutil.relativedelta
import requests
from ratelimit import limits, RateLimitException, sleep_and_retry

d = datetime.datetime.strptime("2013-03-31", "%Y-%m-%d")
d2 = d - dateutil.relativedelta.relativedelta(months=1)


class Momentum:

    def __init__(self):
        self.today_date = self.set_working_date()
        self.api_query = r"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&outputsize=full&apikey=U3XAKFVJ5I3WW3YH"

    def set_working_date(self):
        today_date = datetime.datetime.today().weekday() + 1
        if today_date > 5:
            delta = 7 - today_date
            return (datetime.datetime.today() - dateutil.relativedelta.relativedelta(days=delta))
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
        date_tweleve_mth = self.get_valid_date(date_12_months_ago, prices)
        current_price = float(prices["Time Series (Daily)"][date_now]["5. adjusted close"])
        price_twelve_mths = float(prices["Time Series (Daily)"][date_tweleve_mth]["5. adjusted close"])
        return ((current_price / price_twelve_mths) - 1) * 100

    def get_3_month_return(self, ticker):
        prices = self.make_api_call_alpha_vantage(ticker)["Time Series (Daily)"]
        date_3_months_ago = (self.today_date - dateutil.relativedelta.relativedelta(months=3))
        date_now = self.get_valid_date(self.today_date, prices)
        date_3_mth = self.get_valid_date(date_3_months_ago, prices)
        current_price = float(prices[date_now]["5. adjusted close"])
        price_3_mths = float(prices[date_3_mth]["5. adjusted close"])
        return ((current_price / price_3_mths) - 1) * 100

    def get_valid_date(self, date, prices):
        curr_date = date
        while (not self.date_valid(prices, curr_date)):
            curr_date = curr_date - dateutil.relativedelta.relativedelta(days=1)
        return curr_date.strftime("%Y-%m-%d")

    def date_valid(self, prices, date):
        str_date = date.strftime("%Y-%m-%d")
        if str_date in prices:
            return True
        else:
            return False

    @sleep_and_retry
    @limits(calls=30, period=60)
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