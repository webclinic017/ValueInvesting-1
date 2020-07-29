import requests
import json
import time
from backoff import on_exception, expo
from ratelimit import limits, RateLimitException, sleep_and_retry

def search_metal_mining():
    final_json = list()
       # with open(r'.\stock_json', 'r') as f_read:
    #     json_file = json.load(f_read)
    #     iter_onj = iter(json_file)
    #     while True:
    #         try:
    #             element = next(iter_onj)
    #             get_data(element, final_json)
    #         except StopIteration:
    #             break

    with open(r'.\hong_kong_stocks.json', 'r') as f_read1:
        json_file = json.load(f_read1)
        iter_onj = iter(json_file)
        while True:
            try:
                element = next(iter_onj)
                get_date_mining(element, final_json)
            except StopIteration:
                break

    with open(r'.\filtered_hk_stock.json' , 'w') as f_write:
        json.dump(final_json,f_write)


def get_data(element,final_json):
    try:
        symbol = element['symbol']
        r = make_api_call('https://finnhub.io/api/v1/stock/profile2?symbol=%s&token=brmngnnrh5re15om7qd0' % symbol)
        if not r:
            pass
        else:
           if r["finnhubIndustry"] == 'Metals & Mining':
                company = {"description": element["description"], "ticker": element['symbol']}
                final_json.append(company)
    except Exception as e:
        time.sleep(5)


def get_date_mining(element,final_json):
    try:
        symbol = element['symbol']
        r = make_api_call('https://finnhub.io/api/v1/stock/profile2?symbol=%s&token=brmngnnrh5re15om7qd0' % symbol)
        if not r:
            pass
        else:
            if r["finnhubIndustry"] == 'Utilities' or r["finnhubIndustry"] == 'Financial Services':
                return
            if r["marketCapitalization"] >= 50:
                company = {"description": element["description"], "ticker": element['symbol']}
                final_json.append(company)
    except Exception as e:
        time.sleep(5)

@sleep_and_retry
@limits(calls=60, period=60)
def make_api_call(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception('API response: {}'.format(response.status_code))
    return response.json()

search_metal_mining()










