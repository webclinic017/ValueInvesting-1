from backtest import Backtest
import strategy_sql
backtest = Backtest()


def create_portfolio_for_MF_Pscore_zscore_pm_ui_asc():
    year = 2008
    end_year = 2019

    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.MF_Pscore_zscore_pm_ui_asc % (year,next_year,year,year,backtest.exchange,year,next_year)
        backtest.populate_portfolio_as_per_strategy("MF+Pscore+zscore+pm+ui_asc",sql_query,next_year)
        backtest.get_roi("MF+Pscore+zscore+pm+ui_asc",next_year)
        year +=1

def create_portfolio_for_mf_pscore_zsc_ui_asc():
    year = 2008
    end_year = 2019

    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.mf_pscore_zsc_ui_asc % (
        year,year, year, backtest.exchange, year, next_year)
        backtest.populate_portfolio_as_per_strategy("mf_pscore_zsc_ui_asc", sql_query, next_year)
        backtest.get_roi("mf_pscore_zsc_ui_asc", next_year)
        year += 1

def create_portfolio_for_mf_pscore_zsc_ui_desc():
    year = 2008
    end_year = 2019

    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.mf_pscore_zsc_ui_desc % (
        year,year, year, backtest.exchange, year, next_year)
        backtest.populate_portfolio_as_per_strategy("mf_pscore_zsc_ui_desc", sql_query, next_year)
        backtest.get_roi("mf_pscore_zsc_ui_desc", next_year)
        year += 1

def create_portfolio_for_p_score_pm_zsc_ui_asc():
    year = 2008
    end_year = 2019

    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.p_score_pm_zsc_ui_asc % (
        year,next_year, backtest.exchange, year, next_year)
        backtest.populate_portfolio_as_per_strategy("p_score_pm_zsc_ui_asc", sql_query, next_year)
        backtest.get_roi("p_score_pm_zsc_ui_asc", next_year)
        year += 1

def create_portfolio_for_p_score_pm_zsc():
    year = 2008
    end_year = 2019

    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.p_score_pm_zsc % (
        year,next_year, backtest.exchange, next_year)
        backtest.populate_portfolio_as_per_strategy("p_score_pm_zsc", sql_query, next_year)
        backtest.get_roi("p_score_pm_zsc", next_year)
        year += 1

def create_portfolio_for_fcf_z_score_pm():
    year = 2010
    end_year = 2020
    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.fcf_z_score_pm % (
        backtest.exchange,year,year, next_year, next_year)
        backtest.populate_portfolio_as_per_strategy("fcf_z_score_pm", sql_query, next_year)
        backtest.get_roi("fcf_z_score_pm", next_year)
        year += 1

def create_portfolio_for_fcf_pm():
    year = 2010
    end_year = 2019
    while year <= end_year:
        next_year = year + 1
        sql_query = strategy_sql.fcf_pm % (
        backtest.exchange,year, next_year, next_year)
        backtest.populate_portfolio_as_per_strategy("fcf_pm", sql_query, next_year)
        backtest.get_roi("fcf_pm", next_year)
        year += 1

# create_portfolio_for_fcf_pm()



