import sqlite3
from sqlite3 import Error
from sql_queries import *
import logging
import zscore_sql_query


class StockDatabase:

    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)


    def set_logging_config(self,version = False):
        if not version:
            logging.basicConfig(filename=r'C:\temp\prod_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(filename=r'C:\temp\test_value_investing.log', filemode='w',
                                format='%(name)s - %(levelname)s - %(message)s')
    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(r"C:\sqlite\db\value_investing.db")
            return conn
        except Error as e:
            logging.error(e)

        return conn

    def delete_all_records(self,table):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(delete_all_from % table)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.fetchall()


    def create_companyInfo_table(self):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            logging.info("Create companyInfo Table")
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_create_company_table)
        except Error as e:
            logging.error(e)

    def create_ranked_result_table(self):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            logging.info("Create ranked Result Table")
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_create_rankedResult_table)
            conn.commit()
        except Error as e:
            logging.error(e)

    def create_historic_price_table(self):
        """ create a historic prices table
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_create_historic_prices_table)
            conn.commit()
        except Error as e:
            logging.error(e)

    def create_piotroski_table(self):
        """ create a historic prices table
                :param conn: Connection object
                :param create_table_sql: a CREATE TABLE statement
                :return:
                """
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_piotroski_fscore)
            conn.commit()
        except Error as e:
            logging.error(e)

    def create_zscore_table(self):
        """ create a historic prices table
                :param conn: Connection object
                :param create_table_sql: a CREATE TABLE statement
                :return:
                """
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_zscore)
            conn.commit()
        except Error as e:
            logging.error(e)

    def add_stock_to_db(self, company):
        """
        Create a new company into the companyInfo table
        :param conn:
        :param project:
        :return: company id
        """
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_insert_companyInfo, company)
            conn.commit()
            logging.info("Added %s to Database" % company)
            return cur.lastrowid
        except Error as e:
            logging.error(e)

    def create_test_tables(self):
        try:
            logging.info("Creating Table for Testing Environment")
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(test_create_companyInfo_table)
            c.execute(test__create_ranked_result_table)
            c.execute(test_delete_all_in_rankedResult)
            c.execute(test_piotroski_fscore)
            c.execute(zscore_sql_query.test_zscore)
            conn.commit()
        except Error as e:
            logging.error(e)

    def get_all_stocks(self):
        """
        Query all rows in the tasks table
        :param conn: the Connection object
        :return:
        """
        logging.info("Get all stocks from companyInfo table")
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_get_all_stocks)

        return cur.fetchall()

    def get_stocks_via_exchange(self, exchange):
        """
        gets all stocks for an exchange
        :param exchange:
        :return:
        """
        logging.info("Get all stocks from %s exchange" % exchange)
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_get_all_stocks_via_exchange % exchange)
        return cur.fetchall()

    def delete_all_stocks(self):
        logging.info("Delete all stocks from companyInfo")
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_delete_all_in_companyInfo)
        conn.commit()

    def delete_all_prices(self):
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_delete_prices)
        conn.commit()
        logging.info("Deleted all prices from HistoricPrice table")

    def delete_table(self, table):
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(sql_drop_table % table)
        conn.commit()
        logging.info("Deleted %s table" % table)

    def execute_command(self, statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(statement)
            conn.commit()
        except Exception as e:
            print(e)
        return cur.fetchall()

    def insert_into_ranked_result(self, statement, test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                result = self.execute_command(prod_get_rankedResult % statement[0])
                if not result:
                    cur.execute(prod_insert_rankedResult, statement)
                else:
                    cur.execute(prod_update_rankedresult % (statement[1],statement[2],statement[3],statement[4],statement[5],statement[6],statement[0]))
            else:
                cur.execute(test_insert_rankedResult, statement)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def update_ranking_result(self,statement, test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(prod_update_rank % (statement[1],statement[2],statement[3],statement[0]))
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def insert_price_momemtum(self,statement,test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                result = self.execute_command(prod_get_momentum % statement[0])
                if not result:
                    cur.execute(prod_insert_price_momemtum, statement)
                else:
                    cur.execute(prod_update_momemtum % (statement[1], statement[2], statement[0]))
            else:
                pass
            conn.commit()
        except Exception as e:
            logging.error(e)
            print(e)
        return cur.lastrowid


    def insert_historic_price(self, statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_insert_historic_prices, statement)
            conn.commit()
            logging.info("inserted price for %s\n Eoddate:%s" % (statement[1], statement[0]))
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def insert_score_into_piotroski_table(self, ticker, score, test=False,year =0):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                version = self.execute_command(prod_get_score_from_ticker % ticker)
                if not version:
                    statement = (ticker, 1, score)
                    cur.execute(prod_insert_into_piotroski, statement)
                else:
                    cur.execute(prod_update_piotroski % (score,ticker))
            else:
                version = self.execute_command(test_get_score_from_ticker % (ticker,year) )
                if not version[0][0]:
                    statement = (ticker, 1, score,year)
                    cur.execute(test_insert_into_piotroski, statement)
                else:
                    new_version = version[0][0] + 1
                    statement = (ticker, new_version, score,year)
                    cur.execute(test_insert_into_piotroski, statement)
            conn.commit()
            logging.info("Inserted score(%s) for %s" % (score,ticker))
        except Exception as e:
            logging.error(e)
        return cur.lastrowid


    def insert_into_zscore_table(self, ticker, score,year =0,test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                version = self.execute_command(zscore_sql_query.prod_get_score_from_ticker % ticker)
                if not version:
                    statement = (ticker, 1, score)
                    cur.execute(zscore_sql_query.prod_insert_into_zscore, statement)
                else:
                    cur.execute(zscore_sql_query.prod_update_into_zscore % (score,ticker))
            else:
                version = self.execute_command(zscore_sql_query.test_get_score_from_ticker % (ticker,year))
                if not version[0][0]:
                    statement = (ticker, 1, score,year)
                    cur.execute(zscore_sql_query.test_insert_into_zscore, statement)
                else:
                    new_version = version[0][0] + 1
                    statement = (ticker, new_version, score,year)
                    cur.execute(zscore_sql_query.test_insert_into_zscore, statement)
            conn.commit()
            logging.info("Inserted score(%s) for %s" % (score,ticker))
        except Exception as e:
            logging.error(e)
            print(e)
        return cur.lastrowid


    def insert_into_ulcer_table(self, ticker, three_score , six_mth, ttm_mth,year =0):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            version = self.execute_command(prod_get_max_version_from_ulcer % (ticker,year))
            if not version:
                statement = (ticker, year,1, ttm_mth,three_score,six_mth,ttm_mth)
                cur.execute(prod_insert_into_ulcer, statement)
            else:
                cur.execute(prod_update_ttm_ulcer_index % (three_score,six_mth,ttm_mth,ticker,year))
            conn.commit()
            logging.info("Inserted ulcer score for %s" % (ticker))
            return cur.lastrowid
        except Exception as e:
            logging.error(e)
            print(e)

    def insert_into_peg(self,ticker,peg,year):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            version = self.execute_command(prod_get_peg % (ticker,year))
            if not version:
                statement = (ticker,peg,year)
                cur.execute(prod_insert_into_peg, statement)
            else:
                cur.execute(prod_update_peg % (peg,ticker,year))
            conn.commit()
            logging.info("Inserted peg ratio for %s" % (ticker))
            return cur.lastrowid
        except Exception as e:
            logging.error(e)
            print(e)

    def insert_into_ratio(self,ticker,roe,fair_value,div_yield,fcf_per_share,year):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            version = self.execute_command(prod_get_ratio % (ticker,year))
            if not version:
                statement = (ticker,roe,fair_value,div_yield,fcf_per_share,year)
                cur.execute(prod_insert_into_ratio, statement)
            else:
                cur.execute(prod_update_ratio % (roe,fair_value,div_yield,fcf_per_share,ticker,year))
            conn.commit()
            logging.info("Inserted ratio for %s" % (ticker))
            return cur.lastrowid
        except Exception as e:
            logging.error(e)
            print(e)


    def get_stocks_per_exchange(self, exchange, test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                statement = r"SELECT * from companyInfo where exchange = '%s'" % exchange
                cur.execute(statement)
            else:
                cur.execute(test_get_all_stocks_per_exhg)
            conn.commit()
            logging.info("get stocks as per %s Exchange" % exchange)
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

    def drop_historic_price_table(self):
        try:
            table = "historicPrices"
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(sql_drop_table % table)
            conn.commit()
            logging.info("HistoricPrice table dropped")
        except Exception as e:
            logging.error(e)

    def get_historical_prices_per_ticker(self, ticker):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_get_all_prices_per_ticker % ticker)
            conn.commit()
            logging.info("Retrieved historical prices for %s" % ticker)
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

    def get_historical_prices(self):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_get_all_prices)
            conn.commit()
            logging.info("Retrieved all Historical prices")
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

    def get_piotroski_screened_stocks(self,version,exchange):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not version:
                cur.execute(test_get_piotroski_screened_stock)
                conn.commit()
            else:
                cur.execute(prod_get_piotroski_screened_stock % exchange)
                conn.commit()
            logging.info("Retrieved all Piotroski screened stocks")
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

    def get_ranked_result(self,year,test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(prod_get_ranked_result% year)
                conn.commit()
            else:
                cur.execute(test_get_ranked_result % year)
                conn.commit()
            logging.info("Retrieved all magic formula result")
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

    def get_port_strat(self,statement,test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(statement)
                conn.commit()
            else:
                cur.execute(statement)
                conn.commit()
            logging.info("Retrieved Porfolios ")
        except Exception as e:
            logging.error(e)
        return cur.fetchall()



    def update_ranked_result(self, statement, test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(statement)
            else:
                cur.execute(statement)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.lastrowid


    def insert_into_portfolio(self,statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(insert_into_portfolio, statement)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.lastrowid


    def insert_into_stock_return_table(self,statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            result = self.execute_command(prod_get_stock_return % (statement[0],statement[1]))
            if not result:
                cur.execute(prod_insert_into_stock, statement)
            else:
                cur.execute(prod_update_return % (statement[2],statement[0],statement[1]))
            conn.commit()
            logging.info("Updated stock return for %s" % statement[0] )
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def insert_into_portfolio_return(self,statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_insert_into_portfolio_return, statement)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def get(self, statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(statement)
            conn.commit()
        except Exception as e:
            logging.error(e)
        return cur.fetchall()

