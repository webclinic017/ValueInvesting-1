import sqlite3
from sqlite3 import Error
from sql_queries import *
import logging


class StockDatabase:

    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        pass
        # self.create_companyInfo_table()
        # self.create_ranked_result_table()

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
                cur.execute(prod_insert_rankedResult, statement)
            else:
                cur.execute(test_insert_rankedResult, statement)
            conn.commit()
            logging.info("inserted ranking Info into table")
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def insert_historic_price(self, statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_insert_historic_prices, statement)
            conn.commit()
            logging.info("inserted price for %s\n Eoddate:%s" %(statement[1],statement[0]))
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

    def get_stocks_per_exchange(self, exchange, test=False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(prod_get_all_stocks_per_exhg % exchange)
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


    def insert_piotroski_f_score(self,statement):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(prod_insert_historic_prices, statement)
            conn.commit()
            logging.info("inserted score for %s\n Eoddate:%s" %(statement[1],statement[0]))
        except Exception as e:
            logging.error(e)
        return cur.lastrowid

