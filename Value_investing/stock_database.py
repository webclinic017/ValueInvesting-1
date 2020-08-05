import sqlite3
from sqlite3 import Error
from sql_queries import *


class StockDatabase:

    def __init__(self):
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
            print(e)

        return conn

    def create_companyInfo_table(self):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_create_company_table)
        except Error as e:
            print(e)

    def create_ranked_result_table(self):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(prod_create_rankedResult_table)
        except Error as e:
            print(e)

    def add_stock_to_db(self, company):
        """
        Create a new company into the companyInfo table
        :param conn:
        :param project:
        :return: company id
        """
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_insert_companyInfo, company)
        conn.commit()
        return cur.lastrowid

    def create_test_tables(self):
        try:
            conn = self.create_connection()
            c = conn.cursor()
            c.execute(test_create_companyInfo_table)
            c.execute(test__create_ranked_result_table)
            c.execute(test_delete_all_in_rankedResult)
            c.execute(test_delete_all_in_companyInfo)
            c.execute(test_insert_companyInfo)
        except Error as e:
            print(e)

    def get_all_stocks(self):
        """
        Query all rows in the tasks table
        :param conn: the Connection object
        :return:
        """
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
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute( prod_get_all_stocks_via_exchange % exchange)
        return cur.fetchall()

    def delete_all_stocks(self):
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(prod_delete_all_in_companyInfo)

    def delete_table(self, table):
        conn = self.create_connection()
        cur = conn.cursor()
        cur.execute(sql_drop_table % table)
        conn.commit()

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
                cur.execute(test_insert_rankedResult,statement)
            conn.commit()
        except Exception as e:
            print(e)
        return cur.lastrowid

    def get_stocks_per_exchange(self,exchange,test= False):
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            if not test:
                cur.execute(prod_get_all_stocks_per_exhg % exchange)
            else:
                cur.execute(test_get_all_stocks_per_exhg)
            conn.commit()
        except Exception as e:
            print(e)
        return cur.fetchall()


