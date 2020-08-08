
prod_get_all_stocks_per_exhg = """
                    SELECT * FROM companyInfo i
                    where i.exchange= '%s';
                    """

prod_insert_rankedResult = ''' INSERT INTO rankedResult(rundate,ticker,EBIT,TotalFixedAsset,WorkingCapital,EnterpriseValue,EarningYield,
            ReturnOnCapital,EarningRank,RocRank,FinalRank)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?) '''

prod_insert_companyInfo = ''' INSERT INTO companyInfo(name,ticker,description,industry,marketCapitalization,exchange)
                  VALUES(?,?,?,?,?,?) '''

prod_get_all_stocks = "SELECT * FROM companyInfo"

prod_get_all_stocks_via_exchange = "SELECT * FROM companyInfo where exchange =%s"

sql_drop_table = "DROP TABLE %s"

prod_delete_all_in_companyInfo = "DELETE FROM companyInfo"

prod_create_rankedResult_table = """ CREATE TABLE IF NOT EXISTS rankedResult (
                                                            id integer PRIMARY KEY,
                                                            rundate integer NOT NULL,
                                                            ticker text NOT NULL,
                                                            EBIT integer NOT NULL,
                                                            TotalFixedAsset integer NOT NULL,
                                                            WorkingCapital integer NOT NULL,
                                                            EnterpriseValue integer NOT NULL,
                                                            EarningYield text NOT NULL,
                                                            ReturnOnCapital text NOT NULL,
                                                            EarningRank integer NOT NULL,
                                                            RocRank integer NOT NULL,
                                                            FinalRank integer NOT NULL,
                                                            FOREIGN KEY (ticker)
                                                                REFERENCES companyInfo (ticker) 
                                                        ); """

prod_create_company_table = """ CREATE TABLE IF NOT EXISTS companyInfo (
                                                       id integer PRIMARY KEY,
                                                       name text NOT NULL,
                                                       ticker text NOT NULL UNIQUE,
                                                       description text NOT NULL,
                                                       industry text NOT NULL,
                                                       marketCapitalization integer NOT NULL,
                                                       exchange text NOT NULL
                                                   ); """


prod_create_historic_prices_table = """ CREATE TABLE IF NOT EXISTS historicPrices (
                                                       id integer PRIMARY KEY,
                                                       eoddate integer NOT NULL,
                                                       ticker text NOT NULL,
                                                       open real NOT NULL,
                                                       high real NOT NULL,
                                                       low real NOT NULL,
                                                       close real NOT NULL,
                                                       adjustedClose real NOT NULL,
                                                       volume integer NOT NULL,
                                                       FOREIGN KEY (ticker)
                                                                REFERENCES companyInfo (ticker)
                                                   ); """


prod_insert_historic_prices = """
INSERT INTO historicPrices(eoddate,ticker,open,high,low,close,adjustedClose,
            volume)
                              VALUES(?,?,?,?,?,?,?,?)
"""

prod_delete_prices  = "DELETE FROM historicPrices"

test_create_companyInfo_table = """ CREATE TABLE IF NOT EXISTS TestCompanyInfo (
                                                       id integer PRIMARY KEY,
                                                       name text NOT NULL,
                                                       ticker text NOT NULL UNIQUE,
                                                       description text NOT NULL,
                                                       industry text NOT NULL,
                                                       marketCapitalization integer NOT NULL,
                                                       exchange text NOT NULL
                                                   ); """

test__create_ranked_result_table = """ CREATE TABLE IF NOT EXISTS TestRankedResult (
                                                                  id integer PRIMARY KEY,
                                                                  rundate integer NOT NULL,
                                                                  ticker text NOT NULL,
                                                                  EBIT integer NOT NULL,
                                                                  TotalFixedAsset integer NOT NULL,
                                                                  WorkingCapital integer NOT NULL,
                                                                  EnterpriseValue integer NOT NULL,
                                                                  EarningYield text NOT NULL,
                                                                  ReturnOnCapital text NOT NULL,
                                                                  EarningRank integer NOT NULL,
                                                                  RocRank integer NOT NULL,
                                                                  FinalRank integer NOT NULL,
                                                                  FOREIGN KEY (ticker)
                                                                      REFERENCES TestCompanyInfo (ticker) 
                                                              ); """

test_insert_companyInfo= """
INSERT INTO TestCompanyInfo(id,name,ticker,description,industry,marketCapitalization,exchange)
                  select id, name,ticker,description,industry,marketCapitalization,exchange from companyInfo c
                  WHERE c.exchange = 'US' LIMIT 5
"""

test_insert_rankedResult = """
INSERT INTO TestRankedResult(rundate,ticker,EBIT,TotalFixedAsset,WorkingCapital,EnterpriseValue,EarningYield,
            ReturnOnCapital,EarningRank,RocRank,FinalRank)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?)
"""

test_get_all_stocks_per_exhg = """
                    SELECT * FROM TestCompanyInfo i;
                    """

test_delete_all_in_rankedResult = "DELETE FROM TestRankedResult"
test_delete_all_in_companyInfo = "DELETE FROM TestCompanyInfo"





