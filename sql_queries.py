prod_get_all_stocks_per_exhg = r"""
                    SELECT * FROM companyInfo i
                    where i.exchange= '%s' and description not like '%%ADR%%';
                    """

prod_insert_rankedResult = ''' INSERT INTO rankedResult(ticker,EBIT,TotalFixedAsset,WorkingCapital,EnterpriseValue,EarningYield,
            ReturnOnCapital)
                              VALUES(?,?,?,?,?,?,?) '''
prod_update_rank = """
UPDATE rankedResult SET EarningRank=%s,RocRank=%s,FinalRank=%s WHERE ticker = '%s'
"""

prod_insert_companyInfo = ''' INSERT INTO companyInfo(name,ticker,description,industry,marketCapitalization,exchange)
                  VALUES(?,?,?,?,?,?) '''

prod_get_all_stocks = "SELECT * FROM companyInfo"

prod_get_all_stocks_via_exchange = "SELECT * FROM companyInfo where exchange ='%s'"

sql_drop_table = "DROP TABLE %s"

prod_delete_all_in_companyInfo = "DELETE FROM companyInfo"

prod_create_rankedResult_table = """ CREATE TABLE IF NOT EXISTS rankedResult (
                                                            id integer PRIMARY KEY,
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

prod_piotroski_fscore = """  CREATE TABLE IF NOT EXISTS "PiotroskiFscore" (
                                "id"	INTEGER NOT NULL,
                                "ticker"	TEXT NOT NULL,
                                "version"	INTEGER NOT NULL,
                                "score"	INTEGER NOT NULL,
                                PRIMARY KEY("id" AUTOINCREMENT),
                                FOREIGN KEY("ticker")
                                        REFERENCES companyInfo (ticker)
);
"""

test_piotroski_fscore = """  CREATE TABLE IF NOT EXISTS "PiotroskiFscoreTest" (
                                "id"	INTEGER NOT NULL,
                                "ticker"	TEXT NOT NULL,
                                "version"	INTEGER NOT NULL,
                                "score"	INTEGER NOT NULL,
                                "year" INTEGER NOT NULL,
                                PRIMARY KEY("id" AUTOINCREMENT),
                                FOREIGN KEY("ticker")
                                        REFERENCES companyInfo (ticker)
);
"""

prod_zscore = """  CREATE TABLE IF NOT EXISTS "Zscore" (
                                "id"	INTEGER NOT NULL,
                                "ticker"	TEXT NOT NULL,
                                "version"	INTEGER NOT NULL,
                                "score"	INTEGER NOT NULL,
                                PRIMARY KEY("id" AUTOINCREMENT),
                                FOREIGN KEY("ticker")
                                        REFERENCES companyInfo (ticker)
);
"""

test_zscore = """  CREATE TABLE IF NOT EXISTS "ZscoreTest" (
                                "id"	INTEGER NOT NULL,
                                "ticker"	TEXT NOT NULL,
                                "version"	INTEGER NOT NULL,
                                "score"	INTEGER NOT NULL,
                                "year" INTEGER NOT NULL,
                                PRIMARY KEY("id" AUTOINCREMENT),
                                FOREIGN KEY("ticker")
                                        REFERENCES companyInfo (ticker)
);
"""

test_get_piotroski_screened_stock = """
select c.*,p.score
from  companyInfo c
inner join PiotroskiFscoreTest p on p.ticker = c.ticker
inner join (
    select pf.ticker, max(version) as MaxVersion
    from PiotroskiFscoreTest pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
where p.score > 6
"""

TEMP_SQL = """
SELECT * FROM companyInfo where ticker >= 'ORSN' AND EXCHANGE = 'US'
"""

prod_get_piotroski_screened_stock = """
select c.*,p.score
from  companyInfo c
inner join PiotroskiFscore p on p.ticker = c.ticker
inner join (
    select pf.ticker, max(version) as MaxVersion
    from PiotroskiFscore pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
where p.score > 6 AND c.exchange='%s'
"""

prod_insert_into_piotroski = """
INSERT INTO PiotroskiFscore(ticker,version,score)
                              VALUES(?,?,?)
"""

prod_update_piotroski = """
UPDATE PiotroskiFscore SET score =%s where ticker= '%s'
"""


prod_get_latest_scores_from_piotroski = """
select p.ticker, p.score
from PiotroskiFscore p
inner join (
    select pf.ticker, max(version) as MaxVersion
    from PiotroskiFscore pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
"""

test_get_latest_scores_from_piotroski = """
select p.ticker, p.score,p.year
from PiotroskiFscoreTest p
inner join (
    select pf.ticker, max(version) as MaxVersion
    from PiotroskiFscoreTest pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
"""
test_insert_into_piotroski = """
INSERT INTO PiotroskiFscoreTest(ticker,version,score,year)
                              VALUES(?,?,?,?)
"""

prod_get_score_from_ticker = """
SELECT score from PiotroskiFscore where ticker='%s'
"""

test_get_score_from_ticker = """
SELECT MAX(version) from PiotroskiFscoreTest where ticker='%s' and year = %s
"""

prod_insert_historic_prices = """
INSERT INTO historicPrices(eoddate,ticker,open,high,low,close,adjustedClose,
            volume)
                              VALUES(?,?,?,?,?,?,?,?)
"""

prod_delete_prices = "DELETE FROM historicPrices"

prod_get_all_prices_per_ticker = """
                    SELECT * FROM historicPrices where ticker = '%s';
                    """

prod_get_all_prices = """
                    SELECT * FROM historicPrices;
                    """

test_create_companyInfo_table = """ CREATE TABLE IF NOT EXISTS TestCompanyInfo (
                                                       id integer PRIMARY KEY,
                                                       name text NOT NULL,
                                                       ticker text NOT NULL UNIQUE,
                                                       description text NOT NULL,
                                                       industry text NOT NULL,
                                                       marketCapitalization integer NOT NULL,
                                                       exchange text NOT NULL
                                                   ); """

test__create_ranked_result_table = """ CREATE TABLE IF NOT EXISTS RankedResultTest (
                                                                  id integer PRIMARY KEY,
                                                                  year integer NOT NULL,
                                                                  ticker text NOT NULL,
                                                                  EBIT integer NOT NULL,
                                                                  TotalFixedAsset integer NOT NULL,
                                                                  WorkingCapital integer NOT NULL,
                                                                  EnterpriseValue integer NOT NULL,
                                                                  EarningYield text NOT NULL,
                                                                  ReturnOnCapital text NOT NULL,
                                                                  EarningRank integer,
                                                                  RocRank integer,
                                                                  FinalRank integer,
                                                                  FOREIGN KEY (ticker)
                                                                      REFERENCES CompanyInfo (ticker) 
                                                              ); """

test_create_priceMomemtum = """ CREATE TABLE IF NOT EXISTS PriceMomemtumTest (
                                                                  id integer PRIMARY KEY,
                                                                  year integer NOT NULL,
                                                                  ticker text NOT NULL,
                                                                  ThreeMonths integer NOT NULL,
                                                                  SixMonth integer NOT NULL,
                                                                  FOREIGN KEY (ticker)
                                                                      REFERENCES CompanyInfo (ticker) 
                                                              ); """

prod_create_priceMomemtum = """ CREATE TABLE IF NOT EXISTS PriceMomemtum (
                                                                  id integer PRIMARY KEY,
                                                                  ticker text NOT NULL,
                                                                  ThreeMonths integer NOT NULL,
                                                                  SixMonth integer NOT NULL,
                                                                  FOREIGN KEY (ticker)
                                                                      REFERENCES CompanyInfo (ticker) 
                                                              ); """

test_insert_price_momemtum = """
INSERT INTO PriceMomemtumTest(year,ticker,ThreeMonths,SixMonth)
                  VALUES(?,?,?,?)
"""

prod_insert_price_momemtum = """
INSERT INTO PriceMomemtum(ticker,ThreeMonths,SixMonth)
                  VALUES(?,?,?)
"""
prod_get_momentum = """
select sixMonth from PriceMomemtum where ticker = '%s'
"""
prod_update_momemtum = """
UPDATE PriceMomemtum SET ThreeMonths = %s, SixMonth=%s
where ticker='%s'
"""

test_insert_companyInfo = """
INSERT INTO TestCompanyInfo(id,name,ticker,description,industry,marketCapitalization,exchange)
                  select id, name,ticker,description,industry,marketCapitalization,exchange from companyInfo c
                  WHERE c.exchange = 'US' LIMIT 5
"""

test_insert_rankedResult = """
INSERT INTO RankedResultTest(year,ticker,EBIT,TotalFixedAsset,WorkingCapital,EnterpriseValue,EarningYield,
            ReturnOnCapital)
                              VALUES(?,?,?,?,?,?,?,?)
"""

test_update_rankedresulttest = """
UPDATE RankedResultTest 
SET EarningRank = %s, RocRank = %s, FinalRank = %s
WHERE ticker = %s and year = %s;
"""

prod_update_rankedresult = """
UPDATE rankedResult 
SET EBIT =%s, TotalFixedAsset=%s, WorkingCapital=%s, EnterpriseValue=%s, EarningYield='%s',ReturnOnCapital ='%s'
WHERE ticker = '%s';
"""

prod_update_ranks = """
UPDATE rankedResult EarningRank = %s, RocRank = %s, FinalRank = %s
WHERE ticker = '%s';
"""

prod_get_rankedResult = """
select ticker from  rankedResult where  ticker = '%s'
"""

test_get_all_stocks_per_exhg = """
                    SELECT * FROM TestCompanyInfo i;
                    """
delete_all_from = """
DELETE FROM %s;
"""
test_delete_all_in_rankedResult = "DELETE FROM TestRankedResult"
test_delete_all_in_companyInfo = "DELETE FROM TestCompanyInfo"

get_strategy_result = """
SELECT * from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
where p.year = 2018 and p.score >= 7 and pm.year = 2019 and rr.year = 2018
order by pm.SixMonth desc
"""

test_get_ranked_result = """
SELECT ticker,EarningYield,ReturnOnCapital from RankedResultTest where  year = %s and TotalFixedAsset != 0
"""

prod_get_ranked_result = """
SELECT ticker,EarningYield,ReturnOnCapital from RankedResult where  year = %s and TotalFixedAsset != 0
"""

test_update_rank_table = """
UPDATE RankedResultTest
SET EarningRank = %s, RocRank = %s, FinalRank = %s
WHERE ticker = '%s' and year = %s;
"""

prod_update_rank_table = """
UPDATE RankedResultTest
SET EarningRank = %s, RocRank = %s, FinalRank = %s
WHERE ticker = '%s' and year = %s;
"""

test_get_strategy_p_mf_pm_z = """
with portfolio as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score, pm.SixMonth, pm.ThreeMonths from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pm.year = %s and rr.year = %s and zt.score > 4 and zt.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by pm.SixMonth desc limit 40) 
select * from portfolio 
order by FinalRank ASC limit 20
"""

test_get_strategy_p_mf_pm = """
with portfolio as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score, pm.SixMonth, pm.ThreeMonths from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pm.year = %s and rr.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by pm.SixMonth desc limit 40)
SELECT *  from portfolio 
order by FinalRank ASC limit 20
"""

create_strat_table = """CREATE TABLE IF NOT EXISTS Strategy (
                                                                  id integer PRIMARY KEY,
																  name text NOT NULL UNIQUE
                                                              );
"""

create_portfolio_table = """
CREATE TABLE IF NOT EXISTS Portfolio (
                                                                  id integer PRIMARY KEY,
																  ticker text NOT NULL,
																  p_strategy text NOT NULL,
                                                                  year integer NOT NULL,
																  country text NOT NULL,
																FOREIGN KEY(ticker) REFERENCES CompanyInfo(ticker)
																FOREIGN KEY(p_strategy) REFERENCES Strategy(name)
                                                              );
"""

create_returns_table = """
CREATE TABLE IF NOT EXISTS Returns (
                                                                  id integer PRIMARY KEY,
																  p_strategy text NOT NULL,
                                                                  year integer NOT NULL,
																  country text NOT NULL,
																  SoyPortfolioValue integer,
																  EOYPorfolioValue integer,
                                                                  TotalReturns integer,
																  SharpeRatio integer,
																  CAGR integer,
																  CAPM integer,
																  beta integer,
																  alpha integer,
																  FOREIGN KEY(p_strategy) REFERENCES strategy(name)
                                                              );
"""

insert_into_portfolio = """
INSERT INTO Portfolio(ticker,p_strategy,year,country)
                  VALUES(?,?,?,?)
"""

delete_portfolio = """
DELETE FROM Portfolio where p_strategy= '%s' and year =%s and country = '%s'
"""

get_portfilo_per_stragety = """
SELECT * from Portfolio where p_strategy = '%s' and country = '%s' AND YEAR = %s
"""

prod_create_stock_return = """
CREATE TABLE IF NOT EXISTS StockReturn (
                                                                  id integer PRIMARY KEY,
																  ticker text NOT NULL,
                                                                  year integer NOT NULL,
																  ytd_return integer NOT NULL,
																  country text NOT NULL,
																FOREIGN KEY(ticker) REFERENCES companyInfo(ticker)
                                                              );
"""

prod_insert_into_stock = """
INSERT INTO StockReturn(ticker,year,ytd_return,country)
                  VALUES(?,?,?,?)
"""

prod_update_into_stock = """
UPDATE StockReturn SET  ytd_return=%s where ticker = '%s' and year=%s
"""

prod_create_portfolio_return = """
CREATE TABLE IF NOT EXISTS PortfoiloReturn (
                                                                  id integer PRIMARY KEY,
																  year integer NOT NULL,
                                                                  pf_strategy text NOT NULL,
																  ROI integer not null,
                                                                  SharpeRatio integer,
																  Beta integer,
																  Alpha integer);
"""

prod_insert_into_portfolio_return = """
INSERT INTO PortfoiloReturn(year,pf_strategy,ROI)
                  VALUES(?,?,?)
"""

prod_update_portfolio_return = """
UPDATE PortfoiloReturn SET ROI=%s WHERE year=%s and pf_strategy='%s'
"""

test_sort_by_low_z_score_strat = """
with portfolio as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score, pm.SixMonth, pm.ThreeMonths from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and pm.year = %s and rr.year = %s and zt.score < 1 and zt.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by pm.SixMonth DESC limit 20)
select * from portfolio
"""

get_portfolio_with_returns = """
SELECT p.ticker,st.ytd_return,p.year from companyInfo ci
inner join StockReturn st on st.ticker = ci.ticker
inner join Portfolio p on p.ticker = ci.ticker
where ci.exchange = '%s' and p.year =%s and st.year = %s and p.p_strategy = '%s'
"""

test_get_strategy_p_mf_pm_z_asc = """
with portfolio as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score, pm.SixMonth, pm.ThreeMonths from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pm.year = %s  and rr.year = %s  and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by pm.SixMonth asc limit 40)
SELECT *  from portfolio 
order by FinalRank ASC limit 20
"""

get_roi_per_potfrolio = """
SELECT * from PortfoiloReturn where pf_strategy = '%s'
"""

get_sp500_returns = """
SELECT * FROM PortfoiloReturn where pf_strategy = 'SP500'
"""

get_last_prices = """
SELECT ticker, max(eoddate) from historicPrices
group by ticker
"""

get_last_prices_interval = """
SELECT ticker, max(eoddate) from historicPrices
group by ticker
"""

get_last_prices_interval1 = """SELECT ticker, max(eoddate) from historicPrices
where ticker >= 'SNES'
group by ticker """

prod_create_peg = """
CREATE TABLE "PEG" (
	id integer PRIMARY KEY,
	"peg"	INTEGER NOT NULL,
	"ticker"	TEXT NOT NULL,
	"year"	INTEGER NOT NULL,
	FOREIGN KEY(ticker) REFERENCES companyInfo(ticker)
);
"""

prod_create_ratio = """
CREATE TABLE "Ratio" (
	id integer PRIMARY KEY,
	"roe"	INTEGER,
	"ticker"	TEXT,
	"year"	INTEGER,
	FOREIGN KEY(ticker) REFERENCES companyInfo(ticker)
);
"""

prod_get_ratio = """
select * from Ratio where ticker = '%s' and year = %s
"""

prod_insert_into_ratio = """
INSERT INTO Ratio(ticker,roe,fair_value,dividend_yield,fcf_per_share,year)
                              VALUES(?,?,?,?,?,?)
"""
prod_update_ratio = """
update Ratio set roe =%s,fair_value= %s, dividend_yield=%s,fcf_per_share=%s where ticker = '%s' and year =%s
"""

prod_get_peg = """
select peg from PEG where ticker = '%s' and year = %s
"""

prod_insert_into_peg = """
INSERT INTO PEG(ticker,peg,year)
                              VALUES(?,?,?)
"""

prod_update_peg = """
update PEG set peg =%s where ticker = '%s' and year =%s
"""

prod_create_ulcer_index_table = """
CREATE TABLE IF NOT EXISTS 	ULCERINDEX (
                                                                  id integer PRIMARY KEY,
																  ticker text NOT NULL,
                                                                  year integer NOT NULL,
																  version integer NOT NULL,
																  u_index integer NOT NULL,
																FOREIGN KEY(ticker) REFERENCES companyInfo(ticker)
                                                              );
"""

prod_get_max_version_from_ulcer = """
SELECT MAX(version) from ULCERINDEX where ticker='%s' and year = '%s'
"""


prod_insert_into_ulcer = """
INSERT INTO ULCERINDEX(ticker,year,version,u_index)
                              VALUES(?,?,?,?)
"""

prod_update_ttm_ulcer_index = """
UPDATE ULCERINDEX SET trailing_3m= %s, trailing_6m=%s,trailing_12m=%s WHERE ticker = '%s' and year =%s
"""

prod_get_stock_return = """
select ytd_return from StockReturn where ticker = '%s' and year =%s
"""

prod_update_return = """
UPDATE STOCKRETURN SET ytd_return=%s where ticker='%s' and year=%s
"""

get_min_max_price = """
with temp_table as (
SELECT min(hp.eoddate), hp.eoddate, hp.ticker, hp.adjustedClose as min_price from historicPrices hp
inner join companyInfo i on i.ticker = hp.ticker
where hp.eoddate >= %s and i.exchange = '%s'
group by hp.ticker)

SELECT hp.ticker, i.eoddate as min_date, hp.eoddate as max_date , i.min_price, hp.adjustedClose as max_price from historicPrices hp
inner join temp_table i on i.ticker = hp.ticker
where hp.eoddate =  %s
group by hp.ticker

"""