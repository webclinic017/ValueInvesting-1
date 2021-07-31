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


test_get_zscore_screened_stock = """
select c.*,p.score
from  companyInfo c
inner join ZscoreTest p on p.ticker = c.ticker
inner join (
    select pf.ticker, max(version) as MaxVersion
    from ZscoreTest pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
where p.score > 6
"""

prod_get_zscore_screened_stock = """
select c.*,p.score
from  companyInfo c
inner join Zscore p on p.ticker = c.ticker
inner join (
    select pf.ticker, max(version) as MaxVersion
    from Zscore pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
where p.score > 6 AND c.exchange='%s'
"""

prod_insert_into_zscore = """
INSERT INTO Zscore(ticker,version,score)
                              VALUES(?,?,?)
"""

prod_get_latest_zscore = """
select p.ticker, p.score, p.year
from Zscore p
inner join (
    select pf.ticker, max(version) as MaxVersion
    from PiotroskiFscore pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
"""

test_get_latest_zscore = """
select p.ticker, p.score
from ZscoreTest p
inner join (
    select pf.ticker, max(version) as MaxVersion
    from ZscoreTest pf
    group by pf.ticker
) pff on pff.ticker= p.ticker and p.version = pff.MaxVersion
"""
test_insert_into_zscore = """
INSERT INTO ZscoreTest(ticker,version,score,year)
                              VALUES(?,?,?,?)
"""

prod_update_into_zscore = """
UPDATE Zscore SET score=%s where ticker = '%s'
"""

prod_get_score_from_ticker = """
SELECT score from Zscore where ticker='%s'
"""

test_get_score_from_ticker = """
SELECT MAX(version) from ZscoreTest where ticker='%s' and year= %s
"""