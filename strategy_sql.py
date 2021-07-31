

MF_Pscore_zscore_pm_ui_asc ="""
with portfolio_temp as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score, pm.SixMonth, pm.ThreeMonths from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pm on pm.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pm.year = %s and rr.year = %s and zt.score > 4 and zt.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by pm.SixMonth desc limit 40),

portfolio_temp_2 as (select * from portfolio_temp
order by FinalRank ASC limit 30)

select pt.ticker, sr.ytd_return from portfolio_temp_2 pt
inner join ULCERINDEX ui on ui.ticker = pt.ticker
inner join StockReturn sr on sr.ticker = pt.ticker
where ui.year = %s and sr.year = %s
order by ui.u_index asc limit 20
"""

mf_pscore_zsc_ui_asc = """
with portfolio_temp as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and rr.year = %s and zt.score > 4 and zt.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by FinalRank ASC limit 40)
select pt.ticker, sr.ytd_return from portfolio_temp pt
inner join ULCERINDEX ui on ui.ticker = pt.ticker
inner join StockReturn sr on sr.ticker = pt.ticker
where ui.year = %s and sr.year = %s
order by ui.u_index asc limit 20
"""


mf_pscore_zsc_ui_desc = """
with portfolio_temp as(SELECT DISTINCT ci.ticker, rr.EBIT, rr.TotalFixedAsset,rr.WorkingCapital,rr.EnterpriseValue,rr.FinalRank,p.score from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join RankedResultTest rr on rr.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and rr.year = %s and zt.score > 4 and zt.year = %s and rr.TotalFixedAsset !=0 and ci.exchange = '%s'
order by FinalRank ASC limit 40)
select pt.ticker, sr.ytd_return from portfolio_temp pt
inner join ULCERINDEX ui on ui.ticker = pt.ticker
inner join StockReturn sr on sr.ticker = pt.ticker
where ui.year = %s and sr.year = %s
order by ui.u_index desc limit 20
"""

p_score_pm_zsc_ui_asc = """
with portfolio_temp as(SELECT DISTINCT ci.ticker,p.score from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pp on pp.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pp.year = %s and zt.score >= 4 and ci.exchange = '%s'
order by pp.SixMonth ASC limit 40)
select pt.ticker, sr.ytd_return from portfolio_temp pt
inner join ULCERINDEX ui on ui.ticker = pt.ticker
inner join StockReturn sr on sr.ticker = pt.ticker
where ui.year = %s and sr.year = %s
order by ui.u_index desc limit 20
"""


p_score_pm_zsc = """
with portfolio_temp as(SELECT DISTINCT ci.ticker,p.score from companyInfo ci
inner join PiotroskiFscoreTest p on p.ticker = ci.ticker
inner join PriceMomemtumTest pp on pp.ticker = ci.ticker
inner join ZscoreTest zt on zt.ticker = ci.ticker
where p.year = %s and p.score >= 7 and pp.year = %s and zt.score >= 4 and ci.exchange = '%s'
order by pp.SixMonth ASC limit 20)
select pt.ticker, sr.ytd_return from portfolio_temp pt
inner join StockReturn sr on sr.ticker = pt.ticker
where sr.year = %s
"""

fcf_z_score_pm = """
with temp_1 as (SELECT ci.ticker,r.fcf_per_share FROM companyInfo ci
inner join Ratio r on r.ticker = ci.ticker
inner join ZscoreTest z on z.ticker = ci.ticker
where ci.exchange = '%s'and z.score >= 4 and z.year = %s  and r.year = %s
order by r.fcf_per_share desc limit 100)

select sr.ticker,pm.SixMonth,sr.ytd_return,t.fcf_per_share from PriceMomemtumTest pm
inner join temp_1 t on t.ticker = pm.ticker
inner join StockReturn sr on sr.ticker = t.ticker
where sr.year = %s and pm.year = %s
order by pm.SixMonth desc limit 20
"""

fcf_pm = """
with temp_1 as (SELECT ci.ticker,r.fcf_per_share FROM companyInfo ci
inner join Ratio r on r.ticker = ci.ticker
where ci.exchange = '%s' and r.year = %s
order by r.fcf_per_share desc limit 100)

select sr.ticker,pm.SixMonth,sr.ytd_return,t.fcf_per_share from PriceMomemtumTest pm
inner join temp_1 t on t.ticker = pm.ticker
inner join StockReturn sr on sr.ticker = t.ticker
where sr.year = %s and pm.year = %s
order by pm.SixMonth desc limit 20
"""