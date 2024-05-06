USE NSEBhavcopy
GO


--Converting Daily historical prices to Weekly prices for Listed Stocks

WITH weekly_price_closing_calc_CTE
AS (
	SELECT DISTINCT stock
		,
		max(convert(int,datepart(year, [date]))) AS current_year
		,max(convert(int,datepart(WEEK, [date]))) AS current_week_of_year
		,first_value([open]) OVER (
			PARTITION BY stock, datepart(year, [date])
			,datepart(WEEK, [date]) ORDER BY datepart(weekday, [date])
			) AS week_open
		,max(high) OVER (
			PARTITION BY stock, datepart(year, [date])
			,datepart(WEEK, [date])
			) AS week_high
		,min(low) OVER (
			PARTITION BY stock, datepart(year, [date])
			,datepart(WEEK, [date])
			) AS week_low
		,first_value([close]) OVER (
			PARTITION BY stock, datepart(year, [date])
			,datepart(WEEK, [date]) ORDER BY datepart(weekday, [date]) DESC
			) AS week_close
	FROM stock_ohlc_data
	--where stock  = 'SBILIFE'
	GROUP BY stock
		,[date]
		,[high]
		,[low]
		,[open]
		,[close]

	),

--Calculating STAN WEINSTEIN Investing method based on Weeking Closing Average and 30 Weekly Simple moving average of Stock and its Closing Price difference with Moving Average

STANWEINSTEIN_3OWEEKSMA_PCT_DIFF AS (
SELECT stock
	,current_year
	,current_week_of_year
	,week_open
	,week_high
	,week_low
	,week_close
	,round(avg(week_close) OVER (
			PARTITION BY stock
			ORDER BY current_year
				,current_week_of_year ROWS BETWEEN 29 PRECEDING
					AND CURRENT row
			), 2) AS [30_week_SMA]
	,(
		week_close - round(avg(week_close) OVER (
				PARTITION BY stock 
				ORDER BY current_year
					,current_week_of_year ROWS BETWEEN 29 PRECEDING
						AND CURRENT row
				), 2)
		) / round(avg(week_close) OVER (
			PARTITION BY stock 
			ORDER BY current_year
				,current_week_of_year ROWS BETWEEN 29 PRECEDING
					AND CURRENT row
			), 2) * 100 AS pct_diff
FROM weekly_price_closing_calc_CTE
)


select  stan.stock as 'STOCKNAME', stan.current_year as 'CURRENTYEAR', stan.current_week_of_year as 'CURRENTWEEK_OF_YEAR', 
stan.week_open 'WEEK_OPEN', stan.week_high as 'WEEK_HIGH', 
stan.week_low as 'WEEK_LOW', stan.week_close as 'WEEK_CLOSE', 
stan.[30_week_SMA] as '30_WEEK_SMA', stan.pct_diff as [PCT_DIFF]
from STANWEINSTEIN_3OWEEKSMA_PCT_DIFF stan
where 
stan.week_close >= stan.[30_week_SMA]
--pct_diff between 0 and 3 and
and current_year = year(getdate()) 
and current_week_of_year = datepart(week,getdate()) - 1
order by stan.current_year desc, stan.current_week_of_year desc

