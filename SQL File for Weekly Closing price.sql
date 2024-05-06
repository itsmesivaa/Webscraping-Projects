USE NSEBhavcopy
GO

--Converting Daily historical prices to Weekly prices for Listed Stocks
WITH weekly_price_closing_calc_CTE
AS (
	SELECT DISTINCT stock
		,max(convert(INT, datepart(year, [date]))) AS current_year
		,max(convert(INT, datepart(WEEK, [date]))) AS current_week_of_year
		,first_value([open]) OVER (
			PARTITION BY stock
			,datepart(year, [date])
			,datepart(WEEK, [date]) ORDER BY datepart(weekday, [date])
			) AS week_open
		,max(high) OVER (
			PARTITION BY stock
			,datepart(year, [date])
			,datepart(WEEK, [date])
			) AS week_high
		,min(low) OVER (
			PARTITION BY stock
			,datepart(year, [date])
			,datepart(WEEK, [date])
			) AS week_low
		,first_value([close]) OVER (
			PARTITION BY stock
			,datepart(year, [date])
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
	)
	,


--Calculating STAN WEINSTEIN Investing method based on Weeking Closing Average and 30 Weekly Simple moving average of Stock and its Closing Price difference with Moving Average

STANWEINSTEIN_3OWEEKSMA_PCT_DIFF
AS (
	SELECT stock
		,current_year
		,current_week_of_year
		,week_open
		,week_high
		,week_low
		,week_close
		,round(avg(week_close) OVER (
				PARTITION BY stock ORDER BY current_year
					,current_week_of_year ROWS BETWEEN 29 PRECEDING
						AND CURRENT row
				), 2) AS [30_week_SMA]
		,(
			week_close - round(avg(week_close) OVER (
					PARTITION BY stock ORDER BY current_year
						,current_week_of_year ROWS BETWEEN 29 PRECEDING
							AND CURRENT row
					), 2)
			) / round(avg(week_close) OVER (
				PARTITION BY stock ORDER BY current_year
					,current_week_of_year ROWS BETWEEN 29 PRECEDING
						AND CURRENT row
				), 2) * 100 AS pct_diff
	FROM weekly_price_closing_calc_CTE
	)


--Query to filter the stocks Weekly Closing price greater than 30 Week Moving Average as per Stan Weinstein Trading Strategy

SELECT stan.stock AS 'STOCKNAME'
	,stan.current_year AS 'CURRENTYEAR'
	,stan.current_week_of_year AS 'CURRENTWEEK_OF_YEAR'
	,stan.week_open 'WEEK_OPEN'
	,stan.week_high AS 'WEEK_HIGH'
	,stan.week_low AS 'WEEK_LOW'
	,stan.week_close AS 'WEEK_CLOSE'
	,stan.[30_week_SMA] AS '30_WEEK_SMA'
	,round(stan.pct_diff, 2) AS 'PCT_DIFF'
FROM STANWEINSTEIN_3OWEEKSMA_PCT_DIFF stan
WHERE stan.week_close >= stan.[30_week_SMA]
	AND stan.[pct_diff] BETWEEN 0
		AND 8
	AND current_year = year(getdate())
	AND current_week_of_year = datepart(week, getdate()) - 1
ORDER BY stan.current_year DESC
	,stan.current_week_of_year DESC
	,stan.[PCT_DIFF] DESC