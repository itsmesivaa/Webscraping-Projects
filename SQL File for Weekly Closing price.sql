--Weekly price moving average for Listed Stocks
USE NSEBhavcopy
GO

WITH weekly_closing_calc
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

	)

--Calculating Weeking Closing Average and Weekly Stock Closing Price difference with Moving Average
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
			), 2) * 100 AS pctdiff
FROM weekly_closing_calc
ORDER BY current_year DESC
	,current_week_of_year DESC
	,stock asc