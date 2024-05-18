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
,

Rangebound_6Months AS (
    SELECT stock,
           current_year,
           current_week_of_year,
		   week_open,
		   week_high,
		   week_low,
           week_close,
           [30_week_SMA],
           pct_diff,
		   ((week_high - week_low) / week_low) * 100 AS WEEKLY_ATR_PCT_RANGE,
           COUNT(CASE WHEN week_close > [30_week_SMA] THEN 1 END) OVER (
                PARTITION BY stock
                ORDER BY current_year, current_week_of_year
                ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
           ) AS consecutive_weeks_above_SMA
    FROM STANWEINSTEIN_3OWEEKSMA_PCT_DIFF
)

SELECT r6m.stock AS 'STOCKNAME',
       r6m.current_year AS 'CURRENTYEAR',
       r6m.current_week_of_year AS 'CURRENTWEEK_OF_YEAR',
	   r6m.consecutive_weeks_above_SMA AS 'CONSECUTIVE_WEEKS_Above_SMA',
       r6m.week_open AS 'WEEK_OPEN',
       r6m.week_high AS 'WEEK_HIGH',
       r6m.week_low AS 'WEEK_LOW',
       r6m.week_close AS 'WEEK_CLOSE',
       r6m.[30_week_SMA] AS '30_WEEK_SMA',
       ROUND(r6m.pct_diff, 2) AS '30_WEEK_PCT_DIFF',
	   ROUND(r6m.WEEKLY_ATR_PCT_RANGE,2) AS 'WEEKLY_ATR_PCT_RANGE'
FROM Rangebound_6Months r6m
WHERE 
--r6m.week_close < r6m.[30_week_SMA]
	  r6m.pct_diff BETWEEN -3.5 AND 3.5
      AND r6m.consecutive_weeks_above_SMA >= 6
      AND r6m.current_year = YEAR(GETDATE())
      AND r6m.current_week_of_year = DATEPART(WEEK, GETDATE())
ORDER BY r6m.stock ASC,r6m.current_year DESC, r6m.consecutive_weeks_above_SMA DESC,
r6m.current_week_of_year DESC, r6m.[PCT_DIFF] ASC, r6m.WEEKLY_ATR_PCT_RANGE ASC;


