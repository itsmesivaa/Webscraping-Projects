--Weekly price moving average for Listed Stocks


use NSEBhavcopy
go


with weekly_closing AS (
select DISTINCT stock, 
--[date],
datepart(year,[date]) as current_year, 
datepart(WEEK,[date]) as current_week_of_year,
first_value([open]) over (partition by datepart(year,[date]),  datepart(WEEK,[date]) order by datepart(weekday,[date])) as week_open,
max(high) over (partition by datepart(year,[date]), datepart(WEEK,[date]) ) as week_high,
min(low) over (partition by datepart(year,[date]), datepart(WEEK,[date]) ) as week_low,
first_value([close]) over (partition by datepart(year,[date]),  datepart(WEEK,[date]) order by datepart(weekday,[date]) desc) as week_close
--,[open], [high],[low],[close]
From stock_ohlc_data where stock in('TITAN')
group by  
stock,[date],
[high],[low],[open],[close]
/*order by date desc,
datepart(year,[date]) desc,
datepart(WEEK,[date]) desc
*/
)

--select * From test_CTE


--Calculating Weeking Closing Average and Weekly Stock Closing Price difference with Moving Average
select distinct top 1 stock,
current_year, 
current_week_of_year,
week_open, week_high, week_low, week_close,
round(avg(week_close) over (order by current_year,current_week_of_year ROWS between 29 PRECEDING and current row),2) as weekly_closing_average,
(week_close - round(avg(week_close) over (order by current_year,current_week_of_year ROWS between 29 PRECEDING and current row),2)) / 
round(avg(week_close) over (order by current_year,current_week_of_year ROWS between 29 PRECEDING and current row),2) * 100 as PCTDIFF 
from weekly_closing
order by current_year desc, current_week_of_year desc



/*
select top 30 'temptable',stock, current_year, 
current_week_of_year, week_open, 
week_high, week_low, week_close ,
round(avg(week_close) over (order by current_year,current_week_of_year ROWS between 29 PRECEDING and current row),2) as weekly_closing_average
From #temp
order by current_year desc, current_week_of_year desc

*/