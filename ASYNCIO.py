
import pandas as pd

# Importing the StringIO module.
from io import StringIO

from tqdm import tqdm

# import the requests library 
import requests

#Used for concurrent programming
import asyncio 


#httpx is a fast and multi-purpose HTTP toolkit that allows running multiple probes using the retryablehttp library. 
#It is designed to maintain result reliability with an increased number of threads.
import httpx 

#Importing Datetime module
import datetime
import time


#Defining Start date and End date to load historical prices for the respective stocks

#Passing date as string
start = datetime.date(2000, 1, 1)
end = datetime.date.today()

print("StartDate:",start)
print("End Date:",end)
#Converting string to unix time format
start_date = int(time.mktime(start.timetuple()))
end_date = int(time.mktime(end.timetuple()))

print("StartDate:",start_date)
print("End Date:",end_date)

#start_date= 946684800 #Jan 1 2000 in Unix format

#end_date= 1714003200 #Apr 24 2024 in Unix format

#Defining headers with User agent to establish requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}

    
#Reading CSV file to fetch all listed NSE Stocks from local folder
all_equity_lst = pd.read_csv("./NSE_EQUITY_List.csv")

stock_list = []

for ind_stock in all_equity_lst['SYMBOL']:
    stock_list.append(ind_stock)

#print("Inside Stock_List:",stock_list)
stock_list_length = len(stock_list)

urls = []


def all_url_lists(stock_list,start_date,end_date):
    for url in range(0,len(stock_list)):
        url = "https://query1.finance.yahoo.com/v7/finance/download/{}.NS?period1={}&period2={}&interval=1d&events=history&includeAdjustedClose=true" \
                .format(stock_list[url],start_date,end_date)
        urls.append(url)
    print("Inside URLS",urls, "Length of URLS:", len(urls))


all_url_lists(stock_list,start_date,end_date)



MAX = 10000

timeout = httpx.Timeout(None)
limits = httpx.Limits(max_connections=MAX)


async def fetch():
    
    counter = 0
    iteration = 0
    
    #Creating basedataframe to hold complete data
    base_df = pd.DataFrame()
    
    while(counter < len(urls)):
        
        iteration = iteration + 1
        async with httpx.AsyncClient(timeout = timeout, limits=limits) as client:
            reqs = [client.get(z,headers = headers) for z in urls[counter : counter + 100]]
            print("Inside Fetch function-{}".format(iteration))
            resultzz = await asyncio.gather(*reqs, return_exceptions=True)
    
        print(resultzz)
            
        for p in range(0,len(resultzz)):    
            content = getattr(resultzz[p], 'content')
            print(content)
            df = pd.read_csv(StringIO(content.decode('utf-8')))
            
            #Rephrasing / Adding dataframe by including new column with script name
            df.insert(loc=0,column='Stock',value=stock_list[p + counter])
            
            #Appending the results to the base Dataframe for consolidated view
            base_df = base_df._append(df,ignore_index=True)                 
            print(df)
            
            #time.sleep(2)
            #print(base_df)
        base_df.to_parquet("./sample_csv_processed1.parquet")
        
        counter = counter + 100
        del[resultzz]
        print("Final Print",base_df)
        #time.sleep(30)
        
    #Formating datatypes on Base Dataframe to appropriate format for further SQL data load process
    #Changing Object type to Date format
    base_df['Date'] = pd.to_datetime(base_df['Date'])

    #Formatting Volume to Integer from Float type to make it easier
    base_df.Volume = base_df.Volume.astype('Int64')

    #Rounding the prices to two decimals to multiple columns
    base_df[['Open','High','Low','Close','Adj Close']] = base_df[['Open','High','Low','Close','Adj Close']].round(decimals=2)

    #Changing column name on base Data frame to appropriate format
    base_df.rename(columns={'Adj Close': 'Adj_Close'},inplace= True)

    #Changing all column names to lowercase 
    base_df.columns = map(str.lower,base_df.columns)

    #Removing records which has volume as 0 because it holds discrepency data
    base_df.drop(base_df[base_df.volume==0].index,inplace= True)    
    
    print("Final Base_df",base_df)

    #Inserting stock data into SQL database using sqlachemy
    from sqlalchemy import create_engine

    sql_engine = create_engine('mssql+pyodbc://' + "DESKTOP-EQ55Q8H" + '/' + "NSEBhavcopy" + '?trusted_connection=yes&driver=SQL+Server')

    db_conn= sql_engine.connect()

    #Defining table name on MSSQL server to locate the data
    table_name = 'stock_ohlc_data'

    #DB Actions to load data from Pandas Dataframe to MSSQL
    try:
        base_df
        base_df.to_sql(table_name, db_conn, if_exists= 'replace',index= False)
    except Exception as ex:
        print(ex)
    else:
        print('Stock OHLC price details data successfully inserted into MS SQL table-{}'.format(table_name))
    finally:
        db_conn.close()


begin_time = time.perf_counter()
asyncio.run(fetch())
end_time = time.perf_counter()
        
#print("INSIDE REQUEST function",end_time_normal - begin_time_normal)
print("INSIDE ASYNC function",end_time - begin_time)
print("StockList Size:",len(stock_list))
print('<<<<<STOCK_LIST_LENGTH-{}>>>>>>>'.format(stock_list_length),'<<<<<URL LENGTH-{}>>>>>'.format(len(urls)))        

        
'''

            
'''