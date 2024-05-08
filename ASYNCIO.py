
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
#Creating basedataframe to hold complete data
base_df = pd.DataFrame()
    
#Reading CSV file to fetch all listed NSE Stocks from local folder
all_equity_lst = pd.read_csv("./NSE_EQUITY_List.csv")

stock_list = []

for ind_stock in all_equity_lst['SYMBOL']:
    stock_list.append(ind_stock)

#print("Inside Stock_List:",stock_list)

urls = []

def all_url_lists(stock_list,start_date,end_date):
    for url in range(0,1000):
        url = "https://query1.finance.yahoo.com/v7/finance/download/{}.NS?period1={}&period2={}&interval=1d&events=history&includeAdjustedClose=true" \
                .format(stock_list[url],start_date,end_date)
        urls.append(url)
    print("Inside URLS",urls)

all_url_lists(stock_list,start_date,end_date)


'''def fetch_reqs():
    results = [requests.get(z,headers= headers) for z in urls]
    print(results)
    


begin_time_normal = time.perf_counter()
fetch_reqs()
end_time_normal = time.perf_counter()
'''

MAX = 10000

timeout = httpx.Timeout(None)
limits = httpx.Limits(max_connections=MAX)

async def fetch():
    async with httpx.AsyncClient(timeout = timeout, limits=limits) as client:
        reqs = [client.get(z,headers = headers) for z in urls]
        print("Inside Fetch function")
        resultzz = await asyncio.gather(*reqs, return_exceptions=True)
    
    print(resultzz)
    
    #Creating basedataframe to hold complete data
    base_df = pd.DataFrame()

    for p in range(0,1000):
        
        content = getattr(resultzz[p], 'content')
        print(content)
        df = pd.read_csv(StringIO(content.decode('utf-8')))
        
        #Rephrasing / Adding dataframe by including new column with script name
        df.insert(loc=0,column='Stock',value=stock_list[p])
        print("Inside p-{} loop for content ".format(p),'Length of resultzz:',len(resultzz))
        
        #Appending the results to the base Dataframe for consolidated view
        base_df = base_df._append(df,ignore_index=True)     
        #time.sleep(2)
        #print(base_df)
 
    base_df.to_csv("./sample_csv_processed1.csv")


begin_time = time.perf_counter()
asyncio.run(fetch())
end_time = time.perf_counter()

#print("INSIDE REQUEST function",end_time_normal - begin_time_normal)
print("INSIDE ASYNC function",end_time - begin_time)
print("StockList Size:",len(stock_list))