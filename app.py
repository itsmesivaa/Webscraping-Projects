from datetime import datetime
import pandas as pd
import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from sqlalchemy import create_engine

# Set page layout to wide
st.set_page_config(layout='wide')
pages = st.tabs(['HomePage','StanWeinstein','Mark Minervini','CANSLIM','Stock Analyzer'])

# Establish connection to the database
sql_engine = create_engine('mssql+pyodbc://' + "DESKTOP-EQ55Q8H" + '/' + "NSEBhavcopy" + '?trusted_connection=yes&driver=SQL+Server')

# Function to load stock names from the database
@st.cache_data
def load_stock_names():
    try:
        db_conn = sql_engine.connect()
        # Execute SQL query to get distinct stock names
        stocks_name_query = "SELECT DISTINCT STOCK FROM stock_ohlc_data ORDER BY stock ASC"
        stocks_df = pd.read_sql_query(stocks_name_query, db_conn)
        
        # Close the database connection
        db_conn.close()
        
        # Extract stock names from the DataFrame
        stocks_lst = stocks_df['STOCK'].tolist()
        return stocks_lst
            
    except Exception as e:
        print(str(e)) 
        return []

# Function to load stock prices from the database
@st.cache_data
def load_stock_prices():
    try:
        db_conn = sql_engine.connect()
        # Execute SQL query to get stock prices
        stock_price_query = "SELECT [stock],[date],[open],[high],[low],[close],[volume] FROM stock_ohlc_data ORDER BY [stock] ASC,[date] ASC"
        stock_price_det = pd.read_sql_query(stock_price_query, db_conn)
        # Close the database connection
        db_conn.close()
        
        return stock_price_det
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()

# Function to load Quarterly earnings detils from the database
@st.cache_data
def load_all_stocks_quarterly_earnings():
    try:
        db_conn = sql_engine.connect()
        #Execute SQL query to get earnings details
        stock_earnings_details_query = "SELECT stock_name AS 'STOCK', quarter AS 'QUARTER', eps AS 'EPS', eps_pct_chg AS 'EPS_%CHG', sales_in_cr AS 'SALES_IN_CRORE',\
                                sales_pct_chg AS 'SALES_%CHG' FROM all_stocks_quarterly_earnings "
        stock_earnings_details = pd.read_sql_query(stock_earnings_details_query,db_conn)
        db_conn.close()
        
        return stock_earnings_details
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()    

# Function to load Institution details from the database
@st.cache_data
def load_all_stocks_institution_details():
    try:
        db_conn = sql_engine.connect()
        #Execute SQL query to get earnings details
        stock_institution_data_query = "SELECT StockName AS 'STOCK',Quarter AS 'QUARTERS',[FinancialInstitutions/Banks] AS 'FinancialInstitutions&Banks', \
                                    ForeignPortfolioInvestors ,IndividualInvestors,InsuranceCompanies,MutualFunds,Others,Promoters \
                                    FROM market_smith_stock_institutional_data_test"
        stock_institution_data = pd.read_sql_query(stock_institution_data_query,db_conn)                                    
        db_conn.close()
        return stock_institution_data
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()
    
# Function to load all stock evaluation summary details from the database
@st.cache_data
def load_all_stocks_summary_evaluation_data():
    try:
        db_conn = sql_engine.connect()
        #Execute SQL query to get stock evaluation metrics data
        stock_eval_summary_query = "select stock_name as 'STOCK', market_capitalization 'Market_Capitalization',sales 'Sales',shares_in_float 'Shares_in_Float',\
                            no_of_funds 'No_of_Funds',shares_held_by_funds 'Shares_held_by_Funds',master_score 'Master_Score',eps_rating 'EPS_Rating',\
                            price_strength 'Price_Strength',buyers_demand 'Buyers_Demand',group_rank_out_of_197 'Group_Rank/197',pe_ratio 'PE_Ratio',\
                            return_on_equity 'Return_on_Equity',cash_flow 'Cash_Flow',book_value 'Book_Value'  From market_smith_stock_eval"        
        stock_eval_summary = pd.read_sql_query(stock_eval_summary_query,db_conn)
        db_conn.close()
        return stock_eval_summary
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()   

# Load stock names and prices only once when the app launches
stocks_lst = load_stock_names()
stock_price_det = load_stock_prices()
stock_earnings_details = load_all_stocks_quarterly_earnings()
stock_institution_data = load_all_stocks_institution_details()
stock_eval_summary = load_all_stocks_summary_evaluation_data()

def stanweinstein_results(current_week,range_value):
    try:
        db_conn= sql_engine.connect()
        stanweinstein_criteria_match = "EXEC [STANWEINSTEIN_RS_STOCKS_WITH_MOMENTUM_START] @FROM_DATE = '" + str(current_week) + "', @RANGE_VALUE =" + str(range_value)
        stanweinstein_stocks = pd.read_sql_query(stanweinstein_criteria_match,db_conn)
        db_conn.close()
        return stanweinstein_stocks
    except Exception as e:
        return str(e)

# Function to plot stock price
def plot_stock_price(stock_price_det):
    try:

        # Create a subplot figure with two rows (price on top, volume below)
        fig = make_subplots(rows=2, shared_xaxes=True,row_heights=[800, 300])

        # Primary y-axis for closing price (top row)
        price_trace = go.Scatter(
            x=stock_price_det['date'],
            y=stock_price_det['close'],
            name='Closing Price',
            mode='lines',showlegend= False
        )
        fig.append_trace(price_trace, row=1, col=1)

        # Secondary y-axis for volume (bottom row)
        max_volume = max(stock_price_det['volume'])
        volume_trace = go.Bar(
            x=stock_price_det['date'],
            y=stock_price_det['volume'],
            name='Volume',
            marker=dict(color='royalblue'),
            opacity=0.7,
            base=0,showlegend= False
        )
        fig.append_trace(volume_trace, row=2, col=1)
        
        # Set y-axis titles and ranges
        fig.update_yaxes(title="Price", range=[min(stock_price_det['close']), max(stock_price_det['close'])], showgrid=True, zeroline=False, row=1, side='right')
        fig.update_yaxes(title="Volume", range=[0, (max_volume + max_volume * 0.3)], showgrid=True, zeroline=False, row=2, side='right')

        # Adjust layout for subplots
        fig.update_layout(width = 2000,height=1000)

        # Show the combined chart with separate axes
        st.plotly_chart(fig)

    except Exception as e:
        print(str(e))

# Streamlit pages
with pages[1]:
    st.header("StanWeinstein Relative Strength Stocks", divider='rainbow')
    current_week = st.date_input(label='For Date').strftime("%d/%m/%Y")
    range_value = st.slider("Select Relative Strength Range to Filter", min_value=1, max_value=100, value=None, 
                        step=1, format=None, key=None, help=None, on_change=None, args=None,disabled=False, label_visibility="visible")
    st.write("Below were the stocks emerging from {}-week Consolidation range with Momentum (Stage-1) by breaking 30WeekSMA".format(range_value))
    results = stanweinstein_results(current_week, range_value)
            
    if results.empty:
        st.write("No results found")
    else:
        st.dataframe(results, width=3000, height=800)
        
with pages[4]:
    
    st.header("Stock Price Forecasting", divider='rainbow')
    stock_selected = st.selectbox("Select a Stock", stocks_lst)
    #Filtering from entire dataframe to selected stock
    stock_price_selected = stock_price_det.loc[stock_price_det['stock'] == stock_selected]
    current_stock_earnings_detail = stock_earnings_details.loc[stock_earnings_details['STOCK'] == stock_selected].reset_index(drop=True).dropna()
    stock_institution_data_detail = stock_institution_data.loc[stock_institution_data['STOCK'] == stock_selected].reset_index(drop = True).dropna()
    stock_eval_summary_detail = stock_eval_summary.loc[stock_eval_summary['STOCK'] == stock_selected]
    
    st.header("Stock Summary Details",divider='orange')
  
    acol1, acol2,a_col3, a_col4, a_col5 = st.columns(5)
    
    with acol1:
        st.metric(label="Market Capitalization",value=stock_eval_summary_detail['Market_Capitalization'].iloc[0])

    with acol2:
        st.metric(label="Sales",value=stock_eval_summary_detail['Sales'].iloc[0])

    with a_col3:
        st.metric(label="Shares in Float",value=stock_eval_summary_detail['Shares_in_Float'].iloc[0])

    with a_col4:
        st.metric(label="No of Funds",value=stock_eval_summary_detail['No_of_Funds'].iloc[0])    

    with a_col5:
        st.metric(label="Shares held by Funds",value=stock_eval_summary_detail['Shares_held_by_Funds'].iloc[0])   
    
    b_col1, b_col2, b_col3, b_col4, b_col5 = st.columns(5,gap="small")
    
    with b_col1:
        st.metric(label="Master Score",value=stock_eval_summary_detail['Master_Score'].iloc[0])
        
    with b_col2:
        st.metric(label="EPS Rating",value=stock_eval_summary_detail['EPS_Rating'].iloc[0])
   
    with b_col3:
        st.metric(label="Price Strength",value=stock_eval_summary_detail['Price_Strength'].iloc[0])     
        
    with b_col4:
        st.metric(label="Buyers Demand",value=stock_eval_summary_detail['Buyers_Demand'].iloc[0])    
        
    with b_col5:
        st.metric(label="Group Rank Out of 197",value=stock_eval_summary_detail['Group_Rank/197'].iloc[0])
    
    c_col1,c_col2,c_col3,c_col4,c_col5 = st.columns(5,gap="small")   
    
    with c_col1:
        st.metric(label="PE Ratio",value=stock_eval_summary_detail['PE_Ratio'].iloc[0])            
    
    with c_col2:
        st.metric(label="Return on Equity",value= stock_eval_summary_detail['Return_on_Equity'].iloc[0])    
    
    with c_col3:
        st.metric(label="Cash Flow",value= stock_eval_summary_detail['Cash_Flow'].iloc[0])
        
    with c_col4:
        st.metric(label='Book Value',value= stock_eval_summary_detail['Book_Value'].iloc[0])
    
    st.header("Stock Price Chart & Volume",divider="blue")
    forecast_type = st.multiselect("Select any Forecasting Type Models",['No Forecasting','ARIMA','SARIMA'])
    st.write(forecast_type)
    #if(forecast_type = ['No Forecasting',] )
    plot_stock_price(stock_price_selected)
    
    c_col1, c_col2 = st.columns([2.3, 1.5],gap="small")

    with c_col1:
        st.header("Institution Ownership Pattern",divider="rainbow")
        st.dataframe(stock_institution_data_detail,hide_index=True)
            
    with c_col2:
        st.header("Quarterly Earnings",divider="rainbow")
        st.dataframe(current_stock_earnings_detail, width = 650,hide_index= True)
        
