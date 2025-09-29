"""
Author: Paras Parkash
Zerodha Data Collector

Logs in to Broker with the latest access token found in {token_database_name}.broker_tokens
Gets instrument_tokens from equity_universe_token_list.csv, index_token_list.csv, nifty_options_token_list.csv and bank_nifty_options_token_list.csv in the lookup_tables directory.
Subscribes to all of them in FULL mode
Uses equity_universe_token_table.npy, nifty_options_token_table.npy and bank_nifty_options_token_table.npy to identify the tables to which the tick data should be stored
Uses different SQL statements for Indexes and the rest of the instruments as Index ticks have fewer columns.

brokerConnect ticker field name changes in v4 and later.
https://github.com/zerodha/pykiteconnect?tab=readme-ov-file#v4---breaking-changes
Incorporated already and tested in BrokerConnect Version : 5.0.1
The DB Tables would still be created with the old names to avoid major table alterations on existing setups.
So this is just for reference.
Old Name => New Name:
    timestamp => exchange_timestamp
    last_quantity => last_traded_quantity
    average_price => average_traded_price
    volume => volume_traded
    buy_quantity => total_buy_quantity
    sell_quantity => total_sell_quantity
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt, date
import psycopg2
from kiteconnect import KiteTicker
import traceback
import json
import sys
import os
# Add src directory to Python path to allow imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from market_data_mailer import send_market_data_email
from sqlalchemy import create_engine
from threading import Timer
from utils.logger import get_market_data_ticker_logger
from utils.error_handler import ErrorHandler
from utils.config_manager import config_manager
from utils.db_manager import create_db_manager, BrokerTokenManager

# Load configuration
system_config = config_manager.load_config()

api_key = system_config['api_key']
database_host = system_config['database_host']
database_user = system_config['database_user']
database_password = system_config['database_password']
database_port = system_config['database_port']
market_close_hour = system_config['market_close_hour']
market_close_minute = system_config['market_close_minute']

token_database_name = system_config['token_database_name']
broker_username = system_config['broker_username']
daily_table_name = 'daily_table'
equity_database_name = system_config['equity_database_name']
options_database_name = system_config['options_database_name']
bank_nifty_options_database_name = system_config['banknifty_options_database_name']

# Create database manager and get latest access token
db_manager = create_db_manager(
    database_host,
    database_user,
    database_password,
    database_port,
    token_database_name
)
broker_token_manager = BrokerTokenManager(db_manager, token_database_name)

# Create SQL Alchemy engine for SQL insertion with TimescaleDB
engine = create_engine(f'postgresql+psycopg2://{database_user}:{database_password}@{database_host}:{database_port}/{equity_database_name}')

def log_market_data_ticker(message):
    """
    Log market data ticker messages using centralized logger
    """
    logger = get_market_data_ticker_logger()
    logger.log_info(message)

def bulk_print(ticks):
    #for debugging 
    for tick in ticks:
        print('dict', tick)
        print('dictKeys', tick.keys())
        print('dictValues', tick.values())
        print('dictDTypes', [type(x) for x in tick.values()])
    
# Unwrap depth data or return None if not available
def preprocess_depth(ticks_df):
    ticks_df['depth'] = ticks_df['depth'].apply(lambda x: x if isinstance(x, dict) else {})
    return ticks_df

def extract_depth_values(ticks_df, side, index, key):
    """Extract depth values in a vectorized manner."""
    def extract_value(depth):
        return depth.get(side, [])[index].get(key, None) if depth else None

    # Apply the extraction function across the 'depth' column
    return ticks_df['depth'].apply(extract_value)

def get_depth_values(ticks_df):
    """
    Extract depth values from ticks DataFrame
    """
    ticks_df = preprocess_depth(ticks_df)
    for i in range(5):
        for side, key in [('buy', 'quantity'), ('buy', 'price'), ('buy', 'orders'),
                          ('sell', 'quantity'), ('sell', 'price'), ('sell', 'orders')]:
            col_name = f"{side[0]}{key[0]}{i}"
            ticks_df[col_name] = extract_depth_values(ticks_df, side, i, key)
    return ticks_df
    
# Define the custom insert method for PostgreSQL
def replace_sql_execute_many(table, df_connection, keys, data_iter):
    # Prepare the SQL insert statement as a string
    columns = ', '.join(keys)
    placeholders = ', '.join(['%s' for _ in keys])
    table_name = table.name
    insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (instrument_token, timestamp) DO UPDATE SET "
    
    # Add SET clauses for all columns except the conflict columns
    set_clauses = []
    for col in keys:
        if col not in ['instrument_token', 'timestamp']:
            set_clauses.append(f"{col} = EXCLUDED.{col}")
    insert_stmt += ', '.join(set_clauses)
    
    # Convert data_iter to a list of tuples
    data = list(data_iter)
    
    # Use the raw connection to execute
    with df_connection.connection.cursor() as cursor:
        cursor.executemany(insert_stmt, data)
        df_connection.connection.commit()


def run_market_data_ticker(shutdown_hour, shutdown_minute):
    """
    Main function to run the market data ticker
    """
    message = f'Market Data Ticker called with end time {shutdown_hour}:{shutdown_minute}'
    log_market_data_ticker(message)
    
    # Calculate the duration to run before shutting down
    now = dt.now()
    shutdown_time = now.replace(hour=shutdown_hour, minute=shutdown_minute, second=0, microsecond=0)
    run_duration = (shutdown_time - now).total_seconds()
    
    #Exit if Run Duration is negative. i.e. Close time is in the past
    if run_duration < 0:
        message = f'Market data ticker called with close time in the past. currentTime =>{now}. Shutdown time =>{shutdown_time}. Exiting'
        logger = get_market_data_ticker_logger()
        logger.log_error(message)
        error_handler = ErrorHandler()
        error_handler.handle_error(message)
        return False
    
    def shutdown_ticker():
        # Perform any necessary cleanup, logging, or data saving here
        message = 'Shutting Down Market Data Ticker'
        log_market_data_ticker(message)
        # Close Websocket
        #kws.close()
        #Sleep for 10 seconds to allow handshake/cleanup
        #sleep(10)
        try:
            kws.stop()
            logger = get_market_data_ticker_logger()
            logger.log_info('Ticker Stopped')
        except Exception as e:
            message = f'Ticker Could not be stopped. Exception => {e}'
            logger = get_market_data_ticker_logger()
            logger.log_error(message)
            error_handler = ErrorHandler()
            error_handler.handle_error(message)
            send_market_data_email('Market Data Ticker - Ticker Could not be stopped!!!', message)
        return True

    # Set a timer to stop the ticker at the specified time
    shutdown_timer = Timer(run_duration, shutdown_ticker)
    shutdown_timer.start()

    lookup_dir = 'instrument_lookup_tables'
    
    #Identify Only Nifty and BankNifty as they have a different table structure and hence different SQL statement
    #index_tokens = pd.read_csv(path.join(lookup_dir, 'index_token_list.csv'))['instrument_token'].values.tolist()  
    
    #Equity Universe Tokens and Token Table (Lookup => instrument_token:TableName)
    #Includes Equity Universe, Nifty, BankNifty and their Futures
    equity_universe_tokens = pd.read_csv(path.join(lookup_dir, 'equity_universe_token_list.csv'))['instrument_token'].values.tolist()
    equity_universe_token_table = np.load(path.join(lookup_dir, 'equity_universe_token_table_dict.npy'), allow_pickle=True).item() 
    equity_universe_token_symbol = np.load(path.join(lookup_dir, 'equity_universe_token_symbol_dict.npy'), allow_pickle=True).item() 
    
    #Nifty Options Tokens and Token Table(Lookup => instrument_token:TableName)
    nifty_option_tokens = pd.read_csv(path.join(lookup_dir, 'nifty_options_token_list.csv'))['instrument_token'].values.tolist()
    nifty_options_token_table = np.load(path.join(lookup_dir, 'nifty_options_token_table_dict.npy'), allow_pickle=True).item() 
    
    #BankNifty Options Tokens and Token Table(Lookup => instrument_token:TableName)
    bank_nifty_option_tokens = pd.read_csv(path.join(lookup_dir, 'bank_nifty_options_token_list.csv'))['instrument_token'].values.tolist()
    bank_nifty_options_token_table = np.load(path.join(lookup_dir, 'bank_nifty_options_token_table_dict.npy'), allow_pickle=True).item() 
    
    #Combining all three token lists. Subscribing to this master list
    full_token_list = equity_universe_tokens + nifty_option_tokens + bank_nifty_option_tokens
    #Removing index_tokens for testing
    #full_token_list = [token for token in full_token_list if token not in index_tokens]


    #Retaining 3 for insert check 
    #full_token_list = [345089, 11829506] + index_tokens
    #full_token_list = index_tokens
    
    #Combine all three token:table dictionaries
    main_token_table_dict = {}
    main_token_table_dict.update(equity_universe_token_table)
    main_token_table_dict.update(nifty_options_token_table)
    main_token_table_dict.update(bank_nifty_options_token_table)
    
    #Combine all three token:symbol dictionaries
    main_token_symbol_dict = {}
    main_token_symbol_dict.update(equity_universe_token_symbol)
    main_token_symbol_dict.update(nifty_options_token_table) #Symbol and table_name are same for nifty options
    main_token_symbol_dict.update(bank_nifty_options_token_table) #Symbol and table_name are same for BankNifty options 
    
    #Creating a lookup dictionary for instrument_token:DBName
    token_to_db_name_dict = {token: equity_database_name for token in equity_universe_tokens}
    token_to_db_name_dict.update({token: options_database_name for token in nifty_option_tokens})
    token_to_db_name_dict.update({token: bank_nifty_options_database_name for token in bank_nifty_option_tokens})    
    
    def bulk_replace(ticks):
        """
        Process and store ticks in bulk to database
        """
        global engine

        #Testing iteration time
        #print(dt.now(), f'About to insert list with {len(ticks)} ticks')
        try:
            # Convert list of dictionaries to DataFrame
            ticks_df = pd.DataFrame(ticks)
            
            #print(dt.now(), 'Adding Trading Symbol')
            ticks_df['tradingsymbol'] = ticks_df['instrument_token'].map(main_token_symbol_dict)
            
            #print(dt.now(), 'Adding TableName')
            ticks_df['tablename'] = ticks_df['instrument_token'].map(main_token_table_dict)
            
            #print(dt.now(), 'Adding DBName')
            ticks_df['dbname'] = ticks_df['instrument_token'].map(token_to_db_name_dict)
            
            #print(dt.now(), 'Unwrapping ohlc')
            # Unwrap 'ohlc' dictionary into separate columns
            ticks_df['open'] = ticks_df['ohlc'].apply(lambda x: x.get('open', None))
            ticks_df['high'] = ticks_df['ohlc'].apply(lambda x: x.get('high', None))
            ticks_df['low'] = ticks_df['ohlc'].apply(lambda x: x.get('low', None))
            ticks_df['close'] = ticks_df['ohlc'].apply(lambda x: x.get('close', None))
            
            #Dropping original ohlc column 
            ticks_df.drop(columns=['ohlc'], inplace=True)
            
            #print(dt.now(), 'Done OHLC. Unwrapping depth using get_depth_values')
            #Cases were ticks are purely for index symbols
            if 'depth' not in ticks_df.columns:
                # If 'depth' column does not exist, populate bq*, bp*, bo*, sq*, sp*, and so* columns with None
                for i in range(5):
                    ticks_df[f'bq{i}'] = None
                    ticks_df[f'bp{i}'] = None
                    ticks_df[f'bo{i}'] = None
                    ticks_df[f'sq{i}'] = None
                    ticks_df[f'sp{i}'] = None
                    ticks_df[f'so{i}'] = None
            else:
                # 'depth' column exists; use get_depth_values to extract and assign data
                ticks_df = get_depth_values(ticks_df)
                #Drop original depth column
                ticks_df.drop(columns=['depth'], inplace=True, errors='ignore')
                
            #print(dt.now(), 'Done Unwrapping. Renaming columns.')
            #Dropping original 'depth' column
            #Also dropping 'tradeable' and 'mode' columns
            #Depth might be missing if the ticks contain only index data
            ticks_df.drop(columns=['tradable', 'mode'], inplace=True, errors='ignore')
            
            #Renaming columns to match with friendly column names in the table
            ticks_df.rename(columns={
                'exchange_timestamp': 'timestamp',
                'last_price': 'price',
                'last_traded_quantity': 'qty',
                'average_traded_price': 'avgPrice',
                'volume_traded': 'volume',
                'total_buy_quantity': 'bQty',
                'total_sell_quantity': 'sQty',
                'last_trade_time': 'lastTradeTime',
                'change': 'changeper',
                'oi_day_high': 'oiHigh',
                'oi_day_low': 'oiLow'
            }, inplace=True)

            #print(dt.now(), 'Done Renaming. Inserting DF to SQL.')
            # Insert the DataFrame into SQL
            #ticks_df.to_sql('daily_table', con=engine, if_exists='append', index=False, method='multi')
            
            # Use the custom method with to_sql
            ticks_df.to_sql(f'{daily_table_name}', con=engine, if_exists='append', index=False, method=replace_sql_execute_many)
            
            #method='multi' to reduce SQL statements, round trips and in turn Disk I/O
            #print(dt.now(), 'Done inserting ticks to SQL')
            #could receive multiple ticks for the same instrument_token with the same exchange timestamp.
            #Needs to be handled separately. Make use of the auto increment index to find the latest value for each second
            
            
        except Exception as e:
            message = f'Market Data Ticker - bulk_replace failed for {ticks}. Exception : {e}. Traceback :  {str(traceback.format_exc())}'
            logger = get_market_data_ticker_logger()
            logger.log_error(message)
            error_handler = ErrorHandler()
            error_handler.handle_error('Market Data Ticker - ' + message)
            send_market_data_email('Market Data Ticker - Dump Failed.', message)
    
    try:
        #Get latest access token from DB using the broker token manager
        access_token = broker_token_manager.get_latest_access_token()
        if not access_token:
            raise Exception("No access token found in database")
     
        kws = KiteTicker(api_key, access_token)
        
        def on_ticks(ws, ticks):
            #for tick in ticks: #for debug
            #    bulk_print(ticks)
            bulk_replace(ticks)
                
        def on_connect(ws, response):
            message = "Market Data Ticker : Connection Successful"
            log_market_data_ticker(message)
            ws.subscribe(full_token_list)
            ws.set_mode(ws.MODE_FULL, full_token_list)
            
        def on_close(ws, code, reason):
            # On connection close stop the main loop
            # Reconnection will not happen after executing `ws.stop()`
            ws.stop()
            
        # Assign the callbacks.
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.connect()
        
    except Exception as e:
        message = f'Market Data Ticker - Dump Failed. Exception : {e}. Traceback :  {str(traceback.format_exc())}'
        logger = get_market_data_ticker_logger()
        logger.log_error(message)
        error_handler = ErrorHandler()
        error_handler.handle_error('Market Data Ticker - ' + message)
        send_market_data_email('Market Data Ticker - Dump Failed.', message)
        
if __name__ == '__main__':
    #Create main daily database
    # In PostgreSQL, we'll create the schema if it doesn't exist
    cursor = db_manager.get_connection().cursor()
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {equity_database_name}')
    
    '''
    ticks_df.to_sql is capable of creating the table as well.
    But if for some reason you only the index (NIFTY50, BANK) tokens, the table created will have lesser columns.
    As a result, if you try to store non-index (NIFTY50, BANK) tokens to this table, it will fail
    Sometimes tick includes updated data for the same timestamp. Will result in duplicates if stored as is.
    Hence doing UNIQUE (instrument_token, timestamp) .
    Actual SQL used under the hood is INSERT ON CONFLICT for PostgreSQL. This allows querying from daily_table live, without much complexity.
    Filter on tablename or symbol to get actual data.
    Examples:
        SELECT timestamp, price FROM daily_table WHERE tradingsymbol='NIFTY 50' order by timestamp DESC LIMIT 5;
        SELECT timestamp, price FROM daily_table WHERE tablename='NIFTY' order by timestamp DESC LIMIT 5;
        SELECT timestamp, price, volume FROM daily_table WHERE tradingsymbol='NIFTY24APRFUT' order by timestamp DESC LIMIT 5;
        SELECT timestamp, price, volume FROM daily_table WHERE tablename='NIFTYFUT' order by timestamp DESC LIMIT 5;
        SELECT timestamp, price, volume FROM daily_table WHERE instrument_token=13368834 order by timestamp DESC LIMIT 5;

    Also enforcing column names, datatypes and structure

    column dbname is of no use for live data.
    Creating in case it can be used later to further optimize the daily backup task

    Renamed tick columns while storing to DB
        #exchange_timestamp to timestamp
        #last_price to price
        #last_traded_quantity to qty
        #average_traded_price to avgPrice
        #volume_traded to volume
        #total_buy_quantity to bQty
        #total_sell_quantity to sQty
        #last_trade_time to lastTradeTime
        #change to changeper
        #oi_day_high to oiHigh
        #oi_day_low to oiLow
    '''
    cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {equity_database_name}.{daily_table_name} (
                    instrument_token BIGINT,
                    tradingsymbol VARCHAR(100),
                    tablename VARCHAR(100),
                    dbname VARCHAR(100),
                    timestamp TIMESTAMP, price DECIMAL(19,2),
                    qty INTEGER,
                    avgPrice DECIMAL(19,2),
                    volume BIGINT,
                    bQty INTEGER,
                    sQty INTEGER,
                    open DECIMAL(19,2),
                    high DECIMAL(19,2),
                    low DECIMAL(19,2),
                    close DECIMAL(19,2),
                    changeper DECIMAL(60,10),
                    lastTradeTime TIMESTAMP,
                    oi INTEGER,
                    oiHigh INTEGER,
                    oiLow INTEGER,
                    bq0 INTEGER, bp0 DECIMAL(19,2), bo0 INTEGER,
                    bq1 INTEGER, bp1 DECIMAL(19,2), bo1 INTEGER,
                    bq2 INTEGER, bp2 DECIMAL(19,2), bo2 INTEGER,
                    bq3 INTEGER, bp3 DECIMAL(19,2), bo3 INTEGER,
                    bq4 INTEGER, bp4 DECIMAL(19,2), bo4 INTEGER,
                    sq0 INTEGER, sp0 DECIMAL(19,2), so0 INTEGER,
                    sq1 INTEGER, sp1 DECIMAL(19,2), so1 INTEGER,
                    sq2 INTEGER, sp2 DECIMAL(19,2), so2 INTEGER,
                    sq3 INTEGER, sp3 DECIMAL(19,2), so3 INTEGER,
                    sq4 INTEGER, sp4 DECIMAL(19,2), so4 INTEGER,
                    UNIQUE (instrument_token, timestamp)
                )''')
    
    # Create indexes separately in PostgreSQL
    cursor.execute(f'CREATE INDEX IF NOT EXISTS tablenameindex ON {equity_database_name}.{daily_table_name} (tablename)')
    cursor.execute(f'CREATE INDEX IF NOT EXISTS symbolindex ON {equity_database_name}.{daily_table_name} (tradingsymbol)')
    cursor.execute(f'CREATE INDEX IF NOT EXISTS instrument_token_index ON {equity_database_name}.{daily_table_name} (instrument_token)')
    cursor.execute(f'CREATE INDEX IF NOT EXISTS timestamp_index ON {equity_database_name}.{daily_table_name} (timestamp)')
    
    # Return the connection to the pool
    cursor.close()
    db_manager.return_connection(db_manager.get_connection())

    #If run as standalone, ticker will pickup close values from market_data_config.json
    run_market_data_ticker(market_close_hour, market_close_minute)
    #run_market_data_ticker(12, 44)