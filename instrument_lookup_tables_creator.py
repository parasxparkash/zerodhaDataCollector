"""
Author: Paras Parkash
Source: Market Data Acquisition System

Downloads Instrument List from Broker
Lists the exchange tokens (instrument_token) to be subscribed, based on the lookup table Generated / Updated by equity_universe_updater
Estimate current and next expiry dates for Nifty and BankNifty Options
Generates Instrument Token Lists for 
    - NiftyOptions - current and next expiry 
    - BankNiftyOptions - current and Next expiry 
Creates lookup Dictionaries that will be used for exchangeToken => TableName and exchangeToken => Symbol in the actual ticker
Creates DB and one table for storing daily tick. Can be used for live queries during market hours
Table:
    {equity_database_name}.daily_table
Example queries to get data from live DB (live Ticks):
    SELECT timestamp, price FROM daily_table WHERE tradingsymbol='NIFTY 50' order by timestamp DESC LIMIT 5;
    SELECT timestamp, price FROM daily_table WHERE tablename='NIFTY' order by timestamp DESC LIMIT 5;
    SELECT instrument_token, timestamp, price, volume FROM daily_table WHERE tradingsymbol='NIFTY24APRFUT' order by timestamp DESC LIMIT 5;
    SELECT timestamp, price, volume FROM daily_table WHERE tablename='NIFTYFUT' order by timestamp DESC LIMIT 5;
    SELECT timestamp, price, volume FROM daily_table WHERE instrument_token=13368834 order by timestamp DESC LIMIT 5;
equity_database_name configured in market_data_config.json

Some of the operations for Nifty500 token and Options tokens, 
lookup table creation and SQL table creation could have been combined.
combined vs 3 separate lookup tables for ~1300 tokens doesn't have any measurable performance difference.
But splitting them on purpose, for readability.
Also, if you don't want a part of the ticker, 
you could just remove it from here and the main ticker - market_data_ticker

Check _InstrumentsSubscribed.log to see the list of symbols subscribed to

#Change log - 2024-04-08:
    - market_data_ticker now stores live ticks into one table now. This is to reduce IOPS
    - Hence only creating one table, instead of the previous 'one table per instrument' approach 

#Change log - 2024-11-18
    - get_bank_nifty_expiry now takes 'offset_month' and returns this month or next month expiry.
    - This change is required as BankNifty expiries are no longer weekly.
    - Also simplifying get_nifty_expiry to accept an 'offset_week' and return weekly expiries accordingly

#Change log - 2025-08-06:
    - makedirs(lookup_directory,exist_ok=True) in the off chance that the lookup directory wasn't created. 
        - unlikely,first, standalone run for lookup_table_creator
    - get_nifty_expiry accepts 'offset_expiry' and returns this or next expiry.
        - agnostic of weekly or monthly or fortnightly or whatever the puck SEBI decided to do
    - Similarly, get_bank_nifty_expiry accepts 'offset_expiry'
    
"""

import pandas as pd
import numpy as np
import MySQLdb
from datetime import datetime as dt, date, timedelta
from dateutil.relativedelta import relativedelta, TH, WE
from os import path, makedirs
import json
from market_data_mailer import send_market_data_email
import traceback
from system_error_logger import log_system_error
from time import sleep
import sys
import urllib.request
from validate_system_config import is_system_config_default

RETRY_COUNT = 5

config_file = 'market_data_config.json'
with open(config_file, 'r') as config_file_handle:
    system_config = json.load(config_file_handle)

notification_recipients = system_config['notification_recipients']
database_host = system_config['database_host']
database_user = system_config['database_user']
database_password = system_config['database_password']
database_port = system_config['database_port']

daily_table_name = 'daily_table'
equity_database_name = system_config['equity_database_name']

lookup_directory = 'instrument_lookup_tables'
makedirs(lookup_directory, exist_ok=True)
equity_universe_lookup_file = 'equity_universe_lookup.csv'
equity_instrument_file_path = path.join(lookup_directory, equity_universe_lookup_file)

def log_lookup_table_creator(message):
    """
    Log lookup table creator messages to file and console
    Optimized for performance by reducing I/O operations
    """
    timestamp = dt.now()
    print(timestamp, message)
    log_directory = path.join('logs', str(date.today()) + '_market_data_logs')
    if not path.exists(log_directory):
        makedirs(log_directory)
    log_file = path.join(log_directory, f'lookup_table_creator_logs_{str(date.today())}.log')
    log_message = f'\n{timestamp}    {message}'
    with open(log_file, 'a') as f:
        f.write(log_message)

def log_instrument_list(message):
    """
    Log instrument list messages to file
    Optimized for performance by reducing I/O operations
    """
    timestamp = dt.now()
    log_directory = path.join('logs', str(date.today()) + '_market_data_logs')
    if not path.exists(log_directory):
        makedirs(log_directory)
    log_file = path.join(log_directory, f'instruments_subscribed_{str(date.today())}.log')
    log_message = f'\n{timestamp}    {message}'
    with open(log_file, 'a') as f:
        f.write(log_message)

def download_broker_instrument_file():
    """
    Download the complete broker instrument dump file
    """
    broker_dump_url = 'https://api.kite.trade/instruments'
    broker_instrument_dump_file_name = 'broker_instrument_dump.csv'
    broker_instrument_dump_file_path = path.join('instrument_lookup_tables', broker_instrument_dump_file_name)
    
    for attempt in range(1, RETRY_COUNT + 1):
        try:    
            urllib.request.urlretrieve(broker_dump_url, broker_instrument_dump_file_path)            
            message = 'Downloaded Instrument dump file from Broker'
            log_lookup_table_creator(message)   
            return None
        except Exception as e:
            message = f'Downloading brokerInstrumentsDump from {broker_dump_url} Failed. Attempt No :{attempt} . Exception-> {e} Traceback : {traceback.format_exc()}.\nWill retry after 30 seconds'
            log_lookup_table_creator(message)
            log_system_error('lookup_tables_creator - ' + message)
            sleep(30)
        else:
            break
    else:
        message = f'Downloading brokerInstrumentsDump from {broker_dump_url} Failed after {RETRY_COUNT} attempts. Exiting'
        log_lookup_table_creator(message)
        log_system_error('lookup_tables_creator - ' + message)
        sys.exit()


#broker_instrument_dump_file is used multiple times
#Equity universe - Once for getting instrument_tokens
#Nifty Options - Thrice - Once for each each expiry and once for getting instrument_tokens
#BankNifty Options - Thrice - Once for each each expiry and once for getting instrument_tokens
#So retaining and using a local file if it is fresh
def get_broker_instrument_dump():
    """
    Get the broker instrument dump, using local file if fresh
    """
    broker_instrument_dump_file_name = 'broker_instrument_dump.csv'
    broker_instrument_dump_file_path = path.join('instrument_lookup_tables', broker_instrument_dump_file_name)
    
    if path.exists(broker_instrument_dump_file_path):
        one_hour_ago = dt.now() - timedelta(hours=1)
        #if file exists and is fresh (not older than an hour), use the local file.
        broker_dump_modified_time = dt.fromtimestamp(path.getmtime(broker_instrument_dump_file_path))
        if broker_dump_modified_time >= one_hour_ago:
            message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} is fresh. Using it.'
            log_lookup_table_creator(message)
        else:
            message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} is older than 1 hour. Downloading a new one'
            log_lookup_table_creator(message)
            download_broker_instrument_file()
    else:
        message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} not found. Downloading a new one'
        log_lookup_table_creator(message)
        download_broker_instrument_file()
        
    return pd.read_csv(broker_instrument_dump_file_path)

def get_nifty_expiry(offset_expiry): #offset_expiry=0 => this expiry. offset_expiry=1 => next expiry
    """
    Get the Nifty expiry date for the given offset
    """
    #Uses global variable broker_instruments_dump
    #Filtering Nifty Options from broker_instruments_dump
    broker_instruments_dump = get_broker_instrument_dump()
    nifty_options = broker_instruments_dump[broker_instruments_dump['segment'].isin(['NFO-OPT'])]
    nifty_options = nifty_options[nifty_options['name'].isin(['NIFTY'])]
    #Filtering just CE as we interested only in the expiry dates now and not the actual instruments
    nifty_options = nifty_options[nifty_options['instrument_type'].isin(['CE'])]
    nifty_expiry_dates = sorted(list(nifty_options['expiry'].unique()))

    return str(nifty_expiry_dates[offset_expiry])  

def get_bank_nifty_expiry(offset_expiry): #offset_expiry=0 => this expiry. offset_expiry=1 => next expiry
    """
    Get the Bank Nifty expiry date for the given offset
    """
    #Uses global variable broker_instruments_dump
    #Filtering Nifty Options from broker_instruments_dump
    broker_instruments_dump = get_broker_instrument_dump()
    bank_nifty_options = broker_instruments_dump[broker_instruments_dump['segment'].isin(['NFO-OPT'])]
    bank_nifty_options = bank_nifty_options[bank_nifty_options['name'].isin(['BANKNIFTY'])]
    #Filtering just CE as we interested only in the expiry dates now and not the actual instruments
    bank_nifty_options = bank_nifty_options[bank_nifty_options['instrument_type'].isin(['CE'])]
    bank_nifty_expiry_dates = sorted(list(bank_nifty_options['expiry'].unique()))
    return str(bank_nifty_expiry_dates[offset_expiry])  

def create_equity_universe_lookup_tables():
    """
    Create lookup tables for equity universe instruments
    """
    try:
        '''
        =========================================
        Creating Lookup Tables for Equity Universe - Start
        =========================================
        '''
        message = 'Market Data - Equity Universe Lookup Table Creation Started'
        log_lookup_table_creator(message)
        
        ##Reading Equity Universe instruments +Index + IndexFuture to be subscribed
        ##Reading the lookup table again to create lookup dictionary
        equity_universe_instrument_symbols_tables = pd.read_csv(equity_instrument_file_path)
        equity_universe_instrument_symbols = equity_universe_instrument_symbols_tables['Symbol'].values.tolist()
    
        #Downloading Broker Instrument dump 
        broker_instruments_dump = get_broker_instrument_dump()
        
        ## retaining instrument_token and tradingsymbol only in the instrument dump
        equity_universe_instruments = broker_instruments_dump[broker_instruments_dump['tradingsymbol'].isin(equity_universe_instrument_symbols)]
        #Filtering further.
        #'exchange'.isin(['NSE','NFO']
        #'instrument_type'.isin(['EQ','FUT']
        equity_universe_instruments = equity_universe_instruments[equity_universe_instruments['exchange'].isin(['NSE','NFO'])]
        equity_universe_instruments = equity_universe_instruments[equity_universe_instruments['instrument_type'].isin(['EQ','FUT'])]
        
        ##Retaining instrument_token and tradingsymbol only from the instrument dump 
        equity_universe_instruments = equity_universe_instruments[['instrument_token','tradingsymbol']]
        equity_universe_instruments = equity_universe_instruments.drop_duplicates()
        
        #Creating a lookup dictionary for Symbols. Lookup the exchange token, get the Symbol response
        equity_universe_token_symbol_dict = equity_universe_instruments.set_index('instrument_token')['tradingsymbol'].to_dict()  
        #Saving the exchange_token:Symbol Dictionary
        np.save(path.join(lookup_directory, 'equity_universe_token_symbol_dict.npy'), equity_universe_token_symbol_dict)
        message = 'Saved equity_universe_token_table exchange_token:Symbol Dictionary => equity_universe_token_symbol_dict.npy'
        log_lookup_table_creator(message)        
        
        ##Creating a lookup dictionary - Lookup Symbol, get TableName
        equity_universe_table_name_lookup = equity_universe_instrument_symbols_tables.set_index('Symbol')['TableName'].to_dict()
        #In the original instrument dump, replacing the symbol with the table name.
        #This way, in the ticker, any response for the instrument_token is stored to its table directly.
        #This is necessary as the table name is not the same as the symbol name for a few instruments:
            #Symbol has special character (M&M)
        #The tradingsymbol column now holds the Table Name
        equity_universe_instruments = equity_universe_instruments.rename(columns={'tradingsymbol': 'TableName'})
        equity_universe_instruments.replace(equity_universe_table_name_lookup, inplace=True)
        
        #Creating a lookup dictionary from this.
        #Lookup the exchange token, get the table name as response
        #The tradingsymbol column now holds the Table Name
        equity_universe_token_table_dict = equity_universe_instruments.set_index('instrument_token')['TableName'].to_dict()
        
        #Saving the exchange_token:TableName Dictionary
        np.save(path.join(lookup_directory, 'equity_universe_token_table_dict.npy'), equity_universe_token_table_dict)
        message = 'Saved equity_universe_token_table exchange_token:TableName Dictionary => equity_universe_token_table_dict.npy'
        log_lookup_table_creator(message)
        
        #Separating Indexes NIFTY and BANKNIFTY from the main list
        #They have just two columns in the feed - timestamp and price.
        #Don't have to be removed from the main list.
        #just a list to check and create different table structure at EoD
        index_instruments = equity_universe_instruments.loc[equity_universe_instruments['TableName'].isin(['NIFTY', 'BANKNIFTY'])]
        index_instruments.drop('TableName', axis=1).to_csv(path.join(lookup_directory, 'index_token_list.csv'), index=False)	
        message = 'Saved index_token_list.csv to differentiate Indexes Nifty and BankNifty from the other instruments for SQL store'
        log_lookup_table_creator(message)
        
        #Saving the main exchange_token list to subscribe later
        equity_universe_instruments.drop('TableName', axis=1).to_csv(path.join(lookup_directory, 'equity_universe_token_list.csv'), index=False)		
        message = 'Saved equity_universe_token_list.csv'
        log_lookup_table_creator(message)
        
        message = f'Equity Universe - Subscribing to {len(equity_universe_instruments)} Instruments'
        log_lookup_table_creator(message)
        log_instrument_list(message)
        
        # Creating a string from the 'TableName' column where each item is on a new line
        equity_universe_instrument_name_string = '\n'.join(equity_universe_instrument_symbols)
        message = 'Equity Universe Instruments Subscribed :\n' + equity_universe_instrument_name_string
        log_instrument_list(message)

        return True
    except Exception as e:
        message = f"Instrument Token Lookup Table Creator - Equity Universe - failed with exception {e}. Traceback : {str(traceback.format_exc())}"
        log_lookup_table_creator(message)
        log_system_error('lookup_tables_creator - ' + message)
        send_market_data_email('Market Data Equity Universe Lookup Table Creator Failed', message)
        return False
        '''
        =========================================
        Creating Lookup Tables for Equity Universe - End
        =========================================
        '''        

def create_nifty_options_lookup_tables():
    """        
    =========================================
    Creating Lookup Tables for Nifty Options - Start
    =========================================
    """
    message = 'Market Data - Nifty Options Lookup Table Creation Started'
    log_lookup_table_creator(message)
    try:
        nifty_this_expiry = get_nifty_expiry(0) #This Expiry
        nifty_next_expiry = get_nifty_expiry(1) #Next Expiry
        
        #Downloading Broker Instrument dump 
        broker_instruments_dump = get_broker_instrument_dump()
        
        #Filtering Nifty Options from broker_instruments_dump
        nifty_options_df = broker_instruments_dump[broker_instruments_dump['segment'].isin(['NFO-OPT'])]
        nifty_options_df = nifty_options_df[nifty_options_df['name'].isin(['NIFTY'])]
        
        #Retain this and Next Expiry only
        nifty_options_df = nifty_options_df[nifty_options_df['expiry'].isin([nifty_this_expiry, nifty_next_expiry])]
        
        ##Retaining instrument_token and tradingsymbol only from the instrument dump 
        nifty_options_df = nifty_options_df[['instrument_token','tradingsymbol']]
        nifty_options_df = nifty_options_df.drop_duplicates()
        
        #Trading Symbol can be used as TableName as Index option symbols do not have special characters or spaces
        nifty_options_df = nifty_options_df.rename(columns={'tradingsymbol': 'TableName'})
        
        ##Creating a lookup dictionary - Lookup instrument_token, get TableName
        nifty_options_token_table = nifty_options_df.set_index('instrument_token')['TableName'].to_dict()

        #Saving the exchange_token:TableName Dictionary
        #Same dictionary can be used for instrument_token:Symbol lookup for options
        np.save(path.join(lookup_directory, 'nifty_options_token_table_dict.npy'), nifty_options_token_table)
        message = 'Saved nifty_options_token_table exchange_token:TableName Dictionary => nifty_options_token_table_dict.npy'
        log_lookup_table_creator(message)
        
        #Saving nifty_options instrument_token list to subscribe.
        #This will also be used to save nifty option ticks to a separate DB
        nifty_options_df.drop('TableName', axis=1).to_csv(path.join(lookup_directory, 'nifty_options_token_list.csv'), index=False)	
        message = f'Subscribing to {len(nifty_options_df)} Nifty Options. Saved nifty_options_token_list.csv'
        log_lookup_table_creator(message)
        log_instrument_list(message)
        
        # Creating a string from the 'TableName' column where each item is on a new line
        nifty_options_name_string = '\n'.join(nifty_options_df['TableName'].astype(str))
        message = 'Nifty Option Instruments Subscribed :\n' + nifty_options_name_string
        log_instrument_list(message)

        return True
    except Exception as e:
        message = f"Instrument Token Lookup Table Creator - Nifty Options - failed with exception {e}. Traceback : {str(traceback.format_exc())}"
        log_lookup_table_creator(message)
        log_system_error('lookup_tables_creator - ' + message)
        send_market_data_email('Market Data Nifty Options Lookup Table Creator Failed', message)
        return False            

    '''
    =========================================
    Creating Lookup Tables for Nifty Options - End
    =========================================
    '''            

def create_bank_nifty_options_lookup_tables():
    """        
    =========================================
    Creating Lookup Tables for BankNifty Options - Start
    =========================================               
    """    
    message = 'Market Data - BankNifty Options Lookup Table Creation Started'
    log_lookup_table_creator(message)
    try:        
        bank_nifty_this_expiry = get_bank_nifty_expiry(0) #This Expiry
        bank_nifty_next_expiry = get_bank_nifty_expiry(1) #Next Expiry
        
        #Downloading Broker Instrument dump 
        broker_instruments_dump = get_broker_instrument_dump()
        
        #Filtering Nifty Options from broker_instruments_dump
        bank_nifty_options_df = broker_instruments_dump[broker_instruments_dump['segment'].isin(['NFO-OPT'])]
        bank_nifty_options_df = bank_nifty_options_df[bank_nifty_options_df['name'].isin(['BANKNIFTY'])]
        
        #Retain this and Next Expiry only
        bank_nifty_options_df = bank_nifty_options_df[bank_nifty_options_df['expiry'].isin([bank_nifty_this_expiry, bank_nifty_next_expiry])]
        
        ##Retaining instrument_token and tradingsymbol only from the instrument dump 
        bank_nifty_options_df = bank_nifty_options_df[['instrument_token','tradingsymbol']]
        bank_nifty_options_df = bank_nifty_options_df.drop_duplicates()
        
        #Trading Symbol can be used as TableName as Index option symbols do not have special characters or spaces
        bank_nifty_options_df = bank_nifty_options_df.rename(columns={'tradingsymbol': 'TableName'})
        
        ##Creating a lookup dictionary - Lookup instrument_token, get TableName
        bank_nifty_options_token_table = bank_nifty_options_df.set_index('instrument_token')['TableName'].to_dict()

        #Saving the exchange_token:TableName Dictionary
        #Same dictionary can be used for instrument_token:Symbol lookup for options
        np.save(path.join(lookup_directory, 'bank_nifty_options_token_table_dict.npy'), bank_nifty_options_token_table)
        message = 'Saved bank_nifty_options_token_table exchange_token:TableName Dictionary => bank_nifty_options_token_table_dict.npy'
        log_lookup_table_creator(message)
        
        #Saving BankNiftyOptions instrument_token list to subscribe.
        #This will also be used to save nifty option ticks to a separate DB
        bank_nifty_options_df.drop('TableName', axis=1).to_csv(path.join(lookup_directory, 'bank_nifty_options_token_list.csv'), index=False)	
        message = f'Subscribing to {len(bank_nifty_options_df)} BankNifty Options. Saved bank_nifty_options_token_list.csv'
        log_lookup_table_creator(message)
        log_instrument_list(message)
        
        # Creating a string from the 'TableName' column where each item is on a new line
        bank_nifty_options_name_string = '\n'.join(bank_nifty_options_df['TableName'].astype(str))
        message = 'Bank Nifty Option Instruments Subscribed :\n' + bank_nifty_options_name_string
        log_instrument_list(message)
        return True
    except Exception as e:
        message = f"Instrument Token Lookup Table Creator - BankNifty Options - failed with exception {e}. Traceback : {str(traceback.format_exc())}"
        log_lookup_table_creator(message)
        log_system_error('lookup_tables_creator - ' + message)
        send_market_data_email('Market Data BankNifty Options Lookup Table Creator Failed', message)
        return False   
    '''
    =========================================
    Creating Lookup Tables for Bank Nifty Options - End
    =========================================
    '''          

def create_instrument_lookup_tables():    
    """
    Main function to create all instrument lookup tables
    """
    if is_system_config_default():
        message = 'System Config has defaults. access_token_request is exiting'
        log_lookup_table_creator(message)
        log_system_error(message)
        return False
    #On failure, function would have exited at the False return.
    #Hence no Else required here
    equity_universe_tables_created = create_equity_universe_lookup_tables()
    nifty_options_tables_created = create_nifty_options_lookup_tables()
    bank_options_tables_created = create_bank_nifty_options_lookup_tables()
    
    if not all([equity_universe_tables_created, nifty_options_tables_created, bank_options_tables_created]):
        message = 'One or more Lookup Table Creation Activities FAILED!!!'
        log_lookup_table_creator(message)  
        log_system_error('lookup_tables_creator - ' + message)
        return False   
        
    if equity_universe_tables_created and nifty_options_tables_created and bank_options_tables_created:
        #Create {equity_database_name}
        connection = MySQLdb.connect(host=database_host, user=database_user, passwd=database_password, port=database_port)
        cursor = connection.cursor() 
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS {equity_database_name}')
        
        ##Create Daily table - {equity_database_name}.{daily_table_name} 
        
        cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {equity_database_name}.{daily_table_name} (
                        instrument_token BIGINT(20), 
                        tradingsymbol VARCHAR(10),
                        tablename VARCHAR(100),
                        dbname VARCHAR(100),
                        timestamp DATETIME, price DECIMAL(19,2), 
                        qty INT UNSIGNED,
                        avgPrice DECIMAL(19,2),
                        volume BIGINT,
                        bQty INT UNSIGNED,
                        sQty INT UNSIGNED,  
                        open DECIMAL(19,2),
                        high DECIMAL(19,2),
                        low DECIMAL(19,2),
                        close DECIMAL(19,2),
                        changeper DECIMAL(60,10),
                        lastTradeTime DATETIME,
                        oi INT,
                        oiHigh INT,
                        oiLow INT,  
                        bq0 INT UNSIGNED, bp0 DECIMAL(19,2), bo0 INT UNSIGNED,
                        bq1 INT UNSIGNED, bp1 DECIMAL(19,2), bo1 INT UNSIGNED,
                        bq2 INT UNSIGNED, bp2 DECIMAL(19,2), bo2 INT UNSIGNED,
                        bq3 INT UNSIGNED, bp3 DECIMAL(19,2), bo3 INT UNSIGNED,
                        bq4 INT UNSIGNED, bp4 DECIMAL(19,2), bo4 INT UNSIGNED,  
                        sq0 INT UNSIGNED, sp0 DECIMAL(19,2), so0 INT UNSIGNED,
                        sq1 INT UNSIGNED, sp1 DECIMAL(19,2), so1 INT UNSIGNED,                    
                        sq2 INT UNSIGNED, sp2 DECIMAL(19,2), so2 INT UNSIGNED,                    
                        sq3 INT UNSIGNED, sp3 DECIMAL(19,2), so3 INT UNSIGNED,                    
                        sq4 INT UNSIGNED, sp4 DECIMAL(19,2), so4 INT UNSIGNED,
                        UNIQUE (instrument_token, timestamp),
                        INDEX tablenameindex (tablename),
                        INDEX symbolindex (tradingsymbol),
                        INDEX instrument_token_index (instrument_token),
                        INDEX timestamp_index (timestamp)
                    )''')
        
        message = 'Market Data - All Lookup Table Creation Activities Successful'
        log_lookup_table_creator(message)          
        return True
    #Catch all
    return False
    
if __name__ == '__main__':
    create_instrument_lookup_tables()