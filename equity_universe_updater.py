# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System

Can be Run standalone

Will Fetch Nifty 500 list from https://archives.nseindia.com/content/indices/ind_nifty500list.csv
If local file doesn't exist, 
    - Will create a new 'lookup file with the instrument symbols
    - Add NIFTY, BANKNIFTY and their current month futures to it
    - Create corresponding DB friendly table names
If Local file exists:
    - Cross check symbols there with the symbols in the local file
    - Will add new symbols and create corresponding DB friendly table names
    - Will NOT REMOVE any existing symbols in the file.
    - Only exceptions are NIFTYFUT and BANKNIFTYFUT as the symbol changes every month
Advantages:
    - Removes the need for regular maintenance when Nifty500 is updated
    - Automatically picks up new symbols that result from symbol changes
    - Log such changes to NIFTY500_symbolsAdded.log
Disadvantage:
    - Symbol changes will not be recognized.
        - Example:
            Amara Raja Energy & Mobility Ltd.Changed from AMARAJABAT (Table Name AMARAJABAT) to ARE&M (Table Name ARE_M)
            in September 2023.
            This would result in a new entry added to the list for ARE&M - ARE_M
            While this ensures that the new symbol's data is captured,
                - Splits the data into two tables 
                - Leaves the old table hanging
            Hence when you read data, you would have to Union  AMARAJABAT and ARE_M to get the full picture.
     - The small tables report sent at the end of every day and nifty500_symbolsChanged.log can be used to manage this situation
     - Delisted stocks won't be removed from the existing lookupTables_Nifty500.csv file
ChangeLog for symbols identified manually:
    - Amara Raja Energy & Mobility Ltd.Changed from AMARAJABAT (Table Name AMARAJABAT) to ARE&M (Table Name ARE_M)
    in September 2023.
    - Kennametal India Limited (KENNAET) was removed from the NSE on October 26, 2023
    - TATACOFFEE delisted - https://tradingqna.com/t/everything-you-need-to-know-about-merger-of-tata-coffee-with-tata-consumer-products/158981
    - WELSPUNIND to WELSPUNLIV on 14 Dec 2023
    - ADANITRANS to ADANIENSOL with effect from August 24, 2023
    - MFL changed to EPIGRAL  with effect from September 11, 2023

ChangeLog:
2024-09-23:
    Nifty 500 list URL changed from https://archives.nseindia.com/content/indices/ind_nifty500list.csv to https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv
    And NSE seems to have started blocking programmatic access.
    Hence mimicking a user-agent (generate_random_user_agent) and extracting CSV from the response text.

ChangeLog:
2025-08-07:
    - getCurrentFuture for Nifty and BankNifty were previously check LastThursday and Lastwednesday.
    - Fixed them to rely purely on the Zerodha instrument dump, dynamically finding the active Future for today
    - Resilient to changes in the expiry day (wednesday, 2nd saturday, 45th friday...)

"""

import pandas as pd
from datetime import datetime as dt, timedelta, date
import shutil
import sys
from time import sleep
from market_data_mailer import send_market_data_email
import traceback
from io import StringIO
import requests
import random
import urllib.request
from utils.logger import get_equity_universe_logger
from utils.error_handler import ErrorHandler, retry_on_failure

RETRY_COUNT = 5

lookup_files_directory = 'instrument_lookup_tables'
current_lookup_file_name = 'equity_universe_lookup.csv'
current_lookup_table_path = f'{lookup_files_directory}/{current_lookup_file_name}'
n500_url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"


def generate_random_user_agent():
    """
    Generate a random user agent string to avoid blocking by NSE
    """
    logger = get_equity_universe_logger()
    
    platforms = [
        'Windows NT 10.0; Win64; x64',
        'Macintosh; Intel Mac OS X 10_15_7',
        'X11; Linux x86_64',
        'iPhone; CPU iPhone OS 14_0 like Mac OS X',
        'Android 10; Mobile;'
    ]
    
    browsers = [
        ('Chrome', ['91.0.4472.124', '92.0.4515.159', '93.0.4577.82']),
        ('Firefox', ['89.0', '90.0', '91.0']),
        ('Safari', ['14.0.1', '13.1.2', '12.1.2']),
        ('Edge', ['91.0.864.59', '92.0.902.55'])
    ]
    
    # Randomly select a platform and browser
    platform = random.choice(platforms)
    browser, versions = random.choice(browsers)
    version = random.choice(versions)
    
    # Construct the User-Agent string based on the platform and browser
    if browser == 'Safari':
        user_agent = f'Mozilla/5.0 ({platform}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{version} Safari/605.1.15'
    elif browser == 'Chrome':
        user_agent = f'Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36'
    elif browser == 'Firefox':
        user_agent = f'Mozilla/5.0 ({platform}; rv:{version}) Gecko/20101 Firefox/{version}'
    elif browser == 'Edge':
        user_agent = f'Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/{version}'
    
    message = f'User agent for the day:\n {user_agent}'
    logger.log_info(message)
    return user_agent

def download_broker_instrument_file():
    """
    Download the complete broker instrument dump file
    """
    logger = get_equity_universe_logger()
    error_handler = ErrorHandler()
    
    broker_dump_url = 'https://api.kite.trade/instruments'
    broker_instrument_dump_file_name = 'broker_instrument_dump.csv'
    broker_instrument_dump_file_path = f'instrument_lookup_tables/{broker_instrument_dump_file_name}'
    
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            urllib.request.urlretrieve(broker_dump_url, broker_instrument_dump_file_path)
            message = 'Downloaded Instrument dump file from Broker'
            logger.log_info(message)
            return None
        except Exception as e:
            message = f'Downloading brokerInstrumentsDump from {broker_dump_url} Failed. Attempt No :{attempt} . Exception-> {e} Traceback : {traceback.format_exc()}.\nWill retry after 30 seconds'
            logger.log_error(message)
            error_handler.handle_error('lookup_tables_creator - ' + message)
            sleep(30)
        else:
            break
    else:
        message = f'Downloading brokerInstrumentsDump from {broker_dump_url} Failed after {RETRY_COUNT} attempts. Exiting'
        logger.log_error(message)
        error_handler.handle_error('lookup_tables_creator - ' + message)
        sys.exit()

#Not import this from lookup_table creator, to avoid dependency just for this one function
def get_broker_instrument_dump():
    """
    Get the broker instrument dump, using local file if fresh
    """
    logger = get_equity_universe_logger()
    
    broker_instrument_dump_file_name = 'broker_instrument_dump.csv'
    broker_instrument_dump_file_path = f'instrument_lookup_tables/{broker_instrument_dump_file_name}'
    
    from os import path
    if path.exists(broker_instrument_dump_file_path):
        one_hour_ago = dt.now() - timedelta(hours=1)
        #if file exists and is fresh (not older than an hour), use the local file.
        broker_dump_modified_time = dt.fromtimestamp(path.getmtime(broker_instrument_dump_file_path))
        if broker_dump_modified_time >= one_hour_ago:
            message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} is fresh. Using it.'
            logger.log_info(message)
        else:
            message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} is older than 1 hour. Downloading a new one'
            logger.log_info(message)
            download_broker_instrument_file()
    else:
        message = f'local broker_instrument_dump_file {broker_instrument_dump_file_name} not found. Downloading a new one'
        logger.log_info(message)
        download_broker_instrument_file()
        
    return pd.read_csv(broker_instrument_dump_file_path)


def get_current_future(index_name):
    """
    Get current month future for given index
    Args:
        index_name (str): 'NIFTY' or 'BANKNIFTY'
    Returns:
        str: Trading symbol of the current month future
    """
    broker_instruments_dump = get_broker_instrument_dump()
    filtered_df = broker_instruments_dump[
        (broker_instruments_dump['segment'] == 'NFO-FUT') &
        (broker_instruments_dump['name'] == index_name)
    ]
    fut_expiries = filtered_df['expiry'].dropna().unique().tolist()
    this_fut_expiry_date = min(fut_expiries, key=lambda d: dt.strptime(d, "%Y-%m-%d"))
    current_future = filtered_df[filtered_df['expiry'] == this_fut_expiry_date]['tradingsymbol'].iloc[0]
    return current_future


def replace_special_characters(input_text):
    """
    Replace special characters in symbol names with underscores for database compatibility
    """
    #Remove '-BE' if found at the end of the symbolName
    #Subscribe to the BE symbols, yes.
    #But store tick data in table name without the BE flag
    #This way, if/when the BE flag gets removed, the data will still be in the same table
    if input_text[-3:] == '-BE':
        input_text = input_text[:-3]
    #Replace non table name friendly special characters with _
    return input_text.replace('-BE', '').replace('-', '_').replace('&', '_')

@retry_on_failure(max_attempts=5, delay=30)
def get_nifty500_symbol_list():
    '''
    If Series is of type 'BE' (Book Entry), add '-BE' to the end of the symbol
    This seems to be the valid symbol for Broker.
    Examples:
        TATAINVEST becomes TATAINVEST-BE
        ALOKINDS becomes ALOKINDS-BE
    '''
    logger = get_equity_universe_logger()
    error_handler = ErrorHandler()
    
    #Retry 5 times
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            response = requests.get(n500_url, headers={'User-Agent': generate_random_user_agent()})
            csv_data = StringIO(response.text)
            nifty500_from_nse = pd.read_csv(csv_data)
            nifty500_from_nse.loc[nifty500_from_nse['Series'] == 'BE', 'Symbol'] += '-BE'
            return nifty500_from_nse['Symbol'].tolist()
        except Exception as e:
            message = f'get_nifty500_symbol_list Failed. Attempt No :{attempt} . Exception-> {e} Traceback : {traceback.format_exc()}.\nWill retry after 30 seconds'
            logger.log_error(message)
            error_handler.handle_error('equity_universe_updater - ' + message)
            sleep(30)
        else:
            break

    else:
        message = f'get_nifty500_symbol_list - Failed After {RETRY_COUNT} attempts. Exiting Execution'
        logger.log_error(message)
        error_handler.handle_error('equity_universe_updater - ' + message)
        send_market_data_email('Market Data Nifty 500 - get_nifty500_symbol_list Failed. Exiting', message)
        sys.exit()
     

def create_symbol_table_name_list():
    """
    Create a new symbol and table name lookup file
    """
    logger = get_equity_universe_logger()
    from os import path, makedirs
    
    message = f'{path.join(lookup_files_directory, current_lookup_file_name)} not found. Creating a new lookup_table'
    logger.log_info(message)
    
    #create a new dataframe with Symbol and TableName Columns
    lookup_table = pd.DataFrame(columns=['Symbol', 'TableName'])
    
    #Add Nifty and BankNifty Index
    indexes_to_add = [{'Symbol': 'NIFTY 50', 'TableName': 'NIFTY'},
                      {'Symbol': 'NIFTY BANK', 'TableName': 'BANKNIFTY'}]
    
    # Convert indexes_to_add to a DataFrame before concatenation if it's not already a DataFrame
    indexes_to_add_df = pd.DataFrame(indexes_to_add)
    
    # Add indexes to lookup_table
    lookup_table = pd.concat([lookup_table, indexes_to_add_df], ignore_index=True)
    
    #Add Nifty and BankNifty Futures
    nifty_current_future = get_current_future('NIFTY')
    bank_nifty_current_future = get_current_future('BANKNIFTY')
    futures_to_add = [{'Symbol': nifty_current_future, 'TableName': 'NIFTYFUT'},
                      {'Symbol': bank_nifty_current_future, 'TableName': 'BANKNIFTYFUT'}]
    
    futures_to_add_df = pd.DataFrame(futures_to_add)
    #Add Futures to the lookup table
    lookup_table = pd.concat([lookup_table, futures_to_add_df], ignore_index=True)
    
    #Get Nifty 500 list
    nifty500_symbol_list = get_nifty500_symbol_list()
    nifty500_symbols_df = pd.DataFrame({'Symbol': sorted(nifty500_symbol_list)})
    #Create a 'TableName' column
    #TableName will be almost the same as Symbols,
    #but replaces special characters with _ to make it work with MySQL/ MariaDB
    nifty500_symbols_df['TableName'] = nifty500_symbols_df['Symbol'].apply(lambda x: replace_special_characters(x))

    #Add nifty 500 to the lookup table
    lookup_table = pd.concat([lookup_table, nifty500_symbols_df], ignore_index=True)
    
    #Arrange Alphabetically
    lookup_table = lookup_table.sort_values(by='Symbol')
    
    #Save to File
    if not path.exists(lookup_files_directory):
        makedirs(lookup_files_directory)
    
    new_lookup_table_file_path = path.join(lookup_files_directory, current_lookup_file_name)
    lookup_table.to_csv(new_lookup_table_file_path, index=False)
    
    message = 'lookup Table equity_universe_lookup.csv not found. Created it '
    logger.log_info(message)
    
    #mail(destination_email_address, 'Market Data Main - Access token Failed.',message)
    send_market_data_email('Market Data - equity_universe_lookup created', message)
    

def update_symbol_table_name_list():
    """
    Update the existing symbol and table name lookup file with new symbols
    """
    lookup_table_changed = False
    changes_made = ''
    lookup_table = pd.read_csv(path.join(lookup_files_directory, current_lookup_file_name))
    
    #Checking if nifty500 list has new symbols
    existing_symbols = lookup_table['Symbol'].tolist()
    n50_current_symbols = get_nifty500_symbol_list()
    missing_symbols = list(set(n500_current_symbols) - set(existing_symbols))
    
    if len(missing_symbols) > 0:
        symbols_to_add = pd.DataFrame({'Symbol': sorted(missing_symbols)})
        #Create new Table Names for new symbols, replacing special characters
        symbols_to_add['TableName'] = symbols_to_add['Symbol'].apply(lambda x: replace_special_characters(x))
        lookup_table = pd.concat([lookup_table, symbols_to_add], ignore_index=True)   
        message = f'Found {len(missing_symbols)} new symbol(s) in Nifty500 list.'
        log_equity_universe_changes(message)
        log_equity_universe_update(message)
        changes_made = changes_made + '\nSymbol  TableName'
        for row in range(len(symbols_to_add)):
            changes_made = f"{changes_made}\n{symbols_to_add['Symbol'].iloc[row]}  {symbols_to_add['TableName'].iloc[row]}  "        
        lookup_table_changed = True
        
    #Check if NIFTYFUT and BANKNIFTYFUT have to be updated.
    current_nifty_fut = get_current_future('NIFTY')
    current_bank_nifty_fut = get_current_future('BANKNIFTY')
    
    #Find the existing NIFTYFUT symbol in lookup_table and replace it if necessary
    nifty_fut_in_lookup = lookup_table[lookup_table['TableName'] == 'NIFTYFUT']['Symbol'].iloc[0]
    if nifty_fut_in_lookup != current_nifty_fut:
        #NiftyFut Symbol needs to be updated
        lookup_table.loc[lookup_table['TableName'] == 'NIFTYFUT', 'Symbol'] = current_nifty_fut
        changes_made = f"{changes_made}\nNiftyFut updated from {nifty_fut_in_lookup} to {current_nifty_fut}"        
        lookup_table_changed = True

    #Find the existing BANKNIFTYFUT symbol in lookup_table and replace it if necessary        
    bank_nifty_fut_in_lookup = lookup_table[lookup_table['TableName'] == 'BANKNIFTYFUT']['Symbol'].iloc[0]
    if bank_nifty_fut_in_lookup != current_bank_nifty_fut:
        #NiftyFut Symbol needs to be updated
        lookup_table.loc[lookup_table['TableName'] == 'BANKNIFTYFUT', 'Symbol'] = current_bank_nifty_fut
        changes_made = f"{changes_made}\nBankNiftyFut updated from {bank_nifty_fut_in_lookup} to {current_bank_nifty_fut}"        
        lookup_table_changed = True    

    
    if lookup_table_changed:        
        #Saving original files before replacing and updating.
        #This might be required to investigate potential issues with symbol and table updates to the lookup file
        #Lookup Table
        reference_lookup_table_name = current_lookup_file_name.replace('.csv', '_' + str(date.today()) + '.csv')
        reference_lookup_table_path = path.join(lookup_files_directory, reference_lookup_table_name)
        shutil.copy(current_lookup_table_path, reference_lookup_table_path)
        
        #NSE 500 List
        today_nse500_today_list_name = f"ind_nifty500_list_{str(date.today())}.csv"
        
        # Read the CSV file from the URL and save it directly
        #Pandas could have been skipped here, using requests. Using Pandas as it has lesser operations between read and write
        response = requests.get(n500_url, headers={'User-Agent': generate_random_user_agent()})
        csv_data = StringIO(response.text)
        nse500_df = pd.read_csv(csv_data)
        nse500_df.to_csv(path.join(lookup_files_directory, today_nse500_today_list_name), index=False)
        
        #Arranging lookup_table alphabetically before overwriting the local lookup file
        lookup_table = lookup_table.sort_values(by='Symbol')
        
        #Replacing Original Lookup File with updated list
        lookup_table.to_csv(current_lookup_table_path, index=False)
        changes_made = changes_made + f'\n\n{today_nse500_today_list_name} and {reference_lookup_table_name} backed up for reference\n'
        message = 'lookup Table equity_universe_lookup.csv Updated'
        log_equity_universe_changes(message)
        log_equity_universe_changes(changes_made)
        
        #mail(destination_email_address, 'Market Data Main - Access token Failed.',message)     
        send_market_data_email('Market Data - equity_universe_lookup Updated', changes_made)        
        
    else:
        message = 'Market Data - equity_universe_lookup - No changes made to the lookup_file'
        log_equity_universe_update(message)

def update_equity_universe():
    """
    Main function to update the equity universe
    """
    logger = get_equity_universe_logger()
    error_handler = ErrorHandler()
    from os import path
    
    try:
        if path.exists(path.join(lookup_files_directory, current_lookup_file_name)):
            update_symbol_table_name_list()
        else:
            create_symbol_table_name_list()
        return True
    except Exception as e:
        message = f'equity_universe_updater failed with exception {e}. Traceback - {str(traceback.format_exc())}'
        logger.log_error(message)
        error_handler.handle_error('equity_universe_updater - ' + message)
        send_market_data_email('Market Data - equity_universe_updater FAIL!!!', message)
        return False

if __name__ == '__main__':
    update_equity_universe()