# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System

Dumps the tables from the 'Daily' databases to a backup database.
1. Maintaines efficiency of replace statements in daily tables as they would have one day's data at most 
2. Facilitates regular clean up
3. Helps identify small tables in equity universe, resulting from symbol changes and delisting 
4. Backup tables can be migrated / downloaded even if ticker is running

Checks and reports if any of the tables in equity_universe_lookup.csv are empty at the end of the day.
    This indicates that the corresponding symbol has potentially changed or has been delisted
Creates main databases {equity_database_name}, {options_database_name} and {bank_nifty_options_database_name} . (No _daily suffix)
Copies all tables from the _daily databases to their corresponding main database and drops the tables in the _daily DBs
Reports about backup failures.
End of market_data_main
Returns True if success. Else False.
"""

from datetime import datetime as dt, date
import psycopg2
from market_data_mailer import send_market_data_email
from market_data_attachment_mailer import send_market_data_attachment_email
from os import path, makedirs, cpu_count
import pandas as pd
import json
import traceback
from system_error_logger import log_system_error
import numpy as np
from sqlalchemy import create_engine
import concurrent.futures

config_file = 'market_data_config.json'
with open(config_file, 'r') as config_file_handle:
    system_config = json.load(config_file_handle)

recipient_email_address = system_config['notification_recipients']
#Generate a list if multiple recipients mentioned
recipient_email_address = recipient_email_address.split(',')
sender_email_address = system_config['email_sender']
sender_email_password = system_config['email_password']
database_host = system_config['database_host']
database_user = system_config['database_user']
database_password = system_config['database_password']
database_port = system_config['database_port']

equity_database_name = system_config['equity_database_name']
options_database_name = system_config['options_database_name']
bank_nifty_options_database_name = system_config['banknifty_options_database_name']

backup_worker_count = min(system_config['backup_thread_count'], cpu_count())
#Using excess number of threads fails in some CPU archs, skipping the multithreaded job completely.
#Handle with caution, retest multiple times
#Example: AWS EC2 instances with burstable CPU and CPU credit specification set to unlimited.


daily_table_name = 'daily_table'

#Prepare Lookup tables and lists
lookup_dir = 'instrument_lookup_tables'
#Identify Only Nifty and BankNifty as they have a different table structure and hence different SQL statement
index_tokens = pd.read_csv(path.join(lookup_dir, 'index_token_list.csv'))['instrument_token'].values.tolist()

#Equity Universe Tokens and Token Table (Lookup => instrument_token:TableName)
#Includes Equity Universe, Nifty, BankNifty and their Futures
equity_universe_tokens = pd.read_csv(path.join(lookup_dir, 'equity_universe_token_list.csv'))['instrument_token'].values.tolist()
equity_universe_token_table_dict = np.load(path.join(lookup_dir, 'equity_universe_token_table_dict.npy'), allow_pickle=True).item()
equity_universe_token_symbol_dict = np.load(path.join(lookup_dir, 'equity_universe_token_symbol_dict.npy'), allow_pickle=True).item()


#Nifty Options Tokens and Token Table(Lookup => instrument_token:TableName)
nifty_option_tokens = pd.read_csv(path.join(lookup_dir, 'nifty_options_token_list.csv'))['instrument_token'].values.tolist()
nifty_options_token_table_dict = np.load(path.join(lookup_dir, 'nifty_options_token_table_dict.npy'), allow_pickle=True).item()

#BankNifty Options Tokens and Token Table(Lookup => instrument_token:TableName)
bank_nifty_option_tokens = pd.read_csv(path.join(lookup_dir, 'bank_nifty_options_token_list.csv'))['instrument_token'].values.tolist()
bank_nifty_options_token_table_dict = np.load(path.join(lookup_dir, 'bank_nifty_options_token_table_dict.npy'), allow_pickle=True).item()

#Combine all token lists
full_token_list = index_tokens + equity_universe_tokens + nifty_option_tokens + bank_nifty_option_tokens

#Combine all three token:table dictionaries
main_token_table_dict = {}
main_token_table_dict.update(equity_universe_token_table_dict)
main_token_table_dict.update(nifty_options_token_table_dict)
main_token_table_dict.update(bank_nifty_options_token_table_dict)


#Combine all three token:symbol dictionaries
main_token_symbol_dict = {}
main_token_symbol_dict.update(equity_universe_token_symbol_dict)
main_token_symbol_dict.update(nifty_options_token_table_dict) #Symbol and table_name are same for nifty options
main_token_symbol_dict.update(bank_nifty_options_token_table_dict) #Symbol and table_name are same for BankNifty options

#Creating a lookup dictionary for instrument_token:DBName
token_to_db_name_dict = {token: equity_database_name for token in equity_universe_tokens}
token_to_db_name_dict.update({token: options_database_name for token in nifty_option_tokens})
token_to_db_name_dict.update({token: bank_nifty_options_database_name for token in bank_nifty_option_tokens})

def log_daily_backup(message):
    """
    Log daily backup messages to file and console
    Optimized for performance by reducing I/O operations
    """
    timestamp = dt.now()
    print(timestamp, message)
    log_directory = path.join('logs', str(date.today()) + '_market_data_logs')
    if not path.exists(log_directory):
        makedirs(log_directory)
    log_file = path.join(log_directory, f'daily_market_data_backup_logs_{str(date.today())}.log')
    log_message = f'\n{timestamp}    {str(message)}'
    with open(log_file, 'a') as f:
        f.write(log_message)

def log_daily_backup_no_print(message):
    """
    Log daily backup messages to file only
    Optimized for performance by reducing I/O operations
    """
    timestamp = dt.now()
    log_directory = path.join('logs', str(date.today()) + '_market_data_logs')
    if not path.exists(log_directory):
        makedirs(log_directory)
    log_file = path.join(log_directory, f'daily_market_data_backup_logs_{str(date.today())}.log')
    log_message = f'\n{timestamp}    {str(message)}'
    with open(log_file, 'a') as f:
        f.write(log_message)

def find_symbols_for_table(table_name, symbol_table_df):
    """
    Find symbols that correspond to a given table name
    """
    # Filter the DataFrame where 'TableName' matches tbName and select the 'Symbol' column
    matching_symbols = symbol_table_df[symbol_table_df['TableName'] == table_name]['Symbol']
    # Convert matching symbols to list and join them with commas
    matching_symbols = matching_symbols.tolist()
    return ','.join(matching_symbols)

def find_equity_universe_blank_tables():
    """
    Identify table names for which no data was received.
    Indicates that the corresponding symbol is potentially invalid - Symbol changed or delisted
    """
    lookup_directory = 'instrument_lookup_tables'
    equity_universe_lookup_file = 'equity_universe_lookup.csv'
    equity_universe_file_path = path.join(lookup_directory, equity_universe_lookup_file)
    equity_universe_symbols_tables = pd.read_csv(equity_universe_file_path)
    equity_universe_tables = sorted(equity_universe_symbols_tables['TableName'].values.tolist())
      
    connection = psycopg2.connect(host=database_host, user=database_user, password=database_password, port=database_port)
    cursor = connection.cursor()
    
    cursor.execute(f"SELECT DISTINCT(tablename) FROM {equity_database_name}.{daily_table_name}")
    table_names_in_ticks = list([item[0] for item in cursor.fetchall()])
    no_data_tables = sorted(list(set([item for item in equity_universe_tables if item not in table_names_in_ticks])))
    
    if len(no_data_tables) == 0:
        blank_equity_universe_tables_df = pd.DataFrame()
    if len(no_data_tables) > 0:
        #Found Blank Tables
        blank_equity_universe_tables_df = pd.DataFrame(no_data_tables, columns=['TableName'])
        #Find matching Symbol(s)
        #If multiple symbols are mapped to one table, find all the symbols.
        #A known example would be -BE (Book Entry) instruments where symbols with and without BE mapped to the same table name
        #A simple lookup/ dictionary call won't do
        # call find_symbols_for_table for each row in blank_equity_universe_tables_df
        blank_equity_universe_tables_df['TradingSymbols'] = blank_equity_universe_tables_df['TableName'].apply(lambda x: find_symbols_for_table(x, equity_universe_symbols_tables))
    cursor.close()
    connection.close()
    return blank_equity_universe_tables_df

def backup_one_instrument(instrument_token):
    """
    Backup data for a single instrument from daily table to its individual table
    """
    connection3 = psycopg2.connect(host=database_host, user=database_user, password=database_password, port=database_port)
    cursor3 = connection3.cursor()
    
    #Get Tablename and DBName
    table_name = main_token_table_dict.get(instrument_token)
    db_name = token_to_db_name_dict.get(instrument_token)
    trading_symbol = main_token_symbol_dict.get(instrument_token)
    
    #Insert entry into failed_backup_tables
    #Doing this now and removing later on success.
    #This way, if the backup fails mid-way for some reason and DB cursor becomes unusable, we'll still know that the backup failed
    message = f'Started backing up Instrument Token {instrument_token} into {db_name}.`{table_name}`.'
    log_daily_backup(message)
    
    if instrument_token in full_token_list:
        try:
            if instrument_token in index_tokens:
                #Create Table in the appropriate DB
                #Copy all rows for that instrument token from daily table to main table
                cursor3.execute(f"CREATE TABLE IF NOT EXISTS {db_name}.\"{table_name}\" (timestamp TIMESTAMP UNIQUE, price decimal(12,2))")
                cursor3.execute(f'''
                          INSERT INTO {db_name}.\"{table_name}\"
                          SELECT timestamp, price FROM {equity_database_name}.{daily_table_name}
                          WHERE instrument_token={instrument_token}
                          ON CONFLICT (timestamp) DO UPDATE SET price = EXCLUDED.price
                          ''')
            else:
                #Create Table in the appropriate DB
                #Copy all rows for that instrument token from daily table to main table
                cursor3.execute(f'''
                          CREATE TABLE IF NOT EXISTS {db_name}.\"{table_name}\"
                        	(timestamp TIMESTAMP UNIQUE, price DECIMAL(19,2), qty INTEGER,
                          avgPrice DECIMAL(19,2), volume BIGINT,
                          bQty INTEGER, sQty INTEGER,
                          open DECIMAL(19,2), high DECIMAL(19,2), low DECIMAL(19,2), close DECIMAL(19,2),
                        	changeper DECIMAL(60,10), lastTradeTime TIMESTAMP, oi INTEGER, oiHigh INTEGER, oiLow INTEGER,
                        	bq0 INTEGER, bp0 DECIMAL(19,2), bo0 INTEGER,
                        	bq1 INTEGER, bp1 DECIMAL(19,2), bo1 INTEGER,
                        	bq2 INTEGER, bp2 DECIMAL(19,2), bo2 INTEGER,
                        	bq3 INTEGER, bp3 DECIMAL(19,2), bo3 INTEGER,
                        	bq4 INTEGER, bp4 DECIMAL(19,2), bo4 INTEGER,
                        	sq0 INTEGER, sp0 DECIMAL(19,2), so0 INTEGER,
                        	sq1 INTEGER, sp1 DECIMAL(19,2), so1 INTEGER,
                        	sq2 INTEGER, sp2 DECIMAL(19,2), so2 INTEGER,
                        	sq3 INTEGER, sp3 DECIMAL(19,2), so3 INTEGER,
                        	sq4 INTEGER, sp4 DECIMAL(19,2), so4 INTEGER
                            )
                         '''
                          )
                #Copy data for instrument_token from daily_table to individual table in the main DB
                cursor3.execute(f'''
                          INSERT INTO {db_name}.\"{table_name}\"
                          SELECT
                          timestamp, price, qty, avgPrice, volume,
                          bQty, sQty, open, high, low, close,
                          changeper, lastTradeTime, oi, oiHigh, oiLow,
                          bq0, bp0, bo0, bq1, bp1, bo1,
                          bq2, bp2, bo2, bq3, bp3, bo3,
                          bq4, bp4, bo4,
                          sq0, sp0, so0, sq1, sp1, so1,
                          sq2, sp2, so2, sq3, sp3, so3,
                          sq4, sp4, so4
                          FROM {equity_database_name}.{daily_table_name}
                          WHERE instrument_token={instrument_token}
                          ON CONFLICT (timestamp) DO UPDATE SET
                          price = EXCLUDED.price, qty = EXCLUDED.qty, avgPrice = EXCLUDED.avgPrice,
                          volume = EXCLUDED.volume, bQty = EXCLUDED.bQty, sQty = EXCLUDED.sQty,
                          open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
                          changeper = EXCLUDED.changeper, lastTradeTime = EXCLUDED.lastTradeTime,
                          oi = EXCLUDED.oi, oiHigh = EXCLUDED.oiHigh, oiLow = EXCLUDED.oiLow,
                          bq0 = EXCLUDED.bq0, bp0 = EXCLUDED.bp0, bo0 = EXCLUDED.bo0,
                          bq1 = EXCLUDED.bq1, bp1 = EXCLUDED.bp1, bo1 = EXCLUDED.bo1,
                          bq2 = EXCLUDED.bq2, bp2 = EXCLUDED.bp2, bo2 = EXCLUDED.bo2,
                          bq3 = EXCLUDED.bq3, bp3 = EXCLUDED.bp3, bo3 = EXCLUDED.bo3,
                          bq4 = EXCLUDED.bq4, bp4 = EXCLUDED.bp4, bo4 = EXCLUDED.bo4,
                          sq0 = EXCLUDED.sq0, sp0 = EXCLUDED.sp0, so0 = EXCLUDED.so0,
                          sq1 = EXCLUDED.sq1, sp1 = EXCLUDED.sp1, so1 = EXCLUDED.so1,
                          sq2 = EXCLUDED.sq2, sp2 = EXCLUDED.sp2, so2 = EXCLUDED.so2,
                          sq3 = EXCLUDED.sq3, sp3 = EXCLUDED.sp3, so3 = EXCLUDED.so3,
                          sq4 = EXCLUDED.sq4, sp4 = EXCLUDED.sp4, so4 = EXCLUDED.so4
                          ''')
            #message = f'Instrument Token {instrument_token} backed up into {db_name}.`{table_name}.'
            #log_daily_backup_no_print(message)
            
            #Adding entry in backup success table for tracking failures
            cursor3.execute(f"INSERT INTO {equity_database_name}.backup_success_tables (instrument_token, tradingsymbol, tablename) VALUES (%s,%s,%s)", [instrument_token, trading_symbol, table_name])
            connection3.commit()
            message = f'Finished backing up Instrument Token {instrument_token} into {db_name}.`{table_name}`.'
            log_daily_backup(message)
        except Exception as e:
            message = f'Daily Market Data Backup - Exception while copying instrument token {instrument_token} data to {db_name}.`{table_name} : {e} . Traceback : {traceback.format_exc()}'
            log_daily_backup(message)
            log_system_error('Daily Market Data Backup - ' + message)
        finally:
            cursor3.close()
            connection3.close()
    else:
        message = f'Found data for unsubscribed instrument_token {instrument_token}'
        log_daily_backup(message)
        log_system_error('Daily Market Data Backup - ' + message)

def run_daily_market_data_backup():
    """
    Main function to run the daily market data backup process
    """
    try:
        #Find instrument_tokens in subscription list with no data received.
        blank_tables_found = False
        blank_equity_universe_tables = find_equity_universe_blank_tables()
        
        if len(blank_equity_universe_tables) > 0:
            blank_tables_found = True
            #Store the list locally for future reference
            blank_tables_dir = 'blank_equity_universe_tables'
            if not path.exists(blank_tables_dir):
                makedirs(blank_tables_dir)
            today_blank_tables_name = f'blank_equity_universe_instruments_{str(date.today())}.csv'
            blank_tables_loc_path = path.join(blank_tables_dir, today_blank_tables_name)
            blank_equity_universe_tables.to_csv(blank_tables_loc_path, index=False)
            blank_table_message = f'No ticks received for {len(blank_equity_universe_tables)} symbols provided in equity_universe_lookup.csv.\nStored the list as {today_blank_tables_name}'
            log_daily_backup(blank_table_message)
            
        #Create Databases and Tables
        #Find unique database_names.
        #This is to allow backups irrespective of whether 
        #the same name or different names have been used for the three databases
        db_names = [equity_database_name, options_database_name, bank_nifty_options_database_name]
        #Sorted unique list
        db_names = sorted(list(set(db_names)))
        
        connection = psycopg2.connect(host=database_host, user=database_user, password=database_password, port=database_port)
        cursor = connection.cursor()
        
        #Create schemas
        for db_name in db_names:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
        
        #Find instrument_tokens from the daily table.
        cursor.execute(f"SELECT DISTINCT(instrument_token) FROM {equity_database_name}.{daily_table_name}")
        instrument_tokens_to_store = list([item[0] for item in cursor.fetchall()])
        
        #Find row_count 
        cursor.execute(f"SELECT COUNT(*) FROM {equity_database_name}.{daily_table_name}")
        daily_table_row_count = int(cursor.fetchone()[0])            
    
        message = f'''
            Daily Market Data Backup is about to distribute {daily_table_row_count} rows for {len(instrument_tokens_to_store)} instruments from the daily table into main DBs and tables.
            This is going to take some time
            '''
        log_daily_backup(message)
        
        message = f'Creating {equity_database_name}.backup_success_tables'
        log_daily_backup(message)
        
        #Create TABLE to record list of instrument_tokens that succeeded backup.
        #I tried collecting just failed tables. 
        #But if backup_one_instrument failed or was skipped for some reason, resulting in a false negative of no failed tables
        #Using DB, as backup is multi-threaded and variable sharing between threads is cumbersome
        cursor.execute(f'''
                  CREATE TABLE IF NOT EXISTS 
                  {equity_database_name}.backup_success_tables 
                  (insertid INT AUTO_INCREMENT PRIMARY KEY,
                   instrument_token BIGINT(20),
                   tradingsymbol VARCHAR(100),
                   tablename VARCHAR(100)
                   )                  
                  ''')       
        
        #Calling backup_one_instrument for instrument_tokens_to_store
        message = f'Calling backup_one_instrument for {len(instrument_tokens_to_store)} instrument tokens with {backup_worker_count} concurrent workers'
        log_daily_backup(message)
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=backup_worker_count) as executor:
            executor.map(backup_one_instrument, instrument_tokens_to_store)
        
        message = 'backup_one_instrument completed. Looking for failed backups'
        log_daily_backup(message)
        
        #Subscribed tokens for which at least one tick was received.
        #Unique Instrument tokens found in daily_table - instrument_tokens_to_store
        #Subscribed token list - full_token_list
        subscribed_tokens_in_daily_table = set(instrument_tokens_to_store).intersection(full_token_list)
        
        #Find all tokens for which backup succeeded
        cursor.execute(f"SELECT DISTINCT(instrument_token) FROM {equity_database_name}.backup_success_tables")
        backup_success_tokens = list([item[0] for item in cursor.fetchall()])
                
        backup_failed_token_list = list(set(subscribed_tokens_in_daily_table).difference(backup_success_tokens))
        
        #Drop backup_success_tables
        cursor.execute(f"DROP TABLE {equity_database_name}.backup_success_tables")

        if len(backup_failed_token_list) > 0:
            #Closing the connection and cursor object.
            cursor.close()
            connection.close()
            
            # Initialize an empty list to store the data
            data = []
            
            # Populate the list with data from the dictionaries
            for token in backup_failed_token_list:
                row = {
                    'instrument_token': token,
                    'tradingsymbol': main_token_symbol_dict.get(token, 'N/A'),  # Default to 'N/A' if token not found
                    'tablename': main_token_table_dict.get(token, 'N/A')        # Default to 'N/A' if token not found
                }
                data.append(row)
            
            # Create a DataFrame
            backup_failed_tokens = pd.DataFrame(data, columns=['instrument_token', 'tradingsymbol', 'tablename'])

            #Store failed tables for future reference 
            backup_fails_dir = 'backup_failed_tables'
            if not path.exists(backup_fails_dir):
                makedirs(backup_fails_dir)
            today_backup_fails_name = f'backups_failed_{str(date.today())}.csv'
            failed_tables_loc_path = path.join(backup_fails_dir, today_backup_fails_name)
            backup_failed_tokens.to_csv(failed_tables_loc_path, index=False)
            
            fail_tables_message = f'Daily Market Data Backup failed for {len(backup_failed_token_list)} instrument_token(s).\ndaily_table left untouched.\nStored the list as {today_backup_fails_name}'
            fail_table_string = '\n'.join(f"{item}, {main_token_symbol_dict.get(item)}" for item in backup_failed_token_list)
            log_daily_backup(fail_tables_message)
            log_daily_backup(fail_table_string)
            log_system_error('Daily Market Data Backup - ' + fail_tables_message)
            log_system_error(fail_table_string)
            
            if blank_tables_found:
                blank_table_message = f'No ticks received for {len(blank_equity_universe_tables)} symbols provided in equity_universe_lookup.csv . List attached\n'
                #send_mail_attachment(subject, body, attachment_file_path)
                send_market_data_attachment_email('Daily Market Data Backup failed and instrument_tokens with no data found. Check Logs for more details',
                               blank_table_message + fail_tables_message + fail_table_string,
                               blank_tables_loc_path) 
                log_daily_backup('Blank Table List mailed')
                log_daily_backup('Daily Market Data Backup completed')
                
            else:
                send_market_data_email(fail_tables_message, fail_tables_message + fail_table_string)
            return False
        #Backup succeeded for all tables;
        else:
            #Drop daily_table
            cursor.execute(f"DROP TABLE {equity_database_name}.{daily_table_name}")
            log_daily_backup('Backup successful for all instrument tokens. Daily Table Dropped.')
            
            #Committing and Closing the connection and cursor object.
            connection.commit()
            cursor.close()
            connection.close()
            
            if blank_tables_found:
                blank_table_message = f'No ticks received for {len(blank_equity_universe_tables)} symbols provided in equity_universe_lookup.csv . List attached\n'
                #send_mail_attachment(subject, body, attachment_file_path)
                send_market_data_attachment_email('Market Data - Done for the day. All activities completed successfully. instrument_tokens with no data found. List attached',
                               'Market Data - Daily Market Data Backup completed. ' + blank_table_message,
                               blank_tables_loc_path) 
                log_daily_backup('Blank Table List mailed')
                log_daily_backup('Daily Market Data Backup completed')
            else:
                message = 'Market Data - Done for the day. All activities completed successfully. No Blank Tables found'
                log_daily_backup(message)
                #Mailing all good as this is the last operation in market_data_main
                send_market_data_email(message, 'Market Data - Daily Market Data Backup completed. ' + message)
                log_daily_backup('Daily Market Data Backup completed')
            return True        
        #Mail notifying success and failure here as this is the last activity
        #Success will include blank tables if found
        #Failure will be returned to market_data_main but it will only be logged there. No notify.
        #This avoids duplicate notifications
        #Catch all False indicating potential failures
        return False
        
    except Exception as e:
        message = f'Exception in Daily Market Data Backup : {e} . Traceback : {traceback.format_exc()}'
        log_daily_backup(message)
        send_market_data_email(message, 'Market Data - Daily Market Data Backup failed with exception. ' + message)
        log_system_error('Daily Market Data Backup - ' + message)
        return False          
        
if __name__ == '__main__':
    run_daily_market_data_backup()