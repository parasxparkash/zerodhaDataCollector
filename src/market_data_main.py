# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Zerodha Data Collector
"""

import sys
import multiprocessing
from datetime import datetime as dt, date

import sys
import os
# Add src directory to Python path to allow imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from check_trading_holiday import check_trading_holiday
from broker_access_token_request import request_broker_access_token
from equity_universe_updater import update_equity_universe
from instrument_lookup_tables_creator import create_instrument_lookup_tables
from market_data_ticker import run_market_data_ticker

import traceback
from daily_market_data_backup import run_daily_market_data_backup
from market_data_mailer import send_market_data_email
import json
from validate_system_config import is_system_config_default
from system_error_logger import log_system_error
from utils.logger import get_market_data_main_logger
from utils.error_handler import ErrorHandler

def log_market_data_main(message):
    """
    Log market data main messages using centralized logger
    """
    logger = get_market_data_main_logger()
    logger.log_info(message)

if __name__ == '__main__':
    try:
        #Some of the below checks can be combined into fewer lines with one statement or nested ifs.
        #Doing it this way for better readability
        #Will return True if Holiday
        
        is_trading_holiday = check_trading_holiday(str(date.today())) 
        #is_trading_holiday = False #Uncomment this line if you want to override Holiday check
        
        if is_trading_holiday: 
            message = f'Market Data Main - Today {date.today()} is a trading Holiday. Market Data Main exiting'
            log_market_data_main(message)
            send_market_data_email("Market Data Main - Trading Holiday. Exiting !!!", message)
            log_system_error(message)
            sys.exit()
        else: 
            message = 'Market Data Main - Trading Holiday Check Passed. Proceeding to system_config default check'
            log_market_data_main(message)
            system_config_is_default = is_system_config_default()
            
        if system_config_is_default: 
            message = 'Market Data Main - market_data_config.json has invalid default values. Market Data Main exiting'
            log_market_data_main(message)
            log_system_error(message)
            sys.exit()
        else: 
            message = 'system_config_is_default Check Passed. Proceeding to broker_access_token_request'
            log_market_data_main(message)
            access_token_success = request_broker_access_token()            
        #Are credentials valid?
        if not access_token_success: 
            message = 'Market Data Main - broker_access_token_request Failed. Market Data Main exiting.\nbroker_access_token_request.py failed'
            log_market_data_main(message)
            send_market_data_email("Market Data Main - broker_access_token_request Failed. Exiting !!!", message)
            log_system_error(message)
            sys.exit()
        else:
            message = 'Market Data Main - access token fetched Successfully. Proceeding to equity_universe_updater'
            log_market_data_main(message)
            equity_universe_update_success = update_equity_universe()
        
        #Was equity_universe_updater() successful?
        if not equity_universe_update_success:
            message = 'Market Data Main - equity_universe_updater FAILED!!!. Market Data Main exiting'
            log_market_data_main(message)
            log_system_error(message)
            send_market_data_email("Market Data Main - equity_universe_updater Failed. Exiting !!!", message)
            sys.exit()
        else:
            message = 'Market Data Main - equity_universe_updater succeeded. Proceeding to instrument_lookup_tables_creator'
            log_market_data_main(message)
            lookup_table_creation_success = create_instrument_lookup_tables()
        #Was instrument_lookup_tables_creator() successful?
        if not lookup_table_creation_success:
            message = 'Market Data Main - instrument_lookup_tables_creator FAILED!!!. Market Data Main exiting'
            log_market_data_main(message)
            log_system_error(message)
            send_market_data_email("Market Data Main - instrument_lookup_tables_creator Failed. Exiting !!!", message)
            sys.exit()    
        #An else would do here. But doing elif just in case and why not?
        elif (not is_trading_holiday) and access_token_success and equity_universe_update_success and lookup_table_creation_success:
            message = f'All Pre-checks passed. Calling Market Data ticker with close time {dt.now().replace(hour=market_close_hour, minute=market_close_minute, second=0, microsecond=0)}'
            log_market_data_main(message)
            send_market_data_email(f'Market Data Main - Ticker Started at {str(dt.now())[:19]}', message)
            run_market_data_ticker(market_close_hour, market_close_minute)
            message = 'Market Data Ticker done for the day Successfully. Proceeding to Backup'
            log_market_data_main(message)
            #You are here
            backup_status = run_daily_market_data_backup()
            if backup_status:
                message = "Market Data - All Activities for the day have been completed."
                log_market_data_main(message)
            else:
                message = "Market Data Main - Daily Market Data Backup reported failure. Check logs thoroughly."
                log_market_data_main(message)
                log_system_error(message)
    except Exception as e:
        message = f'Market Data Main Failed. Exception : {e}. Traceback :  {str(traceback.format_exc())}'  
        log_market_data_main(message)
        log_system_error(message)
        send_market_data_email('Market Data Main Failed.', message)