"""
Author: Paras Parkash
Zerodha Data Collector

Based on holiday checking logic from various sources

Install Chrome in Linux 
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb

Version
google-chrome --version
Tested in version => Google Chrome 123.0.6312.58

Get Holiday List => get_holiday_list_from_upstox (Free and Open). 
If fail, get_holiday_list_from_nse. 
If Fail, use the local file.
If not local file, play it safe and say today is NOT a holiday.
Every time get_holiday_list_from_upstox or get_holiday_list_from_nse is successful, the holiday list is stored locally as trading_holidays.csv
Can be Run standalone - Checks if today is a holiday
Return True if the given date is found in the holiday list. Else False.

"""

import sys
import os
# Add src directory to Python path to allow imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import datetime as dt, date, timedelta
from selenium import webdriver #pip install selenium==4.6
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import traceback
import requests
from utils.logger import get_holiday_check_logger
from utils.error_handler import ErrorHandler, retry_on_failure

RETRY_COUNT = 5
    
def get_holiday_list_from_upstox():
    """
    Fetch holiday list from Upstox API
    """
    logger = get_holiday_check_logger()
    error_handler = ErrorHandler()
    
    upstox_endpoint = 'https://api.upstox.com/v2/market/holidays'
    upstox_holiday_list = []
    
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            upstox_response = requests.get(upstox_endpoint)
            if upstox_response.status_code != 200:
                raise Exception(f"get_holiday_list_from_upstox from {upstox_endpoint} Failed. HTTP Response => {upstox_response.status_code}")
            
            holiday_api_response = upstox_response.json()
            if holiday_api_response.get('status') != 'success':
                raise Exception(f"API response status not success: {holiday_api_response.get('status')}")
            
            holidays_list_from_upstox = holiday_api_response.get('data', [])
            
            # Looking for NSE holidays only
            for holiday_item in holidays_list_from_upstox:
                closed_exchanges = holiday_item.get('closed_exchanges', [])
                if 'NSE' in closed_exchanges:
                    upstox_holiday_list.append(holiday_item['date'])
            
            trading_holidays = pd.DataFrame(upstox_holiday_list, columns=['Date'])
            trading_holidays.to_csv('trading_holidays.csv', index=False)
            
            message = 'Successfully fetched trading holiday list from Upstox and saved it Locally'
            logger.log_info(message)
            return upstox_holiday_list

        except Exception as e:
            message = f'get_holiday_list_from_upstox Failed. Attempt No : {attempt}. Exception -> {e} Traceback : {traceback.format_exc()}.\nWill retry after 30 seconds'
            logger.log_error(message)
            error_handler.handle_error('holiday_check - ' + message)
            sleep(30)
        else:
            break
    else:
        message = f'get_holiday_list_from_upstox - Failed After {RETRY_COUNT} attempts. Will try get_holiday_list_from_nse'
        logger.log_error(message)
        error_handler.handle_error('holiday_check - ' + message)
        try:
            from market_data_mailer import send_market_data_email
            send_market_data_email('Market Data - get_holiday_list_from_upstox Failed.', message)
        except ImportError:
            logger.log_error("Could not send email notification - mailer not available")
        return []

@retry_on_failure(max_attempts=5, delay=30)
def get_holiday_list_from_nse():
    """
    Fetch holiday list from NSE website using Selenium
    """
    logger = get_holiday_check_logger()
    error_handler = ErrorHandler()
    
    nse_holiday_list_url = 'https://www.nseindia.com/resources/exchange-communication-holidays'
    options = Options()
    options.headless = True
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')
    options.add_argument('window-size=1920x1080')
    
    # Use Service object to specify path to the Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(nse_holiday_list_url)
    
    # Wait for the initial page elements to load
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "holidayTable")))
    
    # Now, wait for the JavaScript condition: Check if the table is present and has at least one row
    wait_time = 0
    max_wait = 30 # Maximum time to wait in seconds
    while wait_time < max_wait:
        # Check if the holiday table exists and has at least one row
        is_table_populated = driver.execute_script("""
            var table = document.getElementById('holidayTable');
            if (table && table.getElementsByTagName('tr').length > 1) {
                return true;
            }
            return false;
        """)
        if is_table_populated:
            logger.log_info('Table is loaded and populated.')
            break
        sleep(1)  # Wait for a second before checking again
        wait_time += 1

    page_content = driver.page_source
    holidays_df = pd.read_html(page_content, attrs={'id': 'holidayTable'})[0]
    driver.close()
    
    #Convert the dates into yyyy-mm-dd format
    holidays_df['Date'] = holidays_df['Date'].apply(lambda x: dt.strptime(x.strip(), '%d-%b-%Y').strftime('%Y-%m-%d'))
    
    #saving successfull list as as a fallback - trading_holidays.csv
    #This will be used in case get_holiday_list_from_nse fails
    trading_holidays = holidays_df['Date']
    trading_holidays.to_csv('trading_holidays.csv', index=False)
    
    message = 'Successfully fetched trading holiday list from the NSE site and saved it Locally'
    logger.log_info(message)
    return trading_holidays.to_list()

def get_holiday_list():
    """
    Get holiday list from primary source, with fallbacks
    """
    logger = get_holiday_check_logger()
    error_handler = ErrorHandler()
    
    trade_holiday_list = get_holiday_list_from_upstox()
    if len(trade_holiday_list) > 0:
        return trade_holiday_list
    else:
        trade_holiday_list = get_holiday_list_from_nse()
    if len(trade_holiday_list) > 0:
        return trade_holiday_list
    else:
        message = 'get_holiday_list_from_upstox and get_holiday_list_from_nse failed. Using Local file'
        logger.log_error(message)
        error_handler.handle_error('holiday_check - ' + message)
        try:
            from os import path
            if path.exists('trading_holidays.csv'):
                trading_holidays = pd.read_csv('trading_holidays.csv')['Date'].to_list()
                return trading_holidays
            else:
                message = 'get_holiday_list_from_nse failed. trading_holidays.csv not found. Returning empty list'
                logger.log_error(message)
                try:
                    from market_data_mailer import send_market_data_email
                    send_market_data_email('Market Data - get_holiday_list_from_nse Failed', message)
                except ImportError:
                    logger.log_error("Could not send email notification - mailer not available")
                error_handler.handle_error('holiday_check - ' + message)
                
                #Returning an empty list . This will always say today is not a trading holiday
                return []
        except Exception as e2:
            message = f' Fall Back failed with exception {e2} Traceback : {traceback.format_exc()}. Returning empty list'
            logger.log_error(message)
            error_handler.handle_error('holiday_check - ' + message)
            try:
                from market_data_mailer import send_market_data_email
                send_market_data_email('Market Data - get_holiday_list Failed', message)
            except ImportError:
                logger.log_error("Could not send email notification - mailer not available")
            
            #Returning an empty list . This will always say today is not a trading holiday
            return []

#Will return True only if the input date is a trading holiday
def check_trading_holiday(input_date):
    """
    Check if the given date is a trading holiday
    """
    logger = get_holiday_check_logger()
    error_handler = ErrorHandler()
    
    #Double conversion to avoid type mismatch
    input_date_string = str(input_date)
    
    # Convert string to date object
    date_obj = dt.strptime(input_date_string, '%Y-%m-%d').date()
    
    # Check if the date is a Saturday or Sunday
    if date_obj.weekday() in (5, 6):
        message = f'{input_date} is a trading holiday (Weekend). Exit'
        logger.log_info(message)
        return True
    
    input_date = input_date_string
    from os import path
    trade_hols_file_path = 'trading_holidays.csv'
    one_hour_ago = dt.now() - timedelta(hours=1)
    
    #If the tradingholiday file exists and is fresh (not older than one hour, use it)
    if path.exists(trade_hols_file_path):
        holiday_list_modified_time = dt.fromtimestamp(path.getmtime(trade_hols_file_path))
        if holiday_list_modified_time >= one_hour_ago:
            message = 'local trading_holidays.csv is fresh. Using it.'
            logger.log_info(message)
            trading_holiday_list = pd.read_csv(trade_hols_file_path)['Date'].to_list()
        else:
            message = 'local trading_holidays.csv is older than 1 hour. Downloading latest list.'
            logger.log_info(message)
            trading_holiday_list = get_holiday_list()
    else:
        message = 'Fresh trading_holidays.csv file not found locally. Downloading latest list.'
        logger.log_info(message)
        trading_holiday_list = get_holiday_list()

    if input_date in trading_holiday_list:
        message = f'{input_date} is a trading holiday. Exit'
        logger.log_info(message)
        return True
    else:
        message = f'{input_date} is NOT a trading holiday. Holiday check passed. Will continue.'
        logger.log_info(message)
        return False
		
if __name__ == '__main__':
    check_trading_holiday(date.today())