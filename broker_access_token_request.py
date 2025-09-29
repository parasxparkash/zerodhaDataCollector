"""
Author: Paras Parkash
Source: Market Data Acquisition System

Install Chrome in Linux
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb

Version
google-chrome --version
Tested in version => Google Chrome 123.0.6312.58

Gets the access token for the broker API app and stores it in {token_database_name}.broker_tokens
Can be Run standalone - Updates latest access token in DB
Returns True if success. Else False.

is_access_token_in_db_fresh
If the automated process fails for whatever reason, the code checks if the Access Token in the DB is Fresh
(Created after 08:00 the same day)
https://kite.trade/forum/discussion/7759/access-token-validity
And returns True if the token is Fresh
This way, We are good if
    - The access_token_request code fails after storing the access token in the DB
    - access_token_request failed, but the code was generated manually using manual_access_token_request or some other way
- Why not check if the token in the DB is fresh, even before trying to get one automatically?
- Allows the code to try at least once.

"""

import time
from datetime import datetime as dt, date
from selenium import webdriver #pip install selenium==4.6
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from kiteconnect import KiteConnect # pip install kiteconnect==5.0.1
from market_data_mailer import send_market_data_email
from webdriver_manager.chrome import ChromeDriverManager #pip install webdriver_manager==4.0.1
import pyotp #pip install pyotp
import traceback
from validate_system_config import is_system_config_default
from utils.logger import get_access_token_logger
from utils.error_handler import ErrorHandler, retry_on_failure
from utils.config_manager import config_manager
from utils.db_manager import BrokerTokenManager, create_db_manager

RETRY_COUNT = 5

def is_access_token_in_db_fresh():
    """
    Check if the latest access token in the database is from today after 8 AM
    """
    logger = get_access_token_logger()
    error_handler = ErrorHandler()
    
    try:
        # Load configuration
        system_config = config_manager.load_config()
        token_database_name = system_config['token_database_name']
        
        # Create database manager and broker token manager
        db_manager = create_db_manager(
            system_config['database_host'],
            system_config['database_user'],
            system_config['database_password'],
            system_config['database_port'],
            token_database_name
        )
        
        broker_token_manager = BrokerTokenManager(db_manager, token_database_name)
        result = broker_token_manager.is_latest_token_fresh(after_hour=8)
        
        return result
    except Exception as e:
        message = f'Exception in is_access_token_in_db_fresh  -> {e}. Traceback : {traceback.format_exc()}'
        error_handler.handle_error(message)
        logger.log_error(message)
        return {'status': False, 'timestamp': None}

@retry_on_failure(max_attempts=5, delay=30)
def request_broker_access_token():
    """
    Request access token from broker API using Selenium automation
    """
    logger = get_access_token_logger()
    error_handler = ErrorHandler()
    
    if is_system_config_default():
        message = 'System Config has defaults. broker_access_token_request is exiting'
        logger.log_error(message)
        error_handler.handle_error(message)
        return False
    else:
        # Load configuration
        system_config = config_manager.load_config()
        
        broker_username = system_config['broker_username']
        broker_password = system_config['broker_password']
        api_key = system_config['api_key']
        api_secret = system_config['api_secret']
        totp_secret = system_config['totp_secret']
        token_database_name = system_config['token_database_name']
        
        # Create database manager and broker token manager
        db_manager = create_db_manager(
            system_config['database_host'],
            system_config['database_user'],
            system_config['database_password'],
            system_config['database_port'],
            token_database_name
        )
        broker_token_manager = BrokerTokenManager(db_manager, token_database_name)
        
        exception_message = ''
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                kite = KiteConnect(api_key=api_key)
                url = (kite.login_url())
                login_name = broker_username
                password = broker_password
                
                options = Options()
                options.headless = True
                
                # Use Service object to specify path to the Chrome driver
                service = Service(ChromeDriverManager().install())
                
                # Initiate the webdriver instance with the service and options
                driver = webdriver.Chrome(service=service, options=options)
                
                logger.log_info("Market Data - Access Token - Execution started")
                
                driver.get(url)
                logger.log_info("Market Data - Entered Try Block")
                        
                #Wait for the Page to Load completely
                wait = WebDriverWait(driver, 10)
                logger.log_info("Opening the main Login Page")
                
                #Enter credentials and submit
                submit_button = '//*[@id="container"]/div/div/div/form/div[4]/button'

                element = wait.until(EC.element_to_be_clickable((By.XPATH, submit_button)))
                
                driver.find_element(By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[1]/input').send_keys(login_name)
                driver.find_element(By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[2]/input').send_keys(password)
                driver.find_element(By.XPATH, submit_button).click()
                logger.log_info("Credentials Entered and submitted. Waiting for TOTP page to load")
                
                #Wait for the 2FA Page to Load completely
                totp_input_xpath = '//*[@id="container"]/div[2]/div/div[2]/form/div[1]/input'
                
                # Wait for the totp field to load
                element = wait.until(EC.element_to_be_clickable((By.XPATH, totp_input_xpath)))
                
                totp = pyotp.TOTP(totp_secret)
                otp_changers = [29, 30, 31, 59, 0, 1]
                
                #Waiting if OTP is about to change
                while dt.now().second in otp_changers:
                    logger.log_info('otpchanger ' + str(dt.now().second))
                    time.sleep(1)
                    
                #Enter TOTP
                driver.find_element(By.XPATH, totp_input_xpath).send_keys(totp.now())
                
                #Wait for the 2FA Page to Load completely
                logger.log_info("Market Data - Pin entered and submitted.")
                time.sleep(5)
                
                #Capture the redirect URL
                token_url = (driver.current_url)
                request_token = token_url
                
                #Token fetch code start
                length_request = len("request_token=")
                request_token = request_token[request_token.find('request_token=') + length_request:]
                end_char = request_token.find('&')
                if end_char != -1:
                    request_token = request_token[:end_char]
                #Token fetch code end
                
                url_message = "Response URL: " + token_url
                token_message = "Request Token: " + request_token
                logger.log_info(url_message)
                logger.log_info(token_message)
                logger.log_info("Retrieving Access Token")
                
                session = kite.generate_session(request_token, api_secret)
                access_token = session['access_token']
                message = f"Access Token: {access_token}"
                logger.log_info(message)
                
                # Store access token in database
                broker_token_manager.store_access_token(token_url, request_token, access_token)
                
                message = f'Access Token succeeded at {dt.now()}'
                logger.log_info(message)
                
                #send_market_data_email(f'Market Data - Access Token Successful after {attempt} attempt(s)' ,message)
                return True

            except Exception as e:
                exception_message = message = f'Market Data - Access token Failed. Attempt No :{attempt} . Exception-> {e}. Traceback : {traceback.format_exc()}.\nWill check if access token in DB is fresh'
                error_handler.handle_error(message)
                logger.log_error(message)
                
                available_token_good = is_access_token_in_db_fresh()
                #{'status': access_token_latest_timestamp > today_8am, 'timestamp': access_token_latest_timestamp}
                
                if available_token_good['status']:
                    token_fetch_time = available_token_good['timestamp']
                    message = f'Access token available in DB is Fresh.\nFetched at {token_fetch_time}.\nWill use it for the ticker.\n But see why access_token_request failed in the first place'
                    error_handler.handle_error(message)
                    logger.log_info(message)
                    send_market_data_email(f'Market Data - access_token_request FAILED!!!. Using latest token from Today at {token_fetch_time}', f'Check broker_access_token_logs_{str(date.today())}.log to see why it failed and troubleshoot if the issue is not transient')
                    return True
                else:
                    message = '\nAccess token in DB is not fresh.\nRun manual_access_token_request.py if possible.\nWill retry access_token_request in 30 seconds'
                    error_handler.handle_error(message)
                    logger.log_error(message)
                time.sleep(30)
            else:
                break
        else:
            message = f'Market Data - Access token Failed After {RETRY_COUNT} attempts. Exception: {exception_message}'
            logger.log_error(message)
            error_handler.handle_error(message)
            send_market_data_email('Market Data - Access token Failed.', message)
            return False

if __name__ == '__main__':
    request_broker_access_token()