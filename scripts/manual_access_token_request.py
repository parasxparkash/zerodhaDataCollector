"""
Author: Paras Parkash
Zerodha Data Collector

The only automation failure I have faced in the past (almost) 4 years for this project is when Broker makes changes in their Login Page
This breaks the Selenium automation used for access_token_request
One of the reasons I had split access_token_request to be standalone in the previous version of the project => Fix, Test ,Repeat.
In a few occasions, I was able to fix the error and rerun access_token_request before the markets open.
But if you can't fix access_token_request before market hours, run manual_access_token_request.py to manually login to the brokerconnect URL and paste the response URL
manual_access_token_request will then get the access_token using the request_token in the URL you provided
If you run market_data_main after this, access_token_request will return True if the access_token is fresh (generated after 08:00 a.m. today) and will
In short, on automation failure, you can run manual_access_token_request and then rerun market_data_main

"""

import sys
import os
# Add src directory to Python path to allow imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from kiteconnect import KiteConnect # pip install kiteconnect==5.0.1
import psycopg2
import json
from os import path, makedirs
from datetime import datetime as dt, date
from market_data_mailer import send_market_data_email
from system_error_logger import log_system_error

config_file = 'market_data_config.json'
with open(config_file, 'r') as config_file_handle:
    system_config = json.load(config_file_handle)

api_key = system_config['api_key']
api_secret = system_config['api_secret']
database_host = system_config['database_host']
database_user = system_config['database_user']
database_password = system_config['database_password']
database_port = system_config['database_port']
token_database_name = system_config['token_database_name']

def log_manual_access_token(message):
    """
    Log manual access token messages to file and console
    Optimized for performance by reducing I/O operations
    """
    timestamp = dt.now()
    print(timestamp, message)
    log_directory = path.join('logs', str(date.today()) + '_market_data_logs')
    if not path.exists(log_directory):
        makedirs(log_directory)
    log_file = path.join(log_directory, f'manual_access_token_logs_{str(date.today())}.log')
    log_message = f'\n{timestamp}    {str(message)}'
    with open(log_file, 'a') as f:
        f.write(log_message)

def request_manual_access_token():
    """
    Manually request access token by providing the redirect URL
    """
    try:
        connection = psycopg2.connect(host=database_host, user=database_user, password=database_password, port=database_port)
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {token_database_name}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {token_database_name}.broker_tokens (timestamp DATETIME UNIQUE, request_url varchar(255), request_token varchar(255), access_token varchar(255))")
        
        print("Please go to the following URL and log in to your broker account:")
        kite = KiteConnect(api_key=api_key)
        print(kite.login_url())
        print("\nAfter logging in, you will be redirected to a URL that looks like:")
        print("https://kite.trade/connect/login?request_token=XXXXXXXXXXXXXXXX")
        print("\nPlease copy the entire redirect URL and paste it below:")
        
        redirect_url = input("Enter the redirect URL: ")
        
        # Extract request token from URL
        request_token = redirect_url
        length_request = len("request_token=")
        request_token = request_token[request_token.find('request_token=') + length_request:]
        end_char = request_token.find('&')
        if end_char != -1:
            request_token = request_token[:end_char]
        
        # Generate access token using request token
        session = kite.generate_session(request_token, api_secret)
        access_token = session['access_token']
        
        # Store access token in database
        short_sql = f"INSERT into {token_database_name}.broker_tokens values (%s, %s, %s, %s)"
        cursor.execute(short_sql, [dt.now(), redirect_url, request_token, access_token])
        
        cursor.close()
        connection.close()
        
        message = f"Access token successfully stored in database. Token: {access_token[:10]}..."
        log_manual_access_token(message)
        send_market_data_email("Manual Access Token - Success", message)
        return True
        
    except Exception as e:
        message = f"Manual Access Token Request Failed. Exception: {e}"
        log_manual_access_token(message)
        log_system_error(message)
        send_market_data_email("Manual Access Token - Failed", message)
        return False

if __name__ == '__main__':
    request_manual_access_token()