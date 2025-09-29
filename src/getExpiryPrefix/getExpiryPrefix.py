"""
Author: Paras Parkash
Zerodha Data Collector
"""

import pandas as pd
from datetime import datetime as dt, timedelta, date
import json
from os import path

def get_exp_pref_nifty(input_date):
    """
    Get the expiry prefix for Nifty options for a given date
    """
    # Convert input date string to datetime object if needed
    if isinstance(input_date, str):
        input_date = dt.strptime(input_date, '%Y-%m-%d').date()
    
    # Read trading holidays from file
    holidays_file = 'tradingHolidaysAllYears.csv'
    if path.exists(holidays_file):
        trading_holidays_df = pd.read_csv(holidays_file)
        trading_holidays = set(pd.to_datetime(trading_holidays_df['Date']).dt.strftime('%Y-%m-%d').tolist())
    else:
        trading_holidays = set()
    
    # Find expiry date (typically Thursday for weekly, last Thursday for monthly)
    # For weekly options, find the next Thursday
    days_ahead = 3 - input_date.weekday()  # 3 is Thursday
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    expiry_date = input_date + timedelta(days_ahead)
    
    # Check if expiry date is a trading holiday, if so move to previous day
    while expiry_date.strftime('%Y-%m-%d') in trading_holidays:
        expiry_date -= timedelta(days=1)
    
    # Format the expiry prefix
    expiry_str = expiry_date.strftime('%y%b').upper()
    # Replace month abbreviations with expected format
    month_map = {
        'JAN': 'JAN', 'FEB': 'FEB', 'MAR': 'MAR', 'APR': 'APR', 'MAY': 'MAY', 'JUN': 'JUN',
        'JUL': 'JUL', 'AUG': 'AUG', 'SEP': 'SEP', 'OCT': 'OCT', 'NOV': 'NOV', 'DEC': 'DEC'
    }
    
    # Replace month abbreviation in expiry string
    for old, new in month_map.items():
        expiry_str = expiry_str.replace(old, new)
    
    return f'NIFTY{expiry_str}'

def get_exp_pref_bank_nifty(input_date):
    """
    Get the expiry prefix for Bank Nifty options for a given date
    """
    # Convert input date string to datetime object if needed
    if isinstance(input_date, str):
        input_date = dt.strptime(input_date, '%Y-%m-%d').date()
    
    # Read trading holidays from file
    holidays_file = 'tradingHolidaysAllYears.csv'
    if path.exists(holidays_file):
        trading_holidays_df = pd.read_csv(holidays_file)
        trading_holidays = set(pd.to_datetime(trading_holidays_df['Date']).dt.strftime('%Y-%m-%d').tolist())
    else:
        trading_holidays = set()
    
    # Find expiry date (typically Wednesday for weekly BankNifty)
    days_ahead = 2 - input_date.weekday()  # 2 is Wednesday
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    expiry_date = input_date + timedelta(days_ahead)
    
    # Check if expiry date is a trading holiday, if so move to previous day
    while expiry_date.strftime('%Y-%m-%d') in trading_holidays:
        expiry_date -= timedelta(days=1)
    
    # Format the expiry prefix
    expiry_str = expiry_date.strftime('%y%b').upper()
    # Replace month abbreviations with expected format
    month_map = {
        'JAN': 'JAN', 'FEB': 'FEB', 'MAR': 'MAR', 'APR': 'APR', 'MAY': 'MAY', 'JUN': 'JUN',
        'JUL': 'JUL', 'AUG': 'AUG', 'SEP': 'SEP', 'OCT': 'OCT', 'NOV': 'NOV', 'DEC': 'DEC'
    }
    
    # Replace month abbreviation in expiry string
    for old, new in month_map.items():
        expiry_str = expiry_str.replace(old, new)
    
    return f'BANKNIFTY{expiry_str}'

if __name__ == '__main__':
    # Example usage
    in_date = '2020-01-01'
    print(get_exp_pref_bank_nifty(in_date))
    print(get_exp_pref_nifty(in_date))
