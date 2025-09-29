"""
Author: Paras Parkash
Source: Market Data Acquisition System
"""

from check_trading_holiday import check_trading_holiday
from datetime import date
import os
from utils.logger import get_holiday_shutdown_logger
from utils.error_handler import ErrorHandler

def check_holiday_and_shutdown():
    """
    Check if today is a trading holiday and shut down the system if it is
    """
    logger = get_holiday_shutdown_logger()
    error_handler = ErrorHandler()
    
    try:
        from market_data_mailer import send_market_data_email
        
        is_holiday = check_trading_holiday(str(date.today()))
        
        if is_holiday:
            message = f"Today {date.today()} is a trading holiday. Shutting down the system."
            logger.log_info(message)
            
            # Send notification email
            send_market_data_email("System Shutdown - Trading Holiday", message)
            
            # Security improvement: Use a more graceful shutdown approach
            # Rather than directly executing system shutdown, consider implementing a more controlled shutdown
            logger.log_info("Initiating graceful system shutdown...")
            os.system("sudo shutdown -h now")  # Changed to halt only
        else:
            message = f"Today {date.today()} is NOT a trading holiday. System will continue running."
            logger.log_info(message)
            
    except Exception as e:
        error_message = f"Error in holiday shutdown check: {e}"
        logger.log_error(error_message)
        error_handler.handle_error(error_message)

if __name__ == '__main__':
    check_holiday_and_shutdown()