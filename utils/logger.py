# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System
Centralized logging module for the entire system
"""
import os
from datetime import datetime, date
import logging
from logging.handlers import RotatingFileHandler

# ANSI color codes for colored console output
RED_COLOR = '\033[91m'
RESET_COLOR = '\033[0m'

class MarketDataLogger:
    """
    Centralized logger for the market data acquisition system
    """
    def __init__(self, module_name):
        self.module_name = module_name
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """
        Setup logger with file and console handlers
        """
        # Create log directory structure if it doesn't exist
        log_dir = os.path.join('logs', f'{date.today()}_market_data_logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Create logger
        logger = logging.getLogger(f"{self.module_name}_{id(self)}")
        logger.setLevel(logging.INFO)
        
        # Prevent adding handlers multiple times
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler with rotation
        log_file = os.path.join(log_dir, f'{self.module_name}_logs_{date.today()}.log')
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler with color support for errors
        console_handler = ColoredConsoleHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_info(self, message):
        """
        Log informational messages
        """
        self.logger.info(message)
    
    def log_error(self, message):
        """
        Log error messages
        """
        self.logger.error(f"{RED_COLOR}{message}{RESET_COLOR}")
    
    def log_warning(self, message):
        """
        Log warning messages
        """
        self.logger.warning(message)

class ColoredConsoleHandler(logging.StreamHandler):
    """
    Custom console handler that adds color to error messages
    """
    def __init__(self):
        super().__init__()
    
    def emit(self, record):
        try:
            if record.levelno >= logging.ERROR:
                # Add red color to error messages
                record.msg = f"{RED_COLOR}{record.msg}{RESET_COLOR}"
            super().emit(record)
        except Exception:
            self.handleError(record)

# Create specific loggers for different modules
def get_holiday_check_logger():
    return MarketDataLogger('market_data_holiday_check')

def get_access_token_logger():
    return MarketDataLogger('broker_access_token')

def get_equity_universe_logger():
    return MarketDataLogger('market_data_equity_universe_update')

def get_market_data_main_logger():
    return MarketDataLogger('market_data_main')

def get_market_data_ticker_logger():
    return MarketDataLogger('market_data_ticker')

def get_system_error_logger():
    return MarketDataLogger('system_errors')

def get_mailer_logger():
    return MarketDataLogger('market_data_mailer')

def get_config_validation_logger():
    return MarketDataLogger('system_config_validation')

def get_holiday_shutdown_logger():
    return MarketDataLogger('trading_holiday_shutdown')