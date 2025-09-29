"""
Author: Paras Parkash
Source: Market Data Acquisition System
"""

from utils.logger import get_config_validation_logger
from utils.error_handler import ErrorHandler
from utils.config_manager import config_manager

def is_system_config_default():
    """
    Check if the system configuration file contains default placeholder values
    Returns True if defaults are found, False otherwise
    """
    logger = get_config_validation_logger()
    error_handler = ErrorHandler()
    
    has_defaults = False
    system_config = config_manager.load_config()

    # Check each configuration value for default placeholders using the config manager
    if config_manager.is_default_value('notification_recipients'):
        notification_recipients = system_config['notification_recipients']
        message = f'market_data_config.json - notification_recipients has default value {notification_recipients}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('email_sender'):
        email_sender = system_config['email_sender']
        message = f'market_data_config.json - email_sender has default value {email_sender}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('email_password'):
        email_password = system_config['email_password']
        message = f'market_data_config.json - email_password has default value {email_password}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('totp_secret'):
        totp_secret = system_config['totp_secret']
        message = f'market_data_config.json - totp_secret has default value {totp_secret}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('broker_username'):
        broker_username = system_config['broker_username']
        message = f'market_data_config.json - broker_username has default value {broker_username}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('broker_password'):
        broker_password = system_config['broker_password']
        message = f'market_data_config.json - broker_password has default value {broker_password}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('api_key'):
        api_key = system_config['api_key']
        message = f'market_data_config.json - api_key has default value {api_key}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('api_secret'):
        api_secret = system_config['api_secret']
        message = f'market_data_config.json - api_secret has default value {api_secret}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('database_host'):
        database_host = system_config['database_host']
        message = f'market_data_config.json - database_host has default value {database_host}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('database_user'):
        database_user = system_config['database_user']
        message = f'market_data_config.json - database_user has default value {database_user}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    if config_manager.is_default_value('database_password'):
        database_password = system_config['database_password']
        message = f'market_data_config.json - database_password has default value {database_password}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
        
    # Check if database_port is the default PostgreSQL port
    database_port = system_config.get('database_port', 5432)
    if database_port == 5432:  # Default PostgreSQL port for TimescaleDB
        message = f'market_data_config.json - database_port has default PostgreSQL value {database_port}'
        logger.log_error(message)
        error_handler.handle_error(message)
        has_defaults = True
    
    return has_defaults
    
if __name__ == '__main__':
    is_system_config_default()