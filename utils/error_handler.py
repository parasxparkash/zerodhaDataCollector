# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Zerodha Data Collector
Error handling and exception management module
"""
import traceback
import sys
from functools import wraps
from typing import Callable, Any, Optional
from .logger import get_system_error_logger

class ErrorHandler:
    """
    Comprehensive error handling utility
    """
    def __init__(self):
        self.logger = get_system_error_logger()
    
    def handle_error(self, error_message: str, should_exit: bool = False, should_notify: bool = False):
        """
        Handle an error with logging and optional notification/exit
        """
        full_message = f"{error_message} - {traceback.format_exc()}"
        self.logger.log_error(full_message)
        
        if should_notify:
            # Import mailer here to avoid circular dependencies
            try:
                import sys
                import os
                # Add src directory to Python path to allow imports
                sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))
                
                from market_data_mailer import send_market_data_email
                send_market_data_email('Market Data System Error', full_message)
            except ImportError:
                self.logger.log_error("Could not send error notification - mailer not available")
        
        if should_exit:
            sys.exit(1)
    
    def safe_execute(self, func: Callable, *args, should_exit: bool = False, should_notify: bool = False, default_return: Any = None, **kwargs):
        """
        Safely execute a function with error handling
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_message = f"Error in {func.__name__}: {str(e)}"
            self.handle_error(error_message, should_exit, should_notify)
            return default_return

def error_handler(should_exit: bool = False, should_notify: bool = False, default_return: Any = None):
    """
    Decorator for error handling
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            error_handler = ErrorHandler()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_message = f"Error in {func.__name__}: {str(e)}"
                error_handler.handle_error(error_message, should_exit, should_notify)
                return default_return
        return wrapper
    return decorator

def retry_on_failure(max_attempts: int = 3, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """
    Decorator to retry function execution on failure
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:  # Don't sleep on the last attempt
                        import time
                        time.sleep(delay)
                    continue
            
            # If we've exhausted all attempts, raise the last exception
            raise last_exception
        return wrapper
    return decorator

class ValidationException(Exception):
    """
    Custom exception for validation errors
    """
    pass

class DataIntegrityException(Exception):
    """
    Custom exception for data integrity issues
    """
    pass

class ConfigurationException(Exception):
    """
    Custom exception for configuration errors
    """
    pass

def validate_input(value: Any, expected_type: type, field_name: str = "Input"):
    """
    Validate input type and raise exception if invalid
    """
    if not isinstance(value, expected_type):
        raise ValidationException(f"{field_name} must be of type {expected_type.__name__}, got {type(value).__name__}")

def validate_not_empty(value: Any, field_name: str = "Input"):
    """
    Validate that input is not empty and raise exception if it is
    """
    if not value or (hasattr(value, '__len__') and len(value) == 0):
        raise ValidationException(f"{field_name} cannot be empty")